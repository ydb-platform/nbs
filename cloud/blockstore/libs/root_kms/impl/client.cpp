#include "client.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/root_kms/iface/client.h>
#include <cloud/blockstore/public/api/protos/encryption.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/time_point_specialization.h>

#include <contrib/libs/grpc/include/grpcpp/channel.h>
#include <contrib/libs/grpc/include/grpcpp/client_context.h>
#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>
#include <contrib/ydb/public/api/client/yc_private/kms/symmetric_crypto_service.grpc.pb.h>
#include <contrib/ydb/public/api/client/yc_private/kms/symmetric_crypto_service.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/stream/file.h>
#include <util/string/builder.h>

#include <chrono>
#include <thread>

namespace NCloud::NBlockStore {

using namespace NThreading;
using namespace std::chrono_literals;

namespace {

namespace kms = yandex::cloud::priv::kms::v1;

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration DefaultTimeout = 5min;

////////////////////////////////////////////////////////////////////////////////

struct IRequestHandler
{
    virtual ~IRequestHandler() = default;
    virtual void Complete() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TDecryptDataKeyHandler final
    : public IRequestHandler
{
private:
    using TReader =
        grpc::ClientAsyncResponseReader<kms::SymmetricDecryptResponse>;
    using TResult = TResultOrError<TEncryptionKey>;

    grpc::ClientContext ClientContext;
    grpc::Status Status;

    std::unique_ptr<TReader> Reader;

    kms::SymmetricDecryptRequest Request;
    kms::SymmetricDecryptResponse Response;

    TPromise<TResult> Promise = NewPromise<TResult>();

public:
    TDecryptDataKeyHandler(
        TDuration timeout,
        const TString& keyId,
        const TString& ciphertext)
    {
        if (timeout) {
            ClientContext.set_deadline(timeout.ToDeadLine());
        }

        Request.set_key_id(keyId);
        Request.set_ciphertext(ciphertext);
    }

    TFuture<TResult> Execute(
        kms::SymmetricCryptoService::Stub& service,
        grpc::CompletionQueue& cq)
    {
        Reader = service.AsyncDecrypt(&ClientContext, Request, &cq);
        Reader->Finish(&Response, &Status, this);
        return Promise;
    }

    void Complete() override
    {
        if (!Status.ok()) {
            Promise.SetValue(MakeError(
                MAKE_GRPC_ERROR(Status.error_code()),
                Status.error_message()));
        } else {
            Promise.SetValue(TEncryptionKey{Response.plaintext()});
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGenerateDataKeyRequestHandler final
    : public IRequestHandler
{
private:
    using TReader =
        grpc::ClientAsyncResponseReader<kms::GenerateDataKeyResponse>;
    using TResult = TResultOrError<NProto::TKmsKey>;

    grpc::ClientContext ClientContext;
    grpc::Status Status;

    std::unique_ptr<TReader> Reader;

    kms::GenerateDataKeyRequest Request;
    kms::GenerateDataKeyResponse Response;

    TPromise<TResult> Promise = NewPromise<TResult>();

public:
    TGenerateDataKeyRequestHandler(
        TDuration timeout,
        const TString& keyId)
    {
        if (timeout) {
            ClientContext.set_deadline(timeout.ToDeadLine());
        }

        Request.set_key_id(keyId);
        Request.set_data_key_spec(kms::AES_256);
        Request.set_skip_plaintext(true);
    }

    TFuture<TResult> Execute(
        kms::SymmetricCryptoService::Stub& service,
        grpc::CompletionQueue& cq)
    {
        Reader = service.AsyncGenerateDataKey(&ClientContext, Request, &cq);
        Reader->Finish(&Response, &Status, this);
        return Promise;
    }

    void Complete() override
    {
        if (!Status.ok()) {
            Promise.SetValue(MakeError(
                MAKE_GRPC_ERROR(Status.error_code()),
                Status.error_message()));
        } else {
            NProto::TKmsKey kmsKey;
            kmsKey.SetKekId(Response.key_id());
            kmsKey.SetEncryptedDEK(Response.data_key_ciphertext());
            Promise.SetValue(std::move(kmsKey));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const TString& name)
{
    return name
        ? TFileInput(name).ReadAll()
        : TString();
}

////////////////////////////////////////////////////////////////////////////////

class TRootKmsClient final
    : public IRootKmsClient
{
private:
    TGrpcInitializer GrpcInitializer;

    const ILoggingServicePtr Logging;

    const TCreateRootKmsClientParams Params;

    TLog Log;

    grpc::CompletionQueue CQ;
    std::shared_ptr<kms::SymmetricCryptoService::Stub> Service;

    std::thread Thread;

public:
    TRootKmsClient(
        ILoggingServicePtr logging,
        TCreateRootKmsClientParams params);

    ~TRootKmsClient() override;

    void Start() override;
    void Stop() override;

    auto Decrypt(const TString& keyId, const TString& ciphertext)
        -> TFuture<TResultOrError<TEncryptionKey>> override;

    auto GenerateDataEncryptionKey(const TString& keyId)
        -> TFuture<TResultOrError<NProto::TKmsKey>> override;

private:
    void ThreadFn();
};

////////////////////////////////////////////////////////////////////////////////

TRootKmsClient::TRootKmsClient(
        ILoggingServicePtr logging,
        TCreateRootKmsClientParams params)
    : Logging(std::move(logging))
    , Params(std::move(params))
{}

TRootKmsClient::~TRootKmsClient()
{
    Stop();
}

void TRootKmsClient::Start()
{
    Log = Logging->CreateLog("ROOT_KMS_CLIENT");

    grpc::SslCredentialsOptions sslOpts{
        .pem_root_certs = ReadFile(Params.RootCertsFile),
        .pem_private_key = ReadFile(Params.PrivateKeyFile),
        .pem_cert_chain = ReadFile(Params.CertChainFile)
    };

    STORAGE_INFO("Connect to " << Params.Address);

    grpc::ChannelArguments channelArgs;
    channelArgs.SetLoadBalancingPolicyName("round_robin");

    auto channel = grpc::CreateCustomChannel(
        Params.Address,
        grpc::SslCredentials(sslOpts),
        channelArgs);

    Service = std::shared_ptr<kms::SymmetricCryptoService::Stub>(
        kms::SymmetricCryptoService::NewStub(std::move(channel)));

    Thread = std::thread{&TRootKmsClient::ThreadFn, this};
}

void TRootKmsClient::Stop()
{
    if (!Thread.joinable()) {
        return;
    }

    STORAGE_INFO("Stopping...");

    CQ.Shutdown();
    Thread.join();

    STORAGE_INFO("Stopped");
}

auto TRootKmsClient::Decrypt(const TString& keyId, const TString& ciphertext)
    -> TFuture<TResultOrError<TEncryptionKey>>
{
    STORAGE_DEBUG("Decrypt DEK with key " << keyId.Quote());

    auto requestHandler = std::make_unique<TDecryptDataKeyHandler>(
        DefaultTimeout,
        keyId,
        ciphertext);

    auto future = requestHandler->Execute(*Service, CQ);

    // ownership transferred to grpc
    Y_UNUSED(requestHandler.release());

    return future;
}

auto TRootKmsClient::GenerateDataEncryptionKey(const TString& keyId)
    -> TFuture<TResultOrError<NProto::TKmsKey>>
{
    STORAGE_DEBUG("Generate DEK with key " << keyId.Quote());

    auto requestHandler = std::make_unique<TGenerateDataKeyRequestHandler>(
        DefaultTimeout,
        keyId);

    auto future = requestHandler->Execute(*Service, CQ);

    // ownership transferred to grpc
    Y_UNUSED(requestHandler.release());

    return future;
}

void TRootKmsClient::ThreadFn()
{
    void* tag = nullptr;
    bool ok = false;

    while (CQ.Next(&tag, &ok)) {
        std::unique_ptr<IRequestHandler> requestHandler(
            static_cast<IRequestHandler*>(tag)
        );
        requestHandler->Complete();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRootKmsClientPtr CreateRootKmsClient(
    ILoggingServicePtr logging,
    TCreateRootKmsClientParams params)
{
    return std::make_shared<TRootKmsClient>(
        std::move(logging),
        std::move(params));
}

}   // namespace NCloud::NBlockStore
