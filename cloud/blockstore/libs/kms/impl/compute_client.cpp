#include "compute_client.h"

#include <ydb/public/api/client/yc_private/compute/inner/disk_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/compute/inner/disk_service.pb.h>

#include <cloud/blockstore/libs/kms/iface/compute_client.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/time_point_specialization.h>

#include <contrib/libs/grpc/include/grpcpp/channel.h>
#include <contrib/libs/grpc/include/grpcpp/client_context.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>

#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/system/thread.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

namespace compute = yandex::cloud::priv::compute::v1::inner;

////////////////////////////////////////////////////////////////////////////////

const char AUTH_HEADER[] = "authorization";
const char AUTH_METHOD[] = "Bearer";

////////////////////////////////////////////////////////////////////////////////

class TRequestHandler final
{
private:
    using TReader = grpc::ClientAsyncResponseReader<compute::CreateTokenResponse>;
    using TResult = TResultOrError<TString>;

    grpc::ClientContext ClientContext;
    grpc::Status Status;

    std::unique_ptr<TReader> Reader;

    compute::CreateTokenRequest Request;
    compute::CreateTokenResponse Response;

    TPromise<TResult> Promise = NewPromise<TResult>();

public:
    TRequestHandler(
        TDuration timeout,
        TString authToken,
        TString diskId,
        TString taskId)
    {
        if (timeout != TDuration::Zero()) {
            ClientContext.set_deadline(TInstant::Now() + timeout);
        }

        if (authToken) {
            ClientContext.AddMetadata(
                AUTH_HEADER,
                TStringBuilder() << AUTH_METHOD << " " << authToken);
        }

        Request.set_disk_id(diskId);
        Request.set_task_id(taskId);
    }

    TFuture<TResult> Execute(
        compute::DiskService::Stub& service,
        grpc::CompletionQueue* cq,
        void* tag)
    {
        auto future = Promise.GetFuture();
        Reader = service.AsyncCreateToken(&ClientContext, Request, cq);
        Reader->Finish(&Response, &Status, tag);
        // At this point, this object can already be freed after request
        // completion. It is not safe to use it.
        return future;
    }

    void Complete()
    {
        if (!Status.ok()) {
            Promise.SetValue(MakeError(
                MAKE_GRPC_ERROR(Status.error_code()),
                Status.error_message()));
        } else {
            Promise.SetValue(std::move(Response.token()));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TComputeClient final
    : public ISimpleThread
    , public IComputeClient
{
private:
    TGrpcInitializer GrpcInitializer;

    const ILoggingServicePtr Logging;
    const NProto::TGrpcClientConfig Config;

    TLog Log;

    grpc::CompletionQueue CQ;
    std::shared_ptr<compute::DiskService::Stub> Service;

public:
    TComputeClient(
            ILoggingServicePtr logging,
            NProto::TGrpcClientConfig config)
        : Logging(std::move(logging))
        , Config(std::move(config))
    {
    }

    ~TComputeClient()
    {
        Stop();
    }

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_SERVER");

        STORAGE_INFO("Connect to " << Config.GetAddress());

        auto creds = Config.GetInsecure()
            ? grpc::InsecureChannelCredentials()
            : grpc::SslCredentials(grpc::SslCredentialsOptions());

        grpc::ChannelArguments args;
        if (Config.GetSslTargetNameOverride()) {
            args.SetSslTargetNameOverride(Config.GetSslTargetNameOverride());
        }

        auto channel = grpc::CreateCustomChannel(
            Config.GetAddress(),
            std::move(creds),
            args);

        Service = std::shared_ptr<compute::DiskService::Stub>(
            compute::DiskService::NewStub(std::move(channel)));

        ISimpleThread::Start();
    }

    void Stop() override
    {
        CQ.Shutdown();
        Join();
    }

    TFuture<TResponse> CreateTokenForDEK(
        const TString& diskId,
        const TString& taskId,
        const TString& authToken) override
    {
        auto requestHandler = std::make_unique<TRequestHandler>(
            TDuration::MilliSeconds(Config.GetRequestTimeout()),
            authToken,
            diskId,
            taskId);

        auto future = requestHandler->Execute(
            *Service,
            &CQ,
            requestHandler.get());

        requestHandler.release();
        return future;
    }

private:
    void* ThreadProc() override
    {
        void* tag;
        bool ok;
        while (CQ.Next(&tag, &ok)) {
            std::unique_ptr<TRequestHandler> requestHandler(
                static_cast<TRequestHandler*>(tag)
            );
            requestHandler->Complete();
        }
        return nullptr;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IComputeClientPtr CreateComputeClient(
    ILoggingServicePtr logging,
    NProto::TGrpcClientConfig config)
{
    return std::make_shared<TComputeClient>(
        std::move(logging),
        std::move(config));
}

}   // namespace NCloud::NBlockStore
