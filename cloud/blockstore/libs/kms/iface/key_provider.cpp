#include "key_provider.h"

#include "compute_client.h"
#include "kms_client.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/iam/iface/client.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TString> SafeBase64Decode(TStringBuf encoded)
{
    try {
        return Base64Decode(encoded);
    } catch (...) {
        return MakeError(E_ARGUMENT, CurrentExceptionMessage());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TKmsKeyProvider
    : public IKmsKeyProvider
{
private:
    const TExecutorPtr Executor;
    const NIamClient::IIamTokenClientPtr IamTokenClient;
    const IComputeClientPtr ComputeClient;
    const IKmsClientPtr KmsClient;

public:
    TKmsKeyProvider(
            TExecutorPtr executor,
            NIamClient::IIamTokenClientPtr iamTokenClient,
            IComputeClientPtr computeClient,
            IKmsClientPtr kmsClient)
        : Executor(std::move(executor))
        , IamTokenClient(std::move(iamTokenClient))
        , ComputeClient(std::move(computeClient))
        , KmsClient(std::move(kmsClient))
    {}

    TFuture<TResponse> GetKey(
        const NProto::TKmsKey& kmsKey,
        const TString& diskId) override
    {
        return Executor->Execute([=, this] () mutable {
            return DoReadKeyFromKMS(diskId, kmsKey);
        });
    }

private:
    TResponse DoReadKeyFromKMS(
        const TString& diskId,
        const NProto::TKmsKey& kmsKey)
    {
        auto decodeResponse = SafeBase64Decode(kmsKey.GetEncryptedDEK());
        if (HasError(decodeResponse)) {
            const auto& err = decodeResponse.GetError();
            return MakeError(err.GetCode(), TStringBuilder()
                << "failed to decode dek for disk " << diskId
                << ", error: " << err.GetMessage());
        }

        auto iamFuture = IamTokenClient->GetTokenAsync();
        const auto& iamResponse = Executor->WaitFor(iamFuture);
        if (HasError(iamResponse)) {
            const auto& err = iamResponse.GetError();
            return MakeError(err.GetCode(), TStringBuilder()
                << "failed to get iam-token for disk " << diskId
                << ", error: " << err.GetMessage());
        }

        auto computeFuture = ComputeClient->CreateTokenForDEK(
            diskId,
            kmsKey.GetTaskId(),
            iamResponse.GetResult().Token);
        const auto& computeResponse = Executor->WaitFor(computeFuture);
        if (HasError(computeResponse)) {
            const auto& err = computeResponse.GetError();
            return MakeError(err.GetCode(), TStringBuilder()
                << "failed to create token for disk " << diskId
                << ", error: " << err.GetMessage());
        }

        auto kmsFuture = KmsClient->Decrypt(
            kmsKey.GetKekId(),
            decodeResponse.GetResult(),
            computeResponse.GetResult());
        auto kmsResponse = Executor->WaitFor(kmsFuture);
        if (HasError(kmsResponse)) {
            const auto& err = kmsResponse.GetError();
            return MakeError(err.GetCode(), TStringBuilder()
                << "failed to decrypt dek for disk " << diskId
                << ", error: " << err.GetMessage());
        }

        return TEncryptionKey(kmsResponse.ExtractResult());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRootKmsKeyProvider
    : public IKmsKeyProvider
{
private:
    const TExecutorPtr Executor;

public:
    explicit TRootKmsKeyProvider(TExecutorPtr executor)
        : Executor(std::move(executor))
    {}

    TFuture<TResponse> GetKey(
        const NProto::TKmsKey& kmsKey,
        const TString& diskId) override
    {
        return Executor->Execute([=, this] () mutable {
            return DoReadKeyFromRootKMS(diskId, kmsKey);
        });
    }

private:
    TResponse DoReadKeyFromRootKMS(
        const TString& diskId,
        const NProto::TKmsKey& kmsKey)
    {
        if (kmsKey.GetKekId()) {
            return MakeError(E_NOT_IMPLEMENTED, "TODO: get DEK from Root KMS");
        }

        auto [key, error] = SafeBase64Decode(kmsKey.GetEncryptedDEK());
        if (HasError(error)) {
            return MakeError(error.GetCode(), TStringBuilder()
                << "failed to decode dek for disk " << diskId
                << ", error: " << error);
        }

        return TEncryptionKey(key);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IKmsKeyProviderPtr CreateKmsKeyProvider(
    TExecutorPtr executor,
    NIamClient::IIamTokenClientPtr iamTokenClient,
    IComputeClientPtr computeClient,
    IKmsClientPtr kmsClient)
{
    return std::make_shared<TKmsKeyProvider>(
        std::move(executor),
        std::move(iamTokenClient),
        std::move(computeClient),
        std::move(kmsClient));
}

IKmsKeyProviderPtr CreateRootKmsKeyProvider(TExecutorPtr executor)
{
    return std::make_shared<TRootKmsKeyProvider>(std::move(executor));
}

}   // namespace NCloud::NBlockStore
