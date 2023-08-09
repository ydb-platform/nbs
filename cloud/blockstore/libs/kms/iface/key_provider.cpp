#include "key_provider.h"

#include "compute_client.h"
#include "kms_client.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/iam/iface/client.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

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
        const TString& diskId)
    {
        return Executor->Execute([=] () mutable {
            return DoReadKeyFromKMS(diskId, kmsKey);
        });
    }

private:
    TResponse DoReadKeyFromKMS(
        const TString& diskId,
        const NProto::TKmsKey& kmsKey)
    {
        auto iamFuture = IamTokenClient->GetTokenAsync();
        auto iamResponse = Executor->WaitFor(iamFuture);
        if (HasError(iamResponse)) {
            return TErrorResponse(iamResponse.GetError());
        }

        auto computeFuture = ComputeClient->CreateTokenForDEK(
            diskId,
            kmsKey.GetTaskId(),
            iamResponse.GetResult().Token);
        auto computeResponse = Executor->WaitFor(computeFuture);
        if (HasError(computeResponse)) {
            return TErrorResponse(computeResponse.GetError());
        }

        auto kmsFuture = KmsClient->Decrypt(
            kmsKey.GetKekId(),
            kmsKey.GetEncryptedDEK(),
            computeResponse.GetResult());
        auto kmsResponse = Executor->WaitFor(kmsFuture);
        if (HasError(kmsResponse)) {
            return TErrorResponse(kmsResponse.GetError());
        }

        return TEncryptionKey(kmsResponse.ExtractResult());
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

}   // namespace NCloud::NBlockStore
