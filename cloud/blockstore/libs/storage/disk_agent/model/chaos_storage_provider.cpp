#include "chaos_storage_provider.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>

#include <util/random/random.h>

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<NProto::TZeroBlocksResponse> UnderlyingExecute(
    IStorage* storage,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    return storage->ZeroBlocks(std::move(callContext), std::move(request));
}

NThreading::TFuture<NProto::TReadBlocksLocalResponse> UnderlyingExecute(
    IStorage* storage,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    return storage->ReadBlocksLocal(std::move(callContext), std::move(request));
}

NThreading::TFuture<NProto::TWriteBlocksLocalResponse> UnderlyingExecute(
    IStorage* storage,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    return storage->WriteBlocksLocal(
        std::move(callContext),
        std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
constexpr bool IsExactlyWriteRequest =
    std::is_same_v<T, NProto::TWriteBlocksLocalRequest>;

bool TryChaosLuck(double probability)
{
    if (probability == 0.f) {
        return false;
    }

    return RandomNumber<double>() < probability;
}

EWellKnownResultCodes GetChaosErrorCode(
    const google::protobuf::RepeatedField<ui32>& errors)
{
    if (errors.empty()) {
        return E_REJECTED;
    }

    auto result = static_cast<EWellKnownResultCodes>(
        errors[RandomNumber<size_t>(errors.size())]);
    if (SUCCEEDED(result)) {
        return E_REJECTED;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TChaosStorage final: public IStorage
{
private:
    const IStoragePtr Storage;
    const NProto::TChaosConfig ChaosConfig;
    const ui64 MaxCritEventToReport =
        ChaosConfig.GetCritEventReportingPolicy() == NProto::CCERP_FIRST_ERROR
            ? 1
            : std::numeric_limits<ui64>::max();

    ui64 GeneratedErrorCount = 0;

public:
    TChaosStorage(IStoragePtr storage, NProto::TChaosConfig chaosConfig)
        : Storage(std::move(storage))
        , ChaosConfig(std::move(chaosConfig))
    {}

    NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return Execute<NProto::TZeroBlocksResponse>(
            std::move(callContext),
            std::move(request));
    }

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return Execute<NProto::TReadBlocksLocalResponse>(
            std::move(callContext),
            std::move(request));
    }

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return Execute<NProto::TWriteBlocksLocalResponse>(
            std::move(callContext),
            std::move(request));
    }

    NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        return Storage->EraseDevice(method);
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Storage->AllocateBuffer(bytesCount);
    }

    void ReportIOError() override
    {
        Storage->ReportIOError();
    }

private:
    template <class TResponse, class TRequest>
    NThreading::TFuture<TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request)
    {
        if (!TryChaosLuck(ChaosConfig.GetChaosProbability())) {
            return UnderlyingExecute(
                Storage.get(),
                std::move(callContext),
                std::move(request));
        }

        ++GeneratedErrorCount;

        // We flip four coins:
        // 1. will we generate an error (ChaosProbability)
        // 2. will we spoil the write buffer (GetDataDamageProbability).
        //    If the buffer is corrupted, we always respond after completing the
        //    request
        // 3. will we execute the request or respond with an error immediately
        //    (GetImmediateReplyProbability())
        // 4. what error code will we respond with? (from GetErrorCodes())

        EWellKnownResultCodes forcedErrorCode =
            GetChaosErrorCode(ChaosConfig.GetErrorCodes());
        bool damageData = false;
        if constexpr (IsExactlyWriteRequest<TRequest>) {
            damageData = TryChaosLuck(ChaosConfig.GetDataDamageProbability());
            if (damageData) {
                auto& buffers = *request->MutableBlocks()->MutableBuffers();
                for (auto& block: buffers) {
                    ui64 trash = RandomNumber<ui64>();
                    Y_ABORT_UNLESS(block.size() >= sizeof(trash));
                    memcpy(
                        const_cast<char*>(block.data()),
                        &trash,
                        sizeof(trash));
                }
            }
        }

        const bool immediateReply =
            !damageData &&
            TryChaosLuck(ChaosConfig.GetImmediateReplyProbability());

        auto reply = [forcedErrorCode,
                      reportCritEvent =
                          GeneratedErrorCount <= MaxCritEventToReport,
                      damageData](bool before) -> NThreading::TFuture<TResponse>
        {
            if (reportCritEvent) {
                ReportChaosGeneratedError(
                    {{"request", GetBlockStoreRequestName<TRequest>()},
                     {"error", ToString(forcedErrorCode)},
                     {"before", before},
                     {"damageData", damageData}});
            }

            TResponse msg;
            *msg.MutableError() = MakeError(
                forcedErrorCode,
                TStringBuilder()
                    << "Chaos-generated error " << (before ? "before" : "after")
                    << " execution"
                    << (damageData ? " Note! Written data damaged" : ""));
            return NThreading::MakeFuture<TResponse>(std::move(msg));
        };

        if (immediateReply) {
            return reply(true);
        }

        auto future = UnderlyingExecute(
            Storage.get(),
            std::move(callContext),
            std::move(request));

        return future.Apply(
            [reply = std::move(reply)](NThreading::TFuture<TResponse> f)
                -> NThreading::TFuture<TResponse>
            {
                auto originalResponse = f.ExtractValue();
                if (HasError(originalResponse)) {
                    return NThreading::MakeFuture<TResponse>(
                        std::move(originalResponse));
                }
                return reply(false);
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChaosStorageProvider final: public IStorageProvider
{
private:
    IStorageProviderPtr StorageProvider;
    const NProto::TChaosConfig ChaosConfig;

public:
    TChaosStorageProvider(
        IStorageProviderPtr storageProvider,
        const NProto::TChaosConfig& chaosConfig)
        : StorageProvider(std::move(storageProvider))
        , ChaosConfig(chaosConfig)
    {}

    NThreading::TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        auto future =
            StorageProvider->CreateStorage(volume, clientId, accessMode);
        return future.Apply(
            [chaosConfig =
                 ChaosConfig](NThreading::TFuture<IStoragePtr> f) mutable
            {
                IStoragePtr storage = std::make_shared<TChaosStorage>(
                    f.ExtractValue(),
                    std::move(chaosConfig));
                return storage;
            });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateChaosStorageProvider(
    IStorageProviderPtr storageProvider,
    const NProto::TChaosConfig& chaosConfig)
{
    return std::make_shared<TChaosStorageProvider>(
        std::move(storageProvider),
        chaosConfig);
}

}   // namespace NCloud::NBlockStore::NServer
