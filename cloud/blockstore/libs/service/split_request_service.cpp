#include "split_request_service.h"

#include "service.h"
#include "service_method.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
#include <cloud/blockstore/libs/common/split_request_helpers.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/common/media.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

using namespace NThreading;

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
TVector<std::shared_ptr<TRequest>> CreateSubRequests(
    std::shared_ptr<TRequest> request,
    const TVector<TBlockRange64>& subRanges,
    ui32 /*blockSize*/)
{
    TVector<std::shared_ptr<TRequest>> result;
    result.reserve(subRanges.size());

    for (const auto& subRange: subRanges) {
        auto subRequest = std::make_shared<TRequest>(*request);
        subRequest->SetStartIndex(subRange.Start);
        subRequest->SetBlocksCount(subRange.Size());
        result.push_back(std::move(subRequest));
    }

    return result;
}

template <>
TVector<std::shared_ptr<NProto::TReadBlocksLocalRequest>> CreateSubRequests(
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
    const TVector<TBlockRange64>& subRanges,
    ui32 /*blockSize*/)
{
    TVector<std::shared_ptr<NProto::TReadBlocksLocalRequest>> result;

    auto guard = request->Sglist.Acquire();
    if (!guard) {
        return result;
    }

    const auto& srcSgList = guard.Get();

    result.reserve(subRanges.size());
    for (const auto& subRange: subRanges) {
        auto subRequest = std::make_shared<NProto::TReadBlocksLocalRequest>();
        subRequest->CopyFrom(*request);
        subRequest->SetStartIndex(subRange.Start);
        subRequest->SetBlocksCount(subRange.Size());
        subRequest->BlockSize = request->BlockSize;

        auto subSgList = CreateSgListSubRange(
            srcSgList,
            (subRange.Start - request->GetStartIndex()) * request->BlockSize,
            subRange.Size() * request->BlockSize);
        subRequest->Sglist =
            request->Sglist.CreateDepender(std::move(subSgList));

        result.push_back(std::move(subRequest));
    }

    return result;
}

template <>
TVector<std::shared_ptr<NProto::TWriteBlocksRequest>> CreateSubRequests(
    std::shared_ptr<NProto::TWriteBlocksRequest> request,
    const TVector<TBlockRange64>& subRanges,
    ui32 blockSize)
{
    TVector<std::shared_ptr<NProto::TWriteBlocksRequest>> result;
    result.reserve(subRanges.size());

    NProto::TIOVector srcBuffers;
    srcBuffers.Swap(request->MutableBlocks());

    for (const auto& subRange: subRanges) {
        auto subRequest =
            std::make_shared<NProto::TWriteBlocksRequest>(*request);
        subRequest->SetStartIndex(subRange.Start);
        auto& buffers = *subRequest->MutableBlocks()->MutableBuffers();
        for (size_t i = 0; i < subRange.Size(); ++i) {
            Y_ABORT_UNLESS(subRange.Start + i >= request->GetStartIndex());
            const size_t srcIndx = subRange.Start + i - request->GetStartIndex();
            buffers.Add(
                std::move(*srcBuffers.MutableBuffers()->Mutable(srcIndx)));
        }
        if (request->ChecksumsSize() > 0) {
            subRequest->MutableChecksums()->Clear();
            *subRequest->MutableChecksums()->Add() =
                CalculateChecksum(subRequest->GetBlocks(), blockSize);
        }
        result.push_back(std::move(subRequest));
    }

    return result;
}

template <>
TVector<std::shared_ptr<NProto::TWriteBlocksLocalRequest>> CreateSubRequests(
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
    const TVector<TBlockRange64>& subRanges,
    ui32 /*blockSize*/)
{
    TVector<std::shared_ptr<NProto::TWriteBlocksLocalRequest>> result;

    auto guard = request->Sglist.Acquire();
    if (!guard) {
        return result;
    }

    const auto& srcSgList = guard.Get();

    result.reserve(subRanges.size());
    for (const auto& subRange: subRanges) {
        auto subRequest = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        subRequest->CopyFrom(*request);
        subRequest->SetStartIndex(subRange.Start);
        subRequest->BlocksCount = subRange.Size();
        subRequest->BlockSize = request->BlockSize;

        auto subSgList = CreateSgListSubRange(
            srcSgList,
            (subRange.Start - request->GetStartIndex()) * request->BlockSize,
            subRange.Size() * request->BlockSize);
        subRequest->Sglist =
            request->Sglist.CreateDepender(std::move(subSgList));

        result.push_back(std::move(subRequest));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
class TCompositeRequest
    : public std::enable_shared_from_this<
          TCompositeRequest<TRequest, TResponse>>
{
private:
    const TVector<std::shared_ptr<TRequest>> SubRequests;
    TVector<TResponse> SubResponses;
    TPromise<TResponse> Promise = NewPromise<TResponse>();

    TAdaptiveLock Lock;
    size_t SubResponseReceived = 0;

public:
    explicit TCompositeRequest(
        std::shared_ptr<TRequest> request,
        const TVector<TBlockRange64>& subRanges,
        ui32 blockSize)
        : SubRequests(
              CreateSubRequests(std::move(request), subRanges, blockSize))
    {
        SubResponses.resize(SubRequests.size());
    }

    TFuture<TResponse> RunSubRequests(
        TCallContextPtr callContext,
        IBlockStore* service)
    {
        if (SubRequests.empty()) {
            TResponse response;
            *response.MutableError() =
                MakeError(E_CANCELLED, "failed to acquire sglist");
            return MakeFuture(std::move(response));
        }

        for (size_t i = 0; i < SubRequests.size(); ++i) {
            auto subFuture = TBlockStoreAdapter::Execute(
                service,
                callContext,
                SubRequests[i]);
            subFuture.Subscribe(
                [self = this->shared_from_this(), requestIndex = i]   //
                (const TFuture<TResponse>& f)
                {
                    self->SubResponses[requestIndex] = UnsafeExtractValue(f);
                    self->OnSubResponse(requestIndex);
                });
        }

        return Promise.GetFuture();
    }

private:
    void OnSubResponse(size_t requestIndex)
    {
        TResponse& response = SubResponses[requestIndex];

        const bool hasError = HasError(response.GetError());
        TPromise<TResponse> promise;

        // Access Promise field with lock.
        with_lock (Lock) {
            ++SubResponseReceived;

            if (!Promise.Initialized()) {
                // We have already responded with an error.
                return;
            }

            const bool isLastResponse =
                SubResponseReceived == SubRequests.size();

            if (!isLastResponse && !hasError) {
                return;
            }

            promise.Swap(Promise);
        }
        // Reply to client without lock.

        if constexpr (TBlockStoreMethodTraits<TRequest>::IsReadRequest()) {
            promise.SetValue(
                hasError ? std::move(response)
                         : MergeReadResponses(SubResponses));

        } else {
            promise.SetValue(std::move(response));
        }

        if constexpr (TBlockStoreMethodTraits<TRequest>::IsLocalRequest()) {
            if (hasError) {
                // Cancel all sub requests in case of error.
                for (auto& subRequest: SubRequests) {
                    subRequest->Sglist.Close();
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSplitConfig
{
    const ui32 BlockSize = 0;
    const ui64 BlockPerStripeCount = 0;

    static TSplitConfig Create(const NProto::TVolume& volume);

    [[nodiscard]] bool NeedToSplitRequest(TBlockRange64 range) const;
    [[nodiscard]] TVector<TBlockRange64> Split(TBlockRange64 range) const;
};

using TSplitConfigByDiskId = THashMap<TString, TSplitConfig>;

// Finds a split config by the name of the disk, if it is registered.
class TSplitConfigRegistry: public TAtomicRefCount<TSplitConfigRegistry>
{
    const TSplitConfigByDiskId SplitConfig;

public:
    explicit TSplitConfigRegistry(TSplitConfigByDiskId splitConfig)
        : SplitConfig(std::move(splitConfig))
    {}

    [[nodiscard]] const TSplitConfig* GetConfig(const TString& diskId) const
    {
        return SplitConfig.FindPtr(diskId);
    }

    [[nodiscard]] const TSplitConfigByDiskId& GetAllConfigs() const
    {
        return SplitConfig;
    }
};

using TSplitConfigRegistryPtr = TIntrusivePtr<TSplitConfigRegistry>;

////////////////////////////////////////////////////////////////////////////////

class TSplitRequestService
    : public TBlockStoreImpl<TSplitRequestService, IBlockStore>
    , public std::enable_shared_from_this<TSplitRequestService>
{
private:
    const IBlockStorePtr Service;

    // Updating Registry should be done under lock to avoid races.
    TAdaptiveLock Lock;
    THotSwap<TSplitConfigRegistry> Registry;

public:
    explicit TSplitRequestService(IBlockStorePtr service)
        : Service(std::move(service))
    {
        TSplitConfigRegistryPtr registry =
            MakeIntrusive<TSplitConfigRegistry>(TSplitConfigByDiskId());
        Registry.AtomicStore(registry);
    }

    // implements IBlockStore

    void Start() override
    {
        Service->Start();
    }

    void Stop() override
    {
        Service->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

    TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request) override
    {
        return ExecuteRequestWithSplit<NProto::TReadBlocksResponse>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return ExecuteRequestWithSplit<NProto::TReadBlocksLocalResponse>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request) override
    {
        return ExecuteRequestWithSplit<NProto::TWriteBlocksResponse>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return ExecuteRequestWithSplit<NProto::TWriteBlocksLocalResponse>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return ExecuteRequestWithSplit<NProto::TZeroBlocksResponse>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        auto result =
            Service->MountVolume(std::move(callContext), std::move(request));
        result.Subscribe(
            [weakSelf = weak_from_this()](
                const TFuture<NProto::TMountVolumeResponse>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->OnMountVolume(f.GetValue());
                }
            });
        return result;
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        return TBlockStoreAdapter::Execute(
            Service.get(),
            std::move(callContext),
            std::move(request));
    }

private:
    template <typename TResponse, typename TRequest>
    TFuture<TResponse> ExecuteRequestWithSplit(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request);

    void OnMountVolume(const NProto::TMountVolumeResponse& response);
};

////////////////////////////////////////////////////////////////////////////////

// static
TSplitConfig TSplitConfig::Create(const NProto::TVolume& volume)
{
    const ui32 blockSize = volume.GetBlockSize();
    size_t stripeSize = 0;

    if (IsBlobStorageMediaKind(volume.GetStorageMediaKind())) {
        if (volume.GetPartitionsCount() > 1) {
            // Need to find a way to get the size of the stripe for
            // multipartition YDB-based volume.
            // Used default BytesPerStripe from
            // cloud/blockstore/libs/storage/core/config.cpp
            stripeSize = 16_MB;
        }
    } else if (IsDiskRegistryMediaKind(volume.GetStorageMediaKind())) {
        if (volume.GetDevices().size() > 0) {
            const ui64 firstDeviceBlockCount =
                volume.GetDevices().Get(0).GetBlockCount();

            const bool allDevicesHaveSameSize = AllOf(
                volume.GetDevices(),
                [firstDeviceBlockCount](const NProto::TDevice& device)
                { return device.GetBlockCount() == firstDeviceBlockCount; });

            if (allDevicesHaveSameSize) {
                stripeSize = firstDeviceBlockCount * blockSize;
            } else {
                ReportDiskDevicesSizeViolation(
                    "DevicesDifferentSizes",
                    {{"diskId", volume.GetDiskId()}});
            }
        } else {
            ReportDiskDevicesSizeViolation(
                "Empty",
                {{"diskId", volume.GetDiskId()}});
        }
    }

    return TSplitConfig{
        .BlockSize = blockSize,
        .BlockPerStripeCount = stripeSize / blockSize};
}

bool TSplitConfig::NeedToSplitRequest(TBlockRange64 range) const
{
    if (BlockPerStripeCount == 0) {
        return false;
    }
    const bool needToSplit =
        range.Start / BlockPerStripeCount != range.End / BlockPerStripeCount;
    return needToSplit;
}

TVector<TBlockRange64> TSplitConfig::Split(TBlockRange64 range) const
{
    return range.Split(BlockPerStripeCount);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse, typename TRequest>
TFuture<TResponse> TSplitRequestService::ExecuteRequestWithSplit(
    TCallContextPtr callContext,
    std::shared_ptr<TRequest> request)
{
    TSplitConfigRegistryPtr registry = Registry.AtomicLoad();
    const auto* config = registry->GetConfig(request->GetDiskId());

    if (!config) {
        TResponse response;
        *response.MutableError() = MakeError(E_REJECTED, "Volume not mounted");
        return MakeFuture<TResponse>(std::move(response));
    }

    const auto range = TBlockRangeHelper::GetRange(*request, config->BlockSize);

    if (!config->NeedToSplitRequest(range)) {
        return TBlockStoreAdapter::Execute(
            Service.get(),
            std::move(callContext),
            std::move(request));
    }

    auto compositeRequest =
        std::make_shared<TCompositeRequest<TRequest, TResponse>>(
            std::move(request),
            config->Split(range),
            config->BlockSize);

    return compositeRequest->RunSubRequests(
        std::move(callContext),
        Service.get());
}

void TSplitRequestService::OnMountVolume(
    const NProto::TMountVolumeResponse& response)
{
    if (HasError(response)) {
        return;
    }

    const auto& diskId = response.GetVolume().GetDiskId();
    TSplitConfigRegistryPtr registry = Registry.AtomicLoad();
    if (registry->GetConfig(diskId)) {
        return;
    }

    with_lock (Lock) {
        TSplitConfigRegistryPtr currentRegistry = Registry.AtomicLoad();

        if (currentRegistry->GetConfig(diskId)) {
            // Perhaps someone has already created stripe config
            // while we were waiting for the lock.
            return;
        }

        // Expect that the count of mounted disks during the server runtime will
        // never be very large, and the hashmap search is close to O(1), so we
        // do not clean up.
        auto currentConfigs = currentRegistry->GetAllConfigs();
        currentConfigs.emplace(
            diskId,
            TSplitConfig::Create(response.GetVolume()));

        TSplitConfigRegistryPtr newRegistry =
            MakeIntrusive<TSplitConfigRegistry>(std::move(currentConfigs));
        Registry.AtomicStore(newRegistry);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateSplitRequestService(IBlockStorePtr service)
{
    return std::make_shared<TSplitRequestService>(std::move(service));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
