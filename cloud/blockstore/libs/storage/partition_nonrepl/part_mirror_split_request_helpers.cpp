#include "part_mirror_split_request_helpers.h"

#include <cloud/storage/core/libs/common/sglist_block_range.h>

#include <ranges>

namespace NCloud::NBlockStore::NStorage::NSplitRequest {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename TMethod>
NSplitRequest::TSplittedRequest<TMethod> SplitRequestGeneralRead(
    const TRequestRecordType<TMethod>& originalRequest,
    TArrayRef<const TBlockRange64> blockRangeSplittedByDeviceBorders,
    TArrayRef<TVector<TActorId>> partitionsPerDevice)
{
    NSplitRequest::TSplittedRequest<TMethod> result;
    result.reserve(blockRangeSplittedByDeviceBorders.size());

    for (size_t i = 0; i < blockRangeSplittedByDeviceBorders.size(); ++i) {
        const auto& blockRange = blockRangeSplittedByDeviceBorders[i];
        TRequestRecordType<TMethod> copyRequest = originalRequest;
        copyRequest.SetBlocksCount(blockRange.Size());
        copyRequest.SetStartIndex(blockRange.Start);

        result.push_back(
            {std::move(copyRequest),
             std::move(partitionsPerDevice[i]),
             blockRange});
    }

    return result;
}
}   // namespace

TSplittedRequest<TEvService::TReadBlocksMethod> SplitRequestRead(
    const NProto::TReadBlocksRequest& originalRequest,
    TArrayRef<const TBlockRange64> blockRangeSplittedByDeviceBorders,
    TArrayRef<TVector<NActors::TActorId>> partitionsPerDevice)
{
    return SplitRequestGeneralRead<TEvService::TReadBlocksMethod>(
        originalRequest,
        blockRangeSplittedByDeviceBorders,
        partitionsPerDevice);
}

std::optional<TSplittedRequest<TEvService::TReadBlocksLocalMethod>>
SplitRequestReadLocal(
    const NProto::TReadBlocksLocalRequest& originalRequest,
    TArrayRef<const TBlockRange64> blockRangeSplittedByDeviceBorders,
    TArrayRef<TVector<NActors::TActorId>> partitionsPerDevice)
{
    auto result = SplitRequestGeneralRead<TEvService::TReadBlocksLocalMethod>(
        originalRequest,
        blockRangeSplittedByDeviceBorders,
        partitionsPerDevice);

    auto guard = originalRequest.Sglist.Acquire();
    if (!guard) {
        return std::nullopt;
    }

    const auto& originalSglist = guard.Get();
    if (originalSglist.size() == 0) {
        return std::nullopt;
    }

    TSgListBlockRange sglistBlockRange(
        originalSglist,
        originalRequest.BlockSize);
    for (size_t i = 0; i < blockRangeSplittedByDeviceBorders.size(); ++i) {
        auto blocksNeeded = blockRangeSplittedByDeviceBorders[i].Size();

        TSgList newSglist = sglistBlockRange.Next(blocksNeeded);
        size_t newSglistBuffersSize = 0;
        for (auto buffer: newSglist) {
            newSglistBuffersSize += buffer.Size();
        }

        if (newSglistBuffersSize != blocksNeeded * originalRequest.BlockSize) {
            // It means that we doesn't have enough buffers in original request,
            // so it is incorrect.
            return std::nullopt;
        }

        result[i].Request.Sglist =
            originalRequest.Sglist.Create(std::move(newSglist));
    }

    return result;
}

NProto::TReadBlocksResponse UnifyResponsesRead(
    TArrayRef<const TUnifyResponsesContext<TEvService::TReadBlocksMethod>>
        responsesToUnify,
    bool fillZeroResponses,
    size_t blockSize)
{
    NProto::TReadBlocksResponse result;

    const auto* errorResponse = FindIfPtr(
        responsesToUnify,
        [&](const auto& el) { return HasError(el.Response.GetError()); });

    if (errorResponse) {
        result.MutableError()->CopyFrom(errorResponse->Response.GetError());

        return result;
    }

    ui64 throttlerDelaySum = 0;
    bool allZeros = true;
    bool allBlocksEmpty = true;
    for (const auto& [response, blocksCountRequested]: responsesToUnify) {
        allZeros &= response.GetAllZeroes();
        allBlocksEmpty &= response.GetBlocks().BuffersSize() == 0;
        throttlerDelaySum += response.GetThrottlerDelay();
    }

    result.SetThrottlerDelay(throttlerDelaySum);
    result.SetAllZeroes(allZeros);

    if (allBlocksEmpty) {
        return result;
    }
    for (const auto& [response, blocksCountRequested]: responsesToUnify) {
        auto blocks = response.GetBlocks();

        if (blocks.BuffersSize() == 0 && fillZeroResponses) {
            for (size_t i = 0; i < blocksCountRequested; ++i) {
                result.MutableBlocks()->AddBuffers(TString(blockSize, '\0'));
            }
        } else {
            for (auto buffer: blocks.GetBuffers()) {
                result.MutableBlocks()->AddBuffers(std::move(buffer));
            }
        }
    }

    // The unencrypted block mask is not used (Check pr #1771), so we don't have
    // to fill it out.
    return result;
}
}   // namespace NCloud::NBlockStore::NStorage::NSplitRequest
