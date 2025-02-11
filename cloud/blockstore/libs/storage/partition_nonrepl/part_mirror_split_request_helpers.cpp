#include "part_mirror_split_request_helpers.h"

#include <cloud/storage/core/libs/common/sglist_block_range.h>

namespace NCloud::NBlockStore::NStorage::NSplitRequest {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename TMethod>
auto SplitRequestGeneralRead(
    const TRequestRecordType<TMethod>& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders)
    -> TSplitRequest<TMethod>
{
    TSplitRequest<TMethod> result;
    result.reserve(blockRangeSplittedByDeviceBorders.size());

    for (auto blockRange: blockRangeSplittedByDeviceBorders) {
        TRequestRecordType<TMethod> copyRequest = originalRequest;
        copyRequest.SetBlocksCount(blockRange.Size());
        copyRequest.SetStartIndex(blockRange.Start);

        result.push_back({std::move(copyRequest), blockRange});
    }

    return result;
}

template <typename TMethod>
auto MergeResponsesGeneralRead(
    std::span<TMergeResponsesContext<TMethod>> responsesToUnify,
    size_t blockSize) -> NProto::TReadBlocksResponse
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
    for (auto& [response, blocksCountRequested]: responsesToUnify) {
        auto& blocks = response.GetBlocks();

        if (blocks.BuffersSize() == 0) {
            for (size_t i = 0; i < blocksCountRequested; ++i) {
                result.MutableBlocks()->AddBuffers(TString(blockSize, '\0'));
            }
        } else {
            for (auto& buffer: blocks.GetBuffers()) {
                result.MutableBlocks()->AddBuffers(std::move(buffer));
            }
        }
    }

    // The unencrypted block mask is not used (Check pr #1771), so we don't have
    // to fill it out.
    return result;
}
}   // namespace

auto SplitReadRequest(
    const NProto::TReadBlocksRequest& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders)
    -> TSplitRequest<TEvService::TReadBlocksMethod>
{
    return SplitRequestGeneralRead<TEvService::TReadBlocksMethod>(
        originalRequest,
        blockRangeSplittedByDeviceBorders);
}

auto SplitReadRequest(
    const NProto::TReadBlocksLocalRequest& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders)
    -> TSplitRequest<TEvService::TReadBlocksLocalMethod>
{
    auto result = SplitRequestGeneralRead<TEvService::TReadBlocksLocalMethod>(
        originalRequest,
        blockRangeSplittedByDeviceBorders);

    auto guard = originalRequest.Sglist.Acquire();
    if (!guard) {
        return {};
    }

    const auto& originalSglist = guard.Get();
    if (originalSglist.size() == 0) {
        return {};
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
            return {};
        }

        result[i].Request.Sglist =
            originalRequest.Sglist.Create(std::move(newSglist));
    }

    return result;
}

auto MergeReadResponses(
    std::span<TMergeResponsesContext<TEvService::TReadBlocksMethod>>
        responsesToUnify,
    size_t blockSize) -> NProto::TReadBlocksResponse
{
    return MergeResponsesGeneralRead<TEvService::TReadBlocksMethod>(
        responsesToUnify,
        blockSize);
}

auto MergeReadResponses(
    std::span<TMergeResponsesContext<TEvService::TReadBlocksLocalMethod>>
        responsesToUnify,
    size_t blockSize) -> NProto::TReadBlocksResponse
{
    return MergeResponsesGeneralRead<TEvService::TReadBlocksLocalMethod>(
        responsesToUnify,
        blockSize);
}

}   // namespace NCloud::NBlockStore::NStorage::NSplitRequest
