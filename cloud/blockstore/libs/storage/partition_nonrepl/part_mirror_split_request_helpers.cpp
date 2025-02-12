#include "part_mirror_split_request_helpers.h"

#include <cloud/storage/core/libs/common/sglist_block_range.h>

namespace NCloud::NBlockStore::NStorage::NSplitRequest {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

auto SplitReadRequest(
    const NProto::TReadBlocksRequest& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders)
    -> TVector<NProto::TReadBlocksRequest>
{
    auto result = TVector<NProto::TReadBlocksRequest>();
    result.reserve(blockRangeSplittedByDeviceBorders.size());

    for (auto blockRange: blockRangeSplittedByDeviceBorders) {
        auto copyRequest = originalRequest;
        copyRequest.SetBlocksCount(blockRange.Size());
        copyRequest.SetStartIndex(blockRange.Start);

        result.push_back(std::move(copyRequest));
    }

    return result;
}

auto SplitReadRequest(
    const NProto::TReadBlocksLocalRequest& originalRequest,
    std::span<const TBlockRange64> blockRangeSplittedByDeviceBorders)
    -> TVector<NProto::TReadBlocksLocalRequest>
{
    auto result = TVector<NProto::TReadBlocksLocalRequest>();
    result.reserve(blockRangeSplittedByDeviceBorders.size());

    auto guard = originalRequest.Sglist.Acquire();
    if (!guard) {
        return {};
    }

    const auto& originalSglist = guard.Get();
    if (originalSglist.empty()) {
        return {};
    }

    auto sglistBlockRange =
        TSgListBlockRange(originalSglist, originalRequest.BlockSize);
    for (const auto& blockRange: blockRangeSplittedByDeviceBorders) {
        auto copyRequest = originalRequest;
        copyRequest.SetBlocksCount(blockRange.Size());
        copyRequest.SetStartIndex(blockRange.Start);

        auto blocksNeeded = blockRange.Size();

        auto newSglist = sglistBlockRange.Next(blocksNeeded);
        if (SgListGetSize(newSglist) !=
            blocksNeeded * originalRequest.BlockSize)
        {
            // It means that we doesn't have enough buffers in original request,
            // so it is incorrect.
            return {};
        }

        copyRequest.Sglist =
            originalRequest.Sglist.Create(std::move(newSglist));

        result.push_back(std::move(copyRequest));
    }

    return result;
}

auto MergeReadResponses(
    std::span<TSplitReadBlocksResponse> responsesToMerge,
    size_t blockSize) -> NProto::TReadBlocksResponse
{
    NProto::TReadBlocksResponse result;

    ui64 throttlerDelaySum = 0;
    bool allZeros = true;
    bool allBlocksEmpty = true;
    for (const auto& [response, _]: responsesToMerge) {
        if (HasError(response)) {
            return response;
        }
        allZeros &= response.GetAllZeroes();
        allBlocksEmpty &= response.GetBlocks().BuffersSize() == 0;
        throttlerDelaySum += response.GetThrottlerDelay();
    }

    result.SetThrottlerDelay(throttlerDelaySum);
    result.SetAllZeroes(allZeros);

    if (allBlocksEmpty) {
        return result;
    }

    auto& dst = *result.MutableBlocks()->MutableBuffers();
    for (auto& [response, blocksCountRequested]: responsesToMerge) {
        if (response.GetBlocks().BuffersSize()) {
            auto& src = *response.MutableBlocks()->MutableBuffers();
            dst.Add(
                std::make_move_iterator(src.begin()),
                std::make_move_iterator(src.end()));
            continue;
        }

        // generate zeroes
        for (ui32 i = 0; i != blocksCountRequested; ++i) {
            dst.Add()->resize(blockSize, 0);
        }
    }

    // The unencrypted block mask is not used (Check pr #1771), so we don't have
    // to fill it out.
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage::NSplitRequest
