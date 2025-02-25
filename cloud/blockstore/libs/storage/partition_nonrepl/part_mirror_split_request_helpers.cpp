#include "part_mirror_split_request_helpers.h"

#include <cloud/storage/core/libs/common/sglist_block_range.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

auto SplitReadRequest(
    const NProto::TReadBlocksRequest& originalRequest,
    std::span<const TBlockRange64> requestBlockRanges)
    -> TVector<NProto::TReadBlocksRequest>
{
    auto result = TVector<NProto::TReadBlocksRequest>();
    result.reserve(requestBlockRanges.size());

    for (auto blockRange: requestBlockRanges) {
        auto copyRequest = originalRequest;
        copyRequest.SetBlocksCount(blockRange.Size());
        copyRequest.SetStartIndex(blockRange.Start);

        result.push_back(std::move(copyRequest));
    }

    return result;
}

auto SplitReadRequest(
    const NProto::TReadBlocksLocalRequest& originalRequest,
    std::span<const TBlockRange64> requestBlockRanges)
    -> TVector<NProto::TReadBlocksLocalRequest>
{
    auto guard = originalRequest.Sglist.Acquire();
    if (!guard) {
        return {};
    }

    const auto& originalSglist = guard.Get();
    if (originalSglist.empty()) {
        return {};
    }

    auto result = TVector<NProto::TReadBlocksLocalRequest>();
    result.reserve(requestBlockRanges.size());

    auto sglistBlockRange =
        TSgListBlockRange(originalSglist, originalRequest.BlockSize);
    for (const auto& blockRange: requestBlockRanges) {
        auto blocksNeeded = blockRange.Size();

        auto newSglist = sglistBlockRange.Next(blocksNeeded);
        if (SgListGetSize(newSglist) !=
            blocksNeeded * originalRequest.BlockSize)
        {
            // It means that we doesn't have enough buffers in original request,
            // so it is incorrect.
            return {};
        }

        auto& copyRequest = result.emplace_back(originalRequest);
        copyRequest.SetBlocksCount(blockRange.Size());
        copyRequest.SetStartIndex(blockRange.Start);
        copyRequest.Sglist =
            originalRequest.Sglist.Create(std::move(newSglist));
    }

    return result;
}

auto MergeReadResponses(std::span<NProto::TReadBlocksResponse> responsesToMerge)
    -> NProto::TReadBlocksResponse
{
    NProto::TReadBlocksResponse result;

    ui64 throttlerDelaySum = 0;
    bool allZeros = true;
    bool allBlocksEmpty = true;
    for (const auto& response: responsesToMerge) {
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
    for (auto& response: responsesToMerge) {
        auto& src = *response.MutableBlocks()->MutableBuffers();
        dst.Add(
            std::make_move_iterator(src.begin()),
            std::make_move_iterator(src.end()));
    }

    // The unencrypted block mask is not used (Check pr #1771), so we don't have
    // to fill it out.
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage::NSplitRequest
