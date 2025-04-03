#pragma once

#include "public.h"

#include <util/generic/hash.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TFreshBlocksStats
{
private:
    static constexpr ui32 RangeCountThresholdForEstimate = 30;

    THashMap<ui32, ui32> RangeBlocksCount;
    ui32 TotalBlocksCount = 0;

public:
    void IncrementBlocksCountInRange(ui32 rangeId)
    {
        RangeBlocksCount[rangeId]++;
        TotalBlocksCount++;
    }

    void DecrementBlocksCountInRange(ui32 rangeId)
    {
        auto it = RangeBlocksCount.find(rangeId);
        if (it != RangeBlocksCount.end()) {
            TotalBlocksCount--;
            if (--it->second == 0) {
                RangeBlocksCount.erase(it);
            }
        }
    }

    ui32 CalculateBlobsCount(ui32 maxBlobsInRange) const
    {
        if (maxBlobsInRange == 0) {
            return static_cast<ui32>(RangeBlocksCount.size());
        }

        // Estimate the expected number of blobs when the number of ranges
        // is large enough. This is needed to avoid O(n^2) complexity.
        if (RangeBlocksCount.size() > RangeCountThresholdForEstimate) {
            // Consider the worst case scenario
            const ui32 singleBlockRanges =
                static_cast<ui32>(RangeBlocksCount.size() - 1);
            const ui32 blocksInTheLastRange =
                TotalBlocksCount - singleBlockRanges;
            const ui32 blobsInTheLastRange =
                (blocksInTheLastRange + maxBlobsInRange - 1) / maxBlobsInRange;
            return singleBlockRanges + blobsInTheLastRange;
        }

        ui32 result = 0;
        for (const auto& [rangeId, blocksCount]: RangeBlocksCount) {
            if (blocksCount > 0) {
                result += (blocksCount + maxBlobsInRange - 1) / maxBlobsInRange;
            }
        }
        return result;
    }
};

}   // namespace NCloud::NFileStore::NStorage
