#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

constexpr ui64 RangeTrackingAccuracy = 4_MB;

class TRequestBoundsTracker
{
    struct TRangeInfo
    {
        ui64 RequestCount = 0;
    };

    THashMap<ui64, TRangeInfo> RangesWithRequests;
    const ui64 BlockCountPerRange;

public:
    explicit TRequestBoundsTracker(ui64 blockSize)
        : BlockCountPerRange(RangeTrackingAccuracy / blockSize)
    {}

    void AddRequest(TBlockRange64 r)
    {
        auto startRange = r.Start / BlockCountPerRange;
        auto endRange = r.End / BlockCountPerRange;
        for (size_t i = startRange; i <= endRange; ++i) {
            auto& rangeInfo = RangesWithRequests[i];
            rangeInfo.RequestCount += 1;
        }
    }

    void RemoveRequest(TBlockRange64 r)
    {
        auto startRange = r.Start / BlockCountPerRange;
        auto endRange = r.End / BlockCountPerRange;
        for (size_t i = startRange; i <= endRange; ++i) {
            auto it = RangesWithRequests.find(i);

            Y_DEBUG_ABORT_UNLESS(it != RangesWithRequests.end());
            if (it == RangesWithRequests.end()) {
                continue;
            }

            auto& rangeInfo = it->second;
            rangeInfo.RequestCount -= 1;
            if (rangeInfo.RequestCount == 0) {
                RangesWithRequests.erase(it);
            }
        }
    }

    [[nodiscard]] bool OverlapsSomeRange(TBlockRange64 r) const
    {
        auto startRange = r.Start / BlockCountPerRange;
        auto endRange = r.End / BlockCountPerRange;
        for (size_t i = startRange; i <= endRange; ++i) {
            auto it = RangesWithRequests.find(i);
            if (it != RangesWithRequests.end()) {
                return true;
            }
        }

        return false;
    }
};

}   // namespace NCloud::NBlockStore::NStorage
