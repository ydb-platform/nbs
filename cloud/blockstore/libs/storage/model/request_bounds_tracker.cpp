#include "request_bounds_tracker.h"

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

TRequestBoundsTracker::TRequestBoundsTracker(ui64 blockSize)
    : BlockCountPerRange(MigrationRangeSize / blockSize)
{}

void TRequestBoundsTracker::AddRequest(TBlockRange64 r)
{
    auto startRange = r.Start / BlockCountPerRange;
    auto endRange = r.End / BlockCountPerRange;
    for (size_t i = startRange; i <= endRange; ++i) {
        auto& rangeInfo = RangesWithRequests[i];
        rangeInfo.RequestCount += 1;
    }
}

void TRequestBoundsTracker::RemoveRequest(TBlockRange64 r)
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

bool TRequestBoundsTracker::OverlapsWithRequest(TBlockRange64 r) const
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

}   // namespace NCloud::NBlockStore::NStorage
