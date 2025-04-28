#include "request_bounds_tracker.h"

#include "common_constants.h"

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

TRequestBoundsTracker::TRequestBoundsTracker(ui64 blockSize)
    : BlockCountPerRange(MigrationRangeSize / blockSize)
{}

void TRequestBoundsTracker::AddRequest(TBlockRange64 range)
{
    auto startRange = range.Start / BlockCountPerRange;
    auto endRange = range.End / BlockCountPerRange;
    for (size_t i = startRange; i <= endRange; ++i) {
        auto& rangeInfo = RangesWithRequests[i];
        rangeInfo.RequestCount += 1;
    }
}

void TRequestBoundsTracker::RemoveRequest(TBlockRange64 range)
{
    auto startRange = range.Start / BlockCountPerRange;
    auto endRange = range.End / BlockCountPerRange;
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

bool TRequestBoundsTracker::OverlapsWithRequest(TBlockRange64 range) const
{
    auto startRange = range.Start / BlockCountPerRange;
    auto endRange = range.End / BlockCountPerRange;
    for (size_t i = startRange; i <= endRange; ++i) {
        if (RangesWithRequests.contains(i)) {
            return true;
        }
    }

    return false;
}

}   // namespace NCloud::NBlockStore::NStorage
