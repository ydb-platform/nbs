#include "request_bounds_tracker.h"

#include "common_constants.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestBoundsTrackerTest)
{
    Y_UNIT_TEST(ShouldTrackRequestsBounds)
    {
        auto blockSize = 4_KB;
        TRequestBoundsTracker requestsInProgress{blockSize};

        auto blocksPerTrackingRange = MigrationRangeSize / 4_KB;

        requestsInProgress.AddRequest(TBlockRange64::WithLength(0, 10));

        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(0, blocksPerTrackingRange)));
        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(0, 10)));
        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(10, 10)));
        UNIT_ASSERT(
            !requestsInProgress.OverlapsWithRequest(TBlockRange64::WithLength(
                blocksPerTrackingRange,
                blocksPerTrackingRange)));

        requestsInProgress.AddRequest(TBlockRange64::WithLength(10, 10));

        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(0, blocksPerTrackingRange)));
        UNIT_ASSERT(
            !requestsInProgress.OverlapsWithRequest(TBlockRange64::WithLength(
                blocksPerTrackingRange,
                blocksPerTrackingRange)));

        requestsInProgress.RemoveRequest(TBlockRange64::WithLength(0, 10));

        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(0, blocksPerTrackingRange)));
        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(0, 10)));
        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(10, 10)));
        UNIT_ASSERT(
            !requestsInProgress.OverlapsWithRequest(TBlockRange64::WithLength(
                blocksPerTrackingRange,
                blocksPerTrackingRange)));

        requestsInProgress.RemoveRequest(TBlockRange64::WithLength(10, 10));

        UNIT_ASSERT(!requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(0, blocksPerTrackingRange)));
    }

    Y_UNIT_TEST(ShouldMarkSeveralTrackingRangesForBorderRequests)
    {
        auto blockSize = 4_KB;
        TRequestBoundsTracker requestsInProgress{blockSize};

        auto blocksPerTrackingRange = MigrationRangeSize / 4_KB;

        auto firstTrackingRange =
            TBlockRange64::WithLength(0, blocksPerTrackingRange);
        auto secondTrackingRange = TBlockRange64::WithLength(
            blocksPerTrackingRange,
            blocksPerTrackingRange);

        requestsInProgress.AddRequest(
            TBlockRange64::WithLength(blocksPerTrackingRange - 1, 2));

        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(firstTrackingRange));
        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(0, 10)));

        UNIT_ASSERT(
            requestsInProgress.OverlapsWithRequest(secondTrackingRange));
        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(blocksPerTrackingRange + 10, 10)));

        requestsInProgress.RemoveRequest(
            TBlockRange64::WithLength(blocksPerTrackingRange - 1, 2));

        UNIT_ASSERT(
            !requestsInProgress.OverlapsWithRequest(firstTrackingRange));
        UNIT_ASSERT(!requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(0, 10)));

        UNIT_ASSERT(
            !requestsInProgress.OverlapsWithRequest(secondTrackingRange));
        UNIT_ASSERT(!requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(blocksPerTrackingRange + 10, 10)));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
