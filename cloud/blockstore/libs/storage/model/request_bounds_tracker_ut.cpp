#include "request_bounds_tracker.h"

#include "common_constants.h"
#include "requests_in_progress.h"

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
        UNIT_ASSERT(!requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(
                blocksPerTrackingRange,
                blocksPerTrackingRange)));

        requestsInProgress.AddRequest(TBlockRange64::WithLength(10, 10));

        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(0, blocksPerTrackingRange)));
        UNIT_ASSERT(!requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(
                blocksPerTrackingRange,
                blocksPerTrackingRange)));

        requestsInProgress.RemoveRequest(TBlockRange64::WithLength(0, 10));

        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(0, blocksPerTrackingRange)));
        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(0, 10)));
        UNIT_ASSERT(requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(10, 10)));
        UNIT_ASSERT(!requestsInProgress.OverlapsWithRequest(
            TBlockRange64::WithLength(
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

    Y_UNIT_TEST(ShouldTrackRequestsWithBlockRange)
    {
        TRequestsInProgress<EAllowedRequests::ReadWrite, ui32>
            requestsInProgress;

        auto blocksPerTrackingRange = MigrationRangeSize / 4_KB;

        {
            auto id1 = requestsInProgress.GenerateRequestId();

            requestsInProgress.AddWriteRequest(
                id1,
                TBlockRange64::WithLength(0, 10),
                {});

            UNIT_ASSERT(requestsInProgress.OverlapsWithWrites(
                TBlockRange64::WithLength(0, blocksPerTrackingRange)));

            auto id2 = requestsInProgress.GenerateRequestId();
            requestsInProgress.AddWriteRequest(
                id2,
                TBlockRange64::WithLength(10, 10),
                {});
            requestsInProgress.ExtractRequest(id1);

            UNIT_ASSERT(requestsInProgress.OverlapsWithWrites(
                TBlockRange64::WithLength(0, blocksPerTrackingRange)));

            requestsInProgress.ExtractRequest(id2);
            UNIT_ASSERT(!requestsInProgress.OverlapsWithWrites(
                TBlockRange64::WithLength(0, blocksPerTrackingRange)));
        }

        // should mark several tracking ranges if it lays at the border;
        {
            auto id1 = requestsInProgress.GenerateRequestId();

            requestsInProgress.AddWriteRequest(
                id1,
                TBlockRange64::WithLength(blocksPerTrackingRange - 1, 2),
                {});

            UNIT_ASSERT(requestsInProgress.OverlapsWithWrites(
                TBlockRange64::WithLength(0, blocksPerTrackingRange)));
            UNIT_ASSERT(requestsInProgress.OverlapsWithWrites(
                TBlockRange64::WithLength(
                    blocksPerTrackingRange,
                    2 * blocksPerTrackingRange)));
        }
    }

    Y_UNIT_TEST(ShouldTrackAllWriteRequests)
    {
        TRequestsInProgress<EAllowedRequests::ReadWrite, ui32>
            requestsInProgress;

        auto blocksPerTrackingRange = MigrationRangeSize / 4_KB;

        auto id1 = requestsInProgress.GenerateRequestId();
        requestsInProgress.AddWriteRequest(
            id1,
            TBlockRange64::WithLength(0, blocksPerTrackingRange),
            {});

        UNIT_ASSERT(requestsInProgress.OverlapsWithWrites(
            TBlockRange64::WithLength(0, blocksPerTrackingRange)));

        requestsInProgress.RemoveWriteRequest(id1);

        UNIT_ASSERT(!requestsInProgress.OverlapsWithWrites(
            TBlockRange64::WithLength(0, blocksPerTrackingRange)));

        auto id2 = requestsInProgress.AddWriteRequest(
            TBlockRange64::WithLength(0, blocksPerTrackingRange),
            {});
        UNIT_ASSERT(requestsInProgress.OverlapsWithWrites(
            TBlockRange64::WithLength(0, blocksPerTrackingRange)));

        requestsInProgress.RemoveWriteRequest(id2);

        UNIT_ASSERT(!requestsInProgress.OverlapsWithWrites(
            TBlockRange64::WithLength(0, blocksPerTrackingRange)));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
