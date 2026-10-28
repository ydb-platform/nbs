#include "barrier.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBarriersTest)
{
    Y_UNIT_TEST(ShouldKeepTrackOfBarriers)
    {
        TBarriers barriers;

        barriers.AcquireBarrier(10);
        barriers.AcquireBarrierN(30, 3);
        barriers.AcquireBarrierN(20, 2);
        barriers.AcquireBarrierN(2, 4);

        UNIT_ASSERT_EQUAL(barriers.GetMinCommitId(), 2);
        UNIT_ASSERT_EQUAL(barriers.GetMaxCommitId(), 30);

        barriers.ReleaseBarrier(10);

        UNIT_ASSERT_EQUAL(barriers.GetMinCommitId(), 2);
        UNIT_ASSERT_EQUAL(barriers.GetMaxCommitId(), 30);

        barriers.ReleaseBarrierN(30, 2);

        UNIT_ASSERT_EQUAL(barriers.GetMinCommitId(), 2);
        UNIT_ASSERT_EQUAL(barriers.GetMaxCommitId(), 30);

        barriers.ReleaseBarrier(30);

        UNIT_ASSERT_EQUAL(barriers.GetMinCommitId(), 2);
        UNIT_ASSERT_EQUAL(barriers.GetMaxCommitId(), 20);

        barriers.ReleaseBarrier(20);

        UNIT_ASSERT_EQUAL(barriers.GetMinCommitId(), 2);
        UNIT_ASSERT_EQUAL(barriers.GetMaxCommitId(), 20);

        barriers.ReleaseBarrier(20);

        UNIT_ASSERT_EQUAL(barriers.GetMinCommitId(), 2);
        UNIT_ASSERT_EQUAL(barriers.GetMaxCommitId(), 2);

        barriers.ReleaseBarrierN(2, 4);
    }

    Y_UNIT_TEST(ShouldReportLastRelease)
    {
        TBarriers barriers;

        barriers.AcquireBarrierN(10, 3);
        barriers.AcquireBarrier(20);

        UNIT_ASSERT(!barriers.ReleaseBarrier(10));
        UNIT_ASSERT(!barriers.ReleaseBarrierN(10, 1));
        UNIT_ASSERT(barriers.ReleaseBarrier(10));
        UNIT_ASSERT_VALUES_EQUAL(20, barriers.GetMinCommitId());

        barriers.AcquireBarrierN(30, 4);
        UNIT_ASSERT(!barriers.ReleaseBarrierN(30, 3));
        UNIT_ASSERT(barriers.ReleaseBarrierN(30, 1));

        // One call can release several references at once.
        barriers.AcquireBarrierN(40, 2);
        barriers.AcquireBarrier(40);
        UNIT_ASSERT(barriers.ReleaseBarrierN(40, 3));

        UNIT_ASSERT(barriers.ReleaseBarrier(20));
        UNIT_ASSERT_VALUES_EQUAL(Max<ui64>(), barriers.GetMinCommitId());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
