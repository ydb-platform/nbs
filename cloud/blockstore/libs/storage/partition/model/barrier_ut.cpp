#include "barrier.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

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
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
