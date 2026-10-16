#include "part_fresh_blocks_state.h"

#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition/part_schema.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionFreshBlocksStateTest)
{
    Y_UNIT_TEST(ShouldCalculateFreshBlobByteCount)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);

        state.AddFreshBlob(1, 10, TInstant::Zero());
        state.AddFreshBlob(3, 30, TInstant::Zero());
        state.AddFreshBlob(2, 20, TInstant::Zero());
        state.AddFreshBlob(5, 50, TInstant::Zero());
        state.AddFreshBlob(4, 40, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(150, state.GetUntrimmedFreshBlobByteCount());

        state.TrimFreshBlobs(3);

        UNIT_ASSERT_VALUES_EQUAL(90, state.GetUntrimmedFreshBlobByteCount());

        state.AddFreshBlob(7, 70, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(160, state.GetUntrimmedFreshBlobByteCount());

        state.TrimFreshBlobs(10);

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUntrimmedFreshBlobByteCount());
    }

    Y_UNIT_TEST(ShouldAllowIncrementingFlushCountersToMaxValue)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);

        state.AddFreshBlob(1, Max<ui32>(), TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui32>(),
            state.GetUnflushedFreshBlobByteCount());
    }

    Y_UNIT_TEST(ShouldTrackLowestCommitIdFreshBlobAge)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        const auto t0 = TInstant::Seconds(100);

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Max(),
            state.GetLowestCommitIdFreshBlobTimestamp());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            state.GetLowestCommitIdFreshBlobAge(t0));

        state.AddFreshBlob(1, 10, t0);
        state.AddFreshBlob(2, 20, t0 + TDuration::Seconds(1));
        state.AddFreshBlob(3, 30, t0 + TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(
            t0,
            state.GetLowestCommitIdFreshBlobTimestamp());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(5),
            state.GetLowestCommitIdFreshBlobAge(t0 + TDuration::Seconds(5)));
        // Time going backwards does not produce a negative age.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            state.GetLowestCommitIdFreshBlobAge(t0 - TDuration::Seconds(1)));

        // Trimming does not affect unflushed blobs.
        state.TrimFreshBlobs(3);
        UNIT_ASSERT_VALUES_EQUAL(
            t0,
            state.GetLowestCommitIdFreshBlobTimestamp());

        state.FlushFreshBlob(1);
        UNIT_ASSERT_VALUES_EQUAL(
            t0 + TDuration::Seconds(1),
            state.GetLowestCommitIdFreshBlobTimestamp());

        state.FlushFreshBlob(2);
        state.FlushFreshBlob(3);
        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Max(),
            state.GetLowestCommitIdFreshBlobTimestamp());

        // Age restarts from the first blob published into empty state.
        state.AddFreshBlob(4, 40, t0 + TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(
            t0 + TDuration::Seconds(10),
            state.GetLowestCommitIdFreshBlobTimestamp());
    }

    Y_UNIT_TEST(ShouldTreatFreshBlobsWithUnknownTimestampAsOldest)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        const auto now = TInstant::Seconds(100);

        state.AddFreshBlob(2, 20, now);
        state.AddFreshBlob(1, 10, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Zero(),
            state.GetLowestCommitIdFreshBlobTimestamp());
        UNIT_ASSERT_VALUES_EQUAL(
            now - TInstant::Zero(),
            state.GetLowestCommitIdFreshBlobAge(now));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
