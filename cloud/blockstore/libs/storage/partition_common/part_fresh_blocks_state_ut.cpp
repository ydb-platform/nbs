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

    Y_UNIT_TEST(ShouldTrackFirstUntrimmedFreshBlobTimestamp)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        const auto t0 = TInstant::Seconds(100);

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Max(),
            state.GetFirstUntrimmedFreshBlobTimestamp());

        state.AddFreshBlob(1, 10, t0);
        state.AddFreshBlob(2, 20, t0 + TDuration::Seconds(1));
        state.AddFreshBlob(3, 30, t0 + TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(
            t0,
            state.GetFirstUntrimmedFreshBlobTimestamp());

        // Flushing does not end the lifetime of the retained log blob.
        state.FlushFreshBlob(1);
        UNIT_ASSERT_VALUES_EQUAL(
            t0,
            state.GetFirstUntrimmedFreshBlobTimestamp());

        state.TrimFreshBlobs(1);
        UNIT_ASSERT_VALUES_EQUAL(
            t0 + TDuration::Seconds(1),
            state.GetFirstUntrimmedFreshBlobTimestamp());

        state.FlushFreshBlob(2);
        state.FlushFreshBlob(3);
        UNIT_ASSERT_VALUES_EQUAL(t0 + TDuration::Seconds(1),
                                 state.GetFirstUntrimmedFreshBlobTimestamp());
        state.TrimFreshBlobs(3);
        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Max(),
            state.GetFirstUntrimmedFreshBlobTimestamp());

        // Track the first blob published into empty state again.
        state.AddFreshBlob(4, 40, t0 + TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(
            t0 + TDuration::Seconds(10),
            state.GetFirstUntrimmedFreshBlobTimestamp());
    }

    Y_UNIT_TEST(ShouldFindOldestUntrimmedUnflushedBlobRegardlessOfCommitOrder)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetOldestUnflushedFreshBlobTimestamp());

        state.AddFreshBlob(3, 30, TInstant::Seconds(110));
        state.AddFreshBlob(1, 10, TInstant::Seconds(120));
        state.AddFreshBlob(2, 20, TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetOldestUnflushedFreshBlobTimestamp());

        state.FlushFreshBlob(2);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(110),
                                 state.GetOldestUnflushedFreshBlobTimestamp());

        state.AddFreshBlob(4, 40, TInstant::Seconds(90));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(90),
                                 state.GetOldestUnflushedFreshBlobTimestamp());

        state.FlushFreshBlob(4);
        state.TrimFreshBlobs(3);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetOldestUnflushedFreshBlobTimestamp());
    }

    Y_UNIT_TEST(ShouldIncludeUnknownTimestampUntilFlushedOrTrimmed)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        state.AddFreshBlob(1, 10, TInstant::Zero());
        state.AddFreshBlob(2, 20, TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(),
                                 state.GetOldestUnflushedFreshBlobTimestamp());

        state.TrimFreshBlobs(1);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetOldestUnflushedFreshBlobTimestamp());

        state.AddFreshBlob(3, 30, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(),
                                 state.GetOldestUnflushedFreshBlobTimestamp());

        state.FlushFreshBlob(3);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetOldestUnflushedFreshBlobTimestamp());
    }

    Y_UNIT_TEST(ShouldPreserveUnknownFreshBlobTimestamp)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        const auto now = TInstant::Seconds(100);

        state.AddFreshBlob(2, 20, now);
        state.AddFreshBlob(1, 10, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::Zero(),
            state.GetFirstUntrimmedFreshBlobTimestamp());
    }

    Y_UNIT_TEST(ShouldTrackTimestampByCommitIdRegardlessOfInsertionOrder)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        state.AddFreshBlob(3, 30, TInstant::Seconds(110));
        state.AddFreshBlob(1, 10, TInstant::Seconds(120));
        state.AddFreshBlob(2, 20, TInstant::Seconds(100));

        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(120),
                                 state.GetFirstUntrimmedFreshBlobTimestamp());

        state.FlushFreshBlob(1);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(120),
                                 state.GetFirstUntrimmedFreshBlobTimestamp());

        state.TrimFreshBlobs(1);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUntrimmedFreshBlobTimestamp());

        state.FlushFreshBlob(2);
        state.TrimFreshBlobs(2);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(110),
                                 state.GetFirstUntrimmedFreshBlobTimestamp());

        state.FlushFreshBlob(3);
        state.TrimFreshBlobs(3);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUntrimmedFreshBlobTimestamp());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
