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

    Y_UNIT_TEST(ShouldTrackFirstUnflushedFreshBlobTimestamp)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        const auto t0 = TInstant::Seconds(100);

        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        state.AddFreshBlob(1, 10, t0);
        state.AddFreshBlob(2, 20, t0 + TDuration::Seconds(1));
        state.AddFreshBlob(3, 30, t0 + TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(t0,
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        // A flushed blob does not count even before it is trimmed.
        state.FlushFreshBlob(1);
        UNIT_ASSERT_VALUES_EQUAL(t0 + TDuration::Seconds(1),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        state.TrimFreshBlobs(1);
        UNIT_ASSERT_VALUES_EQUAL(t0 + TDuration::Seconds(1),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        state.FlushFreshBlob(2);
        state.FlushFreshBlob(3);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        // Track the first blob published into empty state again.
        state.AddFreshBlob(4, 40, t0 + TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(t0 + TDuration::Seconds(10),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));
    }

    Y_UNIT_TEST(ShouldFindFirstUnflushedBlobAfterTrimBoundary)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        state.AddFreshBlob(3, 30, TInstant::Seconds(110));
        state.AddFreshBlob(1, 10, TInstant::Seconds(100));
        state.AddFreshBlob(5, 50, TInstant::Seconds(120));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        // A dead blob at or below the trim boundary waits for a trim.
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(110),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(110),
                                 state.GetFirstUnflushedFreshBlobTimestamp(2));

        // A flushed blob above the trim boundary waits for the write that
        // holds the boundary.
        state.FlushFreshBlob(3);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(120),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(5));

        state.TrimFreshBlobs(3);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(120),
                                 state.GetFirstUnflushedFreshBlobTimestamp(3));

        state.FlushFreshBlob(5);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(3));
    }

    Y_UNIT_TEST(ShouldIncludeUnknownTimestampAfterTrimBoundary)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        state.AddFreshBlob(1, 10, TInstant::Zero());
        state.AddFreshBlob(2, 20, TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        state.TrimFreshBlobs(1);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        state.AddFreshBlob(3, 30, TInstant::Zero());
        state.FlushFreshBlob(2);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(3));
    }

    Y_UNIT_TEST(ShouldPreserveUnknownFreshBlobTimestamp)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        const auto now = TInstant::Seconds(100);

        state.AddFreshBlob(2, 20, now);
        state.AddFreshBlob(1, 10, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));
    }

    Y_UNIT_TEST(ShouldTrackTimestampByCommitIdRegardlessOfInsertionOrder)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0);
        state.AddFreshBlob(3, 30, TInstant::Seconds(110));
        state.AddFreshBlob(1, 10, TInstant::Seconds(120));
        state.AddFreshBlob(2, 20, TInstant::Seconds(100));

        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(120),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        state.FlushFreshBlob(1);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        state.TrimFreshBlobs(1);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        state.FlushFreshBlob(2);
        state.TrimFreshBlobs(2);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(110),
                                 state.GetFirstUnflushedFreshBlobTimestamp(2));

        state.FlushFreshBlob(3);
        state.TrimFreshBlobs(3);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(3));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
