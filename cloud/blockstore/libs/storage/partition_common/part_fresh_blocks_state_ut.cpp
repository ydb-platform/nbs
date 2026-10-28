#include "part_fresh_blocks_state.h"

#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition/part_schema.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionFreshBlocksStateTest)
{
    Y_UNIT_TEST(ShouldCalculateFreshBlobByteCount)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);

        state.AddFreshBlob(1, 10, TInstant::Zero());
        state.AddFreshBlob(3, 30, TInstant::Zero());
        state.AddFreshBlob(2, 20, TInstant::Zero());
        state.AddFreshBlob(5, 50, TInstant::Zero());
        state.AddFreshBlob(4, 40, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(150, state.GetUntrimmedFreshBlobByteCount());

        UNIT_ASSERT_VALUES_EQUAL(60, state.TrimFreshBlobs(3));

        UNIT_ASSERT_VALUES_EQUAL(90, state.GetUntrimmedFreshBlobByteCount());

        state.AddFreshBlob(7, 70, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(160, state.GetUntrimmedFreshBlobByteCount());

        UNIT_ASSERT_VALUES_EQUAL(160, state.TrimFreshBlobs(10));

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUntrimmedFreshBlobByteCount());
    }

    Y_UNIT_TEST(ShouldAllowIncrementingFlushCountersToMaxValue)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);

        state.AddFreshBlob(1, Max<ui32>(), TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui32>(),
            state.GetUnflushedFreshBlobByteCount());
    }

    Y_UNIT_TEST(ShouldTrackFirstUnflushedFreshBlobTimestamp)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
        const auto t0 = TInstant::Seconds(100);

        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        state.AddFreshBlob(1, 10, t0);
        state.AddFreshBlob(2, 20, t0 + TDuration::Seconds(1));
        state.AddFreshBlob(3, 30, t0 + TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(t0,
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        // A flushed blob does not count even before it is trimmed.
        UNIT_ASSERT_VALUES_EQUAL(10, state.FlushFreshBlob(1, 0));
        UNIT_ASSERT_VALUES_EQUAL(t0 + TDuration::Seconds(1),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        UNIT_ASSERT_VALUES_EQUAL(0, state.TrimFreshBlobs(1));
        UNIT_ASSERT_VALUES_EQUAL(t0 + TDuration::Seconds(1),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        UNIT_ASSERT_VALUES_EQUAL(20, state.FlushFreshBlob(2, 1));
        UNIT_ASSERT_VALUES_EQUAL(30, state.FlushFreshBlob(3, 1));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        // Track the first blob published into empty state again.
        state.AddFreshBlob(4, 40, t0 + TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(t0 + TDuration::Seconds(10),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));
    }

    Y_UNIT_TEST(ShouldFindFirstUnflushedBlobAfterTrimBoundary)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
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
        UNIT_ASSERT_VALUES_EQUAL(30, state.FlushFreshBlob(3, 0));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(120),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(5));

        // The trim removes the dead blob 1 from the unflushed accounting.
        UNIT_ASSERT_VALUES_EQUAL(10, state.TrimFreshBlobs(3));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(120),
                                 state.GetFirstUnflushedFreshBlobTimestamp(3));

        UNIT_ASSERT_VALUES_EQUAL(50, state.FlushFreshBlob(5, 3));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(3));
    }

    Y_UNIT_TEST(ShouldIncludeUnknownTimestampAfterTrimBoundary)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
        state.AddFreshBlob(1, 10, TInstant::Zero());
        state.AddFreshBlob(2, 20, TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        UNIT_ASSERT_VALUES_EQUAL(10, state.TrimFreshBlobs(1));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        state.AddFreshBlob(3, 30, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(20, state.FlushFreshBlob(2, 1));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(3));
    }

    Y_UNIT_TEST(ShouldPreserveUnknownFreshBlobTimestamp)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
        const auto now = TInstant::Seconds(100);

        state.AddFreshBlob(2, 20, now);
        state.AddFreshBlob(1, 10, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));
    }

    Y_UNIT_TEST(ShouldTrackTimestampByCommitIdRegardlessOfInsertionOrder)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
        state.AddFreshBlob(3, 30, TInstant::Seconds(110));
        state.AddFreshBlob(1, 10, TInstant::Seconds(120));
        state.AddFreshBlob(2, 20, TInstant::Seconds(100));

        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(120),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        UNIT_ASSERT_VALUES_EQUAL(10, state.FlushFreshBlob(1, 0));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        UNIT_ASSERT_VALUES_EQUAL(0, state.TrimFreshBlobs(1));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        UNIT_ASSERT_VALUES_EQUAL(20, state.FlushFreshBlob(2, 1));
        UNIT_ASSERT_VALUES_EQUAL(0, state.TrimFreshBlobs(2));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(110),
                                 state.GetFirstUnflushedFreshBlobTimestamp(2));

        UNIT_ASSERT_VALUES_EQUAL(30, state.FlushFreshBlob(3, 2));
        UNIT_ASSERT_VALUES_EQUAL(0, state.TrimFreshBlobs(3));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(3));
    }

    Y_UNIT_TEST(ShouldRemoveTrimmedBlobsFromUnflushedAccounting)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
        state.AddFreshBlob(1, 10, TInstant::Zero());
        state.AddFreshBlob(2, 20, TInstant::Zero());
        state.AddFreshBlob(3, 30, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(10, state.FlushFreshBlob(1, 0));
        UNIT_ASSERT_VALUES_EQUAL(2, state.GetUnflushedFreshBlobCount());
        UNIT_ASSERT_VALUES_EQUAL(50, state.GetUnflushedFreshBlobByteCount());

        // Blob 1 is already flushed, so only blob 2 leaves the unflushed
        // accounting.
        UNIT_ASSERT_VALUES_EQUAL(20, state.TrimFreshBlobs(2));
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetUnflushedFreshBlobCount());
        UNIT_ASSERT_VALUES_EQUAL(30, state.GetUnflushedFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(30, state.GetUntrimmedFreshBlobByteCount());

        // A flush that captured blob 2 before the trim counts nothing for it.
        UNIT_ASSERT_VALUES_EQUAL(0, state.FlushFreshBlob(2, 2));
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetUnflushedFreshBlobCount());
        UNIT_ASSERT_VALUES_EQUAL(30, state.GetUnflushedFreshBlobByteCount());

        UNIT_ASSERT_VALUES_EQUAL(30, state.FlushFreshBlob(3, 2));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUnflushedFreshBlobCount());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUnflushedFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(30, state.GetUntrimmedFreshBlobByteCount());

        UNIT_ASSERT_VALUES_EQUAL(0, state.TrimFreshBlobs(3));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUntrimmedFreshBlobByteCount());
    }

    Y_UNIT_TEST(ShouldCountDeadFreshBlobBytes)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
        state.AddFreshBlob(1, 10, TInstant::Zero());
        // A zero blob is smaller than a block, but counts one block.
        state.AddFreshBlob(2, 2, TInstant::Zero());
        state.AddFreshBlob(3, 30, TInstant::Zero());
        state.AddFreshBlob(4, 40, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDeadFreshBlobByteCount());

        state.MarkFreshBlobDead(2);
        state.MarkFreshBlobDead(3);
        UNIT_ASSERT_VALUES_EQUAL(34, state.GetDeadFreshBlobByteCount());

        state.MarkFreshBlobDead(1);
        UNIT_ASSERT_VALUES_EQUAL(44, state.GetDeadFreshBlobByteCount());

        // Trims remove dead bytes.
        UNIT_ASSERT_VALUES_EQUAL(12, state.TrimFreshBlobs(2));
        UNIT_ASSERT_VALUES_EQUAL(30, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(30, state.TrimFreshBlobs(3));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDeadFreshBlobByteCount());

        state.MarkFreshBlobDead(4);
        UNIT_ASSERT_VALUES_EQUAL(40, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(40, state.TrimFreshBlobs(4));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDeadFreshBlobByteCount());
    }

    Y_UNIT_TEST(ShouldCountDeadFreshBlobBytesForOrdinaryTrim)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
        for (ui64 commitId = 1; commitId <= 6; ++commitId) {
            state.AddFreshBlob(commitId, 10, TInstant::Zero());
        }
        for (ui64 commitId: {1, 2, 4, 5}) {
            state.MarkFreshBlobDead(commitId);
        }
        UNIT_ASSERT_VALUES_EQUAL(40, state.GetDeadFreshBlobByteCount());

        // Blob 3 is live, so OrdinaryTrimToCommitId is 2.
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            state.GetOrdinaryTrimDeadFreshBlobByteCount(2));
        UNIT_ASSERT(state.CheckDeadFreshBlobByteCount({3, 6}));
        // A dead blob reported live and a live blob not reported are caught.
        UNIT_ASSERT(!state.CheckDeadFreshBlobByteCount({2, 3, 6}));
        UNIT_ASSERT(!state.CheckDeadFreshBlobByteCount({3}));

        // Blob 3 dies, and OrdinaryTrimToCommitId moves to 5.
        state.MarkFreshBlobDead(3);
        UNIT_ASSERT_VALUES_EQUAL(
            50,
            state.GetOrdinaryTrimDeadFreshBlobByteCount(5));
        UNIT_ASSERT(state.CheckDeadFreshBlobByteCount({6}));

        UNIT_ASSERT_VALUES_EQUAL(50, state.TrimFreshBlobs(5));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            state.GetOrdinaryTrimDeadFreshBlobByteCount(5));
        UNIT_ASSERT(state.CheckDeadFreshBlobByteCount({6}));
    }

    Y_UNIT_TEST(ShouldTrimFreshBlobsAboveOrdinaryTrimToCommitId)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
        for (ui64 commitId = 1; commitId <= 3; ++commitId) {
            state.AddFreshBlob(commitId, 10, TInstant::Zero());
        }
        state.MarkFreshBlobDead(1);
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            state.GetOrdinaryTrimDeadFreshBlobByteCount(1));

        // A trim may reach past OrdinaryTrimToCommitId, for example when a
        // flush requested it.
        state.MarkFreshBlobDead(2);
        UNIT_ASSERT_VALUES_EQUAL(20, state.TrimFreshBlobs(2));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            state.GetOrdinaryTrimDeadFreshBlobByteCount(2));
        UNIT_ASSERT(state.CheckDeadFreshBlobByteCount({3}));
    }

    Y_UNIT_TEST(ShouldTrackTrimRequests)
    {
        TPartitionTrimFreshLogState state;
        UNIT_ASSERT(!state.IsOrdinaryTrimRequested());
        state.SetOrdinaryTrimRequested(true);
        UNIT_ASSERT(state.IsOrdinaryTrimRequested());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
