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

TPartialBlobId BlobId(ui64 commitId)
{
    return TPartialBlobId(commitId, 0);
}

TVector<ui64> CommitIds(const TVector<TPartialBlobId>& blobIds)
{
    TVector<ui64> result;
    for (const auto& blobId: blobIds) {
        result.push_back(blobId.CommitId());
    }
    return result;
}

// Untrimmed blobs at commits [first, last], 10 bytes each.
TPartitionFreshBlobState MakeFreshBlobState(ui64 first, ui64 last)
{
    TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
    for (ui64 commitId = first; commitId <= last; ++commitId) {
        state.AddFreshBlob(commitId, BlobId(commitId), 10, TInstant::Zero());
    }
    return state;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionFreshBlocksStateTest)
{
    Y_UNIT_TEST(ShouldCalculateFreshBlobByteCount)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);

        state.AddFreshBlob(1, BlobId(1), 10, TInstant::Zero());
        state.AddFreshBlob(3, BlobId(3), 30, TInstant::Zero());
        state.AddFreshBlob(2, BlobId(2), 20, TInstant::Zero());
        state.AddFreshBlob(5, BlobId(5), 50, TInstant::Zero());
        state.AddFreshBlob(4, BlobId(4), 40, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(150, state.GetUntrimmedFreshBlobByteCount());

        UNIT_ASSERT_VALUES_EQUAL(60, state.TrimFreshBlobs(3));

        UNIT_ASSERT_VALUES_EQUAL(90, state.GetUntrimmedFreshBlobByteCount());

        state.AddFreshBlob(7, BlobId(7), 70, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(160, state.GetUntrimmedFreshBlobByteCount());

        UNIT_ASSERT_VALUES_EQUAL(160, state.TrimFreshBlobs(10));

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUntrimmedFreshBlobByteCount());
    }

    Y_UNIT_TEST(ShouldAllowIncrementingFlushCountersToMaxValue)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);

        state.AddFreshBlob(1, BlobId(1), Max<ui32>(), TInstant::Zero());
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

        state.AddFreshBlob(1, BlobId(1), 10, t0);
        state.AddFreshBlob(2, BlobId(2), 20, t0 + TDuration::Seconds(1));
        state.AddFreshBlob(3, BlobId(3), 30, t0 + TDuration::Seconds(2));

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
        state.AddFreshBlob(4, BlobId(4), 40, t0 + TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(t0 + TDuration::Seconds(10),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));
    }

    Y_UNIT_TEST(ShouldFindFirstUnflushedBlobAfterTrimBoundary)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Max(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));

        state.AddFreshBlob(3, BlobId(3), 30, TInstant::Seconds(110));
        state.AddFreshBlob(1, BlobId(1), 10, TInstant::Seconds(100));
        state.AddFreshBlob(5, BlobId(5), 50, TInstant::Seconds(120));
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
        state.AddFreshBlob(1, BlobId(1), 10, TInstant::Zero());
        state.AddFreshBlob(2, BlobId(2), 20, TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        UNIT_ASSERT_VALUES_EQUAL(10, state.TrimFreshBlobs(1));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(100),
                                 state.GetFirstUnflushedFreshBlobTimestamp(1));

        state.AddFreshBlob(3, BlobId(3), 30, TInstant::Zero());
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

        state.AddFreshBlob(2, BlobId(2), 20, now);
        state.AddFreshBlob(1, BlobId(1), 10, TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(),
                                 state.GetFirstUnflushedFreshBlobTimestamp(0));
    }

    Y_UNIT_TEST(ShouldTrackTimestampByCommitIdRegardlessOfInsertionOrder)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
        state.AddFreshBlob(3, BlobId(3), 30, TInstant::Seconds(110));
        state.AddFreshBlob(1, BlobId(1), 10, TInstant::Seconds(120));
        state.AddFreshBlob(2, BlobId(2), 20, TInstant::Seconds(100));

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
        state.AddFreshBlob(1, BlobId(1), 10, TInstant::Zero());
        state.AddFreshBlob(2, BlobId(2), 20, TInstant::Zero());
        state.AddFreshBlob(3, BlobId(3), 30, TInstant::Zero());

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
        state.AddFreshBlob(1, BlobId(1), 10, TInstant::Zero());
        // A zero blob is smaller than a block, but counts one block.
        state.AddFreshBlob(2, BlobId(2), 2, TInstant::Zero());
        state.AddFreshBlob(3, BlobId(3), 30, TInstant::Zero());
        state.AddFreshBlob(4, BlobId(4), 40, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDeadFreshBlobByteCount());

        state.MarkFreshBlobDead(2);
        state.MarkFreshBlobDead(3);
        UNIT_ASSERT_VALUES_EQUAL(34, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(34, state.GetKilledFreshBlobByteCount());

        state.MarkFreshBlobDead(1);
        UNIT_ASSERT_VALUES_EQUAL(44, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(44, state.GetKilledFreshBlobByteCount());

        // Trims remove dead bytes, and the killed bytes stay.
        UNIT_ASSERT_VALUES_EQUAL(12, state.TrimFreshBlobs(2));
        UNIT_ASSERT_VALUES_EQUAL(30, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(30, state.TrimCollectedFreshBlobs({3}));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(44, state.GetKilledFreshBlobByteCount());

        state.MarkFreshBlobDead(4);
        UNIT_ASSERT_VALUES_EQUAL(40, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(84, state.GetKilledFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(40, state.TrimFreshBlobs(4));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDeadFreshBlobByteCount());
    }

    Y_UNIT_TEST(ShouldCountDeadFreshBlobBytesForOrdinaryTrim)
    {
        auto state = MakeFreshBlobState(1, 6);
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

        // A selective trim collects dead blobs above OrdinaryTrimToCommitId.
        UNIT_ASSERT_VALUES_EQUAL(20, state.TrimCollectedFreshBlobs({4, 5}));
        UNIT_ASSERT_VALUES_EQUAL(20, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            state.GetOrdinaryTrimDeadFreshBlobByteCount(2));

        // Blob 3 dies, and OrdinaryTrimToCommitId moves to 5.
        state.MarkFreshBlobDead(3);
        UNIT_ASSERT_VALUES_EQUAL(
            30,
            state.GetOrdinaryTrimDeadFreshBlobByteCount(5));
        UNIT_ASSERT(state.CheckDeadFreshBlobByteCount({6}));

        // OrdinaryTrimToCommitId passes blob 6 while a selective trim that
        // collects it is in flight.
        state.AddFreshBlob(7, BlobId(7), 10, TInstant::Zero());
        state.MarkFreshBlobDead(6);
        UNIT_ASSERT_VALUES_EQUAL(
            40,
            state.GetOrdinaryTrimDeadFreshBlobByteCount(6));
        UNIT_ASSERT_VALUES_EQUAL(10, state.TrimCollectedFreshBlobs({6}));
        UNIT_ASSERT_VALUES_EQUAL(30, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(
            30,
            state.GetOrdinaryTrimDeadFreshBlobByteCount(6));
        UNIT_ASSERT(state.CheckDeadFreshBlobByteCount({7}));

        UNIT_ASSERT_VALUES_EQUAL(30, state.TrimFreshBlobs(6));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDeadFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            state.GetOrdinaryTrimDeadFreshBlobByteCount(6));
        UNIT_ASSERT(state.CheckDeadFreshBlobByteCount({7}));
    }

    Y_UNIT_TEST(ShouldTrimFreshBlobsAboveOrdinaryTrimToCommitId)
    {
        auto state = MakeFreshBlobState(1, 3);
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

        // After a scan that found nothing, the next one waits until another
        // threshold of bytes is killed.
        UNIT_ASSERT(state.IsSelectiveScanAllowed(0));
        state.DeferSelectiveScan(100, 30);
        UNIT_ASSERT(!state.IsSelectiveScanAllowed(129));
        UNIT_ASSERT(state.IsSelectiveScanAllowed(130));
    }

    Y_UNIT_TEST(ShouldBuildSelectiveTrimPlan)
    {
        // OrdinaryTrimToCommitId is 10 and SafeCommitId is 19, with a write at
        // 20 in flight and no earlier soft barrier. L is live and D is dead.
        //
        // commit:  11  12  13  14  15  16  17  18  19
        // state:    L   D   D   L   D   D   L   D   L
        auto state = MakeFreshBlobState(11, 20);
        const TVector<ui64> live = {11, 14, 17, 19};

        {
            auto plan = state.BuildSelectiveTrimPlan(10, 19, live, 0, 0, 10);
            UNIT_ASSERT(plan);
            UNIT_ASSERT_VALUES_EQUAL(18, plan->SoftBarrierCommitId);
            UNIT_ASSERT_VALUES_EQUAL(
                TVector<ui64>({11, 14, 17}),
                CommitIds(plan->Keep));
            UNIT_ASSERT(!plan->DoNotKeep);
            UNIT_ASSERT_VALUES_EQUAL(
                TVector<ui64>({12, 13, 15, 16, 18}),
                plan->CollectingCommitIds);
        }

        // The flag budget stops the barrier on the last dead blob that fits.
        {
            auto plan = state.BuildSelectiveTrimPlan(10, 19, live, 0, 0, 2);
            UNIT_ASSERT(plan);
            UNIT_ASSERT_VALUES_EQUAL(16, plan->SoftBarrierCommitId);
            UNIT_ASSERT_VALUES_EQUAL(
                TVector<ui64>({11, 14}),
                CommitIds(plan->Keep));
            UNIT_ASSERT_VALUES_EQUAL(
                TVector<ui64>({12, 13, 15, 16}),
                plan->CollectingCommitIds);
        }

        // A budget that no dead blob fits in moves nothing.
        UNIT_ASSERT(!state.BuildSelectiveTrimPlan(10, 19, live, 0, 0, 0));
    }

    Y_UNIT_TEST(ShouldNotBuildSelectiveTrimPlanWithoutDeadBlobAboveFloor)
    {
        auto state = MakeFreshBlobState(11, 15);

        // Every blob is live.
        UNIT_ASSERT(!state.BuildSelectiveTrimPlan(
            10,
            15,
            {11, 12, 13, 14, 15},
            0,
            0,
            10));

        // Dead blobs above SafeCommitId are not collected.
        UNIT_ASSERT(!state.BuildSelectiveTrimPlan(10, 12, {11, 12}, 0, 0, 10));

        // Nothing is untrimmed above OrdinaryTrimToCommitId.
        UNIT_ASSERT(!state.BuildSelectiveTrimPlan(15, 15, {}, 0, 0, 10));
    }

    Y_UNIT_TEST(ShouldResendFlagsAfterFailedSelectiveTrim)
    {
        // A soft command at 16 failed, so no blob at or below it is known to
        // carry a Keep flag.
        //
        // commit:  11  12  13  14  15  16  17  18  19  20  21  22
        // state:    L   D   D   D   D   D   L   D   L   D   D   L
        auto state = MakeFreshBlobState(11, 22);
        const TVector<ui64> live = {11, 17, 19, 22};

        auto plan = state.BuildSelectiveTrimPlan(10, 22, live, 0, 16, 10);
        UNIT_ASSERT(plan);
        UNIT_ASSERT_VALUES_EQUAL(21, plan->SoftBarrierCommitId);
        // 11 is mandatory, 17 and 19 are below the new barrier.
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({11, 17, 19}),
            CommitIds(plan->Keep));
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({12, 13, 14, 15, 16}),
            CommitIds(plan->DoNotKeep));
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({18, 20, 21, 12, 13, 14, 15, 16}),
            plan->CollectingCommitIds);

        // Mandatory Keep flags are never dropped by the budget, and the
        // barrier does not drop below the failed one.
        plan = state.BuildSelectiveTrimPlan(10, 22, live, 0, 16, 1);
        UNIT_ASSERT(plan);
        UNIT_ASSERT_VALUES_EQUAL(16, plan->SoftBarrierCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>({11}), CommitIds(plan->Keep));
        UNIT_ASSERT(!plan->DoNotKeep);
        UNIT_ASSERT(!plan->CollectingCommitIds);

        // Blobs kept by a successful command need no flag.
        plan = state.BuildSelectiveTrimPlan(10, 22, live, 16, 16, 10);
        UNIT_ASSERT(plan);
        UNIT_ASSERT_VALUES_EQUAL(21, plan->SoftBarrierCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({17, 19}),
            CommitIds(plan->Keep));
    }

    Y_UNIT_TEST(ShouldReleaseDeadKeptBlobsWithinFlagBudget)
    {
        // Every blob at or below 15 was kept; 12, 13 and 14 died since.
        //
        // commit:  11  12  13  14  15  16  17
        // state:    L   D   D   D   L   L   L
        auto state = MakeFreshBlobState(11, 17);
        const TVector<ui64> live = {11, 15, 16, 17};

        auto plan = state.BuildSelectiveTrimPlan(10, 17, live, 15, 15, 10);
        UNIT_ASSERT(plan);
        // Only flags are sent; the barrier stays where it is.
        UNIT_ASSERT_VALUES_EQUAL(15, plan->SoftBarrierCommitId);
        UNIT_ASSERT(!plan->Keep);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({12, 13, 14}),
            CommitIds(plan->DoNotKeep));
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({12, 13, 14}),
            plan->CollectingCommitIds);

        // DoNotKeep gets the budget that Keep left; the rest waits.
        plan = state.BuildSelectiveTrimPlan(10, 17, live, 15, 15, 2);
        UNIT_ASSERT(plan);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({12, 13}),
            CommitIds(plan->DoNotKeep));
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({12, 13}),
            plan->CollectingCommitIds);
    }

    Y_UNIT_TEST(ShouldPlaceSelectiveTrimFloorAtOrdinaryTrimToCommitId)
    {
        // OrdinaryTrimToCommitId is 14 and passed the soft barrier at 12, so
        // nothing below it needs a flag.
        auto state = MakeFreshBlobState(15, 18);
        const TVector<ui64> live = {15, 18};

        auto plan = state.BuildSelectiveTrimPlan(14, 18, live, 12, 12, 10);
        UNIT_ASSERT(plan);
        UNIT_ASSERT_VALUES_EQUAL(17, plan->SoftBarrierCommitId);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>({15}), CommitIds(plan->Keep));
        UNIT_ASSERT(!plan->DoNotKeep);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({16, 17}),
            plan->CollectingCommitIds);
    }

#if defined(NDEBUG)
    // Debug builds abort on a referenced commit without an untrimmed entry.
    Y_UNIT_TEST(ShouldNotPassReferencedCommitWithoutUntrimmedBlob)
    {
        TPartitionFreshBlobState state(/*tabletId=*/0, BlockSize);
        for (ui64 commitId: {11, 12, 14, 15}) {
            state.AddFreshBlob(
                commitId,
                BlobId(commitId),
                10,
                TInstant::Zero());
        }

        // 13 is referenced but has no entry: the barrier stops below it.
        auto plan = state.BuildSelectiveTrimPlan(10, 15, {11, 13}, 0, 0, 10);
        UNIT_ASSERT(plan);
        UNIT_ASSERT_VALUES_EQUAL(12, plan->SoftBarrierCommitId);

        // Below the floor, the list of blobs to keep would be incomplete.
        UNIT_ASSERT(!state.BuildSelectiveTrimPlan(10, 15, {11, 13}, 0, 14, 10));
    }
#endif

    Y_UNIT_TEST(ShouldTrimCollectedFreshBlobs)
    {
        auto state = MakeFreshBlobState(1, 4);
        UNIT_ASSERT_VALUES_EQUAL(10, state.FlushFreshBlob(2, 0));

        // The flush already removed the unflushed entry of blob 2.
        UNIT_ASSERT_VALUES_EQUAL(10, state.TrimCollectedFreshBlobs({2, 3}));
        UNIT_ASSERT_VALUES_EQUAL(20, state.GetUntrimmedFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(2, state.GetUnflushedFreshBlobCount());
        UNIT_ASSERT_VALUES_EQUAL(20, state.GetUnflushedFreshBlobByteCount());

        // A flush that captured blob 3 accepts that a selective trim at or
        // above it removed it.
        UNIT_ASSERT_VALUES_EQUAL(0, state.FlushFreshBlob(3, 3));

        UNIT_ASSERT_VALUES_EQUAL(20, state.TrimFreshBlobs(4));
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUntrimmedFreshBlobByteCount());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetUnflushedFreshBlobCount());
    }

    Y_UNIT_TEST(ShouldTrackSelectiveTrimState)
    {
        TPartitionTrimFreshLogState state;
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetKeptToCommitId());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetSoftBarrierCommitId());

        state.StartSelectiveTrim(10, {5, 7});
        UNIT_ASSERT(state.IsSelectiveTrimInFlight());
        // The attempted barrier is the floor even before it succeeds.
        UNIT_ASSERT_VALUES_EQUAL(10, state.GetSoftBarrierCommitId());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetKeptToCommitId());

        state.FailSelectiveTrim();
        UNIT_ASSERT(!state.IsSelectiveTrimInFlight());
        UNIT_ASSERT_VALUES_EQUAL(10, state.GetSoftBarrierCommitId());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetKeptToCommitId());

        state.StartSelectiveTrim(12, {5, 7, 11});
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>({5, 7, 11}),
            state.CompleteSelectiveTrim());
        UNIT_ASSERT(!state.IsSelectiveTrimInFlight());
        UNIT_ASSERT_VALUES_EQUAL(12, state.GetKeptToCommitId());
        UNIT_ASSERT_VALUES_EQUAL(12, state.GetSoftBarrierCommitId());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
