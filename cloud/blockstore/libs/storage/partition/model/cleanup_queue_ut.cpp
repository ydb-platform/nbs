#include "cleanup_queue.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

void EnsureEqual(
    const TVector<TCleanupQueueItem>& queue,
    const TVector<ui32>& steps)
{
    UNIT_ASSERT_VALUES_EQUAL(queue.size(), steps.size());
    for (size_t i = 0; i < queue.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(queue[i].BlobId.Step(), steps[i]);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCleanupQueueTest)
{
    static const TVector<ui32> Steps = {8, 9, 7, 6, 4, 2, 10, 5, 1, 3};

    Y_UNIT_TEST(ShouldKeepItemsSorted)
    {
        TCleanupQueue queue(1024);

        ui32 deletionStep = 10;
        for (ui32 step: Steps) {
            queue.Add(
                {TPartialBlobId(1, step, 3, 1024, 0, 0),
                 MakeCommitId(1, ++deletionStep),
                 {}});
        }

        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(MakeCommitId(1, 20)), 10);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(MakeCommitId(1, 15)), 5);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(MakeCommitId(1, 11)), 1);

        EnsureEqual(queue.GetItems(MakeCommitId(1, 20)), {8, 9, 7, 6, 4, 2, 10, 5, 1, 3});
        EnsureEqual(queue.GetItems(MakeCommitId(1, 15)), {8, 9, 7, 6, 4});
        EnsureEqual(queue.GetItems(MakeCommitId(1, 11)), {8});
    }

    Y_UNIT_TEST(ShouldTrimQueue)
    {
        TCleanupQueue queue(1024);

        ui32 deletionStep = 10;
        for (ui32 step: Steps) {
            queue.Add({
                TPartialBlobId(1, step, 3, 1024, 0, 0),
                MakeCommitId(1, ++deletionStep),
                {}
            });
        }

        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(MakeCommitId(1, 20)), 10);

        for (const auto& item: queue.GetItems(MakeCommitId(1, 15))) {
            UNIT_ASSERT(queue.Remove(item));
        }

        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(MakeCommitId(1, 20)), 5);

        EnsureEqual(queue.GetItems(MakeCommitId(1, 20)), {2, 10, 5, 1, 3});
    }

    Y_UNIT_TEST(ShouldTrackBlobPresence)
    {
        TCleanupQueue queue(1024);

        TPartialBlobId blobId(1, 1, 3, 1024, 0, 0);

        queue.Add({
            TPartialBlobId(1, 1, 3, 1024, 0, 0),
            MakeCommitId(1, 1),
            {}
        });

        UNIT_ASSERT(queue.HasBlob(blobId));

        TPartialBlobId anotherBlobId(1, 2, 3, 1024, 0, 0);
        UNIT_ASSERT(!queue.HasBlob(anotherBlobId));

        queue.Add({anotherBlobId, MakeCommitId(1, 2), {}});

        UNIT_ASSERT(queue.HasBlob(anotherBlobId));
    }

    // TODO:_ check this test
    Y_UNIT_TEST(ShouldSkipItemsNeededByCheckpoint)
    {
        // TODO:_ testcase with invalid commit ids?
        TCleanupQueue queue(1024);

        const ui64 minCheckpointCommitId = MakeCommitId(1, 40);
        const ui64 maxCheckpointCommitId = MakeCommitId(1, 60);
        const ui64 maxCommitId = MakeCommitId(1, 100);
        const ui64 deletionCommitId = MakeCommitId(1, 80);

        NProto::TBlobMeta mergedBelowMax;
        {
            auto& merged = *mergedBelowMax.MutableMergedBlocks();
            merged.SetStart(0);
            merged.SetEnd(3);
        }

        NProto::TBlobMeta mergedAboveMax;
        {
            auto& merged = *mergedAboveMax.MutableMergedBlocks();
            merged.SetStart(10);
            merged.SetEnd(13);
        }

        // deletionCommitId < min — always eligible
        queue.Add({
            TPartialBlobId(1, 10, 3, 1024, 0, 0),
            MakeCommitId(1, 35),
            mergedBelowMax});

        // blobCommitId > max — garbage
        queue.Add({
            TPartialBlobId(1, 70, 3, 1024, 0, 0),
            deletionCommitId,
            mergedAboveMax});

        // blobCommitId <= max — needed by checkpoint
        queue.Add({
            TPartialBlobId(1, 50, 3, 1024, 0, 0),
            deletionCommitId,
            mergedBelowMax});

        EnsureEqual(
            queue.GetItems(
                maxCommitId,
                100,
                minCheckpointCommitId,
                maxCheckpointCommitId),
            {10, 70});

        // Without checkpoint bounds, all items are returned (sorted by
        // deletionCommitId, then blobId).
        EnsureEqual(queue.GetItems(maxCommitId), {10, 50, 70});
    }

}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
