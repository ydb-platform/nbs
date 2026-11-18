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

        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(), 10);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCount(
                0,
                TPartialBlobId(Max(), Max()),
                MakeCommitId(1, 15),
                100),
            5);

        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCount(
                MakeCommitId(1, 15),
                TPartialBlobId(Max(), Max()),
                MakeCommitId(1, 20),
                100),
            5);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCount(
                MakeCommitId(1, 11),
                TPartialBlobId(Max(), Max()),
                MakeCommitId(1, 15),
                100),
            4);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCount(
                MakeCommitId(1, 20),
                TPartialBlobId(Max(), Max()),
                MakeCommitId(1, 20),
                100),
            0);

        EnsureEqual(
            queue.GetItems(MakeCommitId(1, 20)),
            {8, 9, 7, 6, 4, 2, 10, 5, 1, 3});
        EnsureEqual(queue.GetItems(MakeCommitId(1, 15)), {8, 9, 7, 6, 4});
        EnsureEqual(queue.GetItems(MakeCommitId(1, 11)), {8});

        EnsureEqual(
            queue.GetItems(MakeCommitId(1, 15), 3 /* limit */),
            {8, 9, 7});
        EnsureEqual(queue.GetItems(), {8, 9, 7, 6, 4, 2, 10, 5, 1, 3});
        EnsureEqual(
            queue.GetItems(
                0,
                TPartialBlobId(Max(), Max()),
                MakeCommitId(1, 15),
                100),
            {8, 9, 7, 6, 4});

        EnsureEqual(
            queue.GetItems(
                MakeCommitId(1, 15),
                TPartialBlobId(Max(), Max()),
                MakeCommitId(1, 20),
                100),
            {2, 10, 5, 1, 3});
        EnsureEqual(
            queue.GetItems(
                MakeCommitId(1, 11),
                TPartialBlobId(Max(), Max()),
                MakeCommitId(1, 15),
                100),
            {9, 7, 6, 4});
        EnsureEqual(
            queue.GetItems(
                MakeCommitId(1, 20),
                TPartialBlobId(Max(), Max()),
                MakeCommitId(1, 20),
                100),
            {});
        EnsureEqual(
            queue.GetItems(
                MakeCommitId(1, 15),
                TPartialBlobId(Max(), Max()),
                MakeCommitId(1, 20),
                2 /* limit */),
            {2, 10});
    }

    Y_UNIT_TEST(ShouldGetItemsStartingFromBlobId)
    {
        TCleanupQueue queue(1024);

        const ui32 deletionStep = 10;
        for (ui32 step: Steps) {
            queue.Add(
                {TPartialBlobId(1, step, 3, 1024, 0, 0),
                 // All items share the same deletion commit id.
                 MakeCommitId(1, deletionStep),
                 {}});
        }

        const ui64 commitId = MakeCommitId(1, deletionStep);
        const TPartialBlobId blob1(1, 1, 3, 1024, 0, 0);
        const TPartialBlobId blob4(1, 4, 3, 1024, 0, 0);
        const TPartialBlobId blob7(1, 7, 3, 1024, 0, 0);
        const TPartialBlobId blob10(1, 10, 3, 1024, 0, 0);

        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCount(commitId, blob1, commitId, 100),
            9);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCount(commitId, blob4, commitId, 100),
            6);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCount(commitId, blob7, commitId, 100),
            3);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCount(commitId, blob10, commitId, 100),
            0);

        EnsureEqual(
            queue.GetItems(commitId, blob1, commitId, 100),
            {2, 3, 4, 5, 6, 7, 8, 9, 10});
        EnsureEqual(
            queue.GetItems(commitId, blob4, commitId, 100),
            {5, 6, 7, 8, 9, 10});
        EnsureEqual(queue.GetItems(commitId, blob7, commitId, 100), {8, 9, 10});
        EnsureEqual(queue.GetItems(commitId, blob10, commitId, 100), {});
    }

    Y_UNIT_TEST(ShouldLimitCount)
    {
        TCleanupQueue queue(1024);

        ui32 deletionStep = 10;
        for (ui32 step: Steps) {
            queue.Add(
                {TPartialBlobId(1, step, 3, 1024, 0, 0),
                 MakeCommitId(1, ++deletionStep),
                 {}});
        }

        const TPartialBlobId blobId(Max(), Max());
        const ui64 toCommitId = MakeCommitId(1, 20);

        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(0, blobId, toCommitId, 0), 0);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(0, blobId, toCommitId, 3), 3);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(0, blobId, toCommitId, 10), 10);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(0, blobId, toCommitId, 11), 10);

        // The limit only matters if there are enough items in the range.
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCount(0, blobId, MakeCommitId(1, 12), 5),
            2);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCount(MakeCommitId(1, 15), blobId, toCommitId, 3),
            3);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCount(MakeCommitId(1, 18), blobId, toCommitId, 3),
            2);
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

}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
