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
                 MakeCommitId(1, ++deletionStep)});
        }

        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(MakeCommitId(1, 20)), 10);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(MakeCommitId(1, 15)), 5);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(MakeCommitId(1, 11)), 1);

        EnsureEqual(
            queue.GetItems(MakeCommitId(1, 20)),
            {8, 9, 7, 6, 4, 2, 10, 5, 1, 3});
        EnsureEqual(queue.GetItems(MakeCommitId(1, 15)), {8, 9, 7, 6, 4});
        EnsureEqual(queue.GetItems(MakeCommitId(1, 11)), {8});
    }

    Y_UNIT_TEST(ShouldTrimQueue)
    {
        TCleanupQueue queue(1024);

        ui32 deletionStep = 10;
        for (ui32 step: Steps) {
            queue.Add(
                {TPartialBlobId(1, step, 3, 1024, 0, 0),
                 MakeCommitId(1, ++deletionStep)});
        }

        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(MakeCommitId(1, 20)), 10);

        for (const auto& item: queue.GetItems(MakeCommitId(1, 15))) {
            UNIT_ASSERT(queue.Remove(item));
        }

        UNIT_ASSERT_VALUES_EQUAL(queue.GetCount(MakeCommitId(1, 20)), 5);

        EnsureEqual(queue.GetItems(MakeCommitId(1, 20)), {2, 10, 5, 1, 3});
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
