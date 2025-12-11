#include "garbage_queue.h"

#include <cloud/storage/core/libs/tablet/gc_logic.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

void EnsureEqual(
    const TVector<TPartialBlobId>& queue,
    const TVector<ui32>& steps)
{
    UNIT_ASSERT_VALUES_EQUAL(queue.size(), steps.size());
    for (size_t i = 0; i < queue.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(queue[i].Step(), steps[i]);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TGarbageQueueTest)
{
    static const TVector<ui32> Steps = {8, 9, 7, 6, 4, 2, 10, 5, 1, 3};

    Y_UNIT_TEST(ShouldKeepItemsSorted)
    {
        TGarbageQueue queue;
        for (ui32 step: Steps) {
            queue.AddNewBlob(TPartialBlobId(1, step, 3, 1024, 0, 0));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetNewBlobsCount(MakeCommitId(1, 10)),
            10);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetNewBlobsCount(MakeCommitId(1, 5)), 5);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetNewBlobsCount(MakeCommitId(1, 1)), 1);

        {
            auto newBlobs = queue.GetNewBlobs(MakeCommitId(1, 10));
            EnsureEqual(newBlobs, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        }

        {
            auto newBlobs = queue.GetNewBlobs(MakeCommitId(1, 5));
            EnsureEqual(newBlobs, {1, 2, 3, 4, 5});
        }

        {
            auto newBlobs = queue.GetNewBlobs(MakeCommitId(1, 1));
            EnsureEqual(newBlobs, {1});
        }
    }

    Y_UNIT_TEST(ShouldGCNewBlobIfItIsGarbageAndCreatedInPreviousGeneration)
    {
        TGarbageQueue queue;
        for (ui32 step: Steps) {
            queue.AddNewBlob(TPartialBlobId(1, step, 3, 1024, 0, 0));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetNewBlobsCount(MakeCommitId(1, 10)),
            10);

        for (ui32 i = 5; i <= Steps.size(); ++i) {
            queue.AddGarbageBlob(TPartialBlobId(1, i, 3, 1024, 0, 0));
        }

        ui64 collectCommitId = MakeCommitId(2, 10);
        auto newBlobs = queue.GetNewBlobs(collectCommitId);
        auto garbageBlobs = queue.GetGarbageBlobs(collectCommitId);

        RemoveDuplicates(newBlobs, garbageBlobs, collectCommitId);

        EnsureEqual(newBlobs, {1, 2, 3, 4});
        EnsureEqual(garbageBlobs, {5, 6, 7, 8, 9, 10});
    }

    Y_UNIT_TEST(ShouldDeduplicateBlobsFromCurrentGeneration)
    {
        TGarbageQueue queue;
        for (ui32 step: Steps) {
            queue.AddNewBlob(TPartialBlobId(1, step, 3, 1024, 0, 0));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetNewBlobsCount(MakeCommitId(1, 10)),
            10);

        for (ui32 step: Steps) {
            queue.AddGarbageBlob(TPartialBlobId(1, step, 3, 1024, 0, 0));
        }

        ui64 collectCommitId = MakeCommitId(1, 11);
        auto newBlobs = queue.GetNewBlobs(collectCommitId);
        auto garbageBlobs = queue.GetGarbageBlobs(collectCommitId);

        RemoveDuplicates(newBlobs, garbageBlobs, collectCommitId);

        EnsureEqual(newBlobs, {});
        EnsureEqual(garbageBlobs, {});
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
