#include "garbage_queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

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
        TGarbageQueue queue(TDefaultAllocator::Instance());
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

    Y_UNIT_TEST(ShouldKeepTrackOfBarriers)
    {
        TGarbageQueue queue(TDefaultAllocator::Instance());
        for (ui32 step: Steps) {
            queue.AddNewBlob(TPartialBlobId(1, step, 3, 1024, 0, 0));
        }

        queue.AcquireCollectBarrier(MakeCommitId(1, 5));

        for (ui32 i = 5; i <= Steps.size(); ++i) {
            queue.AddGarbageBlob(TPartialBlobId(1, i, 3, 1024, 0, 0));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetCollectCommitId(),
            MakeCommitId(1, 4));
    }
}

}   // namespace NCloud::NFileStore::NStorage
