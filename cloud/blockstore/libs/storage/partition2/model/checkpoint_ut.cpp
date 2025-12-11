#include "checkpoint.h"

#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>   // XXX

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TPartialBlobId blobId1 = {1, 0};
static const TPartialBlobId blobId2 = {2, 0};
static const TPartialBlobId blobId3 = {3, 0};
static const TPartialBlobId blobId4 = {4, 0};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCheckpointStorageTest)
{
    Y_UNIT_TEST(ShouldAddFindRemove)
    {
        TCheckpointStorage checkpoints;

        NProto::TCheckpointMeta meta;
        meta.SetCommitId(1);
        meta.SetCheckpointId("c1");

        checkpoints.Add(meta);

        UNIT_ASSERT(!checkpoints.Find("c2"));

        auto found = checkpoints.Find("c1");
        UNIT_ASSERT(found);
        UNIT_ASSERT_VALUES_EQUAL(meta.DebugString(), found->DebugString());

        UNIT_ASSERT_VALUES_EQUAL(1, checkpoints.GetCommitId("c1"));

        UNIT_ASSERT(!checkpoints.Remove("c2"));
        UNIT_ASSERT(checkpoints.Remove("c1"));
        UNIT_ASSERT(!checkpoints.Find("c1"));
    }

    Y_UNIT_TEST(ShouldRebaseCommits)
    {
        TCheckpointStorage checkpoints;

        NProto::TCheckpointMeta meta;
        meta.SetCommitId(1);
        meta.SetCheckpointId("c1");
        checkpoints.Add(meta);

        meta.SetCommitId(5);
        meta.SetCheckpointId("c2");
        checkpoints.Add(meta);

        meta.SetCommitId(8);
        meta.SetCheckpointId("c3");
        checkpoints.Add(meta);

        UNIT_ASSERT_VALUES_EQUAL(1, checkpoints.RebaseCommitId(1));
        UNIT_ASSERT_VALUES_EQUAL(5, checkpoints.RebaseCommitId(2));
        UNIT_ASSERT_VALUES_EQUAL(5, checkpoints.RebaseCommitId(5));
        UNIT_ASSERT_VALUES_EQUAL(8, checkpoints.RebaseCommitId(6));
        UNIT_ASSERT_VALUES_EQUAL(8, checkpoints.RebaseCommitId(8));
        UNIT_ASSERT_VALUES_EQUAL(
            InvalidCommitId,
            checkpoints.RebaseCommitId(9));
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCheckpointsToDeleteTest)
{
    Y_UNIT_TEST(ShouldPutExtractDelete)
    {
        TCheckpointsToDelete checkpoints;

        UNIT_ASSERT(checkpoints.IsEmpty());

        checkpoints.Put(1, TCheckpointInfo({blobId1, blobId2}));

        checkpoints.Put(5, TCheckpointInfo({blobId3, blobId4}));

        UNIT_ASSERT(!checkpoints.IsEmpty());

        ui64 commitId = 0;

        ASSERT_VECTORS_EQUAL(
            checkpoints.ExtractBlobsToCleanup(1, &commitId),
            TVector<TPartialBlobId>({blobId1}));
        UNIT_ASSERT_VALUES_EQUAL(0, commitId);

        ASSERT_VECTORS_EQUAL(
            checkpoints.ExtractBlobsToCleanup(1, &commitId),
            TVector<TPartialBlobId>({blobId2}));
        UNIT_ASSERT_VALUES_EQUAL(1, commitId);

        ASSERT_VECTORS_EQUAL(
            checkpoints.DeleteNextCheckpoint(),
            TVector<TPartialBlobId>({blobId1, blobId2}));

        commitId = 0;

        ASSERT_VECTORS_EQUAL(
            checkpoints.ExtractBlobsToCleanup(1, &commitId),
            TVector<TPartialBlobId>({blobId3}));
        UNIT_ASSERT_VALUES_EQUAL(0, commitId);

        ASSERT_VECTORS_EQUAL(
            checkpoints.ExtractBlobsToCleanup(1, &commitId),
            TVector<TPartialBlobId>({blobId4}));
        UNIT_ASSERT_VALUES_EQUAL(5, commitId);

        ASSERT_VECTORS_EQUAL(
            checkpoints.DeleteNextCheckpoint(),
            TVector<TPartialBlobId>({blobId3, blobId4}));

        UNIT_ASSERT(checkpoints.IsEmpty());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
