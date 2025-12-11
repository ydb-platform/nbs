#include "checkpoint.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCheckpointTest)
{
    Y_UNIT_TEST(ShouldCorrectlyGetCommitId)
    {
        TCheckpointStore store;

        TCheckpoint checkpoint(
            "cp1",             // checkpointId
            1,                 // commitId
            "",                // idempotenceId
            TInstant::Now(),   // dateCreated
            NProto::TPartitionStats());
        TCheckpoint checkpoint2(
            "cp2",             // checkpointId
            5,                 // commitId
            "",                // idempotenceId
            TInstant::Now(),   // dateCreated
            NProto::TPartitionStats());

        store.Add(checkpoint);
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetCommitId("cp1", false));

        store.Add(checkpoint2);
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetCommitId("cp1", false));
        UNIT_ASSERT_VALUES_EQUAL(5, store.GetCommitId("cp2", false));

        THashMap<TString, ui64> checkpointId2CommitId;
        checkpointId2CommitId["cp3"] = 10;

        store.SetCheckpointMappings(checkpointId2CommitId);

        UNIT_ASSERT_VALUES_EQUAL(1, store.GetCommitId("cp1", false));
        UNIT_ASSERT_VALUES_EQUAL(5, store.GetCommitId("cp2", false));
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetCommitId("cp3", false));
        UNIT_ASSERT_VALUES_EQUAL(10, store.GetCommitId("cp3", true));
    }

    Y_UNIT_TEST(ShouldCorrectlyAddSameCheckpoint)
    {
        TCheckpointStore store;

        TCheckpoint checkpoint(
            "cp1",             // checkpointId
            1,                 // commitId
            "",                // idempotenceId
            TInstant::Now(),   // dateCreated
            NProto::TPartitionStats());
        TCheckpoint checkpoint2(
            "cp2",             // checkpointId
            5,                 // commitId
            "",                // idempotenceId
            TInstant::Now(),   // dateCreated
            NProto::TPartitionStats());

        // create checkpoint and try to add same checkpoint multiple times
        UNIT_ASSERT_VALUES_EQUAL(true, store.Add(checkpoint));
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetCommitId("cp1", false));
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetCommitId("cp1", true));
        UNIT_ASSERT_VALUES_EQUAL(false, store.Add(checkpoint));
        UNIT_ASSERT_VALUES_EQUAL(true, store.Delete("cp1"));
        UNIT_ASSERT_VALUES_EQUAL(false, store.Add(checkpoint));
        UNIT_ASSERT_VALUES_EQUAL(true, store.DeleteCheckpointMapping("cp1"));
        UNIT_ASSERT_VALUES_EQUAL(true, store.Add(checkpoint));

        // create checkpoint without data and try to add same checkpoint
        // multiple times
        UNIT_ASSERT_VALUES_EQUAL(true, store.AddCheckpointMapping(checkpoint2));
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetCommitId("cp2", false));
        UNIT_ASSERT_VALUES_EQUAL(5, store.GetCommitId("cp2", true));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.AddCheckpointMapping(checkpoint2));
        UNIT_ASSERT_VALUES_EQUAL(false, store.Delete("cp2"));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.AddCheckpointMapping(checkpoint2));
        UNIT_ASSERT_VALUES_EQUAL(true, store.DeleteCheckpointMapping("cp2"));
        UNIT_ASSERT_VALUES_EQUAL(true, store.AddCheckpointMapping(checkpoint2));
    }

    Y_UNIT_TEST(ShouldCorrectlyAddCheckpointWithoutData)
    {
        TCheckpointStore store;

        TCheckpoint checkpoint(
            "cp1",             // checkpointId
            1,                 // commitId
            "",                // idempotenceId
            TInstant::Now(),   // dateCreated
            NProto::TPartitionStats());
        TCheckpoint checkpoint2(
            "cp2",             // checkpointId
            5,                 // commitId
            "",                // idempotenceId
            TInstant::Now(),   // dateCreated
            NProto::TPartitionStats());

        // create checkpoint with data, delete data, delete checkpoint
        UNIT_ASSERT_VALUES_EQUAL(true, store.Add(checkpoint));
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetCommitId("cp1", false));
        UNIT_ASSERT_VALUES_EQUAL(true, store.Delete("cp1"));
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetCommitId("cp1", false));
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetCommitId("cp1", true));
        UNIT_ASSERT_VALUES_EQUAL(true, store.DeleteCheckpointMapping("cp1"));
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetCommitId("cp1", true));

        // create checkpoint without data, delete data (should fail), delete
        // checkpoint
        UNIT_ASSERT_VALUES_EQUAL(true, store.AddCheckpointMapping(checkpoint2));
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetCommitId("cp2", false));
        UNIT_ASSERT_VALUES_EQUAL(5, store.GetCommitId("cp2", true));
        UNIT_ASSERT_VALUES_EQUAL(false, store.Delete("cp2"));
        UNIT_ASSERT_VALUES_EQUAL(true, store.DeleteCheckpointMapping("cp2"));
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetCommitId("cp2", true));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
