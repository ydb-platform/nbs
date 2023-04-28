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
            "cp1",  // checkpointId
            1,  // commitId
            "",  // idempotenceId
            TInstant::Now(),  // dateCreated
            NProto::TPartitionStats()
        );
        TCheckpoint checkpoint2(
            "cp2",  // checkpointId
            5,  // commitId
            "",  // idempotenceId
            TInstant::Now(),  // dateCreated
            NProto::TPartitionStats()
        );

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
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
