#include "commit_ids_state.h"

#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

Y_UNIT_TEST_SUITE(TCommitIdsStateTest)
{
    Y_UNIT_TEST(ShouldReturnInvalidCommitIdWhenItOverflows)
    {
        TCommitIdsState state(0, Max());

        UNIT_ASSERT(state.GenerateCommitId() == InvalidCommitId);
    }
}
}   // namespace NCloud::NBlockStore::NStorage::NPartition
