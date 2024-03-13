#include "rebase_logic.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRebaseLogicTest)
{
    Y_UNIT_TEST(TestRebaseBlocks)
    {
        const ui64 defaultNodeId = 111;
        const auto b = [=] (ui32 blockIndex, ui64 minCommitId, ui64 maxCommitId)
        {
            return TBlock(defaultNodeId, blockIndex, minCommitId, maxCommitId);
        };
        TVector<TBlock> blocks = {
            b(10, 5, 15),
            b(11, 7, 20),
            b(11, 20, InvalidCommitId),
            b(12, 20, 31),
            b(13, 31, InvalidCommitId),
            b(14, 32, InvalidCommitId),
        };

        const ui64 c1 = 3;
        const ui64 c2 = 6;
        const ui64 c3 = 10;

        const auto findCheckpoint = [&] (ui64 nodeId, ui64 commitId) {
            UNIT_ASSERT_VALUES_EQUAL(defaultNodeId, nodeId);

            if (commitId < c1) {
                return c1;
            }

            if (commitId < c2) {
                return c2;
            }

            if (commitId < c3) {
                return c3;
            }

            return InvalidCommitId;
        };

        const THashSet<ui32> freshBlocks = {11, 14};
        const auto findBlock = [&] (ui64 nodeId, ui32 blockIndex) {
            UNIT_ASSERT_VALUES_EQUAL(defaultNodeId, nodeId);

            return freshBlocks.contains(blockIndex);
        };

        const auto rebaseResult = RebaseBlocks(
            blocks,
            40,
            findCheckpoint,
            findBlock);

        UNIT_ASSERT_VALUES_EQUAL(5, rebaseResult.LiveBlocks);
        UNIT_ASSERT_VALUES_EQUAL(1, rebaseResult.GarbageBlocks);
        UNIT_ASSERT_VALUES_EQUAL(2, rebaseResult.CheckpointBlocks);
        UNIT_ASSERT_VALUES_EQUAL(2, rebaseResult.UsedCheckpoints.size());
        UNIT_ASSERT(rebaseResult.UsedCheckpoints.contains(c2));
        UNIT_ASSERT(rebaseResult.UsedCheckpoints.contains(c3));
    }
}

}   // namespace NCloud::NFileStore::NStorage
