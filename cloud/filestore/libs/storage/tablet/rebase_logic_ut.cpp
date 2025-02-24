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
            b(10, 5, 9),
            b(11, 7, 20),
            b(11, 20, InvalidCommitId),
            b(12, 20, 31),
            b(13, 31, InvalidCommitId),
            b(14, 32, InvalidCommitId),
        };

        const ui64 c1 = 3;
        const ui64 c2 = 6;
        const ui64 c3 = 10;
        const ui64 lastCommitId = 40;

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
            lastCommitId,
            findCheckpoint,
            findBlock);

        UNIT_ASSERT_VALUES_EQUAL(5, rebaseResult.LiveBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(1, rebaseResult.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(2, rebaseResult.CheckpointBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(2, rebaseResult.UsedCheckpoints.size());
        UNIT_ASSERT(rebaseResult.UsedCheckpoints.contains(c2));
        UNIT_ASSERT(rebaseResult.UsedCheckpoints.contains(c3));

        UNIT_ASSERT_VALUES_EQUAL(6, blocks.size());

        UNIT_ASSERT_VALUES_EQUAL(defaultNodeId, blocks[0].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(10, blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(c2, blocks[0].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(c3, blocks[0].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(defaultNodeId, blocks[1].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(11, blocks[1].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(7, blocks[1].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(20, blocks[1].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(defaultNodeId, blocks[2].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(11, blocks[2].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(20, blocks[2].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[2].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(defaultNodeId, blocks[3].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(12, blocks[3].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(0, blocks[3].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(0, blocks[3].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(defaultNodeId, blocks[4].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(13, blocks[4].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(lastCommitId, blocks[4].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[4].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(defaultNodeId, blocks[5].NodeId);
        UNIT_ASSERT_VALUES_EQUAL(14, blocks[5].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(32, blocks[5].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[5].MaxCommitId);
    }
}

}   // namespace NCloud::NFileStore::NStorage
