#include "rebase_logic.h"

#include "block.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TPivotalCommitStorageImpl final: IPivotalCommitStorage
{
    TVector<ui64> CommitIds;

    TPivotalCommitStorageImpl(TVector<ui64> commitIds)
        : CommitIds(std::move(commitIds))
    {
        UNIT_ASSERT(IsSorted(CommitIds.begin(), CommitIds.end()));
    }

    ui64 RebaseCommitId(ui64 commitId) const override
    {
        auto it = LowerBound(CommitIds.begin(), CommitIds.end(), commitId);

        return it == CommitIds.end() ? InvalidCommitId : *it;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRebaseLogicTest)
{
    Y_UNIT_TEST(ShouldCorrectlyRebaseCommits)
    {
        const TPivotalCommitStorageImpl pivotalCommitStorage({2, 5, 8});

        auto isFrozenBlock = [](ui32 blockIndex)
        {
            return blockIndex == 10 || blockIndex == 20 || blockIndex == 50;
        };

        const ui64 lastCommitId = 15;

        TVector<TBlock> blocks = {
            {1, 1, InvalidCommitId, false},
            {2, 10, 11, false},
            {3, 10, InvalidCommitId, false},
            {4, 1, 6, false},
            {5, 6, 7, false},
            {10, 1, 3, false},
            {20, 6, InvalidCommitId, false},
        };
        TSet<ui64> pivotalCommits;
        auto blockCounts = RebaseBlocks(
            pivotalCommitStorage,
            isFrozenBlock,
            lastCommitId,
            blocks,
            pivotalCommits);

        UNIT_ASSERT_VALUES_EQUAL(5, blockCounts.LiveBlocks);
        UNIT_ASSERT_VALUES_EQUAL(2, blockCounts.GarbageBlocks);
        UNIT_ASSERT_VALUES_EQUAL(2, blockCounts.CheckpointBlocks);

        UNIT_ASSERT_VALUES_EQUAL(7, blocks.size());

        UNIT_ASSERT_VALUES_EQUAL(1, blocks[0].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(2, blocks[0].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[0].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(2, blocks[1].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(0, blocks[1].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(0, blocks[1].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(3, blocks[2].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(15, blocks[2].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[2].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(4, blocks[3].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(2, blocks[3].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(8, blocks[3].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(5, blocks[4].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(0, blocks[4].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(0, blocks[4].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(10, blocks[5].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(1, blocks[5].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(3, blocks[5].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(20, blocks[6].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(6, blocks[6].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, blocks[6].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(1, pivotalCommits.size());
        auto pivotalCommit = pivotalCommits.begin();
        UNIT_ASSERT_VALUES_EQUAL(2, *pivotalCommit);

        blocks.push_back({50, 6, 9, false});

        pivotalCommits.clear();
        blockCounts = RebaseBlocks(
            pivotalCommitStorage,
            isFrozenBlock,
            lastCommitId,
            blocks,
            pivotalCommits);

        UNIT_ASSERT_VALUES_EQUAL(6, blockCounts.LiveBlocks);
        UNIT_ASSERT_VALUES_EQUAL(2, blockCounts.GarbageBlocks);
        UNIT_ASSERT_VALUES_EQUAL(3, blockCounts.CheckpointBlocks);

        UNIT_ASSERT_VALUES_EQUAL(8, blocks.size());

        UNIT_ASSERT_VALUES_EQUAL(50, blocks[7].BlockIndex);
        UNIT_ASSERT_VALUES_EQUAL(6, blocks[7].MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(9, blocks[7].MaxCommitId);

        UNIT_ASSERT_VALUES_EQUAL(2, pivotalCommits.size());
        pivotalCommit = pivotalCommits.begin();
        UNIT_ASSERT_VALUES_EQUAL(2, *pivotalCommit);
        ++pivotalCommit;
        UNIT_ASSERT_VALUES_EQUAL(8, *pivotalCommit);
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateCheckpointBlocks)
    {
        const TPivotalCommitStorageImpl pivotalCommitStorage({2, 5, 8});

        const ui64 lastCommitId = 15;

        TVector<TBlock> blocks = {
            {1, 1, InvalidCommitId, false},
            {2, 10, 11, false},
            {3, 10, 11, false},
            {4, 1, 6, false},
        };

        auto blocks0 = blocks;
        TSet<ui64> pivotalCommits;
        auto blockCounts = RebaseBlocks(
            pivotalCommitStorage,
            [](ui32) { return false; },
            lastCommitId,
            blocks0,
            pivotalCommits);

        UNIT_ASSERT_VALUES_EQUAL(1, blockCounts.CheckpointBlocks);
    }

    Y_UNIT_TEST(ShouldCorrectlyMarkOverwrittenBlocks)
    {
        auto b =
            [](ui32 index, ui64 minCommitId, ui64 maxCommitId = InvalidCommitId)
        {
            return TBlock(index, minCommitId, maxCommitId, false);
        };

        const TVector<TBlock> overwritten = {
            b(0, 10, 20),
            b(3, 11),
            b(3, 5, 11),
        };
        TVector<TBlock> blocks = {
            b(0, 15, 25),
            b(0, 20, 20),
            b(0, 10, 15),
            b(0, 5, 10),
            b(1, 10),
            b(1, 6, 10),
            b(2, 5),
            b(3, 15),
            b(3, 5, 10),
        };

        UNIT_ASSERT_VALUES_EQUAL(6, MarkOverwrittenBlocks(overwritten, blocks));
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TBlock>({
                b(0, 15, 15),
                b(0, 20, 20),
                b(0, 10, 10),
                b(0, 5, 5),
                b(1, 10),
                b(1, 6, 10),
                b(2, 5),
                b(3, 15, 15),
                b(3, 5, 5),
            }),
            blocks);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
