#include "block_list.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/random/random.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 NodeId = 1;
constexpr ui64 InitialCommitId = MakeCommitId(12, 345);
constexpr ui32 FirstBlockIndex = 123456;

////////////////////////////////////////////////////////////////////////////////

TVector<TBlock> GenerateSeqBlocks(size_t blocksCount, size_t groupsCount)
{
    TVector<TBlock> blocks;

    ui32 blockIndex = FirstBlockIndex;
    for (size_t i = 0; i < groupsCount; ++i) {
        for (size_t j = 0; j < blocksCount / groupsCount; ++j) {
            blocks.emplace_back(
                NodeId,
                blockIndex++,
                InitialCommitId,
                InvalidCommitId);
        }
        blockIndex += 100;
    }

    return blocks;
}

TVector<TBlock> GenerateRandomBlocks(size_t blocksCount)
{
    TVector<TBlock> blocks;

    for (size_t i = 0; i < blocksCount; ++i) {
        auto blockIndex = FirstBlockIndex + RandomNumber(10000u);
        auto it = FindIf(blocks, [=] (const auto& block) {
            return block.BlockIndex == blockIndex
                && block.MaxCommitId == InvalidCommitId;
        });

        ui64 minCommitId;
        if (it == blocks.end()) {
            minCommitId = InitialCommitId + RandomNumber(10u);
        } else {
            minCommitId = it->MinCommitId + 1 + RandomNumber(10u);
            it->MaxCommitId = minCommitId;
        }

        blocks.emplace_back(
            NodeId,
            blockIndex,
            minCommitId,
            InvalidCommitId);
    }

    Sort(blocks, TBlockCompare());
    return blocks;
}

size_t GetDeletionMarkersCount(const TVector<TBlock>& blocks)
{
    size_t count = 0;
    for (const auto& block: blocks) {
        if (block.MaxCommitId != InvalidCommitId) {
            ++count;
        }
    }
    return count;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockListTest)
{
    Y_UNIT_TEST(ShouldEncodeNoneBlocks)
    {
        TVector<TBlock> blocks;

        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());

        auto stats = list.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockEntries, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockGroups, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionMarkers, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionGroups, 0);

        auto iter = list.FindBlocks(0, 0, 0, 0);
        UNIT_ASSERT(!iter.Next());
    }

    Y_UNIT_TEST(ShouldEncodeMergedBlocks)
    {
        constexpr size_t BlocksCount = 100;

        TBlock block(NodeId, FirstBlockIndex, InitialCommitId, InvalidCommitId);

        auto list = TBlockList::EncodeBlocks(block, BlocksCount, TDefaultAllocator::Instance());

        auto stats = list.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockEntries, BlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockGroups, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionMarkers, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionGroups, 0);

        auto iter = list.FindBlocks(
            NodeId,
            InitialCommitId,
            FirstBlockIndex,
            BlocksCount);
        for (size_t i = 0; i < BlocksCount; i += iter.BlocksCount) {
            UNIT_ASSERT(iter.Next());

            for (ui32 j = 0; j < iter.BlocksCount; j++) {
                UNIT_ASSERT_VALUES_EQUAL(iter.BlobOffset + j, i + j);
                UNIT_ASSERT_VALUES_EQUAL(iter.Block.NodeId, NodeId);
                UNIT_ASSERT_VALUES_EQUAL(iter.Block.BlockIndex + j, FirstBlockIndex + i + j);
                UNIT_ASSERT_VALUES_EQUAL(iter.Block.MinCommitId, InitialCommitId);
                UNIT_ASSERT_VALUES_EQUAL(iter.Block.MaxCommitId, InvalidCommitId);
            }
        }

        UNIT_ASSERT(!iter.Next());

        {
            // Expect other nodes to be filtered
            auto iter = list.FindBlocks(
                NodeId + 1,
                InitialCommitId,
                FirstBlockIndex,
                BlocksCount);
            UNIT_ASSERT(!iter.Next());
        }
    }

    Y_UNIT_TEST(ShouldEncodeSeqBlocks)
    {
        constexpr size_t BlocksCount = 100;
        constexpr size_t GroupsCount = 10;

        auto blocks = GenerateSeqBlocks(BlocksCount, GroupsCount);
        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());

        auto stats = list.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockEntries, BlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockGroups, GroupsCount);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionMarkers, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionGroups, 0);

        auto iter = list.FindBlocks(
            NodeId,
            InitialCommitId,
            FirstBlockIndex,
            Max<ui32>() - FirstBlockIndex);
        for (size_t i = 0; i < BlocksCount; i += iter.BlocksCount) {
            UNIT_ASSERT(iter.Next());

            for (ui32 j = 0; j < iter.BlocksCount; j++) {
                const auto& block = blocks[i];
                UNIT_ASSERT_VALUES_EQUAL(iter.BlobOffset + j, i + j);
                UNIT_ASSERT_VALUES_EQUAL(iter.Block.NodeId, block.NodeId);
                UNIT_ASSERT_VALUES_EQUAL(iter.Block.BlockIndex + j, block.BlockIndex +j);
                UNIT_ASSERT_VALUES_EQUAL(iter.Block.MinCommitId, block.MinCommitId);
                UNIT_ASSERT_VALUES_EQUAL(iter.Block.MaxCommitId, block.MaxCommitId);
            }
        }

        UNIT_ASSERT(!iter.Next());
    }

    Y_UNIT_TEST(ShouldEncodeRandomBlocks)
    {
        constexpr size_t BlocksCount = 1000;

        auto blocks = GenerateRandomBlocks(BlocksCount);
        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());

        size_t deletionMarkers = GetDeletionMarkersCount(blocks);

        auto stats = list.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockEntries, BlocksCount);
        UNIT_ASSERT(stats.BlockGroups > 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionMarkers, deletionMarkers);
        UNIT_ASSERT(stats.DeletionGroups > 0);

        size_t expectedBlocksToFind = 0;
        for (const auto& block: blocks) {
            if (block.MinCommitId <= InitialCommitId) {
                expectedBlocksToFind++;
            }
        }

        auto iter = list.FindBlocks(
            NodeId,
            InitialCommitId,
            FirstBlockIndex,
            Max<ui32>() - FirstBlockIndex);
        for (size_t i = 0; i < expectedBlocksToFind; ++i) {
            UNIT_ASSERT(iter.Next());

            const auto& block = blocks[iter.BlobOffset];
            UNIT_ASSERT_VALUES_EQUAL(iter.Block.NodeId, block.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(iter.Block.BlockIndex, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(iter.Block.MinCommitId, block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(iter.Block.MaxCommitId, block.MaxCommitId);
        }

        UNIT_ASSERT(!iter.Next());
    }

    Y_UNIT_TEST(ShouldDecodeExactSeqBlocks)
    {
        constexpr size_t BlocksCount = 100;
        constexpr size_t GroupsCount = 10;

        auto blocks = GenerateSeqBlocks(BlocksCount, GroupsCount);
        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());

        auto decodedBlocks = list.DecodeBlocks();
        UNIT_ASSERT_VALUES_EQUAL(decodedBlocks.size(), BlocksCount);

        for (size_t i = 0; i < BlocksCount; ++i) {
            const auto& block = blocks[i];
            const auto& decodedBlock = decodedBlocks[i];
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.NodeId, block.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.BlockIndex, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.MinCommitId, block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.MaxCommitId, block.MaxCommitId);
        }
    }

    Y_UNIT_TEST(ShouldDecodeExactRandomBlocks)
    {
        constexpr size_t BlocksCount = 1000;

        auto blocks = GenerateRandomBlocks(BlocksCount);
        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());

        auto decodedBlocks = list.DecodeBlocks();
        UNIT_ASSERT_VALUES_EQUAL(decodedBlocks.size(), BlocksCount);

        for (size_t i = 0; i < BlocksCount; ++i) {
            const auto& block = blocks[i];
            const auto& decodedBlock = decodedBlocks[i];
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.NodeId, block.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.BlockIndex, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.MinCommitId, block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.MaxCommitId, block.MaxCommitId);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
