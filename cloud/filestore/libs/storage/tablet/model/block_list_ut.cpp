#include "block_list.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/random/random.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 NodeId = 12345;
constexpr ui64 InitialCommitId = MakeCommitId(12, 345);
constexpr ui32 FirstBlockIndex = 123456;

////////////////////////////////////////////////////////////////////////////////

TVector<TBlock> GenerateSeqBlocks(size_t blocksCount, size_t groupsCount)
{
    constexpr ui64 maxCommitId = InvalidCommitId;

    TVector<TBlock> blocks;

    ui32 blockIndex = FirstBlockIndex;
    for (size_t i = 0; i < groupsCount; ++i) {
        for (size_t j = 0; j < blocksCount / groupsCount; ++j) {
            blocks.emplace_back(
                NodeId,
                blockIndex++,
                InitialCommitId,
                maxCommitId);
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
        UNIT_ASSERT_VALUES_EQUAL(0, stats.BlockEntries);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.BlockGroups);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.DeletionMarkers);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.DeletionGroups);

        auto iter = list.FindBlocks(0, 0, 0, 0);
        UNIT_ASSERT(!iter.Next());
    }

    Y_UNIT_TEST(ShouldEncodeMergedBlocks)
    {
        constexpr ui64 nodeId = 1;
        constexpr ui64 minCommitId = MakeCommitId(12, 345);
        constexpr ui64 maxCommitId = InvalidCommitId;

        constexpr ui32 blockIndex = 123456;
        constexpr size_t blocksCount = 100;

        TBlock block(nodeId, blockIndex, minCommitId, maxCommitId);

        auto list = TBlockList::EncodeBlocks(block, blocksCount, TDefaultAllocator::Instance());

        auto stats = list.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(blocksCount, stats.BlockEntries);
        UNIT_ASSERT_VALUES_EQUAL(1, stats.BlockGroups);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.DeletionMarkers);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.DeletionGroups);

        auto iter = list.FindBlocks(
            nodeId,
            minCommitId,
            blockIndex,
            blocksCount);
        for (size_t i = 0; i < blocksCount; ++i) {
            UNIT_ASSERT(iter.Next());

            UNIT_ASSERT_VALUES_EQUAL(i, iter.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(nodeId, iter.Block.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(blockIndex + i, iter.Block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(minCommitId, iter.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(maxCommitId, iter.Block.MaxCommitId);
        }

        UNIT_ASSERT(!iter.Next());
    }

    Y_UNIT_TEST(ShouldEncodeSeqBlocks)
    {
        constexpr size_t blocksCount = 100;
        constexpr size_t groupsCount = 10;

        auto blocks = GenerateSeqBlocks(blocksCount, groupsCount);
        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());

        auto stats = list.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(blocksCount, stats.BlockEntries);
        UNIT_ASSERT_VALUES_EQUAL(groupsCount, stats.BlockGroups);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.DeletionMarkers);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.DeletionGroups);

        auto iter = list.FindBlocks(
            NodeId,
            InitialCommitId,
            FirstBlockIndex,
            Max<ui32>() - FirstBlockIndex);
        for (size_t i = 0; i < blocksCount; ++i) {
            UNIT_ASSERT(iter.Next());

            const auto& block = blocks[i];
            UNIT_ASSERT_VALUES_EQUAL(i, iter.BlobOffset);
            UNIT_ASSERT_VALUES_EQUAL(block.NodeId, iter.Block.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(block.BlockIndex, iter.Block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(block.MinCommitId, iter.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(block.MaxCommitId, iter.Block.MaxCommitId);
        }

        UNIT_ASSERT(!iter.Next());
    }

    Y_UNIT_TEST(ShouldEncodeRandomBlocks)
    {
        constexpr size_t blocksCount = 1000;

        auto blocks = GenerateRandomBlocks(blocksCount);
        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());

        size_t deletionMarkers = GetDeletionMarkersCount(blocks);

        auto stats = list.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(blocksCount, stats.BlockEntries);
        UNIT_ASSERT(stats.BlockGroups > 0);
        UNIT_ASSERT_VALUES_EQUAL(deletionMarkers, stats.DeletionMarkers);
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
            UNIT_ASSERT_VALUES_EQUAL(block.NodeId, iter.Block.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(block.BlockIndex, iter.Block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(block.MinCommitId, iter.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(block.MaxCommitId, iter.Block.MaxCommitId);
        }

        UNIT_ASSERT(!iter.Next());
    }

    Y_UNIT_TEST(ShouldDecodeExactSeqBlocks)
    {
        constexpr size_t blocksCount = 100;
        constexpr size_t groupsCount = 10;

        auto blocks = GenerateSeqBlocks(blocksCount, groupsCount);
        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());

        auto decodedBlocks = list.DecodeBlocks();
        UNIT_ASSERT_VALUES_EQUAL(decodedBlocks.size(), blocksCount);

        for (size_t i = 0; i < blocksCount; ++i) {
            const auto& block = blocks[i];
            const auto& decodedBlock = decodedBlocks[i];
            UNIT_ASSERT_VALUES_EQUAL(block.NodeId, decodedBlock.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(block.BlockIndex, decodedBlock.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(block.MinCommitId, decodedBlock.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(block.MaxCommitId, decodedBlock.MaxCommitId);
        }
    }

    Y_UNIT_TEST(ShouldDecodeExactRandomBlocks)
    {
        constexpr size_t blocksCount = 1000;

        auto blocks = GenerateRandomBlocks(blocksCount);
        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());

        auto decodedBlocks = list.DecodeBlocks();
        UNIT_ASSERT_VALUES_EQUAL(decodedBlocks.size(), blocksCount);

        for (size_t i = 0; i < blocksCount; ++i) {
            const auto& block = blocks[i];
            const auto& decodedBlock = decodedBlocks[i];
            UNIT_ASSERT_VALUES_EQUAL(block.NodeId, decodedBlock.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(block.BlockIndex, decodedBlock.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(block.MinCommitId, decodedBlock.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(block.MaxCommitId, decodedBlock.MaxCommitId);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
