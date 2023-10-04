#include "block_list.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/random/random.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<TBlock> GenerateSeqBlocks(size_t blocksCount, size_t groupsCount)
{
    constexpr ui64 nodeId = 1;
    constexpr ui64 minCommitId = MakeCommitId(12, 345);
    constexpr ui64 maxCommitId = InvalidCommitId;

    constexpr ui32 firstBlockIndex = 123456;

    TVector<TBlock> blocks;

    ui32 blockIndex = firstBlockIndex;
    for (size_t i = 0; i < groupsCount; ++i) {
        for (size_t j = 0; j < blocksCount / groupsCount; ++j) {
            blocks.emplace_back(
                nodeId,
                blockIndex++,
                minCommitId,
                maxCommitId);
        }
        blockIndex += 100;
    }

    return blocks;
}

TVector<TBlock> GenerateRandomBlocks(size_t blocksCount)
{
    constexpr ui64 nodeId = 1;
    constexpr ui64 initialCommitId = MakeCommitId(12, 345);

    constexpr ui32 firstBlockIndex = 123456;

    TVector<TBlock> blocks;

    for (size_t i = 0; i < blocksCount; ++i) {
        auto blockIndex = firstBlockIndex + RandomNumber(10000u);
        auto it = FindIf(blocks, [=] (const auto& block) {
            return block.BlockIndex == blockIndex
                && block.MaxCommitId == InvalidCommitId;
        });

        ui64 minCommitId;
        if (it == blocks.end()) {
            minCommitId = initialCommitId + RandomNumber(10u);
        } else {
            minCommitId = it->MinCommitId + 1 + RandomNumber(10u);
            it->MaxCommitId = minCommitId;
        }

        blocks.emplace_back(
            nodeId,
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

        auto iter = list.FindBlocks();
        UNIT_ASSERT(!iter->Next());
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
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockEntries, blocksCount);
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockGroups, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionMarkers, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionGroups, 0);

        auto iter = list.FindBlocks();
        for (size_t i = 0; i < blocksCount; ++i) {
            UNIT_ASSERT(iter->Next());

            UNIT_ASSERT_VALUES_EQUAL(iter->BlobOffset, i);
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.NodeId, nodeId);
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.BlockIndex, blockIndex + i);
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.MinCommitId, minCommitId);
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.MaxCommitId, maxCommitId);
        }

        UNIT_ASSERT(!iter->Next());
    }

    Y_UNIT_TEST(ShouldEncodeSeqBlocks)
    {
        constexpr size_t blocksCount = 100;
        constexpr size_t groupsCount = 10;

        auto blocks = GenerateSeqBlocks(blocksCount, groupsCount);
        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());

        auto stats = list.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockEntries, blocksCount);
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockGroups, groupsCount);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionMarkers, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionGroups, 0);

        auto iter = list.FindBlocks();
        for (size_t i = 0; i < blocksCount; ++i) {
            UNIT_ASSERT(iter->Next());

            const auto& block = blocks[i];
            UNIT_ASSERT_VALUES_EQUAL(iter->BlobOffset, i);
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.NodeId, block.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.BlockIndex, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.MinCommitId, block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.MaxCommitId, block.MaxCommitId);
        }

        UNIT_ASSERT(!iter->Next());
    }

    Y_UNIT_TEST(ShouldEncodeRandomBlocks)
    {
        constexpr size_t blocksCount = 1000;

        auto blocks = GenerateRandomBlocks(blocksCount);
        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());

        size_t deletionMarkers = GetDeletionMarkersCount(blocks);

        auto stats = list.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.BlockEntries, blocksCount);
        UNIT_ASSERT(stats.BlockGroups > 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.DeletionMarkers, deletionMarkers);
        UNIT_ASSERT(stats.DeletionGroups > 0);

        auto iter = list.FindBlocks();
        for (size_t i = 0; i < blocksCount; ++i) {
            UNIT_ASSERT(iter->Next());

            const auto& block = blocks[iter->BlobOffset];
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.NodeId, block.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.BlockIndex, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.MinCommitId, block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(iter->Block.MaxCommitId, block.MaxCommitId);
        }

        UNIT_ASSERT(!iter->Next());
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
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.NodeId, block.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.BlockIndex, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.MinCommitId, block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.MaxCommitId, block.MaxCommitId);
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
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.NodeId, block.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.BlockIndex, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.MinCommitId, block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(decodedBlock.MaxCommitId, block.MaxCommitId);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
