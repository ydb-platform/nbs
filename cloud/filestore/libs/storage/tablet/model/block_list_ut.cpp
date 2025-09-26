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

TVector<TBlock> GenerateRandomBlockGroups(
    size_t blockGroups,
    size_t deletionGroups)
{
    Y_UNUSED(deletionGroups);

    TVector<TBlock> blocks;
    auto blockIndex = FirstBlockIndex;

    auto writeNewBlock = [&]()
    {
        blocks.emplace_back(
            NodeId,
            blockIndex,
            InitialCommitId,
            InvalidCommitId);
    };

    for (size_t i = 0; i < blockGroups; ++i) {
        const auto maxBlocksInGroup = 1 + RandomNumber(50u);

        if (RandomNumber(2u) == 0) {
            // Merged block group
            for (size_t j = 0; j < maxBlocksInGroup; ++j) {
                writeNewBlock();
                blockIndex++;
            }
        } else {
            // Mixed block group
            for (size_t j = 0; j < maxBlocksInGroup; ++j) {
                if (RandomNumber(2u) == 0){
                    writeNewBlock();
                }
                blockIndex++;
            }
        }

        blockIndex += 1 + RandomNumber(10000u);
    }

    ui16 blobOffset = 0;

    auto rewriteBlock = [&]()
    {
        Y_ABORT_UNLESS(blobOffset < blocks.size());
        auto& block = blocks[blobOffset];
        block.MaxCommitId = block.MinCommitId + 1 + RandomNumber(100u);

        auto b = block;
        b.MinCommitId = b.MaxCommitId;
        b.MaxCommitId = InvalidCommitId;
        blocks.push_back(std::move(b));
    };

    for (size_t i = 0; i < deletionGroups; ++i) {
        const auto maxDeletionsInGroup = 1 + RandomNumber(70u);
        Y_ABORT_UNLESS(blobOffset + maxDeletionsInGroup < blocks.size());

        if (RandomNumber(2u) == 0) {
            // Merged deletion group
            for (size_t j = 0; j < maxDeletionsInGroup; ++j) {
                rewriteBlock();
                blobOffset++;
            }

        } else {
            // Mixed deletion group
            for (size_t j = 0; j < maxDeletionsInGroup; ++j) {
                if (RandomNumber(2u) == 0) {
                    rewriteBlock();
                }

                blobOffset++;
            }
        }

        blobOffset += 1 + RandomNumber(10u);
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

void CheckFindBlocksIterator(
    TBlockIterator& iter,
    size_t expectedBlocksToFind,
    const TVector<TBlock>& expectedBlocks)
{
    for (size_t i = 0; i < expectedBlocksToFind; i += iter.BlocksInCurrentIteration) {
        UNIT_ASSERT(iter.Next());
        UNIT_ASSERT(iter.BlocksInCurrentIteration > 0);

        for (ui32 j = 0; j < iter.BlocksInCurrentIteration; j++) {
            const auto& block = expectedBlocks[iter.BlobOffset + j];
            UNIT_ASSERT_VALUES_EQUAL(block.NodeId, iter.Block.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(block.BlockIndex, iter.Block.BlockIndex + j);
            UNIT_ASSERT_VALUES_EQUAL(block.MinCommitId, iter.Block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(block.MaxCommitId, iter.Block.MaxCommitId);
        }
    }

    UNIT_ASSERT(!iter.Next());
}

void CheckFindBlocks(
    const TBlockList& list,
    const TVector<TBlock>& expectedBlocks,
    ui64 minCommitId)
{
    size_t expectedBlocksToFind = 0;
    for (const auto& block: expectedBlocks) {
        if (block.MinCommitId <= minCommitId &&
            minCommitId < block.MaxCommitId)
        {
            expectedBlocksToFind++;
        }
    }

    auto iter = list.FindBlocks(
        NodeId,
        minCommitId,
        FirstBlockIndex,
        Max<ui32>() - FirstBlockIndex);
    CheckFindBlocksIterator(iter, expectedBlocksToFind, expectedBlocks);
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
        for (size_t i = 0; i < blocksCount; i += iter.BlocksInCurrentIteration) {
            UNIT_ASSERT(iter.Next());
            UNIT_ASSERT(iter.BlocksInCurrentIteration > 0);

            for (ui32 j = 0; j < iter.BlocksInCurrentIteration; j++) {
                UNIT_ASSERT_VALUES_EQUAL(i + j, iter.BlobOffset + j);
                UNIT_ASSERT_VALUES_EQUAL(nodeId, iter.Block.NodeId);
                UNIT_ASSERT_VALUES_EQUAL(
                    blockIndex + i + j,
                    iter.Block.BlockIndex + j);
                UNIT_ASSERT_VALUES_EQUAL(minCommitId, iter.Block.MinCommitId);
                UNIT_ASSERT_VALUES_EQUAL(maxCommitId, iter.Block.MaxCommitId);
            }
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
        CheckFindBlocksIterator(iter, blocksCount, blocks);
    }

    void TestEncodeBlocks(TVector<TBlock> blocks)
    {
        auto list = TBlockList::EncodeBlocks(blocks, TDefaultAllocator::Instance());
        size_t deletionMarkers = GetDeletionMarkersCount(blocks);

        auto stats = list.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(blocks.size(), stats.BlockEntries);
        UNIT_ASSERT(stats.BlockGroups > 0);
        UNIT_ASSERT_VALUES_EQUAL(deletionMarkers, stats.DeletionMarkers);
        if (deletionMarkers > 0) {
            UNIT_ASSERT(stats.DeletionGroups > 0);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(0, stats.DeletionGroups);
        }

        CheckFindBlocks(list, blocks, InitialCommitId);
        CheckFindBlocks(list, blocks, InitialCommitId + 1);
    }

    Y_UNIT_TEST(ShouldEncodeRandomBlocks)
    {
        TestEncodeBlocks(GenerateRandomBlocks(1000));
    }

    Y_UNIT_TEST(ShouldEncodeRandomBlockGroupsWithoutDeletions)
    {
        TestEncodeBlocks(
            GenerateRandomBlockGroups(
                /*blockGroups=*/10,
                /*deletionGroups=*/0));
    }

    Y_UNIT_TEST(ShouldEncodeRandomBlockGroupsWithDeletions)
    {
        TestEncodeBlocks(
            GenerateRandomBlockGroups(
                /*blockGroups=*/10,
                /*deletionGroups=*/4));
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
