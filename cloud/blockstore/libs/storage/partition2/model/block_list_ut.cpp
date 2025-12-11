#include "block_list.h"

#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>   // XXX move ut_helpers to another place

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockListTest)
{
    Y_UNIT_TEST(ShouldEncodeSeqBlocks)
    {
        ui32 blockIndex = 123456;
        ui64 commitId = MakeCommitId(12, 345);

        TVector<TBlock> blocks;
        for (size_t i = 0; i < 100; ++i) {
            blocks
                .emplace_back(blockIndex + i, commitId, InvalidCommitId, false);
        }

        auto list = BuildBlockList(blocks);
        ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(blocks, list.GetBlocks());

        {
            auto block = list.FindBlock(blocks[0].BlockIndex);
            UNIT_ASSERT(block);
            UNIT_ASSERT_VALUES_EQUAL(block->BlobOffset, 0);
            UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, blocks[0].MinCommitId);
        }
        {
            auto block = list.FindBlock(blocks[50].BlockIndex);
            UNIT_ASSERT(block);
            UNIT_ASSERT_VALUES_EQUAL(block->BlobOffset, 50);
            UNIT_ASSERT_VALUES_EQUAL(
                block->MinCommitId,
                blocks[50].MinCommitId);
        }
        {
            auto block = list.FindBlock(100500);
            UNIT_ASSERT(!block);
        }
    }

    Y_UNIT_TEST(ShouldEncodeRandomBlocks)
    {
        ui32 firstBlockIndex = 123456;
        ui64 initialCommitId = MakeCommitId(12, 345);

        TVector<TBlock> blocks;
        for (size_t i = 0; i < 1000; ++i) {
            auto blockIndex = firstBlockIndex + RandomNumber(10000u);
            auto it = FindIf(
                blocks,
                [=](const auto& b) {
                    return b.MaxCommitId == InvalidCommitId &&
                           blockIndex == b.BlockIndex;
                });
            ui64 commitId;
            if (it == blocks.end()) {
                commitId = initialCommitId + RandomNumber(10u);
            } else {
                commitId = it->MinCommitId + 1 + RandomNumber(10u);
                it->MaxCommitId = commitId;
            }

            bool zeroed = RandomNumber(10u) == 0;
            blocks.emplace_back(blockIndex, commitId, InvalidCommitId, zeroed);
        }

        auto list = BuildBlockList(blocks);
        ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(blocks, list.GetBlocks());

        for (ui32 i = 0; i < 1000; ++i) {
            auto block =
                list.FindBlock(blocks[i].BlockIndex, blocks[i].MaxCommitId - 1);
            UNIT_ASSERT(block);
            UNIT_ASSERT_VALUES_EQUAL(block->BlobOffset, i);
            UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, blocks[i].MinCommitId);
        }

        {
            auto block = list.FindBlock(100500);
            UNIT_ASSERT(!block);
        }
    }

    Y_UNIT_TEST(ShouldEncodeDifferentVersions)
    {
        ui32 blockIndex = 123456;
        ui64 commitId = MakeCommitId(12, 345);

        TVector<TBlock> blocks;
        for (size_t i = 0; i < 100; ++i) {
            blocks
                .emplace_back(blockIndex, commitId + i, InvalidCommitId, false);
        }

        auto list = BuildBlockList(blocks);
        ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(blocks, list.GetBlocks());

        {
            auto block = list.FindBlock(blockIndex, commitId);
            UNIT_ASSERT(block);
            UNIT_ASSERT_VALUES_EQUAL(block->BlobOffset, 0);
            UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, commitId);
        }
        {
            auto block = list.FindBlock(blockIndex, commitId + 50);
            UNIT_ASSERT(block);
            UNIT_ASSERT_VALUES_EQUAL(block->BlobOffset, 50);
            UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, commitId + 50);
        }
    }

    Y_UNIT_TEST(ShouldEncodeDeletedBlocks)
    {
        TVector<TBlock> blocks;
        for (size_t i = 0; i < 100; ++i) {
            if (i % 10 == 0) {
                blocks.emplace_back(i, 1, 2, false);
            } else {
                blocks.emplace_back(i, 1, InvalidCommitId, false);
            }
        }

        auto list = BuildBlockList(blocks);
        ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(blocks, list.GetBlocks());

        {
            auto block = list.FindBlock(0);
            UNIT_ASSERT(block);
            UNIT_ASSERT_VALUES_EQUAL(block->BlobOffset, 0);
            UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, 1);
        }
        {
            auto maxCommitId = list.FindDeletedBlock(0);
            UNIT_ASSERT_VALUES_EQUAL(maxCommitId, 2);
        }
        {
            auto maxCommitId = list.FindDeletedBlock(1);
            UNIT_ASSERT_VALUES_EQUAL(maxCommitId, InvalidCommitId);
        }

        UNIT_ASSERT_VALUES_EQUAL(100, list.CountBlocks());
    }

    Y_UNIT_TEST(ShouldEncodeSeqWithZeroedBlocks)
    {
        ui32 blockIndex = 123456;
        ui64 commitId = MakeCommitId(12, 345);

        TVector<TBlock> blocks;
        for (size_t i = 0; i < 100; ++i) {
            blocks.emplace_back(
                blockIndex + i,
                commitId,
                InvalidCommitId,
                i % 2 == 0);   // zeroed
        }

        auto list = BuildBlockList(blocks);
        ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(blocks, list.GetBlocks());

        {
            auto block = list.FindBlock(blocks[0].BlockIndex);
            UNIT_ASSERT(block);
            UNIT_ASSERT_VALUES_EQUAL(block->BlobOffset, 0);
            UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, blocks[0].MinCommitId);
        }
        {
            auto block = list.FindBlock(blocks[50].BlockIndex);
            UNIT_ASSERT(block);
            UNIT_ASSERT_VALUES_EQUAL(block->BlobOffset, 50);
            UNIT_ASSERT_VALUES_EQUAL(
                block->MinCommitId,
                blocks[50].MinCommitId);
        }
        {
            auto block = list.FindBlock(100500);
            UNIT_ASSERT(!block);
        }
    }

    Y_UNIT_TEST(ShouldEncodeDifferentVersionsWithZeroedBlocks)
    {
        ui32 blockIndex = 123456;
        ui64 commitId = MakeCommitId(12, 345);

        TVector<TBlock> blocks;
        for (size_t i = 0; i < 100; ++i) {
            blocks.emplace_back(
                blockIndex,
                commitId + i,
                InvalidCommitId,
                i % 2 == 0);   // zeroed
        }

        auto list = BuildBlockList(blocks);
        ASSERT_PARTITION2_BLOCK_LISTS_EQUAL(blocks, list.GetBlocks());

        {
            auto block = list.FindBlock(blockIndex, commitId);
            UNIT_ASSERT(block);
            UNIT_ASSERT_VALUES_EQUAL(block->BlobOffset, 0);
            UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, commitId);
        }
        {
            auto block = list.FindBlock(blockIndex, commitId + 50);
            UNIT_ASSERT(block);
            UNIT_ASSERT_VALUES_EQUAL(block->BlobOffset, 50);
            UNIT_ASSERT_VALUES_EQUAL(block->MinCommitId, commitId + 50);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
