#include "mixed_index_blocks_filter.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

constexpr ui64 BlocksPerRange = TCompressedBitmap::CHUNK_SIZE;
constexpr size_t RangeCount = 3;
constexpr size_t BlockCount = RangeCount * BlocksPerRange;

bool MayHaveBlock(
    const TMixedBlocksFilter& filter,
    ui32 blockIndex,
    ui64 commitId = Max<ui64>())
{
    return filter.MayHaveBlocksInMixedIndex(
        TBlockRange32::WithLength(blockIndex, 1),
        commitId);
}

void LoadBitmap(
    TMixedBlocksFilter& filter,
    const TCompressedBitmap& bitmap)
{
    auto serializer = bitmap.RangeSerializer(0, bitmap.Capacity());
    TCompressedBitmap::TSerializedChunk chunk;
    while (serializer.Next(&chunk)) {
        filter.UpdateChunk(chunk);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMixedIndexBlocksFilterTest)
{
    Y_UNIT_TEST(ShouldBeConservativeUntilRangeIsInitialized)
    {
        TMixedBlocksFilter filter(BlocksPerRange, BlockCount);

        UNIT_ASSERT(MayHaveBlock(filter, 0));
        UNIT_ASSERT(MayHaveBlock(filter, BlocksPerRange));
        UNIT_ASSERT(MayHaveBlock(filter, BlockCount - 1));

        const ui32 rangeIndex = 1;
        const ui32 blockIndex = rangeIndex * BlocksPerRange;
        const ui64 rangeCommitId = 10;
        filter.UpdateRangeCommitId(rangeIndex, rangeCommitId);

        UNIT_ASSERT(MayHaveBlock(filter, blockIndex, rangeCommitId - 1));
        UNIT_ASSERT(!MayHaveBlock(filter, blockIndex, rangeCommitId));
        UNIT_ASSERT(!MayHaveBlock(filter, blockIndex, rangeCommitId + 1));

        UNIT_ASSERT(filter.MayHaveBlocksInMixedIndex(
            TBlockRange32::MakeClosedInterval(
                blockIndex - 1,
                blockIndex),
            rangeCommitId));
    }

    Y_UNIT_TEST(ShouldLoadSerializedBlocks)
    {
        TMixedBlocksFilter filter(BlocksPerRange, BlockCount);
        TCompressedBitmap bitmap(BlockCount);

        bitmap.Set(0, 1);
        bitmap.Set(BlocksPerRange - 1, BlocksPerRange);
        bitmap.Set(BlocksPerRange + 7, BlocksPerRange + 8);
        bitmap.Set(BlockCount - 1, BlockCount);

        LoadBitmap(filter, bitmap);
        for (ui32 rangeIndex = 0; rangeIndex < RangeCount; ++rangeIndex) {
            filter.UpdateRangeCommitId(rangeIndex, 0);
        }

        for (ui32 blockIndex = 0; blockIndex < BlockCount; ++blockIndex) {
            UNIT_ASSERT_VALUES_EQUAL(
                bitmap.Test(blockIndex),
                MayHaveBlock(filter, blockIndex));
        }

        UNIT_ASSERT(filter.MayHaveBlocksInMixedIndex(
            TBlockRange32::MakeClosedInterval(1, BlocksPerRange - 1),
            Max<ui64>()));
        UNIT_ASSERT(!filter.MayHaveBlocksInMixedIndex(
            TBlockRange32::MakeClosedInterval(1, BlocksPerRange - 2),
            Max<ui64>()));
    }

    Y_UNIT_TEST(ShouldOnlyAddBlocksVisibleAtRangeCommitId)
    {
        TMixedBlocksFilter filter(BlocksPerRange, BlockCount);
        const ui64 rangeCommitId = 10;
        filter.UpdateRangeCommitId(0, rangeCommitId);

        filter.AddBlocksToMixedIndex(0, rangeCommitId - 1);
        filter.AddBlocksToMixedIndex(1, rangeCommitId);
        filter.AddBlocksToMixedIndex(2, rangeCommitId + 1);

        UNIT_ASSERT(!MayHaveBlock(filter, 0, rangeCommitId));
        UNIT_ASSERT(MayHaveBlock(filter, 1, rangeCommitId));
        UNIT_ASSERT(MayHaveBlock(filter, 2, rangeCommitId));

        UNIT_ASSERT(MayHaveBlock(filter, 3, rangeCommitId - 1));
        UNIT_ASSERT(!MayHaveBlock(filter, 3, rangeCommitId));
    }

    Y_UNIT_TEST(ShouldReplaceCompactedRangeWithConcurrentWrites)
    {
        TMixedBlocksFilter filter(BlocksPerRange, BlockCount);
        filter.UpdateRangeCommitId(0, 1);

        filter.AddBlocksToMixedIndex(0, 2);
        filter.StartCompactionRange(0, 10);
        filter.AddBlocksToMixedIndex(1, 9);
        filter.AddBlocksToMixedIndex(2, 10);
        filter.AddBlocksToMixedIndex(3, 11);

        filter.CompactionRangeFinished(0);

        UNIT_ASSERT(!MayHaveBlock(filter, 0));
        UNIT_ASSERT(!MayHaveBlock(filter, 1));
        UNIT_ASSERT(MayHaveBlock(filter, 2));
        UNIT_ASSERT(MayHaveBlock(filter, 3));

        UNIT_ASSERT(MayHaveBlock(filter, 0, 9));
        UNIT_ASSERT(!MayHaveBlock(filter, 0, 10));
    }

    Y_UNIT_TEST(ShouldClearRangeWhenCompactionHasNoConcurrentWrites)
    {
        TMixedBlocksFilter filter(BlocksPerRange, BlockCount);
        filter.UpdateRangeCommitId(0, 1);
        filter.AddBlocksToMixedIndex(17, 2);

        filter.StartCompactionRange(0, 10);
        filter.CompactionRangeFinished(0);

        UNIT_ASSERT(!MayHaveBlock(filter, 17));
        UNIT_ASSERT(MayHaveBlock(filter, 17, 9));
    }

    Y_UNIT_TEST(ShouldLeaveRangeUnchangedWhenCompactionFails)
    {
        TMixedBlocksFilter filter(BlocksPerRange, BlockCount);
        filter.UpdateRangeCommitId(0, 1);
        filter.AddBlocksToMixedIndex(0, 2);

        filter.StartCompactionRange(0, 10);
        filter.AddBlocksToMixedIndex(1, 10);
        filter.CompactionRangeFailed(0);

        UNIT_ASSERT(MayHaveBlock(filter, 0));
        UNIT_ASSERT(MayHaveBlock(filter, 1));
        UNIT_ASSERT(!MayHaveBlock(filter, 2));
        UNIT_ASSERT(MayHaveBlock(filter, 2, 0));
    }

    Y_UNIT_TEST(ShouldHandleQueuedCompactionsInCommitOrder)
    {
        TMixedBlocksFilter filter(BlocksPerRange, BlockCount);
        filter.UpdateRangeCommitId(0, 1);
        filter.AddBlocksToMixedIndex(0, 2);

        filter.StartCompactionRange(0, 10);
        filter.AddBlocksToMixedIndex(1, 15);
        filter.StartCompactionRange(0, 20);
        filter.AddBlocksToMixedIndex(2, 25);

        filter.CompactionRangeFinished(0);

        UNIT_ASSERT(!MayHaveBlock(filter, 0));
        UNIT_ASSERT(MayHaveBlock(filter, 1));
        UNIT_ASSERT(MayHaveBlock(filter, 2));

        filter.CompactionRangeFinished(0);

        UNIT_ASSERT(!MayHaveBlock(filter, 0));
        UNIT_ASSERT(!MayHaveBlock(filter, 1));
        UNIT_ASSERT(MayHaveBlock(filter, 2));
        UNIT_ASSERT(MayHaveBlock(filter, 1, 19));
    }

    Y_UNIT_TEST(ShouldUpdateOnlyTheCompactedRange)
    {
        TMixedBlocksFilter filter(BlocksPerRange, BlockCount);
        filter.UpdateRangeCommitId(0, 1);
        filter.UpdateRangeCommitId(1, 1);

        const ui32 blockInFirstRange = 7;
        const ui32 blockInSecondRange = BlocksPerRange + 7;
        filter.AddBlocksToMixedIndex(blockInFirstRange, 2);
        filter.AddBlocksToMixedIndex(blockInSecondRange, 2);

        filter.StartCompactionRange(1, 10);
        filter.AddBlocksToMixedIndex(blockInSecondRange + 1, 10);
        filter.CompactionRangeFinished(1);

        UNIT_ASSERT(MayHaveBlock(filter, blockInFirstRange));
        UNIT_ASSERT(!MayHaveBlock(filter, blockInSecondRange));
        UNIT_ASSERT(MayHaveBlock(filter, blockInSecondRange + 1));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
