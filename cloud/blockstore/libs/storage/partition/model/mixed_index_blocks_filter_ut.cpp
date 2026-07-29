#include "mixed_index_blocks_filter.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

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

void InitializeRanges(
    TMixedBlocksFilter& filter,
    TVector<ui32> rangeIndices,
    ui64 commitId)
{
    filter.CompactionStarted(std::move(rangeIndices), commitId);
    filter.CompactionFinished();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMixedIndexBlocksFilterTest)
{
    Y_UNIT_TEST(ShouldBeConservativeUntilRangeIsInitialized)
    {
        TMixedBlocksFilter filter(0, BlocksPerRange, BlockCount);

        UNIT_ASSERT(MayHaveBlock(filter, 0));
        UNIT_ASSERT(MayHaveBlock(filter, BlocksPerRange));
        UNIT_ASSERT(MayHaveBlock(filter, BlockCount - 1));

        const ui32 rangeIndex = 1;
        const ui32 blockIndex = rangeIndex * BlocksPerRange;
        const ui64 rangeCommitId = 10;
        InitializeRanges(filter, {rangeIndex}, rangeCommitId);

        UNIT_ASSERT(MayHaveBlock(filter, blockIndex, rangeCommitId - 1));
        UNIT_ASSERT(!MayHaveBlock(filter, blockIndex, rangeCommitId));
        UNIT_ASSERT(!MayHaveBlock(filter, blockIndex, rangeCommitId + 1));

        UNIT_ASSERT(filter.MayHaveBlocksInMixedIndex(
            TBlockRange32::MakeClosedInterval(blockIndex - 1, blockIndex),
            rangeCommitId));
    }

    Y_UNIT_TEST(ShouldOnlyAddBlocksVisibleAtRangeCommitId)
    {
        TMixedBlocksFilter filter(0, BlocksPerRange, BlockCount);
        const ui64 rangeCommitId = 10;
        InitializeRanges(filter, {0}, rangeCommitId);

        filter.BlocksAddedToMixedIndex(0, 1, rangeCommitId - 1);
        filter.BlocksAddedToMixedIndex(1, 1, rangeCommitId);
        filter.BlocksAddedToMixedIndex(2, 1, rangeCommitId + 1);

        UNIT_ASSERT(!MayHaveBlock(filter, 0, rangeCommitId));
        UNIT_ASSERT(MayHaveBlock(filter, 1, rangeCommitId));
        UNIT_ASSERT(MayHaveBlock(filter, 2, rangeCommitId));

        UNIT_ASSERT(MayHaveBlock(filter, 3, rangeCommitId - 1));
        UNIT_ASSERT(!MayHaveBlock(filter, 3, rangeCommitId));
    }

    Y_UNIT_TEST(ShouldReplaceCompactedRangeWithConcurrentWrites)
    {
        TMixedBlocksFilter filter(0, BlocksPerRange, BlockCount);
        InitializeRanges(filter, {0}, 1);

        filter.BlocksAddedToMixedIndex(0, 1, 2);
        filter.CompactionStarted({0}, 10);
        filter.BlocksAddedToMixedIndex(1, 1, 9);
        filter.BlocksAddedToMixedIndex(2, 1, 10);
        filter.BlocksAddedToMixedIndex(3, 1, 11);

        filter.CompactionFinished();

        UNIT_ASSERT(!MayHaveBlock(filter, 0));
        UNIT_ASSERT(!MayHaveBlock(filter, 1));
        UNIT_ASSERT(MayHaveBlock(filter, 2));
        UNIT_ASSERT(MayHaveBlock(filter, 3));

        UNIT_ASSERT(MayHaveBlock(filter, 0, 9));
        UNIT_ASSERT(!MayHaveBlock(filter, 0, 10));
    }

    Y_UNIT_TEST(ShouldClearRangeWhenCompactionHasNoConcurrentWrites)
    {
        TMixedBlocksFilter filter(0, BlocksPerRange, BlockCount);
        InitializeRanges(filter, {0}, 1);
        filter.BlocksAddedToMixedIndex(17, 1, 2);

        filter.CompactionStarted({0}, 10);
        filter.CompactionFinished();

        UNIT_ASSERT(!MayHaveBlock(filter, 17));
        UNIT_ASSERT(MayHaveBlock(filter, 17, 9));
    }

    Y_UNIT_TEST(ShouldLeaveRangeUnchangedWhenCompactionFails)
    {
        TMixedBlocksFilter filter(0, BlocksPerRange, BlockCount);
        InitializeRanges(filter, {0}, 1);
        filter.BlocksAddedToMixedIndex(0, 1, 2);

        filter.CompactionStarted({0}, 10);
        filter.BlocksAddedToMixedIndex(1, 1, 10);
        filter.CompactionFailed();

        UNIT_ASSERT(MayHaveBlock(filter, 0));
        UNIT_ASSERT(MayHaveBlock(filter, 1));
        UNIT_ASSERT(!MayHaveBlock(filter, 2));
        UNIT_ASSERT(MayHaveBlock(filter, 2, 0));
    }

    Y_UNIT_TEST(ShouldHandleQueuedCompactionsInCommitOrder)
    {
        TMixedBlocksFilter filter(0, BlocksPerRange, BlockCount);
        InitializeRanges(filter, {0}, 1);
        filter.BlocksAddedToMixedIndex(0, 1, 2);

        filter.CompactionStarted({0}, 10);
        filter.BlocksAddedToMixedIndex(1, 1, 15);
        filter.CompactionStarted({0}, 20);
        filter.BlocksAddedToMixedIndex(2, 1, 25);

        filter.CompactionFinished();

        UNIT_ASSERT(!MayHaveBlock(filter, 0));
        UNIT_ASSERT(MayHaveBlock(filter, 1));
        UNIT_ASSERT(MayHaveBlock(filter, 2));

        filter.CompactionFinished();

        UNIT_ASSERT(!MayHaveBlock(filter, 0));
        UNIT_ASSERT(!MayHaveBlock(filter, 1));
        UNIT_ASSERT(MayHaveBlock(filter, 2));
        UNIT_ASSERT(MayHaveBlock(filter, 1, 19));
    }

    Y_UNIT_TEST(ShouldUpdateOnlyTheCompactedRange)
    {
        TMixedBlocksFilter filter(0, BlocksPerRange, BlockCount);
        InitializeRanges(filter, {0, 1}, 1);

        const ui32 blockInFirstRange = 7;
        const ui32 blockInSecondRange = BlocksPerRange + 7;
        filter.BlocksAddedToMixedIndex(blockInFirstRange, 1, 2);
        filter.BlocksAddedToMixedIndex(blockInSecondRange, 1, 2);

        filter.CompactionStarted({1}, 10);
        filter.BlocksAddedToMixedIndex(blockInSecondRange + 1, 1, 10);
        filter.CompactionFinished();

        UNIT_ASSERT(MayHaveBlock(filter, blockInFirstRange));
        UNIT_ASSERT(!MayHaveBlock(filter, blockInSecondRange));
        UNIT_ASSERT(MayHaveBlock(filter, blockInSecondRange + 1));
    }

    Y_UNIT_TEST(ShouldAddBlockRangesAcrossCompactionRanges)
    {
        TMixedBlocksFilter filter(0, BlocksPerRange, BlockCount);
        InitializeRanges(filter, {0}, 10);
        InitializeRanges(filter, {1}, 20);

        filter.BlocksAddedToMixedIndex(BlocksPerRange - 1, 3, 15);

        UNIT_ASSERT(MayHaveBlock(filter, BlocksPerRange - 1));
        UNIT_ASSERT(!MayHaveBlock(filter, BlocksPerRange));
        UNIT_ASSERT(!MayHaveBlock(filter, BlocksPerRange + 1));

        filter.CompactionStarted({0, 1}, 30);
        filter.BlocksAddedToMixedIndex(BlocksPerRange - 1, 3, 30);
        filter.CompactionFinished();

        UNIT_ASSERT(MayHaveBlock(filter, BlocksPerRange - 1));
        UNIT_ASSERT(MayHaveBlock(filter, BlocksPerRange));
        UNIT_ASSERT(MayHaveBlock(filter, BlocksPerRange + 1));
    }

    Y_UNIT_TEST(ShouldHandlePartialLastRange)
    {
        constexpr ui32 PartialBlockCount = BlocksPerRange + 7;
        TMixedBlocksFilter filter(0, BlocksPerRange, PartialBlockCount);

        InitializeRanges(filter, {1}, 10);
        filter.BlocksAddedToMixedIndex(PartialBlockCount - 2, 2, 10);

        UNIT_ASSERT(MayHaveBlock(filter, PartialBlockCount - 2));
        UNIT_ASSERT(MayHaveBlock(filter, PartialBlockCount - 1));

        filter.CompactionStarted({1}, 20);
        filter.CompactionFinished();

        UNIT_ASSERT(!MayHaveBlock(filter, PartialBlockCount - 2));
        UNIT_ASSERT(!MayHaveBlock(filter, PartialBlockCount - 1));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
