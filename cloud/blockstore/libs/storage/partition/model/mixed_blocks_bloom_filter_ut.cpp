#include "mixed_blocks_bloom_filter.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

// TMixedBlocksBloomFilter iterates with blockIndex < range.End, so End is
// treated as exclusive. MakeClosedInterval maps naturally to that convention.
TBlockRange32 MakeHalfOpenRange(ui32 start, ui32 endExclusive)
{
    return TBlockRange32::MakeClosedInterval(start, endExclusive);
}

TBlockRange32 MakeSingleBlockRange(ui32 blockIndex)
{
    return TBlockRange32::WithLength(blockIndex, 1);
}

void AddPrimaryBloomFilter(TMixedBlocksBloomFilter& filter, ui32 rangeIndex)
{
    filter.AddSecondaryBloomFilter(rangeIndex);
    filter.PromoteSecondaryBloomFilter(rangeIndex);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMixedBlocksBloomFilterTest)
{
    Y_UNIT_TEST(ShouldReadMixedIndexWhenRangeFilterIsMissing)
    {
        constexpr ui64 blocksPerRange = 100;
        TMixedBlocksBloomFilter filter(blocksPerRange, 10, 0.01);

        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(0)));
        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(150)));

        // Add secondary bloom filter to make sure it doesn't affect the result.
        filter.AddSecondaryBloomFilter(0);
        filter.AddBlocks(MakeSingleBlockRange(5));

        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(5)));
        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(6)));

        filter.PromoteSecondaryBloomFilter(0);

        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(5)));
        UNIT_ASSERT(!filter.HasRangeInMixedIndex(MakeSingleBlockRange(6)));
    }

    Y_UNIT_TEST(ShouldNotReadMixedIndexForEmptyFilter)
    {
        constexpr ui64 blocksPerRange = 100;
        TMixedBlocksBloomFilter filter(blocksPerRange, 10, 0.01);

        AddPrimaryBloomFilter(filter, 0);

        UNIT_ASSERT(!filter.HasRangeInMixedIndex(MakeSingleBlockRange(0)));
        UNIT_ASSERT(!filter.HasRangeInMixedIndex(MakeSingleBlockRange(42)));
    }

    Y_UNIT_TEST(ShouldFindAddedBlocks)
    {
        constexpr ui64 blocksPerRange = 100;
        TMixedBlocksBloomFilter filter(blocksPerRange, 10, 0.01);

        AddPrimaryBloomFilter(filter, 0);
        filter.AddBlocks(MakeHalfOpenRange(5, 16));

        for (ui32 blockIndex = 5; blockIndex < 16; ++blockIndex) {
            UNIT_ASSERT_C(
                filter.HasRangeInMixedIndex(MakeSingleBlockRange(blockIndex)),
                "Block " << blockIndex << " must be present");
        }
    }

    Y_UNIT_TEST(ShouldNotFindBlocksThatWereNotAdded)
    {
        constexpr ui64 blocksPerRange = 100;
        TMixedBlocksBloomFilter filter(blocksPerRange, 10, 0.01);

        AddPrimaryBloomFilter(filter, 0);
        filter.AddBlocks(MakeSingleBlockRange(5));

        UNIT_ASSERT(!filter.HasRangeInMixedIndex(MakeSingleBlockRange(6)));
        UNIT_ASSERT(!filter.HasRangeInMixedIndex(MakeSingleBlockRange(99)));
    }

    Y_UNIT_TEST(ShouldIsolatePerRangeFilters)
    {
        constexpr ui64 blocksPerRange = 100;
        TMixedBlocksBloomFilter filter(blocksPerRange, 10, 0.01);

        AddPrimaryBloomFilter(filter, 0);
        AddPrimaryBloomFilter(filter, 1);

        filter.AddBlocks(MakeSingleBlockRange(5));
        filter.AddBlocks(MakeSingleBlockRange(150));

        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(5)));
        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(150)));

        UNIT_ASSERT(!filter.HasRangeInMixedIndex(MakeSingleBlockRange(50)));
        UNIT_ASSERT(!filter.HasRangeInMixedIndex(MakeSingleBlockRange(180)));
        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(200)));
    }

    Y_UNIT_TEST(ShouldStopAddingBlocksWhenRangeFilterIsMissing)
    {
        constexpr ui64 blocksPerRange = 100;
        TMixedBlocksBloomFilter filter(blocksPerRange, 10, 0.01);

        AddPrimaryBloomFilter(filter, 0);

        // Range 1 has no filter, so AddBlocks must stop at block 100.
        filter.AddBlocks(MakeHalfOpenRange(50, 110));

        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(50)));
        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(99)));
        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(100)));
    }

    Y_UNIT_TEST(ShouldFindBlocksAcrossMultipleRangesInQuery)
    {
        constexpr ui64 blocksPerRange = 100;
        TMixedBlocksBloomFilter filter(blocksPerRange, 10, 0.01);

        AddPrimaryBloomFilter(filter, 0);
        AddPrimaryBloomFilter(filter, 1);

        filter.AddBlocks(MakeSingleBlockRange(5));
        filter.AddBlocks(MakeSingleBlockRange(150));

        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeHalfOpenRange(0, 200)));
    }

    Y_UNIT_TEST(ShouldPromoteSecondaryBloomFilter)
    {
        constexpr ui64 blocksPerRange = 100;
        TMixedBlocksBloomFilter filter(blocksPerRange, 10, 0.01);

        AddPrimaryBloomFilter(filter, 0);
        filter.AddSecondaryBloomFilter(0);
        filter.AddBlocks(MakeSingleBlockRange(10));

        filter.PromoteSecondaryBloomFilter(0);

        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(10)));
        UNIT_ASSERT(!filter.HasRangeInMixedIndex(MakeSingleBlockRange(20)));
    }

    Y_UNIT_TEST(ShouldKeepOnlySecondaryContentsAfterPromotion)
    {
        constexpr ui64 blocksPerRange = 100;
        TMixedBlocksBloomFilter filter(blocksPerRange, 10, 0.01);

        AddPrimaryBloomFilter(filter, 0);
        filter.AddBlocks(MakeSingleBlockRange(5));

        filter.AddSecondaryBloomFilter(0);
        filter.AddBlocks(MakeSingleBlockRange(10));

        filter.PromoteSecondaryBloomFilter(0);

        // Primary now contains only what was in the secondary filter before
        // promotion.
        UNIT_ASSERT(!filter.HasRangeInMixedIndex(MakeSingleBlockRange(5)));
        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(10)));
    }

    Y_UNIT_TEST(ShouldHaveNoFalseNegativesForAddedBlocks)
    {
        constexpr ui64 blocksPerRange = 100;
        constexpr ui32 blocksInRange = 50;
        TMixedBlocksBloomFilter filter(blocksPerRange, blocksInRange, 0.01);

        AddPrimaryBloomFilter(filter, 0);

        for (ui32 blockIndex = 0; blockIndex < blocksInRange; ++blockIndex) {
            filter.AddBlocks(MakeSingleBlockRange(blockIndex));
        }

        for (ui32 blockIndex = 0; blockIndex < blocksInRange; ++blockIndex) {
            UNIT_ASSERT_C(
                filter.HasRangeInMixedIndex(MakeSingleBlockRange(blockIndex)),
                "Block " << blockIndex << " must be present");
        }
    }

    Y_UNIT_TEST(ShouldAddBlocksToBothPrimaryAndSecondaryBloomFilters)
    {
        constexpr ui64 blocksPerRange = 100;
        constexpr ui32 blocksInRange = 50;
        TMixedBlocksBloomFilter filter(blocksPerRange, blocksInRange, 0.01);

        AddPrimaryBloomFilter(filter, 0);
        filter.AddSecondaryBloomFilter(0);

        filter.AddBlocks(MakeSingleBlockRange(0));
        filter.AddBlocks(MakeSingleBlockRange(10));

        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(0)));
        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(10)));

        filter.PromoteSecondaryBloomFilter(0);

        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(0)));
        UNIT_ASSERT(filter.HasRangeInMixedIndex(MakeSingleBlockRange(10)));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
