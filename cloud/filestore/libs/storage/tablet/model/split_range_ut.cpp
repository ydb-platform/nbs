#include "split_range.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TBlockRange = std::pair<ui32, ui32>;

TVector<TBlockRange>
GetSplittedRanges(ui32 blockIndex, ui32 blocksCount, ui32 maxBlocksCount = 1024)
{
    TVector<TBlockRange> ranges;
    SplitRange(
        blockIndex,
        blocksCount,
        maxBlocksCount,
        [&](ui32 blockOffset, ui32 blocksCount)
        { ranges.emplace_back(blockOffset + blockIndex, blocksCount); });

    return ranges;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSplitRangeTest)
{
    Y_UNIT_TEST(ShouldHandleEmptyRange)
    {
        auto ranges = GetSplittedRanges(0, 0);

        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 0);
    }

    Y_UNIT_TEST(ShouldHandleSmallRange)
    {
        auto ranges = GetSplittedRanges(100, 200);

        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ranges[0], TBlockRange(100, 200));
    }

    Y_UNIT_TEST(ShouldHandleHeadAndTailBlocks)
    {
        auto ranges = GetSplittedRanges(100, 1000);

        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ranges[0], TBlockRange(100, 924));
        UNIT_ASSERT_VALUES_EQUAL(ranges[1], TBlockRange(1024, 76));
    }
}

}   // namespace NCloud::NFileStore::NStorage
