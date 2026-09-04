#include "inflight_compaction_counters.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 RangeSize = 8;
constexpr ui64 TabletId = 42;

struct TFixture
{
    TCompactionMap CompactionMap{RangeSize, BuildDefaultCompactionPolicy(5, 0)};
    TCompressedBitmap UsedBlocks{4 * RangeSize};
    TInflightCompactionCounters Counters{TabletId, CompactionMap, UsedBlocks};
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInflightCompactionCountersTest)
{
    Y_UNIT_TEST(ShouldReturnCountersOnlyForRequestedRanges)
    {
        TFixture fixture;

        fixture.Counters.CompactionStarted(100, {3, 1});

        UNIT_ASSERT(fixture.Counters.GetCompactionCounters(0).empty());
        UNIT_ASSERT(fixture.Counters.GetCompactionCounters(2).empty());
        UNIT_ASSERT(fixture.Counters.GetCompactionCounters(4).empty());

        const auto firstRange = fixture.Counters.GetCompactionCounters(1);
        UNIT_ASSERT_VALUES_EQUAL(1, firstRange.size());
        UNIT_ASSERT_VALUES_EQUAL(RangeSize, firstRange[0]->BlockIndex);

        const auto thirdRange = fixture.Counters.GetCompactionCounters(3);
        UNIT_ASSERT_VALUES_EQUAL(1, thirdRange.size());
        UNIT_ASSERT_VALUES_EQUAL(3 * RangeSize, thirdRange[0]->BlockIndex);
    }

    Y_UNIT_TEST(ShouldReturnCountersForAllOverlappingCompactions)
    {
        TFixture fixture;

        fixture.Counters.CompactionStarted(100, {1, 2});
        fixture.Counters.CompactionStarted(200, {2, 3});

        UNIT_ASSERT_VALUES_EQUAL(
            1, fixture.Counters.GetCompactionCounters(1).size());
        UNIT_ASSERT_VALUES_EQUAL(
            2, fixture.Counters.GetCompactionCounters(2).size());
        UNIT_ASSERT_VALUES_EQUAL(
            1, fixture.Counters.GetCompactionCounters(3).size());
    }

    Y_UNIT_TEST(ShouldClearCountersForSpecifiedCompaction)
    {
        TFixture fixture;

        fixture.Counters.CompactionStarted(100, {1, 2});
        fixture.Counters.CompactionStarted(200, {2, 3});

        auto firstRange = fixture.Counters.GetCompactionCounters(1);
        auto overlappingRange = fixture.Counters.GetCompactionCounters(2);
        auto thirdRange = fixture.Counters.GetCompactionCounters(3);

        firstRange[0]->Stat.BlobCount = 10;
        overlappingRange[0]->Stat.BlobCount = 20;
        overlappingRange[1]->Stat.BlobCount = 21;
        thirdRange[0]->Stat.BlobCount = 30;

        fixture.Counters.ClearCountersForCompaction(100);

        UNIT_ASSERT_VALUES_EQUAL(0, firstRange[0]->Stat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(30, thirdRange[0]->Stat.BlobCount);

        ui32 clearedCounterCount = 0;
        ui32 preservedCounterCount = 0;
        for (const auto* counter: overlappingRange) {
            if (counter->Stat.BlobCount == 0) {
                ++clearedCounterCount;
            } else {
                ++preservedCounterCount;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(1, clearedCounterCount);
        UNIT_ASSERT_VALUES_EQUAL(1, preservedCounterCount);
    }

    Y_UNIT_TEST(ShouldUpdateCompactionMapWhenCompactionFinishes)
    {
        TFixture fixture;

        fixture.UsedBlocks.Set(1, 4);
        fixture.UsedBlocks.Set(2 * RangeSize, 2 * RangeSize + 2);

        fixture.Counters.CompactionStarted(100, {2, 0});

        auto firstRange = fixture.Counters.GetCompactionCounters(0);
        firstRange[0]->Stat.BlobCount = 1;
        firstRange[0]->Stat.BlockCount = 5;
        firstRange[0]->Stat.NewlyZeroedBlocks = 1;

        auto thirdRange = fixture.Counters.GetCompactionCounters(2);
        thirdRange[0]->Stat.BlobCount = 2;
        thirdRange[0]->Stat.BlockCount = 7;

        const auto finishedRanges = fixture.Counters.FinishRangeCompaction(100);

        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>({0, 2}), finishedRanges);
        UNIT_ASSERT(fixture.Counters.GetCompactionCounters(0).empty());
        UNIT_ASSERT(fixture.Counters.GetCompactionCounters(2).empty());

        const auto firstRangeStat = fixture.CompactionMap.Get(0);
        UNIT_ASSERT_VALUES_EQUAL(1, firstRangeStat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(5, firstRangeStat.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(3, firstRangeStat.UsedBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(1, firstRangeStat.NewlyZeroedBlocks);
        UNIT_ASSERT(firstRangeStat.Compacted);

        const auto thirdRangeStat = fixture.CompactionMap.Get(2 * RangeSize);
        UNIT_ASSERT_VALUES_EQUAL(2, thirdRangeStat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(7, thirdRangeStat.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(2, thirdRangeStat.UsedBlockCount);
        UNIT_ASSERT(thirdRangeStat.Compacted);
    }

    Y_UNIT_TEST(ShouldDiscardCountersWhenCompactionFails)
    {
        TFixture fixture;

        fixture.CompactionMap.Update(RangeSize, 4, 6, 5, 0, 0, false);

        fixture.Counters.CompactionStarted(100, {1});
        auto counters = fixture.Counters.GetCompactionCounters(1);
        counters[0]->Stat.BlobCount = 1;
        counters[0]->Stat.BlockCount = 2;

        fixture.Counters.CompactionFailed(100);

        UNIT_ASSERT(fixture.Counters.GetCompactionCounters(1).empty());

        const auto rangeStat = fixture.CompactionMap.Get(RangeSize);
        UNIT_ASSERT_VALUES_EQUAL(4, rangeStat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(6, rangeStat.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(5, rangeStat.UsedBlockCount);
        UNIT_ASSERT(!rangeStat.Compacted);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
