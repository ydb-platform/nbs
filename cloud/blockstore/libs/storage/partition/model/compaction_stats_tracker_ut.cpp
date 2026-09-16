#include "compaction_stats_tracker.h"

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
    TCompactionStatsTracker Tracker{TabletId, CompactionMap, UsedBlocks};
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompactionStatsTrackerTest)
{
    Y_UNIT_TEST(ShouldTrackActiveCompaction)
    {
        TFixture fixture;

        UNIT_ASSERT(!fixture.Tracker.HasCompaction());
        UNIT_ASSERT(!fixture.Tracker.AccessCompactionCounter(0));

        fixture.Tracker.StartCompaction(100, {1});

        UNIT_ASSERT(fixture.Tracker.HasCompaction());
        UNIT_ASSERT(fixture.Tracker.AccessCompactionCounter(1));

        fixture.Tracker.AbortCompaction();

        UNIT_ASSERT(!fixture.Tracker.HasCompaction());
        UNIT_ASSERT(!fixture.Tracker.AccessCompactionCounter(1));
    }

    Y_UNIT_TEST(ShouldReturnCountersOnlyForRequestedRanges)
    {
        TFixture fixture;

        fixture.Tracker.StartCompaction(100, {3, 1});

        UNIT_ASSERT(!fixture.Tracker.AccessCompactionCounter(0));
        UNIT_ASSERT(!fixture.Tracker.AccessCompactionCounter(2));
        UNIT_ASSERT(!fixture.Tracker.AccessCompactionCounter(4));

        const auto* firstRange = fixture.Tracker.AccessCompactionCounter(1);
        UNIT_ASSERT(firstRange);
        UNIT_ASSERT_VALUES_EQUAL(RangeSize, firstRange->BlockIndex);

        const auto* thirdRange = fixture.Tracker.AccessCompactionCounter(3);
        UNIT_ASSERT(thirdRange);
        UNIT_ASSERT_VALUES_EQUAL(3 * RangeSize, thirdRange->BlockIndex);
    }

    Y_UNIT_TEST(ShouldStartSameCompactionOnlyOnce)
    {
        TFixture fixture;

        fixture.Tracker.StartCompaction(100, {3, 1});

        auto* counter = fixture.Tracker.AccessCompactionCounter(1);
        UNIT_ASSERT(counter);
        counter->Stat.BlobCount = 10;

        fixture.Tracker.StartCompaction(100, {1, 3});

        UNIT_ASSERT_VALUES_EQUAL(
            counter,
            fixture.Tracker.AccessCompactionCounter(1));
        UNIT_ASSERT_VALUES_EQUAL(10, counter->Stat.BlobCount);
    }

    Y_UNIT_TEST(ShouldResetCountersForActiveCompaction)
    {
        TFixture fixture;

        fixture.Tracker.StartCompaction(100, {1, 2});

        auto* firstRange = fixture.Tracker.AccessCompactionCounter(1);
        auto* secondRange = fixture.Tracker.AccessCompactionCounter(2);
        UNIT_ASSERT(firstRange);
        UNIT_ASSERT(secondRange);

        firstRange->Stat.BlobCount = 10;
        firstRange->Stat.BlockCount = 20;
        firstRange->Stat.NewlyZeroedBlocks = 30;
        firstRange->Stat.MixedBlockCount = 40;
        secondRange->Stat.BlobCount = 50;

        fixture.Tracker.ResetCompaction();

        UNIT_ASSERT_VALUES_EQUAL(0, firstRange->Stat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(0, firstRange->Stat.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(0, firstRange->Stat.NewlyZeroedBlocks);
        UNIT_ASSERT_VALUES_EQUAL(0, firstRange->Stat.MixedBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(0, secondRange->Stat.BlobCount);
        UNIT_ASSERT(fixture.Tracker.HasCompaction());
    }

    Y_UNIT_TEST(ShouldUpdateCompactionMapWhenCompactionFinishes)
    {
        TFixture fixture;

        fixture.UsedBlocks.Set(1, 4);
        fixture.UsedBlocks.Set(2 * RangeSize, 2 * RangeSize + 2);

        fixture.Tracker.StartCompaction(100, {2, 0});

        auto* firstRange = fixture.Tracker.AccessCompactionCounter(0);
        UNIT_ASSERT(firstRange);
        firstRange->Stat.BlobCount = 1;
        firstRange->Stat.BlockCount = 5;
        firstRange->Stat.NewlyZeroedBlocks = 1;
        firstRange->Stat.MixedBlockCount = 3;

        auto* thirdRange = fixture.Tracker.AccessCompactionCounter(2);
        UNIT_ASSERT(thirdRange);
        thirdRange->Stat.BlobCount = 2;
        thirdRange->Stat.BlockCount = 7;
        thirdRange->Stat.MixedBlockCount = 4;

        const auto finishedRanges = fixture.Tracker.FinishCompaction();

        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>({0, 2}), finishedRanges);
        UNIT_ASSERT(!fixture.Tracker.HasCompaction());
        UNIT_ASSERT(!fixture.Tracker.AccessCompactionCounter(0));
        UNIT_ASSERT(!fixture.Tracker.AccessCompactionCounter(2));

        const auto firstRangeStat = fixture.CompactionMap.Get(0);
        UNIT_ASSERT_VALUES_EQUAL(1, firstRangeStat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(5, firstRangeStat.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(3, firstRangeStat.UsedBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(1, firstRangeStat.NewlyZeroedBlocks);
        UNIT_ASSERT_VALUES_EQUAL(3, firstRangeStat.MixedBlockCount);
        UNIT_ASSERT(firstRangeStat.Compacted);

        const auto thirdRangeStat = fixture.CompactionMap.Get(2 * RangeSize);
        UNIT_ASSERT_VALUES_EQUAL(2, thirdRangeStat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(7, thirdRangeStat.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(2, thirdRangeStat.UsedBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(4, thirdRangeStat.MixedBlockCount);
        UNIT_ASSERT(thirdRangeStat.Compacted);
    }

    Y_UNIT_TEST(ShouldDiscardCountersWhenCompactionIsAborted)
    {
        TFixture fixture;

        fixture.CompactionMap.Update(RangeSize, 4, 6, 5, 2, 3, false);

        fixture.Tracker.StartCompaction(100, {1});

        auto* counter = fixture.Tracker.AccessCompactionCounter(1);
        UNIT_ASSERT(counter);
        counter->Stat.BlobCount = 1;
        counter->Stat.BlockCount = 2;

        fixture.Tracker.AbortCompaction();

        UNIT_ASSERT(!fixture.Tracker.HasCompaction());
        UNIT_ASSERT(!fixture.Tracker.AccessCompactionCounter(1));

        const auto rangeStat = fixture.CompactionMap.Get(RangeSize);
        UNIT_ASSERT_VALUES_EQUAL(4, rangeStat.BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(6, rangeStat.BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(5, rangeStat.UsedBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(2, rangeStat.NewlyZeroedBlocks);
        UNIT_ASSERT_VALUES_EQUAL(3, rangeStat.MixedBlockCount);
        UNIT_ASSERT(!rangeStat.Compacted);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
