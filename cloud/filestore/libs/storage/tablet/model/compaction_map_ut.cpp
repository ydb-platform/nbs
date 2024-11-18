#include "compaction_map.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompactionMapTest)
{
    Y_UNIT_TEST(ShouldKeepRangeStats)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 10, 100, 1000, false);
        compactionMap.Update(1, 11, 101, 2000, false);
        compactionMap.Update(10000, 20, 200, 100, false);
        compactionMap.Update(20000, 5, 50, 300, false);

        auto stats = compactionMap.Get(0);
        UNIT_ASSERT_VALUES_EQUAL(10, stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(100, stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(1000, stats.GarbageBlocksCount);

        stats = compactionMap.Get(1);
        UNIT_ASSERT_VALUES_EQUAL(11, stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(101, stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(2000, stats.GarbageBlocksCount);

        stats = compactionMap.Get(10000);
        UNIT_ASSERT_VALUES_EQUAL(20, stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(200, stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(100, stats.GarbageBlocksCount);

        stats = compactionMap.Get(20000);
        UNIT_ASSERT_VALUES_EQUAL(5, stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(50, stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(300, stats.GarbageBlocksCount);

        stats = compactionMap.Get(30000);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GarbageBlocksCount);
    }

    Y_UNIT_TEST(ShouldSelectTopRangesByCompactionScore)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 10, 100, 1000, false);
        compactionMap.Update(1, 11, 101, 2000, false);
        compactionMap.Update(10000, 20, 200, 100, false);
        compactionMap.Update(20000, 5, 50, 300, false);

        auto counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 20);

        compactionMap.Update(10000, 1, 200, 100, false);

        counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 11);

        compactionMap.Update(1, 1, 101, 2000, false);

        counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 10);

        compactionMap.Update(0, 1, 100, 1000, false);

        counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 20000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 5);

        compactionMap.Update(20000, 1, 50, 300, false);

        counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 1);
    }

    Y_UNIT_TEST(ShouldSelectTopRangesByCleanupScore)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 10, 100, 1000, false);
        compactionMap.Update(1, 11, 101, 2000, false);
        compactionMap.Update(10000, 20, 200, 100, false);
        compactionMap.Update(20000, 5, 50, 300, false);

        auto counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 200);

        compactionMap.Update(10000, 20, 0, 100, false);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 101);

        compactionMap.Update(1, 11, 0, 2000, false);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 100);

        compactionMap.Update(0, 10, 0, 1000, false);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 20000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 50);

        compactionMap.Update(20000, 5, 0, 300, false);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 0);
    }

    Y_UNIT_TEST(ShouldSelectTopRangesByGarbageScore)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 10, 100, 1000, false);
        compactionMap.Update(1, 11, 101, 2000, false);
        compactionMap.Update(10000, 20, 200, 100, false);
        compactionMap.Update(20000, 5, 50, 300, false);

        auto counter = compactionMap.GetTopGarbageScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 2000);

        compactionMap.Update(1, 11, 101, 500, false);

        counter = compactionMap.GetTopGarbageScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 1000);

        compactionMap.Update(0, 10, 100, 0, false);

        counter = compactionMap.GetTopGarbageScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 500);

        compactionMap.Update(1, 11, 101, 0, false);

        counter = compactionMap.GetTopGarbageScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 20000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 300);

        compactionMap.Update(20000, 5, 50, 0, false);

        counter = compactionMap.GetTopGarbageScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 100);

        compactionMap.Update(10000, 20, 200, 0, false);

        counter = compactionMap.GetTopGarbageScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 0);
    }

    Y_UNIT_TEST(ShouldSelectTopRangesForMonitoring)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 10, 444, 1000, false);
        compactionMap.Update(1, 11, 222, 2000, false);
        compactionMap.Update(10000, 20, 111, 200, false);
        compactionMap.Update(20000, 5, 333, 300, false);

        auto topRanges = compactionMap.GetTopRangesByCompactionScore(3);
        UNIT_ASSERT_VALUES_EQUAL(3, topRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(10000, topRanges[0].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(20, topRanges[0].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(111, topRanges[0].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(200, topRanges[0].Stats.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(1, topRanges[1].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(11, topRanges[1].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(222, topRanges[1].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(2000, topRanges[1].Stats.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(0, topRanges[2].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(10, topRanges[2].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(444, topRanges[2].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(1000, topRanges[2].Stats.GarbageBlocksCount);

        topRanges = compactionMap.GetTopRangesByCleanupScore(3);
        UNIT_ASSERT_VALUES_EQUAL(3, topRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(0, topRanges[0].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(10, topRanges[0].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(444, topRanges[0].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(1000, topRanges[0].Stats.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(20000, topRanges[1].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(5, topRanges[1].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(333, topRanges[1].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(300, topRanges[1].Stats.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(1, topRanges[2].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(11, topRanges[2].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(222, topRanges[2].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(2000, topRanges[2].Stats.GarbageBlocksCount);

        topRanges = compactionMap.GetTopRangesByGarbageScore(3);
        UNIT_ASSERT_VALUES_EQUAL(3, topRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(1, topRanges[0].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(11, topRanges[0].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(222, topRanges[0].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(2000, topRanges[0].Stats.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(0, topRanges[1].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(10, topRanges[1].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(444, topRanges[1].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(1000, topRanges[1].Stats.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(20000, topRanges[2].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(5, topRanges[2].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(333, topRanges[2].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(300, topRanges[2].Stats.GarbageBlocksCount);
    }

    Y_UNIT_TEST(ShouldPessimizeCompactedRanges)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 10, 100, 1000, false);
        compactionMap.Update(1, 20, 200, 2000, false);
        compactionMap.Update(10000, 30, 300, 3000, false);
        compactionMap.Update(10001, 40, 400, 4000, false);

        auto counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10001);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 40);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10001);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 400);

        counter = compactionMap.GetTopGarbageScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10001);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 4000);

        auto topRanges = compactionMap.GetTopRangesByCompactionScore(3);
        UNIT_ASSERT_VALUES_EQUAL(3, topRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(10001, topRanges[0].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(40, topRanges[0].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(400, topRanges[0].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(4000, topRanges[0].Stats.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(10000, topRanges[1].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(30, topRanges[1].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(300, topRanges[1].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(3000, topRanges[1].Stats.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(1, topRanges[2].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(20, topRanges[2].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(200, topRanges[2].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(2000, topRanges[2].Stats.GarbageBlocksCount);

        compactionMap.Update(10001, 40, 400, 4000, true);

        counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 30);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10001);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 400);

        counter = compactionMap.GetTopGarbageScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 3000);

        topRanges = compactionMap.GetTopRangesByCompactionScore(3);
        UNIT_ASSERT_VALUES_EQUAL(3, topRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(10000, topRanges[0].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(30, topRanges[0].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(300, topRanges[0].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(3000, topRanges[0].Stats.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(1, topRanges[1].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(20, topRanges[1].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(200, topRanges[1].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(2000, topRanges[1].Stats.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(0, topRanges[2].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(10, topRanges[2].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(100, topRanges[2].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(1000, topRanges[2].Stats.GarbageBlocksCount);

        compactionMap.Update(10000, 30, 300, 3000, true);

        counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 20);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10001);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 400);

        counter = compactionMap.GetTopGarbageScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 2000);

        topRanges = compactionMap.GetTopRangesByCompactionScore(3);
        UNIT_ASSERT_VALUES_EQUAL(2, topRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(1, topRanges[0].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(20, topRanges[0].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(200, topRanges[0].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(2000, topRanges[0].Stats.GarbageBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL(0, topRanges[1].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(10, topRanges[1].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(100, topRanges[1].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(1000, topRanges[1].Stats.GarbageBlocksCount);

        compactionMap.Update(1, 20, 200, 2000, true);
        compactionMap.Update(0, 10, 100, 1000, true);

        topRanges = compactionMap.GetTopRangesByCompactionScore(3);
        UNIT_ASSERT_VALUES_EQUAL(0, topRanges.size());

        counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(0, counter.Score);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(10001, counter.RangeId);
        UNIT_ASSERT_VALUES_EQUAL(400, counter.Score);

        counter = compactionMap.GetTopGarbageScore();
        UNIT_ASSERT_VALUES_EQUAL(0, counter.Score);
    }

    Y_UNIT_TEST(ShouldReturnNonEmptyRanges)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 15, 100, 1000, false);
        compactionMap.Update(1, 14, 101, 2000, false);
        compactionMap.Update(3, 13, 101, 300, false);
        compactionMap.Update(256, 40, 101, 200, false);
        compactionMap.Update(10000, 45, 200, 3000, false);
        compactionMap.Update(20000, 50, 50, 500, false);
        compactionMap.Update(Max<ui32>(), 100, 50, 400, false);

        auto ranges = compactionMap.GetNonEmptyCompactionRanges();

        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 7);
        UNIT_ASSERT_VALUES_EQUAL(ranges[0], 0);
        UNIT_ASSERT_VALUES_EQUAL(ranges[1], 1);
        UNIT_ASSERT_VALUES_EQUAL(ranges[2], 3);
        UNIT_ASSERT_VALUES_EQUAL(ranges[3], 256);
        UNIT_ASSERT_VALUES_EQUAL(ranges[4], 10000);
        UNIT_ASSERT_VALUES_EQUAL(ranges[5], 20000);
        UNIT_ASSERT_VALUES_EQUAL(ranges[6], Max<ui32>());
    }

    Y_UNIT_TEST(ShouldReleaseEmptyRanges)
    {
        const auto group = TCompactionMap::GroupSize;
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(100, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            compactionMap.GetStats(1).AllocatedRangesCount);

        compactionMap.Update(100, 100, 100, 100, false);
        UNIT_ASSERT_VALUES_EQUAL(
            1 * group,
            compactionMap.GetStats(1).AllocatedRangesCount);

        compactionMap.Update(100, 0, 100, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            1 * group,
            compactionMap.GetStats(1).AllocatedRangesCount);

        compactionMap.Update(100, 100, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            1 * group,
            compactionMap.GetStats(1).AllocatedRangesCount);

        compactionMap.Update(101, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            1 * group,
            compactionMap.GetStats(1).AllocatedRangesCount);

        compactionMap.Update(1000, 10, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            2 * group,
            compactionMap.GetStats(1).AllocatedRangesCount);

        compactionMap.Update(100, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            1 * group,
            compactionMap.GetStats(1).AllocatedRangesCount);

        compactionMap.Update(1000, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            compactionMap.GetStats(1).AllocatedRangesCount);

        compactionMap.Update(1000, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            compactionMap.GetStats(1).AllocatedRangesCount);

        compactionMap.Update(1000, 0, 0, 100, false);
        UNIT_ASSERT_VALUES_EQUAL(
            1 * group,
            compactionMap.GetStats(1).AllocatedRangesCount);
    }

    Y_UNIT_TEST(ShouldKeepTrackOfNonEmptyRanges)
    {
        const auto group = TCompactionMap::GroupSize;
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            compactionMap.GetStats(1).AllocatedRangesCount);
        UNIT_ASSERT_VALUES_EQUAL(0, compactionMap.GetStats(1).UsedRangesCount);

        for (ui32 i = 1; i <= TCompactionMap::GroupSize; ++i) {
            compactionMap.Update(i - 1, 0, 0, 0, false);
            UNIT_ASSERT_VALUES_EQUAL(
                i - 1,
                compactionMap.GetStats(1).UsedRangesCount);

            compactionMap.Update(i - 1, 100, 0, 0, false);
            UNIT_ASSERT_VALUES_EQUAL(
                1 * group,
                compactionMap.GetStats(1).AllocatedRangesCount);
            UNIT_ASSERT_VALUES_EQUAL(
                i,
                compactionMap.GetStats(1).UsedRangesCount);

            compactionMap.Update(i - 1, 0, 100, 0, false);
            UNIT_ASSERT_VALUES_EQUAL(
                1 * group,
                compactionMap.GetStats(1).AllocatedRangesCount);
            UNIT_ASSERT_VALUES_EQUAL(
                i,
                compactionMap.GetStats(1).UsedRangesCount);

            compactionMap.Update(i - 1, 100, 100, 0, false);
            UNIT_ASSERT_VALUES_EQUAL(
                1 * group,
                compactionMap.GetStats(1).AllocatedRangesCount);
            UNIT_ASSERT_VALUES_EQUAL(
                i,
                compactionMap.GetStats(1).UsedRangesCount);
        }

        compactionMap.Update(1000000, 100, 100, 100, false);
        UNIT_ASSERT_VALUES_EQUAL(
            2 * group,
            compactionMap.GetStats(1).AllocatedRangesCount);
        UNIT_ASSERT_VALUES_EQUAL(
            TCompactionMap::GroupSize + 1,
            compactionMap.GetStats(1).UsedRangesCount);

        for (ui32 i = TCompactionMap::GroupSize; i > 0; --i) {
            compactionMap.Update(i - 1, 100, 0, 0, false);
            UNIT_ASSERT_VALUES_EQUAL(
                i + 1,
                compactionMap.GetStats(1).UsedRangesCount);
            UNIT_ASSERT_VALUES_EQUAL(
                2 * group,
                compactionMap.GetStats(1).AllocatedRangesCount);

            compactionMap.Update(i - 1, 0, 100, 0, false);
            UNIT_ASSERT_VALUES_EQUAL(
                i + 1,
                compactionMap.GetStats(1).UsedRangesCount);
            UNIT_ASSERT_VALUES_EQUAL(
                2 * group,
                compactionMap.GetStats(1).AllocatedRangesCount);

            compactionMap.Update(i - 1, 0, 0, 0, false);
            UNIT_ASSERT_VALUES_EQUAL(
                i,
                compactionMap.GetStats(1).UsedRangesCount);

            ui32 groups = i - 1 > 0 ? 2 : 1;
            UNIT_ASSERT_VALUES_EQUAL(
                groups * group,
                compactionMap.GetStats(1).AllocatedRangesCount);
        }

        compactionMap.Update(1000000, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            compactionMap.GetStats(1).AllocatedRangesCount);
        UNIT_ASSERT_VALUES_EQUAL(0, compactionMap.GetStats(1).UsedRangesCount);
    }

    Y_UNIT_TEST(ShouldReturnTopRanges)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        {
            auto stats = compactionMap.GetStats(2);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore.size(), 0);
        }

        compactionMap.Update(1, 10, 20, 30, false);

        {
            auto stats = compactionMap.GetStats(2);
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats.TopRangesByCleanupScore.size());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats.TopRangesByCleanupScore[0].RangeId);
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                stats.TopRangesByCleanupScore[0].Stats.BlobsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                20,
                stats.TopRangesByCleanupScore[0].Stats.DeletionsCount);

            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats.TopRangesByCompactionScore.size());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats.TopRangesByCompactionScore[0].RangeId);
            UNIT_ASSERT_VALUES_EQUAL(
                10,
                stats.TopRangesByCompactionScore[0].Stats.BlobsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                20,
                stats.TopRangesByCompactionScore[0].Stats.DeletionsCount);
        }

        compactionMap.Update(0, 1000, 444, 1000, false);
        compactionMap.Update(300, 40, 522, 300, false);
        compactionMap.Update(400, 50, 22, 2000, false);
        compactionMap.Update(10000, 20, 211, 5000, false);
        compactionMap.Update(20000, 100500, 4, 7000, false);
        compactionMap.Update(70000, 500, 100, 0, false);
        compactionMap.Update(200000, 100, 400, 30, false);

        {
            auto stats = compactionMap.GetStats(2);
            UNIT_ASSERT_VALUES_EQUAL(2, stats.TopRangesByCleanupScore.size());

            UNIT_ASSERT_VALUES_EQUAL(
                300,
                stats.TopRangesByCleanupScore[0].RangeId);
            UNIT_ASSERT_VALUES_EQUAL(
                40,
                stats.TopRangesByCleanupScore[0].Stats.BlobsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                522,
                stats.TopRangesByCleanupScore[0].Stats.DeletionsCount);

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                stats.TopRangesByCleanupScore[1].RangeId);
            UNIT_ASSERT_VALUES_EQUAL(
                1000,
                stats.TopRangesByCleanupScore[1].Stats.BlobsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                444,
                stats.TopRangesByCleanupScore[1].Stats.DeletionsCount);

            UNIT_ASSERT_VALUES_EQUAL(
                2,
                stats.TopRangesByCompactionScore.size());

            UNIT_ASSERT_VALUES_EQUAL(
                20000,
                stats.TopRangesByCompactionScore[0].RangeId);
            UNIT_ASSERT_VALUES_EQUAL(
                100500,
                stats.TopRangesByCompactionScore[0].Stats.BlobsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                4,
                stats.TopRangesByCompactionScore[0].Stats.DeletionsCount);

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                stats.TopRangesByCompactionScore[1].RangeId);
            UNIT_ASSERT_VALUES_EQUAL(
                1000,
                stats.TopRangesByCompactionScore[1].Stats.BlobsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                444,
                stats.TopRangesByCompactionScore[1].Stats.DeletionsCount);

            UNIT_ASSERT_VALUES_EQUAL(2, stats.TopRangesByGarbageScore.size());

            UNIT_ASSERT_VALUES_EQUAL(
                20000,
                stats.TopRangesByGarbageScore[0].RangeId);
            UNIT_ASSERT_VALUES_EQUAL(
                100500,
                stats.TopRangesByGarbageScore[0].Stats.BlobsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                4,
                stats.TopRangesByGarbageScore[0].Stats.DeletionsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                7000,
                stats.TopRangesByGarbageScore[0].Stats.GarbageBlocksCount);

            UNIT_ASSERT_VALUES_EQUAL(
                10000,
                stats.TopRangesByGarbageScore[1].RangeId);
            UNIT_ASSERT_VALUES_EQUAL(
                20,
                stats.TopRangesByGarbageScore[1].Stats.BlobsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                211,
                stats.TopRangesByGarbageScore[1].Stats.DeletionsCount);
            UNIT_ASSERT_VALUES_EQUAL(
                5000,
                stats.TopRangesByGarbageScore[1].Stats.GarbageBlocksCount);
        }
    }

    Y_UNIT_TEST(ShouldTrackTotals)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(1, 10, 20, 100, false);
        compactionMap.Update(2, 5, 40, 200, false);
        compactionMap.Update(2, 15, 50, 90, false);
        compactionMap.Update(100, 7, 100, 3000, false);

        auto stats = compactionMap.GetStats(1);
        UNIT_ASSERT_VALUES_EQUAL(32, stats.TotalBlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(170, stats.TotalDeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(3190, stats.TotalGarbageBlocksCount);
    }

    Y_UNIT_TEST(ShouldUpdateCompactionMapByGroups)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        TVector<TCompactionRangeInfo> rangeInfos;
        const auto groupSize = TCompactionMap::GroupSize;
        ui32 rangeId = 0;
        ui32 totalBlobsCount = 0;
        ui32 totalDeletionsCount = 0;
        ui32 totalGarbageBlocksCount = 0;
        while (rangeId < 3 * groupSize) {
            rangeInfos.emplace_back(
                rangeId,
                TCompactionStats{rangeId * 2, rangeId + 1, rangeId * 10});
            const auto& last = rangeInfos.back();
            totalBlobsCount += last.Stats.BlobsCount;
            totalDeletionsCount += last.Stats.DeletionsCount;
            totalGarbageBlocksCount += last.Stats.GarbageBlocksCount;
            rangeId += 2;
        }
        compactionMap.Update(rangeInfos);

        for (ui32 i = 0; i < 3 * groupSize; ++i) {
            auto stats = compactionMap.Get(i);
            if (i % 2) {
                UNIT_ASSERT_VALUES_EQUAL(0, stats.BlobsCount);
                UNIT_ASSERT_VALUES_EQUAL(0, stats.DeletionsCount);
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GarbageBlocksCount);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(i * 2, stats.BlobsCount);
                UNIT_ASSERT_VALUES_EQUAL(i + 1, stats.DeletionsCount);
                UNIT_ASSERT_VALUES_EQUAL(i * 10, stats.GarbageBlocksCount);
            }
        }

        compactionMap.Update(1, 10000, 20000, 30000, false);
        auto stats = compactionMap.GetStats(2);
        UNIT_ASSERT_VALUES_EQUAL(
            totalBlobsCount + 10000,
            stats.TotalBlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(
            totalDeletionsCount + 20000,
            stats.TotalDeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(
            totalGarbageBlocksCount + 30000,
            stats.TotalGarbageBlocksCount);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            stats.TopRangesByCompactionScore[0].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(
            10000,
            stats.TopRangesByCompactionScore[0].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(
            20000,
            stats.TopRangesByCompactionScore[0].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(
            30000,
            stats.TopRangesByCompactionScore[0].Stats.GarbageBlocksCount);

        UNIT_ASSERT_VALUES_EQUAL(
            3 * groupSize - 2,
            stats.TopRangesByCompactionScore[1].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(
            (3 * groupSize - 2) * 2,
            stats.TopRangesByCompactionScore[1].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(
            3 * groupSize - 1,
            stats.TopRangesByCompactionScore[1].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(
            (3 * groupSize - 2) * 10,
            stats.TopRangesByCompactionScore[1].Stats.GarbageBlocksCount);
    }
}

}   // namespace NCloud::NFileStore::NStorage
