#include "compaction_map.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompactionMapTest)
{
    Y_UNIT_TEST(ShouldKeepRangeStats)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 10, 100);
        compactionMap.Update(1, 11, 101);
        compactionMap.Update(10000, 20, 200);
        compactionMap.Update(20000, 5, 50);

        auto stats = compactionMap.Get(0);
        UNIT_ASSERT_VALUES_EQUAL(10, stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(100, stats.DeletionsCount);

        stats = compactionMap.Get(1);
        UNIT_ASSERT_VALUES_EQUAL(11, stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(101, stats.DeletionsCount);

        stats = compactionMap.Get(10000);
        UNIT_ASSERT_VALUES_EQUAL(20, stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(200, stats.DeletionsCount);

        stats = compactionMap.Get(20000);
        UNIT_ASSERT_VALUES_EQUAL(5, stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(50, stats.DeletionsCount);

        stats = compactionMap.Get(30000);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.DeletionsCount);
    }

    Y_UNIT_TEST(ShouldSelectTopRangesByCompactionScore)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 10, 100);
        compactionMap.Update(1, 11, 101);
        compactionMap.Update(10000, 20, 200);
        compactionMap.Update(20000, 5, 50);

        auto counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 20);

        compactionMap.Update(10000, 1, 200);

        counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 11);

        compactionMap.Update(1, 1, 101);

        counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 10);

        compactionMap.Update(0, 1, 100);

        counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 20000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 5);

        compactionMap.Update(20000, 1, 50);

        counter = compactionMap.GetTopCompactionScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 1);
    }

    Y_UNIT_TEST(ShouldSelectTopRangesByCleanupScore)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 10, 100);
        compactionMap.Update(1, 11, 101);
        compactionMap.Update(10000, 20, 200);
        compactionMap.Update(20000, 5, 50);

        auto counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 10000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 200);

        compactionMap.Update(10000, 20, 0);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 101);

        compactionMap.Update(1, 11, 0);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 0);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 100);

        compactionMap.Update(0, 10, 0);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.RangeId, 20000);
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 50);

        compactionMap.Update(20000, 5, 0);

        counter = compactionMap.GetTopCleanupScore();
        UNIT_ASSERT_VALUES_EQUAL(counter.Score, 0);
    }

    Y_UNIT_TEST(ShouldSelectTopRangesForMonitoring)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 10, 444);
        compactionMap.Update(1, 11, 222);
        compactionMap.Update(10000, 20, 111);
        compactionMap.Update(20000, 5, 333);

        auto topRanges = compactionMap.GetTopRangesByCompactionScore(3);
        UNIT_ASSERT_VALUES_EQUAL(3, topRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(10000, topRanges[0].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(20, topRanges[0].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(111, topRanges[0].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(1, topRanges[1].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(11, topRanges[1].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(222, topRanges[1].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(0, topRanges[2].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(10, topRanges[2].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(444, topRanges[2].Stats.DeletionsCount);

        topRanges = compactionMap.GetTopRangesByCleanupScore(3);
        UNIT_ASSERT_VALUES_EQUAL(3, topRanges.size());
        UNIT_ASSERT_VALUES_EQUAL(0, topRanges[0].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(10, topRanges[0].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(444, topRanges[0].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(20000, topRanges[1].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(5, topRanges[1].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(333, topRanges[1].Stats.DeletionsCount);
        UNIT_ASSERT_VALUES_EQUAL(1, topRanges[2].RangeId);
        UNIT_ASSERT_VALUES_EQUAL(11, topRanges[2].Stats.BlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(222, topRanges[2].Stats.DeletionsCount);
    }

    Y_UNIT_TEST(ShouldReturnNonEmptyRanges)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(0, 15, 100);
        compactionMap.Update(1, 14, 101);
        compactionMap.Update(3, 13, 101);
        compactionMap.Update(256, 40, 101);
        compactionMap.Update(10000, 45, 200);
        compactionMap.Update(20000, 50, 50);
        compactionMap.Update(Max<ui32>(), 100, 50);

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

        compactionMap.Update(100, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 0);

        compactionMap.Update(100, 100, 100);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 1 * group);

        compactionMap.Update(100, 0, 100);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 1 * group);

        compactionMap.Update(100, 100, 0);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 1 * group);

        compactionMap.Update(101, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 1 * group);

        compactionMap.Update(1000, 10, 0);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 2 * group);

        compactionMap.Update(100, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 1 * group);

        compactionMap.Update(1000, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 0);

        compactionMap.Update(1000, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 0);
    }

    Y_UNIT_TEST(ShouldKeepTrackNonEmptyRanges)
    {
        const auto group = TCompactionMap::GroupSize;
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).UsedRangesCount, 0);

        for (ui32 i = 1; i <= TCompactionMap::GroupSize; ++i) {
            compactionMap.Update(i - 1, 0, 0);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).UsedRangesCount, i - 1);

            compactionMap.Update(i - 1, 100, 0);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 1 * group);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).UsedRangesCount, i);

            compactionMap.Update(i - 1, 0, 100);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 1 * group);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).UsedRangesCount, i);

            compactionMap.Update(i - 1, 100, 100);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 1 * group);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).UsedRangesCount, i);
        }

        compactionMap.Update(1000000, 100, 100);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 2 * group);
        UNIT_ASSERT_VALUES_EQUAL(
            compactionMap.GetStats(1).UsedRangesCount,
            TCompactionMap::GroupSize + 1);

        for (ui32 i = TCompactionMap::GroupSize; i > 0; --i) {
            compactionMap.Update(i - 1, 100, 0);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).UsedRangesCount, i + 1);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 2 * group);

            compactionMap.Update(i - 1, 0, 100);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).UsedRangesCount, i + 1);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 2 * group);

            compactionMap.Update(i - 1, 0, 0);
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).UsedRangesCount, i);

            ui32 groups = i - 1 > 0 ? 2 : 1;
            UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, groups * group);
        }

        compactionMap.Update(1000000, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).AllocatedRangesCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(compactionMap.GetStats(1).UsedRangesCount, 0);
    }

    Y_UNIT_TEST(ShouldReturnTopRanges)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        {
            auto stats = compactionMap.GetStats(2);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore.size(), 0);
        }

        compactionMap.Update(1, 10, 20);

        {
            auto stats = compactionMap.GetStats(2);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore[0].RangeId, 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore[0].Stats.BlobsCount, 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore[0].Stats.DeletionsCount, 20);

            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore[0].RangeId, 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore[0].Stats.BlobsCount, 10);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore[0].Stats.DeletionsCount, 20);
        }

        compactionMap.Update(0, 1000, 444);
        compactionMap.Update(300, 40, 522);
        compactionMap.Update(400, 50, 22);
        compactionMap.Update(10000, 20, 211);
        compactionMap.Update(20000, 100500, 4);
        compactionMap.Update(70000, 500, 100);
        compactionMap.Update(200000, 100, 400);

        {
            auto stats = compactionMap.GetStats(2);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore.size(), 2);

            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore[0].RangeId, 300);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore[0].Stats.BlobsCount, 40);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore[0].Stats.DeletionsCount, 522);

            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore[1].RangeId, 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore[1].Stats.BlobsCount, 1000);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCleanupScore[1].Stats.DeletionsCount, 444);

            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore.size(), 2);

            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore[0].RangeId, 20000);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore[0].Stats.BlobsCount, 100500);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore[0].Stats.DeletionsCount, 4);

            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore[1].RangeId, 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore[1].Stats.BlobsCount, 1000);
            UNIT_ASSERT_VALUES_EQUAL(stats.TopRangesByCompactionScore[1].Stats.DeletionsCount, 444);
        }
    }

    Y_UNIT_TEST(ShouldTrackTotalBlobsCountAndDeletionsCount)
    {
        TCompactionMap compactionMap(TDefaultAllocator::Instance());

        compactionMap.Update(1, 10, 20);
        compactionMap.Update(2, 5, 40);
        compactionMap.Update(2, 15, 50);
        compactionMap.Update(100, 7, 100);

        auto stats = compactionMap.GetStats(1);
        UNIT_ASSERT_VALUES_EQUAL(32, stats.TotalBlobsCount);
        UNIT_ASSERT_VALUES_EQUAL(170, stats.TotalDeletionsCount);
    }
}

}   // namespace NCloud::NFileStore::NStorage
