#include "compaction_map.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui32 RangeSize = 1024;

ui32 GetGroupIndex(ui32 group)
{
    return group * RangeSize * TCompactionMap::GroupSize;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompactionMapTest)
{
    Y_UNIT_TEST(ShouldBeEmptyAtStart)
    {
        TCompactionMap map(RangeSize, BuildDefaultCompactionPolicy(5));
        for (size_t i = 1; i <= 100; ++i) {
            const auto stat = map.Get(GetGroupIndex(i));
            UNIT_ASSERT_VALUES_EQUAL(0, stat.BlobCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.BlockCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.UsedBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.ReadRequestCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.ReadRequestBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(false, stat.Compacted);
        }
    }

    Y_UNIT_TEST(ShouldKeepCompactionCounters)
    {
        TCompactionMap map(RangeSize, BuildDefaultCompactionPolicy(5));
        for (size_t i = 1; i <= 100; ++i) {
            map.Update(GetGroupIndex(i), i, i * 10, i * 5, false);
            map.RegisterRead(GetGroupIndex(i), i + 1, i * 10 + 5);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, map.Get(GetGroupIndex(0)).BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(0, map.Get(GetGroupIndex(0)).BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(0, map.Get(GetGroupIndex(0)).UsedBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(0, map.Get(GetGroupIndex(0)).ReadRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(0, map.Get(GetGroupIndex(0)).ReadRequestBlobCount);
        UNIT_ASSERT_VALUES_EQUAL(0, map.Get(GetGroupIndex(0)).ReadRequestBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(false, map.Get(GetGroupIndex(0)).Compacted);

        UNIT_ASSERT_VALUES_EQUAL(1, map.Get(GetGroupIndex(1)).BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(10, map.Get(GetGroupIndex(1)).BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(5, map.Get(GetGroupIndex(1)).UsedBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(1, map.Get(GetGroupIndex(1)).ReadRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(2, map.Get(GetGroupIndex(1)).ReadRequestBlobCount);
        UNIT_ASSERT_VALUES_EQUAL(15, map.Get(GetGroupIndex(1)).ReadRequestBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(false, map.Get(GetGroupIndex(1)).Compacted);

        UNIT_ASSERT_VALUES_EQUAL(10, map.Get(GetGroupIndex(10)).BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(100, map.Get(GetGroupIndex(10)).BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(50, map.Get(GetGroupIndex(10)).UsedBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(1, map.Get(GetGroupIndex(10)).ReadRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(11, map.Get(GetGroupIndex(10)).ReadRequestBlobCount);
        UNIT_ASSERT_VALUES_EQUAL(105, map.Get(GetGroupIndex(10)).ReadRequestBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(false, map.Get(GetGroupIndex(10)).Compacted);

        UNIT_ASSERT_VALUES_EQUAL(100, map.Get(GetGroupIndex(100)).BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(1000, map.Get(GetGroupIndex(100)).BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(500, map.Get(GetGroupIndex(100)).UsedBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(1, map.Get(GetGroupIndex(100)).ReadRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(101, map.Get(GetGroupIndex(100)).ReadRequestBlobCount);
        UNIT_ASSERT_VALUES_EQUAL(1005, map.Get(GetGroupIndex(100)).ReadRequestBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(false, map.Get(GetGroupIndex(100)).Compacted);

        map.Update(GetGroupIndex(1), 22, 33, 11, true);
        UNIT_ASSERT_VALUES_EQUAL(22, map.Get(GetGroupIndex(1)).BlobCount);
        UNIT_ASSERT_VALUES_EQUAL(33, map.Get(GetGroupIndex(1)).BlockCount);
        UNIT_ASSERT_VALUES_EQUAL(11, map.Get(GetGroupIndex(1)).UsedBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(0, map.Get(GetGroupIndex(1)).ReadRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(0, map.Get(GetGroupIndex(1)).ReadRequestBlobCount);
        UNIT_ASSERT_VALUES_EQUAL(0, map.Get(GetGroupIndex(1)).ReadRequestBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(-1000, map.Get(GetGroupIndex(1)).CompactionScore.Score);
        UNIT_ASSERT_VALUES_EQUAL(true, map.Get(GetGroupIndex(1)).Compacted);
    }

    Y_UNIT_TEST(ShouldTrackTopCounters)
    {
        TCompactionMap map(RangeSize, BuildDefaultCompactionPolicy(5));
        const auto blockCount = 123;
        const auto usedBlockCount = 23;
        for (size_t i = 1; i <= 100; ++i) {
            map.Update(GetGroupIndex(i), i, blockCount, usedBlockCount, false);
        }

        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(100), map.GetTop().BlockIndex);

        map.Update(GetGroupIndex(50), 101, blockCount, usedBlockCount, false);
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(50), map.GetTop().BlockIndex);

        map.Update(GetGroupIndex(0), 102, blockCount, usedBlockCount, false);
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(0), map.GetTop().BlockIndex);

        map.Update(GetGroupIndex(0), 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(50), map.GetTop().BlockIndex);

        map.Update(GetGroupIndex(50), 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(100), map.GetTop().BlockIndex);

        map.Update(GetGroupIndex(100), 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(99), map.GetTop().BlockIndex);

        map.Update(
            GetGroupIndex(10) + RangeSize,
            103,
            blockCount,
            usedBlockCount,
            false
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(10) + RangeSize,
            map.GetTop().BlockIndex
        );
    }

    Y_UNIT_TEST(ShouldTrackTopByGarbageBlockCount)
    {
        TCompactionMap map(RangeSize, BuildDefaultCompactionPolicy(5));
        const auto blobCount = 3;
        for (size_t i = 1; i <= 100; ++i) {
            map.Update(GetGroupIndex(i), blobCount, i * 10, i * 5, false);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(100),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(50), blobCount, 1010, 505, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(50),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(0), blobCount, 600, 1, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(0),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(0), 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(50),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(50), 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(100),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(100), 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(99),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(10) + RangeSize, blobCount, 1030, 515, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(10) + RangeSize,
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(10) + RangeSize, blobCount, 1030, 515, true);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(99),
            map.GetTopByGarbageBlockCount().BlockIndex
        );
    }

    Y_UNIT_TEST(ShouldBeEmptyAfterClear)
    {
        TCompactionMap map(RangeSize, BuildDefaultCompactionPolicy(5));
        for (size_t i = 1; i <= 100; ++i) {
            map.Update(GetGroupIndex(i), i, i * 10, i * 5, false);
        }

        map.Clear();

        for (size_t i = 1; i <= 100; ++i) {
            const auto stat = map.Get(GetGroupIndex(i));
            UNIT_ASSERT_VALUES_EQUAL(0, stat.BlobCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.BlockCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.UsedBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.ReadRequestCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.ReadRequestBlockCount);
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyUpdateFromCounterList)
    {
        TCompactionMap map(RangeSize, BuildDefaultCompactionPolicy(5));
        TCompressedBitmap used(3 * RangeSize);
        used.Set(512, 2048);
        map.Update(
            {
                {0, {1, 1000, 0, 0, 0, 0, false, 0}},
                {RangeSize, {2, 2000, 0, 0, 0, 0, false, 0}},
                {2 * RangeSize, {3, 3000, 0, 0, 0, 0, false, 0}},
            },
            &used
        );

        {
            const auto stat = map.Get(0);
            UNIT_ASSERT_VALUES_EQUAL(1, stat.BlobCount);
            UNIT_ASSERT_VALUES_EQUAL(1000, stat.BlockCount);
            UNIT_ASSERT_VALUES_EQUAL(512, stat.UsedBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.ReadRequestCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.ReadRequestBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(true, stat.Compacted);
        }

        {
            const auto stat = map.Get(RangeSize);
            UNIT_ASSERT_VALUES_EQUAL(2, stat.BlobCount);
            UNIT_ASSERT_VALUES_EQUAL(2000, stat.BlockCount);
            UNIT_ASSERT_VALUES_EQUAL(1024, stat.UsedBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.ReadRequestCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.ReadRequestBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(false, stat.Compacted);
        }

        {
            const auto stat = map.Get(2 * RangeSize);
            UNIT_ASSERT_VALUES_EQUAL(3, stat.BlobCount);
            UNIT_ASSERT_VALUES_EQUAL(3000, stat.BlockCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.UsedBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.ReadRequestCount);
            UNIT_ASSERT_VALUES_EQUAL(0, stat.ReadRequestBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(false, stat.Compacted);
        }
    }

    Y_UNIT_TEST(ShouldHaveNonEmptyRanges)
    {
        TCompactionMap map(RangeSize, BuildDefaultCompactionPolicy(5));
        const auto blockCount = 123;
        const auto usedBlockCount = 23;
        for (size_t i = 1; i <= 100; ++i) {
            map.Update(GetGroupIndex(i), i, blockCount, usedBlockCount, false);
        }

        {
            const auto nonEmptyCount = map.GetNonEmptyRanges().size();
            UNIT_ASSERT_VALUES_EQUAL(nonEmptyCount, map.GetNonEmptyRangeCount());
            UNIT_ASSERT_VALUES_EQUAL(nonEmptyCount, 100);
        }

        // empty range must be skipped
        map.Update(GetGroupIndex(46), 0, blockCount, usedBlockCount, false);
        {
            const auto nonEmptyCount = map.GetNonEmptyRanges().size();
            UNIT_ASSERT_VALUES_EQUAL(nonEmptyCount, map.GetNonEmptyRangeCount());
            UNIT_ASSERT_VALUES_EQUAL(nonEmptyCount, 99);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
