#include "compaction_map.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui32 RangeSize = 1024;

ui32 GetGroupIndex(ui32 group)
{
    return group * RangeSize * TCompactionMap::GroupSize;
}

////////////////////////////////////////////////////////////////////////////////

struct TReferenceImplementation
{
    const ui32 RangeSize;
    const ICompactionPolicyPtr Policy;
    const float CompactedRangeScore = -1000;

    THashMap<ui32, TRangeStat> Stats;

    TReferenceImplementation(ui32 rangeSize, ICompactionPolicyPtr policy)
        : RangeSize(rangeSize)
        , Policy(std::move(policy))
    {}

    void Update(
        ui32 blockIndex,
        ui32 blobCount,
        ui32 blockCount,
        ui32 usedBlockCount,
        ui32 newlyZeroedBlocks,
        bool compacted)
    {
        const ui32 rangeStart =
            TCompactionMap::GetRangeStart(blockIndex, RangeSize);
        auto& stat = Stats[rangeStart];

        TCompactionMap::UpdateCompactionCounter(blobCount, &stat.BlobCount);
        TCompactionMap::UpdateCompactionCounter(blockCount, &stat.BlockCount);
        TCompactionMap::UpdateCompactionCounter(
            usedBlockCount,
            &stat.UsedBlockCount);
        TCompactionMap::UpdateCompactionCounter(
            newlyZeroedBlocks,
            &stat.NewlyZeroedBlocks);

        if (compacted) {
            stat.ReadRequestCount = 0;
            stat.ReadRequestBlobCount = 0;
            stat.ReadRequestBlockCount = 0;
        }
        stat.Compacted = compacted;

        stat.CompactionScore = compacted
            ? TCompactionScore(CompactedRangeScore)
            : Policy->CalculateScore(stat);
    }

    const TRangeStat* Get(ui32 blockIndex) const
    {
        const ui32 rangeStart =
            TCompactionMap::GetRangeStart(blockIndex, RangeSize);

        if (const auto* stat = Stats.FindPtr(rangeStart)) {
            return stat;
        }
        return nullptr;
    }

    TCompactionCounter GetTop() const
    {
        TCompactionCounter top(0, {});
        top.Stat.CompactionScore = Policy->CalculateScore({});

        for (const auto& [rangeStart, stat] : Stats) {
            if (stat.CompactionScore.Score > top.Stat.CompactionScore.Score) {
                top = {rangeStart, stat};
            }
        }
        return top;
    }

    TCompactionCounter GetTopByGarbageBlockCount() const
    {
        TCompactionCounter top(0, {});
        ui16 maxGarbage = 0;

        for (const auto& [rangeStart, stat] : Stats) {
            if (!stat.Compacted) {
                const auto garbage = stat.GarbageBlockCount();
                if (garbage > maxGarbage) {
                    maxGarbage = garbage;
                    top = {rangeStart, stat};
                }
            }
        }
        return top;
    }

    TCompactionCounter GetTopByGarbageIgnoringZeroed() const
    {
        TCompactionCounter top(0, {});
        ui16 maxGarbage = 0;

        for (const auto& [rangeStart, stat] : Stats) {
            if (!stat.Compacted) {
                const auto garbage = stat.GarbageIgnoringZeroed();
                if (garbage > maxGarbage) {
                    maxGarbage = garbage;
                    top = {rangeStart, stat};
                }
            }
        }
        return top;
    }

    TVector<TCompactionCounter> GetTop(size_t count) const
    {
        TVector<TCompactionCounter> result(Reserve(Stats.size()));
        for (const auto& [rangeStart, stat] : Stats) {
            if (stat.BlobCount > 0) {
                result.emplace_back(rangeStart, stat);
            }
        }

        Sort(result, [](const auto& l, const auto& r) {
            return l.Stat.CompactionScore.Score > r.Stat.CompactionScore.Score;
        });

        result.crop(count);
        return result;
    }

    TVector<TCompactionCounter> GetTopByGarbageBlockCount(size_t count) const
    {
        TVector<TCompactionCounter> result(Reserve(Stats.size()));
        for (const auto& [rangeStart, stat] : Stats) {
            if (stat.BlobCount > 0) {
                result.emplace_back(rangeStart, stat);
            }
        }

        Sort(result, [](const auto& l, const auto& r) {
            if (l.Stat.Compacted != r.Stat.Compacted) {
                return r.Stat.Compacted;
            }

            return l.Stat.GarbageBlockCount() > r.Stat.GarbageBlockCount();
        });

        result.crop(count);
        return result;
    }

    TVector<TCompactionCounter> GetTopByGarbageIgnoringZeroed(
        size_t count) const
    {
        TVector<TCompactionCounter> result(Reserve(Stats.size()));
        for (const auto& [rangeStart, stat] : Stats) {
            if (stat.BlobCount > 0) {
                result.emplace_back(rangeStart, stat);
            }
        }

        Sort(result, [](const auto& l, const auto& r) {
            if (l.Stat.Compacted != r.Stat.Compacted) {
                return r.Stat.Compacted;
            }

            return l.Stat.GarbageIgnoringZeroed() >
                   r.Stat.GarbageIgnoringZeroed();
        });

        result.crop(count);
        return result;
    }

    ui32 GetNonEmptyRangeCount() const
    {
        ui32 count = 0;
        for (const auto& [_, stat] : Stats) {
            if (stat.BlobCount > 0) {
                ++count;
            }
        }
        return count;
    }
};

void AssertRangeStatEqual(const TRangeStat& expected, const TRangeStat& actual)
{
    UNIT_ASSERT_VALUES_EQUAL(expected.BlobCount, actual.BlobCount);
    UNIT_ASSERT_VALUES_EQUAL(expected.BlockCount, actual.BlockCount);
    UNIT_ASSERT_VALUES_EQUAL(expected.UsedBlockCount, actual.UsedBlockCount);
    UNIT_ASSERT_VALUES_EQUAL(
        expected.ReadRequestCount,
        actual.ReadRequestCount);
    UNIT_ASSERT_VALUES_EQUAL(
        expected.ReadRequestBlobCount,
        actual.ReadRequestBlobCount);
    UNIT_ASSERT_VALUES_EQUAL(
        expected.ReadRequestBlockCount,
        actual.ReadRequestBlockCount);
    UNIT_ASSERT_VALUES_EQUAL(
        expected.NewlyZeroedBlocks,
        actual.NewlyZeroedBlocks);
    UNIT_ASSERT_VALUES_EQUAL(expected.Compacted, actual.Compacted);
    UNIT_ASSERT_DOUBLES_EQUAL(
        expected.CompactionScore.Score,
        actual.CompactionScore.Score,
        1e-6);
    UNIT_ASSERT_VALUES_EQUAL(
        static_cast<int>(expected.CompactionScore.Type),
        static_cast<int>(actual.CompactionScore.Type));
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
            map.Update(GetGroupIndex(i), i, i * 10, i * 5, 0, false);
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

        map.Update(GetGroupIndex(1), 22, 33, 11, 0, true);
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
            map.Update(GetGroupIndex(i), i, blockCount, usedBlockCount, 0, false);
        }

        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(100), map.GetTop().BlockIndex);

        map.Update(GetGroupIndex(50), 101, blockCount, usedBlockCount, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(50), map.GetTop().BlockIndex);

        map.Update(GetGroupIndex(0), 102, blockCount, usedBlockCount, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(0), map.GetTop().BlockIndex);

        map.Update(GetGroupIndex(0), 0, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(50), map.GetTop().BlockIndex);

        map.Update(GetGroupIndex(50), 0, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(100), map.GetTop().BlockIndex);

        map.Update(GetGroupIndex(100), 0, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(GetGroupIndex(99), map.GetTop().BlockIndex);

        map.Update(
            GetGroupIndex(10) + RangeSize,
            103,
            blockCount,
            usedBlockCount,
            0,
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
            map.Update(GetGroupIndex(i), blobCount, i * 10, i * 5, 0, false);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(100),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(50), blobCount, 1010, 505, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(50),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(0), blobCount, 600, 1, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(0),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(0), 0, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(50),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(50), 0, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(100),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(100), 0, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(99),
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(10) + RangeSize, blobCount, 1030, 515, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(10) + RangeSize,
            map.GetTopByGarbageBlockCount().BlockIndex
        );

        map.Update(GetGroupIndex(10) + RangeSize, blobCount, 1030, 515, 0, true);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(99),
            map.GetTopByGarbageBlockCount().BlockIndex
        );
    }

    Y_UNIT_TEST(ShouldTrackTopByGarbageIgnoringZeroed)
    {
        TCompactionMap map(RangeSize, BuildDefaultCompactionPolicy(5));
        const auto blobCount = 3;
        for (size_t i = 1; i <= 100; ++i) {
            // BlockCount = i * 10, UsedBlockCount = i * 5, NewlyZeroedBlocks = i
            // GarbageIgnoringZeroed = BlockCount - UsedBlockCount - NewlyZeroedBlocks
            //                      = i * 10 - i * 5 - i = i * 4
            map.Update(GetGroupIndex(i), blobCount, i * 10, i * 5, i, false);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(100),
            map.GetTopByGarbageIgnoringZeroed().BlockIndex
        );

        // GarbageIgnoringZeroed = 1010 - 550 - 0 = 460
        map.Update(GetGroupIndex(50), blobCount, 1010, 550, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(50),
            map.GetTopByGarbageIgnoringZeroed().BlockIndex
        );
        UNIT_ASSERT(
            map.GetTopByGarbageIgnoringZeroed().BlockIndex !=
            map.GetTopByGarbageBlockCount().BlockIndex);

        // GarbageIgnoringZeroed = 600 - 1 - 10 = 589
        map.Update(GetGroupIndex(0), blobCount, 600, 1, 10, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(0),
            map.GetTopByGarbageIgnoringZeroed().BlockIndex
        );

        map.Update(GetGroupIndex(0), 0, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(50),
            map.GetTopByGarbageIgnoringZeroed().BlockIndex
        );

        map.Update(GetGroupIndex(50), 0, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(100),
            map.GetTopByGarbageIgnoringZeroed().BlockIndex
        );

        map.Update(GetGroupIndex(100), 0, 0, 0, 0, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(99),
            map.GetTopByGarbageIgnoringZeroed().BlockIndex
        );

        // GarbageIgnoringZeroed = 1030 - 300 - 300 = 430
        map.Update(GetGroupIndex(10) + RangeSize, blobCount, 1030, 300, 300, false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(10) + RangeSize,
            map.GetTopByGarbageIgnoringZeroed().BlockIndex
        );

        map.Update(GetGroupIndex(10) + RangeSize, blobCount, 1030, 300, 300, true);
        UNIT_ASSERT_VALUES_EQUAL(
            GetGroupIndex(99),
            map.GetTopByGarbageIgnoringZeroed().BlockIndex
        );
    }

    Y_UNIT_TEST(ShouldBeEmptyAfterClear)
    {
        TCompactionMap map(RangeSize, BuildDefaultCompactionPolicy(5));
        for (size_t i = 1; i <= 100; ++i) {
            map.Update(GetGroupIndex(i), i, i * 10, i * 5, 0, false);
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
            map.Update(GetGroupIndex(i), i, blockCount, usedBlockCount, 0, false);
        }

        {
            const auto nonEmptyCount = map.GetNonEmptyRanges().size();
            UNIT_ASSERT_VALUES_EQUAL(nonEmptyCount, map.GetNonEmptyRangeCount());
            UNIT_ASSERT_VALUES_EQUAL(nonEmptyCount, 100);
        }

        // empty range must be skipped
        map.Update(GetGroupIndex(46), 0, blockCount, usedBlockCount, 0, false);
        {
            const auto nonEmptyCount = map.GetNonEmptyRanges().size();
            UNIT_ASSERT_VALUES_EQUAL(nonEmptyCount, map.GetNonEmptyRangeCount());
            UNIT_ASSERT_VALUES_EQUAL(nonEmptyCount, 99);
        }
    }

    Y_UNIT_TEST(RandomizedUpdates)
    {
        const TVector<ui32> blockIndices = {
            GetGroupIndex(0),
            GetGroupIndex(0) + RangeSize,
            GetGroupIndex(0) + 2 * RangeSize,
            GetGroupIndex(1),
            GetGroupIndex(1) + 3 * RangeSize,
            GetGroupIndex(3),
        };

        const auto policy = BuildDefaultCompactionPolicy(5);
        TCompactionMap map(RangeSize, policy);
        TReferenceImplementation ref(RangeSize, policy);

        const ui32 iterationsCount = 1000;

        for (size_t i = 0; i < iterationsCount; ++i) {
            const ui32 blockIndex =
                blockIndices[
                    RandomNumber<ui32>(blockIndices.size())];
            const ui32 blobCount = RandomNumber<ui32>(4);
            const ui32 blockCount = RandomNumber<ui32>(6);
            const bool compacted = RandomNumber<ui32>(10) >= 8;
            const ui32 usedBlockCount = compacted ? blockCount :
                RandomNumber<ui32>(blockCount + 1);
            const ui32 maxNewlyZeroedBlocks = blockCount - usedBlockCount;
            const ui32 newlyZeroedBlocks = compacted ? 0 :
                RandomNumber<ui32>(maxNewlyZeroedBlocks + 1);

            map.Update(
                blockIndex,
                blobCount,
                blockCount,
                usedBlockCount,
                newlyZeroedBlocks,
                compacted);
            ref.Update(
                blockIndex,
                blobCount,
                blockCount,
                usedBlockCount,
                newlyZeroedBlocks,
                compacted);

            for (ui32 index : blockIndices) {
                auto refStat = ref.Get(index);
                if (refStat) {
                    AssertRangeStatEqual(map.Get(index), *refStat);
                }
            }

            UNIT_ASSERT_DOUBLES_EQUAL(
                map.GetTop().Stat.CompactionScore.Score,
                ref.GetTop().Stat.CompactionScore.Score,
                1e-6);
            UNIT_ASSERT_VALUES_EQUAL(
                map.GetTopByGarbageBlockCount().Stat.GarbageBlockCount(),
                ref.GetTopByGarbageBlockCount().Stat.GarbageBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(
                map.GetTopByGarbageIgnoringZeroed().Stat.GarbageIgnoringZeroed(),
                ref.GetTopByGarbageIgnoringZeroed().Stat.GarbageIgnoringZeroed());

            {
                const auto mapTops = map.GetTop(3);
                const auto refTops = ref.GetTop(3);
                UNIT_ASSERT_VALUES_EQUAL(mapTops.size(), refTops.size());
                for (size_t j = 0; j < mapTops.size(); ++j) {
                    UNIT_ASSERT_DOUBLES_EQUAL(
                        mapTops[j].Stat.CompactionScore.Score,
                        refTops[j].Stat.CompactionScore.Score,
                        1e-6);
                }
            }
            {
                const auto mapTops = map.GetTopByGarbageBlockCount(3);
                const auto refTops = ref.GetTopByGarbageBlockCount(3);
                UNIT_ASSERT_VALUES_EQUAL(mapTops.size(), refTops.size());
                for (size_t j = 0; j < mapTops.size(); ++j) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        mapTops[j].Stat.GarbageBlockCount(),
                        refTops[j].Stat.GarbageBlockCount());
                }
            }
            {
                const auto mapTops = map.GetTopByGarbageIgnoringZeroed(3);
                const auto refTops = ref.GetTopByGarbageIgnoringZeroed(3);
                UNIT_ASSERT_VALUES_EQUAL(mapTops.size(), refTops.size());
                for (size_t j = 0; j < mapTops.size(); ++j) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        mapTops[j].Stat.GarbageIgnoringZeroed(),
                        refTops[j].Stat.GarbageIgnoringZeroed());
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(
                map.GetNonEmptyRangeCount(),
                ref.GetNonEmptyRangeCount());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
