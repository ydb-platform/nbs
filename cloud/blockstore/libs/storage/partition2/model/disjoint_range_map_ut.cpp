#include "disjoint_range_map.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>
#include <util/random/shuffle.h>
#include <util/stream/output.h>

#include <numeric>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

#define COMPARE_MARKS(expected, marks, range)                     \
    {                                                             \
        UNIT_ASSERT_VALUES_EQUAL_C(                               \
            expected.size(),                                      \
            marks.size(),                                         \
            DescribeRange(range));                                \
        for (size_t i = 0; i < marks.size(); ++i) {               \
            UNIT_ASSERT_VALUES_EQUAL_C(expected[i], marks[i], i); \
        }                                                         \
    }

#define CHECK_MARKS(m, range, maxMark, expected) \
    {                                            \
        TVector<TDeletedBlock> marks;            \
        m.FindMarks(range, maxMark, &marks);     \
        COMPARE_MARKS(expected, marks, range)    \
    }                                            \
    // CHECK_MARKS

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TReferenceImplementation
{
    TBlockRange32 Range;
    TVector<ui64> Marks;

    TReferenceImplementation(TBlockRange32 range)
        : Range(range)
        , Marks(range.Size())
    {}

    void Mark(const TBlockRange32& blockRange, const ui64 mark)
    {
        for (auto b: xrange(blockRange)) {
            Marks[b - Range.Start] = Max(mark, Marks[b - Range.Start]);
        }
    }

    void FindMarks(
        const TBlockRange32& blockRange,
        const ui64 maxMark,
        TVector<TDeletedBlock>* marks) const
    {
        for (auto b: xrange(blockRange)) {
            auto m = Marks[b - Range.Start];
            if (m && m <= maxMark) {
                marks->push_back({b, m});
            }
        }
    }

    void Visit(const TRangeVisitor& visitor) const
    {
        for (ui32 b = 0; b < Marks.size(); ++b) {
            const auto m = Marks[b];
            if (m) {
                visitor(TBlockRange32::MakeOneBlock(Range.Start + b));
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDisjointRangeMapTest)
{
    void ShouldWorkImpl(EOptimizationMode mode)
    {
        TDisjointRangeMap m(mode);

        m.Mark(TBlockRange32::MakeClosedInterval(1, 3), 1);

        CHECK_MARKS(
            m,
            TBlockRange32::MakeClosedInterval(0, 100),
            0,
            TVector<TDeletedBlock>{});
        CHECK_MARKS(
            m,
            TBlockRange32::MakeClosedInterval(0, 100),
            1,
            (TVector<TDeletedBlock>{
                {1, 1},
                {2, 1},
                {3, 1},
            }));
        CHECK_MARKS(
            m,
            TBlockRange32::MakeClosedInterval(0, 100),
            2,
            (TVector<TDeletedBlock>{
                {1, 1},
                {2, 1},
                {3, 1},
            }));

        m.Mark(TBlockRange32::MakeClosedInterval(2, 5), 2);

        CHECK_MARKS(
            m,
            TBlockRange32::MakeClosedInterval(0, 100),
            1,
            (TVector<TDeletedBlock>{
                {1, 1},
            }));
        CHECK_MARKS(
            m,
            TBlockRange32::MakeClosedInterval(0, 100),
            2,
            (TVector<TDeletedBlock>{
                {1, 1},
                {2, 2},
                {3, 2},
                {4, 2},
                {5, 2},
            }));

        m.Mark(TBlockRange32::MakeClosedInterval(2, 7), 4);
        m.Mark(TBlockRange32::MakeClosedInterval(19, 20), 6);
        m.Mark(TBlockRange32::MakeClosedInterval(5, 9), 3);
        m.Mark(TBlockRange32::MakeClosedInterval(4, 5), 7);
        m.Mark(TBlockRange32::MakeClosedInterval(20, 22), 5);

        CHECK_MARKS(
            m,
            TBlockRange32::MakeClosedInterval(0, 100),
            7,
            (TVector<TDeletedBlock>{
                {1, 1},
                {2, 4},
                {3, 4},
                {4, 7},
                {5, 7},
                {6, 4},
                {7, 4},
                {8, 3},
                {9, 3},
                {19, 6},
                {20, 6},
                {21, 5},
                {22, 5},
            }));

        m.Mark(
            TBlockRange32::MakeClosedInterval(65536 + 1024, 65536 + 1026),
            8);

        CHECK_MARKS(
            m,
            TBlockRange32::MakeClosedInterval(0, 2 * 65536),
            8,
            (TVector<TDeletedBlock>{
                {1, 1},
                {2, 4},
                {3, 4},
                {4, 7},
                {5, 7},
                {6, 4},
                {7, 4},
                {8, 3},
                {9, 3},
                {19, 6},
                {20, 6},
                {21, 5},
                {22, 5},
                {65536 + 1024, 8},
                {65536 + 1025, 8},
                {65536 + 1026, 8},
            }));

        CHECK_MARKS(
            m,
            TBlockRange32::MakeClosedInterval(256, 2 * 65536),
            8,
            (TVector<TDeletedBlock>{
                {65536 + 1024, 8},
                {65536 + 1025, 8},
                {65536 + 1026, 8},
            }));

        TVector<ui32> visited;
        m.Visit(
            [&](const TBlockRange32& range)
            {
                for (ui32 i = range.Start; i <= range.End; ++i) {
                    visited.push_back(i);
                }
            });

        UNIT_ASSERT_VALUES_EQUAL(16, visited.size());
        UNIT_ASSERT_VALUES_EQUAL(1, visited[0]);
        UNIT_ASSERT_VALUES_EQUAL(2, visited[1]);
        UNIT_ASSERT_VALUES_EQUAL(3, visited[2]);
        UNIT_ASSERT_VALUES_EQUAL(4, visited[3]);
        UNIT_ASSERT_VALUES_EQUAL(5, visited[4]);
        UNIT_ASSERT_VALUES_EQUAL(6, visited[5]);
        UNIT_ASSERT_VALUES_EQUAL(7, visited[6]);
        UNIT_ASSERT_VALUES_EQUAL(8, visited[7]);
        UNIT_ASSERT_VALUES_EQUAL(9, visited[8]);
        UNIT_ASSERT_VALUES_EQUAL(19, visited[9]);
        UNIT_ASSERT_VALUES_EQUAL(20, visited[10]);
        UNIT_ASSERT_VALUES_EQUAL(21, visited[11]);
        UNIT_ASSERT_VALUES_EQUAL(22, visited[12]);
        UNIT_ASSERT_VALUES_EQUAL(65536 + 1024, visited[13]);
        UNIT_ASSERT_VALUES_EQUAL(65536 + 1025, visited[14]);
        UNIT_ASSERT_VALUES_EQUAL(65536 + 1026, visited[15]);
    }

    Y_UNIT_TEST(ShouldWorkInOptimizeForLongRangesMode)
    {
        ShouldWorkImpl(EOptimizationMode::OptimizeForLongRanges);
    }

    Y_UNIT_TEST(ShouldWorkInOptimizeForShortRangesMode)
    {
        ShouldWorkImpl(EOptimizationMode::OptimizeForShortRanges);
    }

    void RandomizedMarkAndFindAndVisitTestImpl(
        EOptimizationMode mode,
        ui32 markedRanges)
    {
        const auto diskRange = TBlockRange32::WithLength(0, 1024 * 1024);
        TFastRng<ui32> gen(12345);

        TDisjointRangeMap m(mode);
        TReferenceImplementation r(diskRange);

        TVector<ui32> marks(markedRanges);
        std::iota(marks.begin(), marks.end(), 1);
        Shuffle(marks.begin(), marks.end());

        for (ui32 i = 0; i < markedRanges; ++i) {
            TBlockRange32 range;
            range.Start = gen.GenRand64() % diskRange.Size();
            range.End = range.Start + gen.GenRand64() % 1024;
            if (range.End > diskRange.End) {
                range.End = diskRange.End;
            }
            m.Mark(range, marks[i]);
            r.Mark(range, marks[i]);

            if (markedRanges < 100) {
                Cdbg << "MARKED: " << DescribeRange(range) << Endl;
            }
        }

        if (markedRanges < 100) {
            m.Visit([](const TBlockRange32& range)
                    { Cdbg << "VISIT: " << DescribeRange(range) << Endl; });
        }

        ui32 start = 0;
        while (start <= diskRange.End) {
            TBlockRange32 range;
            range.Start = start;
            range.End = start + gen.GenRand64() % 1024;
            if (range.End > diskRange.End) {
                range.End = diskRange.End;
            }

            TVector<TDeletedBlock> actual;
            m.FindMarks(range, Max<ui64>(), &actual);

            TVector<TDeletedBlock> expected;
            r.FindMarks(range, Max<ui64>(), &expected);

            COMPARE_MARKS(expected, actual, range);

            start = range.End + 1;
        }

        {
            TSet<ui32> visited;
            m.Visit(
                [&](const TBlockRange32& range)
                {
                    for (auto b: xrange(range)) {
                        visited.insert(b);
                    }
                });

            TSet<ui32> expected;
            r.Visit(
                [&](const TBlockRange32& range)
                {
                    for (auto b: xrange(range)) {
                        expected.insert(b);
                    }
                });

            auto it = visited.begin();
            auto eit = expected.begin();
            while (it != visited.end()) {
                UNIT_ASSERT(eit != expected.end());
                UNIT_ASSERT_VALUES_EQUAL(*it, *eit);
                ++it;
                ++eit;
            }

            UNIT_ASSERT(eit == expected.end());
        }
    }

    Y_UNIT_TEST(RandomizedMarkAndFindTestForLongRangesMode)
    {
        RandomizedMarkAndFindAndVisitTestImpl(
            EOptimizationMode::OptimizeForLongRanges,
            32768);
        RandomizedMarkAndFindAndVisitTestImpl(
            EOptimizationMode::OptimizeForLongRanges,
            32);
    }

    Y_UNIT_TEST(RandomizedMarkAndFindTestForShortRangesMode)
    {
        RandomizedMarkAndFindAndVisitTestImpl(
            EOptimizationMode::OptimizeForShortRanges,
            32768);
        RandomizedMarkAndFindAndVisitTestImpl(
            EOptimizationMode::OptimizeForShortRanges,
            32);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2

template <>
inline void Out<NCloud::NBlockStore::NStorage::NPartition2::TDeletedBlock>(
    IOutputStream& out,
    const NCloud::NBlockStore::NStorage::NPartition2::TDeletedBlock& bm)
{
    out << "Index=" << bm.BlockIndex << ", Mark=" << bm.CommitId;
}
