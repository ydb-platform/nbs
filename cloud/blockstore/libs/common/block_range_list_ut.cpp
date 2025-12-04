#include "block_range_list.h"

#include <library/cpp/testing/unittest/registar.h>

#include <span>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

void AddRanges(
    const std::span<const TBlockRange64>& ranges,
    TBlockRangeList* list)
{
    for (const auto& r: ranges) {
        list->AddRange(r);
    }
}

void RemoveRanges(
    const std::span<const TBlockRange64>& ranges,
    TBlockRangeList* list)
{
    for (const auto& r: ranges) {
        list->RemoveRange(r);
    }
}

void TestOverlaps(
    const TBlockRangeList& list,
    const std::span<const TBlockRange64>& ranges,
    TBlockRange64 rangeToCheck)
{
    TStringBuilder sb;
    sb << "List: " << list.DebugPrint() << ", ranges: ";
    for (const auto& r: ranges) {
        sb << r.Print();
    }
    sb << " failed: ";

    auto expected = [&](TBlockRange64 range) -> bool
    {
        for (auto r: ranges) {
            if (r.Overlaps(range)) {
                return true;
            }
        }
        return false;
    };

    for (ui64 i = rangeToCheck.Start; i <= rangeToCheck.End; ++i) {
        for (ui64 j = i; j <= rangeToCheck.End; ++j) {
            auto r = TBlockRange64::MakeClosedInterval(i, j);

            if (expected(r)) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    true,
                    list.Overlaps(r),
                    sb + r.Print());
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    false,
                    list.Overlaps(r),
                    sb + r.Print());
            }
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeListTest)
{
    Y_UNIT_TEST(Empty)
    {
        const TVector<TBlockRange64> ranges = {};

        TBlockRangeList list;
        AddRanges(ranges, &list);

        UNIT_ASSERT_VALUES_EQUAL("", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges, &list);
        TestOverlaps(list, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OneBlock)
    {
        const TVector<TBlockRange64> ranges = {TBlockRange64::MakeOneBlock(2)};

        TBlockRangeList list;
        AddRanges(ranges, &list);

        UNIT_ASSERT_VALUES_EQUAL("[2..2]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges, &list);
        TestOverlaps(list, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OneBlockOnLeft)
    {
        const TVector<TBlockRange64> ranges = {TBlockRange64::MakeOneBlock(0)};

        TBlockRangeList list;
        AddRanges(ranges, &list);

        UNIT_ASSERT_VALUES_EQUAL("[0..0]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 3));

        RemoveRanges(ranges, &list);
        TestOverlaps(list, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(RangeOnLeft)
    {
        const TVector<TBlockRange64> ranges = {TBlockRange64::WithLength(0, 3)};

        TBlockRangeList list;
        AddRanges(ranges, &list);

        UNIT_ASSERT_VALUES_EQUAL("[0..2]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 5));

        RemoveRanges(ranges, &list);
        TestOverlaps(list, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OneRange)
    {
        const TVector<TBlockRange64> ranges = {TBlockRange64::WithLength(2, 3)};

        TBlockRangeList list;
        AddRanges(ranges, &list);

        UNIT_ASSERT_VALUES_EQUAL("[2..4]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges, &list);
        TestOverlaps(list, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(WithGaps)
    {
        const TVector<TBlockRange64> ranges = {
            TBlockRange64::WithLength(2, 3),
            TBlockRange64::MakeOneBlock(6),
            TBlockRange64::WithLength(10, 3)};

        TBlockRangeList list;
        AddRanges(ranges, &list);

        UNIT_ASSERT_VALUES_EQUAL("[2..4][6..6][10..12]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 15));

        RemoveRanges(ranges, &list);
        TestOverlaps(list, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(SideBySide)
    {
        const TVector<TBlockRange64> ranges = {
            TBlockRange64::WithLength(2, 3),
            TBlockRange64::MakeOneBlock(5),
            TBlockRange64::WithLength(6, 3)};

        TBlockRangeList list;
        AddRanges(ranges, &list);

        UNIT_ASSERT_VALUES_EQUAL("[2..4][5..5][6..8]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges, &list);
        TestOverlaps(list, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(SameOverlappedRanges)
    {
        const TVector<TBlockRange64> ranges = {TBlockRange64::WithLength(3, 3)};

        TBlockRangeList list;
        AddRanges(ranges, &list);
        AddRanges(ranges, &list);

        UNIT_ASSERT_VALUES_EQUAL("[3..5][3..5]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges, &list);
        RemoveRanges(ranges, &list);
        TestOverlaps(list, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OverlappedRangesWithSameStart)
    {
        const TVector<TBlockRange64> ranges = {TBlockRange64::WithLength(3, 3)};

        TBlockRangeList list;
        AddRanges(ranges, &list);
        list.AddRange(TBlockRange64::MakeClosedInterval(3, 4));
        list.AddRange(TBlockRange64::MakeClosedInterval(3, 3));

        UNIT_ASSERT_VALUES_EQUAL("[3..3][3..4][3..5]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OverlappedRangesWithSameEnd)
    {
        const TVector<TBlockRange64> ranges = {TBlockRange64::WithLength(3, 3)};

        TBlockRangeList list;
        AddRanges(ranges, &list);
        list.AddRange(TBlockRange64::MakeClosedInterval(4, 5));
        list.AddRange(TBlockRange64::MakeClosedInterval(5, 5));

        UNIT_ASSERT_VALUES_EQUAL("[3..5][4..5][5..5]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OverlappedRanges)
    {
        const TVector<TBlockRange64> ranges = {
            TBlockRange64::MakeClosedInterval(3, 9)};

        TBlockRangeList list;
        AddRanges(ranges, &list);
        list.AddRange(TBlockRange64::MakeClosedInterval(4, 8));
        list.AddRange(TBlockRange64::MakeClosedInterval(5, 7));
        list.AddRange(TBlockRange64::MakeClosedInterval(6, 6));

        UNIT_ASSERT_VALUES_EQUAL("[6..6][5..7][4..8][3..9]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 12));
    }

    Y_UNIT_TEST(BigGap)
    {
        const TVector<TBlockRange64> ranges = {
            TBlockRange64::MakeClosedInterval(2, 4),
            TBlockRange64::MakeClosedInterval(8, 10)};

        TBlockRangeList list;
        list.AddRange(TBlockRange64::MakeClosedInterval(2, 4));
        list.AddRange(TBlockRange64::MakeClosedInterval(9, 9));
        list.AddRange(TBlockRange64::MakeClosedInterval(8, 10));

        UNIT_ASSERT_VALUES_EQUAL("[2..4][9..9][8..10]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 12));
    }

    Y_UNIT_TEST(HugeGap)
    {
        const TVector<TBlockRange64> ranges = {
            TBlockRange64::MakeClosedInterval(2, 4),
            TBlockRange64::MakeClosedInterval(1002, 1004)};

        TBlockRangeList list;
        AddRanges(ranges, &list);

        UNIT_ASSERT_VALUES_EQUAL("[2..4][1002..1004]", list.DebugPrint());
        TestOverlaps(list, ranges, TBlockRange64::WithLength(0, 10));
        TestOverlaps(list, ranges, TBlockRange64::WithLength(500, 10));
        TestOverlaps(list, ranges, TBlockRange64::WithLength(1000, 10));
    }
}

}   // namespace NCloud::NBlockStore
