#include "block_range_map.h"

#include <library/cpp/testing/unittest/registar.h>

#include <span>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

void AddRanges(
    const std::span<const TBlockRangeMap::TKeyAndRange>& ranges,
    TBlockRangeMap* map)
{
    for (const auto& r: ranges) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            true,
            map->AddRange(r.Key, r.Range),
            TStringBuilder() << r.Key << r.Range.Print());
    }
}

void RemoveRanges(
    const std::span<const TBlockRangeMap::TKeyAndRange>& ranges,
    TBlockRangeMap* map)
{
    for (const auto& r: ranges) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            true,
            map->RemoveRange(r.Key),
            TStringBuilder() << r.Key << r.Range.Print());
    }
}

void TestOverlaps(
    const TBlockRangeMap& map,
    const std::span<const TBlockRangeMap::TKeyAndRange>& ranges,
    TBlockRange64 rangeToCheck)
{
    TStringBuilder sb;
    sb << "List: " << map.DebugPrint() << ", ranges: ";
    for (const auto& r: ranges) {
        sb << r.Key << r.Range.Print();
    }
    sb << " failed: ";

    auto cmp = [](TBlockRange64 lhs, TBlockRange64 rhs)
    {
        return std::tie(lhs.End, lhs.Start) < std::tie(rhs.End, rhs.Start);
    };

    auto expected =
        [&](TBlockRange64 range) -> std::optional<TBlockRangeMap::TKeyAndRange>
    {
        std::optional<TBlockRangeMap::TKeyAndRange> result = std::nullopt;
        for (auto r: ranges) {
            if (r.Range.Overlaps(range)) {
                if (!result) {
                    result = r;
                } else if (cmp(r.Range, result->Range)) {
                    result = r;
                }
            }
        }
        return result;
    };

    for (ui64 i = rangeToCheck.Start; i <= rangeToCheck.End; ++i) {
        for (ui64 j = i; j <= rangeToCheck.End; ++j) {
            auto r = TBlockRange64::MakeClosedInterval(i, j);
            auto expectedResult = expected(r);
            auto result = map.Overlaps(r);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedResult.has_value(),
                result.has_value(),
                sb + r.Print());

            if (expectedResult) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    expectedResult->Key,
                    result->Key,
                    sb + r.Print());
                UNIT_ASSERT_VALUES_EQUAL_C(
                    expectedResult->Range,
                    result->Range,
                    sb + r.Print());
            }
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeMapTest)
{
    Y_UNIT_TEST(Empty)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {};

        TBlockRangeMap map;
        UNIT_ASSERT_VALUES_EQUAL(0, map.Size());
        UNIT_ASSERT_VALUES_EQUAL(true, map.Empty());

        UNIT_ASSERT_VALUES_EQUAL("", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(AddOnce)
    {
        TBlockRangeMap map;
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            map.AddRange(111, TBlockRange64::WithLength(10, 20)));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            map.AddRange(111, TBlockRange64::WithLength(10, 20)));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            map.AddRange(111, TBlockRange64::WithLength(20, 10)));

        UNIT_ASSERT_VALUES_EQUAL("111[10..29]", map.DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(1, map.Size());
        UNIT_ASSERT_VALUES_EQUAL(false, map.Empty());

        UNIT_ASSERT_VALUES_EQUAL(true, map.RemoveRange(111));
        UNIT_ASSERT_VALUES_EQUAL(0, map.Size());
        UNIT_ASSERT_VALUES_EQUAL(true, map.Empty());
        UNIT_ASSERT_VALUES_EQUAL(false, map.RemoveRange(111));
    }

    Y_UNIT_TEST(OneBlock)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {
            {.Key = 1, .Range = TBlockRange64::MakeOneBlock(2)}};

        TBlockRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[2..2]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OneBlockOnLeft)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {
            {.Key = 10, .Range = TBlockRange64::MakeOneBlock(0)}};

        TBlockRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("10[0..0]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 3));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(RangeOnLeft)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {
            {.Key = 10, .Range = TBlockRange64::WithLength(0, 3)}};

        TBlockRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("10[0..2]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 5));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OneRange)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {
            {.Key = 10, .Range = TBlockRange64::WithLength(2, 3)}};

        TBlockRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("10[2..4]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(WithGaps)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {
            {.Key = 1, .Range = TBlockRange64::WithLength(2, 3)},
            {.Key = 2, .Range = TBlockRange64::MakeOneBlock(6)},
            {.Key = 3, .Range = TBlockRange64::WithLength(10, 3)}};

        TBlockRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[2..4]2[6..6]3[10..12]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 15));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(SideBySide)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {
            {.Key = 1, .Range = TBlockRange64::WithLength(2, 3)},
            {.Key = 2, .Range = TBlockRange64::MakeOneBlock(5)},
            {.Key = 3, .Range = TBlockRange64::WithLength(6, 3)}};

        TBlockRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[2..4]2[5..5]3[6..8]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(SameOverlappedRanges)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges1 = {
            {.Key = 1, .Range = TBlockRange64::WithLength(3, 3)}};
        const TVector<TBlockRangeMap::TKeyAndRange> ranges2 = {
            {.Key = 2, .Range = TBlockRange64::WithLength(3, 3)}};

        TBlockRangeMap map;
        AddRanges(ranges1, &map);
        AddRanges(ranges2, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[3..5]2[3..5]", map.DebugPrint());
        TestOverlaps(map, ranges1, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges1, &map);
        UNIT_ASSERT_VALUES_EQUAL("2[3..5]", map.DebugPrint());
        TestOverlaps(map, ranges2, TBlockRange64::WithLength(0, 10));

        RemoveRanges(ranges2, &map);
        TestOverlaps(map, {}, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OverlappedRangesWithSameStart)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {
            {.Key = 1, .Range = TBlockRange64::MakeClosedInterval(3, 3)},
            {.Key = 2, .Range = TBlockRange64::MakeClosedInterval(3, 4)},
            {.Key = 3, .Range = TBlockRange64::MakeClosedInterval(3, 5)}};

        TBlockRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[3..3]2[3..4]3[3..5]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OverlappedRangesWithSameEnd)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {
            {.Key = 1, .Range = TBlockRange64::MakeClosedInterval(3, 5)},
            {.Key = 2, .Range = TBlockRange64::MakeClosedInterval(4, 5)},
            {.Key = 3, .Range = TBlockRange64::MakeClosedInterval(5, 5)}};

        TBlockRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[3..5]2[4..5]3[5..5]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));
    }

    Y_UNIT_TEST(OverlappedRanges)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {
            {.Key = 1, .Range = TBlockRange64::MakeClosedInterval(3, 9)},
            {.Key = 2, .Range = TBlockRange64::MakeClosedInterval(4, 8)},
            {.Key = 3, .Range = TBlockRange64::MakeClosedInterval(5, 7)},
            {.Key = 4, .Range = TBlockRange64::MakeClosedInterval(6, 6)}};

        TBlockRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL(
            "4[6..6]3[5..7]2[4..8]1[3..9]",
            map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 12));
    }

    Y_UNIT_TEST(BigGap)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {
            {.Key = 1, .Range = TBlockRange64::MakeClosedInterval(2, 4)},
            {.Key = 2, .Range = TBlockRange64::MakeClosedInterval(9, 9)},
            {.Key = 3, .Range = TBlockRange64::MakeClosedInterval(8, 10)}};

        TBlockRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("1[2..4]2[9..9]3[8..10]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 12));
    }

    Y_UNIT_TEST(HugeGap)
    {
        const TVector<TBlockRangeMap::TKeyAndRange> ranges = {
            {.Key = 2, .Range = TBlockRange64::MakeClosedInterval(2, 4)},
            {.Key = 1, .Range = TBlockRange64::MakeClosedInterval(1002, 1004)}};

        TBlockRangeMap map;
        AddRanges(ranges, &map);

        UNIT_ASSERT_VALUES_EQUAL("2[2..4]1[1002..1004]", map.DebugPrint());
        TestOverlaps(map, ranges, TBlockRange64::WithLength(0, 10));
        TestOverlaps(map, ranges, TBlockRange64::WithLength(500, 10));
        TestOverlaps(map, ranges, TBlockRange64::WithLength(1000, 10));
    }
}

}   // namespace NCloud::NBlockStore
