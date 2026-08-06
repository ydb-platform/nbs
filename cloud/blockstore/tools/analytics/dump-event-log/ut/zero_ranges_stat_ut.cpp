#include <cloud/blockstore/tools/analytics/dump-event-log/zero_ranges_stat.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TZeroRangesStatTest)
{
    Y_UNIT_TEST(ShouldDumpEmptyRangesWithoutNan)
    {
        TZeroRangesStat::TZeroRanges ranges;

        auto dump = ranges.Dump();

        UNIT_ASSERT_VALUES_EQUAL(0, dump["Known"].GetUIntegerSafe());
        UNIT_ASSERT_VALUES_EQUAL(0, dump["Zero"].GetUIntegerSafe());
        UNIT_ASSERT_VALUES_EQUAL(0, dump["Fraction"].GetDoubleSafe());
    }

    Y_UNIT_TEST(ShouldDumpZeroRangesCountAndFraction)
    {
        TZeroRangesStat::TZeroRanges ranges;

        ranges.Set(0, true);
        ranges.Set(1, false);
        ranges.Set(2, true);
        ranges.Set(3, false);

        auto dump = ranges.Dump();

        UNIT_ASSERT_VALUES_EQUAL(4, dump["Known"].GetUIntegerSafe());
        UNIT_ASSERT_VALUES_EQUAL(2, dump["Zero"].GetUIntegerSafe());
        UNIT_ASSERT_VALUES_EQUAL(50, dump["Fraction"].GetDoubleSafe());
    }
}

}   // namespace NCloud::NBlockStore
