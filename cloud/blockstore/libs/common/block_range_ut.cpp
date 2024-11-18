#include "block_range.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeTest)
{
    Y_UNIT_TEST(Difference)
    {
        {   // cut left
            auto result = TBlockRange64::MakeClosedInterval(10, 20).Difference(
                TBlockRange64::MakeClosedInterval(0, 15));
            auto expect = TBlockRange64::MakeClosedInterval(16, 20);
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        {   // cut left
            auto result = TBlockRange64::MakeClosedInterval(10, 20).Difference(
                TBlockRange64::MakeClosedInterval(10, 15));
            auto expect = TBlockRange64::MakeClosedInterval(16, 20);
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        {   // cut right
            auto result = TBlockRange64::MakeClosedInterval(10, 20).Difference(
                TBlockRange64::MakeClosedInterval(16, 25));
            auto expect = TBlockRange64::MakeClosedInterval(10, 15);
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        {   // cut right
            auto result = TBlockRange64::MakeClosedInterval(10, 20).Difference(
                TBlockRange64::MakeClosedInterval(16, 20));
            auto expect = TBlockRange64::MakeClosedInterval(10, 15);
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        {   // cut from middle
            auto result = TBlockRange64::MakeClosedInterval(10, 20).Difference(
                TBlockRange64::MakeClosedInterval(16, 18));
            auto expectFirst = TBlockRange64::MakeClosedInterval(10, 15);
            auto expectSecond = TBlockRange64::MakeClosedInterval(19, 20);
            UNIT_ASSERT_VALUES_EQUAL(expectFirst, *result.First);
            UNIT_ASSERT_VALUES_EQUAL(expectSecond, *result.Second);
        }
    }

    Y_UNIT_TEST(MakeClosedIntervalWithLimit)
    {
        {
            const auto expect = TBlockRange64::MakeClosedInterval(10, 20);
            UNIT_ASSERT_VALUES_EQUAL(
                expect,
                TBlockRange64::MakeClosedIntervalWithLimit(10, 20, 30));
            UNIT_ASSERT_VALUES_EQUAL(
                expect,
                TBlockRange64::MakeClosedIntervalWithLimit(10, 30, 20));
        }
        {
            // Check ui32 overflow.
            const auto expect =
                TBlockRange32::MakeClosedInterval(10, TBlockRange32::MaxIndex);
            UNIT_ASSERT_VALUES_EQUAL(
                expect,
                TBlockRange32::MakeClosedIntervalWithLimit(
                    10,
                    static_cast<ui64>(TBlockRange32::MaxIndex) + 20,
                    static_cast<ui64>(TBlockRange32::MaxIndex) + 10));
        }
    }
}

} // namespace NCloud::NBlockStore
