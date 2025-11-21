#include "interval.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIntervalTest)
{
    Y_UNIT_TEST(ShouldCalculateIntersection)
    {
        auto checkIntersection = [](const auto& interval1,
                                    const auto& interval2,
                                    const auto& expected)
        {
            auto intersection = interval1.Intersection(interval2);
            UNIT_ASSERT_VALUES_EQUAL(expected.Start, intersection.Start);
            UNIT_ASSERT_VALUES_EQUAL(expected.End, intersection.End);
        };

        checkIntersection(
            TInterval<ui64>{10, 20},
            TInterval<ui64>{15, 30},
            TInterval<ui64>{15, 20});
        checkIntersection(
            TInterval<TString>{"aaaa", "zzzz"},
            TInterval<TString>{"bbbb", "zzzzz"},
            TInterval<TString>{"bbbb", "zzzz"});

        auto intersection = TInterval<ui64>{10, 20}.Intersection({20, 30});
        UNIT_ASSERT(intersection.Empty());
    }

    Y_UNIT_TEST(ShouldContains)
    {
        UNIT_ASSERT(TInterval<ui64>(10, 20).Contains(15));
        UNIT_ASSERT(TInterval<ui64>(10, 20).Contains(10));
        UNIT_ASSERT(!TInterval<ui64>(10, 20).Contains(20));

        UNIT_ASSERT(TInterval<TString>("aaaa", "zzzz").Contains("bbbb"));
        UNIT_ASSERT(TInterval<TString>("aaaa", "zzzz").Contains("aaaa"));
        UNIT_ASSERT(!TInterval<TString>("aaaa", "zzzz").Contains("zzzz"));
    }
}

}   // namespace NCloud
