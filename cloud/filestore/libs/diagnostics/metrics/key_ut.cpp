#include "key.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TKeyTest)
{
    Y_UNIT_TEST(ShouldDefaultKeysBeEqual)
    {
        TMetricKey k1, k2;

        UNIT_ASSERT(k1 == k2);

        UNIT_ASSERT_VALUES_EQUAL(0, *k1);
        UNIT_ASSERT_VALUES_EQUAL(0, *k2);
    }

    Y_UNIT_TEST(ShouldCorrectlyCheckDefaultKeysWithValues)
    {
        TMetricKey k1(1), k2(1), k3(2);

        UNIT_ASSERT(k1 == k2);
        UNIT_ASSERT(k1 != k3);
        UNIT_ASSERT(k2 != k3);

        UNIT_ASSERT_VALUES_EQUAL(1, *k1);
        UNIT_ASSERT_VALUES_EQUAL(1, *k2);
        UNIT_ASSERT_VALUES_EQUAL(2, *k3);
    }

    Y_UNIT_TEST(ShouldCorrectlyCheckKeysWithUInts)
    {
        TMetricKey k11(1, 1), k12(1, 1), k13(1, 2);
        TMetricKey k21(2, 1), k22(2, 1), k23(2, 2);

        UNIT_ASSERT(k11 == k12);
        UNIT_ASSERT(k11 != k13);
        UNIT_ASSERT(k12 != k13);

        UNIT_ASSERT(k21 == k22);
        UNIT_ASSERT(k21 != k23);
        UNIT_ASSERT(k22 != k23);

        UNIT_ASSERT(k11 != k21);
        UNIT_ASSERT(k11 != k22);
        UNIT_ASSERT(k11 != k23);

        UNIT_ASSERT(k12 != k21);
        UNIT_ASSERT(k12 != k22);
        UNIT_ASSERT(k12 != k23);

        UNIT_ASSERT(k13 != k21);
        UNIT_ASSERT(k13 != k22);
        UNIT_ASSERT(k13 != k23);

        UNIT_ASSERT_VALUES_EQUAL(1, *k11);
        UNIT_ASSERT_VALUES_EQUAL(1, *k12);
        UNIT_ASSERT_VALUES_EQUAL(2, *k13);
        UNIT_ASSERT_VALUES_EQUAL(1, *k21);
        UNIT_ASSERT_VALUES_EQUAL(1, *k22);
        UNIT_ASSERT_VALUES_EQUAL(2, *k23);
    }

    Y_UNIT_TEST(ShouldCorrectlyCheckKeysWithPtrs)
    {
        const auto a = []() {
        };
        const auto b = []() {
        };

        TMetricKey k11(&a, 1), k12(&a, 1), k13(&a, 2);
        TMetricKey k21(&b, 1), k22(&b, 1), k23(&b, 2);

        UNIT_ASSERT(k11 == k12);
        UNIT_ASSERT(k11 != k13);
        UNIT_ASSERT(k12 != k13);

        UNIT_ASSERT(k21 == k22);
        UNIT_ASSERT(k21 != k23);
        UNIT_ASSERT(k22 != k23);

        UNIT_ASSERT(k11 != k21);
        UNIT_ASSERT(k11 != k22);
        UNIT_ASSERT(k11 != k23);

        UNIT_ASSERT(k12 != k21);
        UNIT_ASSERT(k12 != k22);
        UNIT_ASSERT(k12 != k23);

        UNIT_ASSERT(k13 != k21);
        UNIT_ASSERT(k13 != k22);
        UNIT_ASSERT(k13 != k23);

        UNIT_ASSERT_VALUES_EQUAL(1, *k11);
        UNIT_ASSERT_VALUES_EQUAL(1, *k12);
        UNIT_ASSERT_VALUES_EQUAL(2, *k13);
        UNIT_ASSERT_VALUES_EQUAL(1, *k21);
        UNIT_ASSERT_VALUES_EQUAL(1, *k22);
        UNIT_ASSERT_VALUES_EQUAL(2, *k23);
    }
}

}   // namespace NCloud::NFileStore::NMetrics
