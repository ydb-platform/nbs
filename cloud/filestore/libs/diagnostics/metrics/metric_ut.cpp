#include "metric.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMetricTest)
{
    Y_UNIT_TEST(ShouldGetAtomicValue)
    {
        std::atomic<i64> value(42);

        auto counter = CreateMetric(value);
        UNIT_ASSERT_VALUES_EQUAL(42, counter->Get());

        value = 322;
        UNIT_ASSERT_VALUES_EQUAL(322, counter->Get());
    }

    Y_UNIT_TEST(ShouldGetDeprecatedAtomicValue)
    {
        TAtomic value = 42;

        auto counter = CreateMetric(value);
        UNIT_ASSERT_VALUES_EQUAL(42, counter->Get());

        AtomicSet(value, 5);
        UNIT_ASSERT_VALUES_EQUAL(5, counter->Get());
    }

    Y_UNIT_TEST(ShouldGetFunctionValue)
    {
        i64 value = 0;

        auto counter = CreateMetric([&v = value] { return ++v; });
        UNIT_ASSERT_VALUES_EQUAL(1, counter->Get());
        UNIT_ASSERT_VALUES_EQUAL(2, counter->Get());

        AtomicSet(value, 5);
        UNIT_ASSERT_VALUES_EQUAL(6, counter->Get());
    }

    // TODO: Other metrics
}

}   // namespace NCloud::NFileStore::NMetrics
