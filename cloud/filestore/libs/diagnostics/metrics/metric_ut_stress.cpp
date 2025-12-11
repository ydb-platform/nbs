#include "metric.h"

#include <cloud/filestore/libs/diagnostics/metrics/ut_stress/utils.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMetricStressTest)
{
    Y_UNIT_TEST(ShouldConcurrentlyAccessAtomicGauge)
    {
        constexpr ui64 COUNT = 5'000;

        std::atomic<i64> value(0);
        auto counter = CreateMetric(value);
        NTests::TScopedTasks tasks;

        tasks.Add(
            [&value, counter]
            {
                for (size_t i = 0; i * 2 < COUNT; ++i) {
                    counter->Get();
                    ++value;
                }
            });

        tasks.Add(
            [&value, counter]
            {
                for (size_t i = 0; i * 2 < COUNT; ++i) {
                    counter->Get();
                    ++value;
                }
            });

        tasks.Start();
        tasks.Stop();

        UNIT_ASSERT_VALUES_EQUAL(COUNT, value.load());
        UNIT_ASSERT_VALUES_EQUAL(COUNT, counter->Get());
    }

    Y_UNIT_TEST(ShouldConcurrentlyAccessDeprecatedAtomicGauge)
    {
        constexpr ui64 COUNT = 5'000;

        TAtomic value(0);
        auto counter = CreateMetric(value);
        NTests::TScopedTasks tasks;

        tasks.Add(
            [&value, counter]
            {
                for (size_t i = 0; i * 2 < COUNT; ++i) {
                    counter->Get();
                    AtomicGetAndIncrement(value);
                }
            });

        tasks.Add(
            [&value, counter]
            {
                for (size_t i = 0; i * 2 < COUNT; ++i) {
                    counter->Get();
                    AtomicGetAndIncrement(value);
                }
            });

        tasks.Start();
        tasks.Stop();

        UNIT_ASSERT_VALUES_EQUAL(COUNT, AtomicGet(value));
        UNIT_ASSERT_VALUES_EQUAL(COUNT, counter->Get());
    }
}

}   // namespace NCloud::NFileStore::NMetrics
