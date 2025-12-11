#include "operations.h"

#include <cloud/filestore/libs/diagnostics/metrics/ut_stress/utils.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TOperationsStressTest)
{
    Y_UNIT_TEST(ShouldConcurrentlyAccess)
    {
        constexpr ui64 COUNT = 600'000;
        constexpr ui64 PER_THREAD_OPERATIONS = COUNT / 6;

        std::atomic<i64> value{0};
        TAtomic valueDeprecated{0};

        std::atomic<ui64> count{0};

        NTests::TScopedTasks tasks;

        tasks.Add(
            [&]
            {
                for (ui64 i = 0; i < PER_THREAD_OPERATIONS; ++i) {
                    count.fetch_add(1);

                    Load(value);
                    Load(valueDeprecated);
                }
            });

        tasks.Add(
            [&]
            {
                for (ui64 i = 0; i < PER_THREAD_OPERATIONS; ++i) {
                    const i64 v = count.fetch_add(1);

                    Store(value, v);
                    Store(valueDeprecated, v);
                }
            });

        tasks.Add(
            [&]
            {
                for (ui64 i = 0; i < PER_THREAD_OPERATIONS; ++i) {
                    count.fetch_add(1);

                    Inc(value);
                    Inc(valueDeprecated);
                }
            });

        tasks.Add(
            [&]
            {
                for (ui64 i = 0; i < PER_THREAD_OPERATIONS; ++i) {
                    count.fetch_add(1);

                    Dec(value);
                    Dec(valueDeprecated);
                }
            });

        tasks.Add(
            [&]
            {
                for (ui64 i = 0; i < PER_THREAD_OPERATIONS; ++i) {
                    const i64 v = count.fetch_add(1);

                    Add(value, v);
                    Add(valueDeprecated, v);
                }
            });

        tasks.Add(
            [&]
            {
                for (ui64 i = 0; i < PER_THREAD_OPERATIONS; ++i) {
                    const i64 v = count.fetch_add(1);

                    Sub(value, v);
                    Sub(valueDeprecated, v);
                }
            });

        tasks.Start();
        tasks.Stop();

        UNIT_ASSERT_VALUES_EQUAL(COUNT, count.load());
    }
}

}   // namespace NCloud::NFileStore::NMetrics
