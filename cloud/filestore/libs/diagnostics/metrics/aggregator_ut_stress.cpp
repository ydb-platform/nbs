#include "aggregator.h"

#include "metric.h"

#include <cloud/filestore/libs/diagnostics/metrics/ut_stress/utils.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/synchronized/synchronized.h>

#include <util/generic/hash_set.h>
#include <util/system/spinlock.h>

#include <array>
#include <atomic>

namespace NCloud::NFileStore::NMetrics::NImpl {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAggregatorStressTest)
{
    Y_UNIT_TEST(ShouldConcurrentlyUse)
    {
        constexpr ui64 COUNT = 5'000;
        constexpr ui64 KEYS_PER_THREAD = 100;

        NThreading::TSynchronized<THashSet<TMetricKey>, TAdaptiveLock> keys;

        NTests::TScopedTasks tasks;

        std::atomic<ui64> count;

        TAggregator aggregator(
            EAggregationType::AT_SUM,
            EMetricType::MT_ABSOLUTE);

        for (size_t i = 0; i < 5; ++i) {
            tasks.Add(
                [&count, &a = aggregator, &ks = keys]
                {
                    for (size_t j = 0; j < COUNT / 5; ++j) {
                        std::array<TMetricKey, KEYS_PER_THREAD> keys;
                        for (size_t k = 0; k < KEYS_PER_THREAD; ++k) {
                            keys[k] =
                                a.Register(CreateMetric([j] { return j; }));
                            ks.Do([&k = keys[k]](THashSet<TMetricKey>& s)
                                  { UNIT_ASSERT(s.insert(k).second); });
                        }

                        a.Aggregate(TInstant::Now());

                        for (size_t k = 0; k < KEYS_PER_THREAD; ++k) {
                            a.Unregister(keys[k]);
                        }

                        ++count;
                    }
                });
        }

        for (size_t i = 0; i < 3; ++i) {
            tasks.Add(
                [i, &count, &a = aggregator]
                {
                    while (count < COUNT) {
                        UNIT_ASSERT_VALUES_EQUAL(
                            EAggregationType::AT_SUM,
                            a.GetAggregationType());

                        a.Aggregate(TInstant::Now());

                        UNIT_ASSERT_VALUES_EQUAL(
                            EMetricType::MT_ABSOLUTE,
                            a.GetMetricType());

                        sleep(i + 1);
                    }
                });
        }

        tasks.Start();
        tasks.Stop();

        UNIT_ASSERT_VALUES_EQUAL(COUNT, count.load());
        UNIT_ASSERT_VALUES_EQUAL(
            COUNT * KEYS_PER_THREAD,
            keys.Access()->size());

        const auto key = aggregator.Register(CreateMetric([] { return 1; }));
        keys.Do([&k = key](THashSet<TMetricKey>& s)
                { UNIT_ASSERT(s.insert(k).second); });

        UNIT_ASSERT_VALUES_EQUAL(COUNT * KEYS_PER_THREAD, *key);
        UNIT_ASSERT(aggregator.Unregister(key));

        aggregator.Aggregate(TInstant::Now());
        UNIT_ASSERT_VALUES_EQUAL(0, aggregator.Aggregate(TInstant::Now()));
    }
}

}   // namespace NCloud::NFileStore::NMetrics::NImpl
