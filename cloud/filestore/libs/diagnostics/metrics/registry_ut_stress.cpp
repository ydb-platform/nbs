#include "registry.h"

#include "label.h"
#include "metric.h"
#include "visitor.h"

#include <cloud/filestore/libs/diagnostics/metrics/ut_stress/utils.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/synchronized/synchronized.h>

#include <util/generic/hash_set.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NMetrics {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv: public NUnitTest::TBaseFixture
{
    TStringStream Data;
    NMonitoring::TDynamicCountersPtr Counters;

    IRegistryVisitorPtr Visitor;
    IMainMetricsRegistryPtr Registry;

    TEnv()
        : Counters(MakeIntrusive<NMonitoring::TDynamicCounters>())
        , Visitor(CreateRegistryVisitor(Data))
    {
        SetupRegistry();
    }

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {}

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}

    void SetupRegistry(TLabels commonLabels = {})
    {
        Registry = CreateMetricsRegistry(std::move(commonLabels), Counters);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMetricRegistryStressTest)
{
    Y_UNIT_TEST_F(ShouldConcurrentlyAccess, TEnv)
    {
        constexpr ui64 COUNT = 2'500;

        NThreading::TSynchronized<THashSet<TMetricKey>, TAdaptiveLock> keys;

        SetupRegistry({
            CreateLabel("test_project", "filestore"),
            CreateLabel("test_component", "test_fs"),
        });

        std::atomic<ui64> metricsCount(0);

        NTests::TScopedTasks tasks;

        for (size_t i = 0; i < 5; ++i) {
            tasks.Add(
                [index = i, r = Registry, &ks = keys, &metricsCount]
                {
                    std::atomic<i64> value(1'000'000 * index);
                    for (size_t i = 0; i < COUNT / 5; ++i) {
                        const auto key = r->Register(
                            {CreateLabel("test_name", "test_value"),
                             CreateSensor(
                                 TStringBuilder()
                                 << "test_sensor_" << value.load())},
                            value,
                            EAggregationType::AT_MIN);

                        ks.Do([&k = key](THashSet<TMetricKey>& s)
                              { UNIT_ASSERT(s.insert(k).second); });

                        ++value;

                        r->Unregister(key);

                        ++metricsCount;
                    }
                });
        }

        for (size_t i = 0; i < 3; ++i) {
            tasks.Add(
                [i, r = Registry, &metricsCount]
                {
                    TStringStream data;
                    auto visitor = CreateRegistryVisitor(data);
                    while (metricsCount < COUNT) {
                        r->Visit(TInstant::Now(), *visitor);
                        data.Clear();
                        sleep(i + 1);
                    }
                });
        }

        tasks.Start();
        tasks.Stop();

        UNIT_ASSERT_VALUES_EQUAL(COUNT, metricsCount.load());
        UNIT_ASSERT_VALUES_EQUAL(COUNT, keys.Access()->size());
    }

    Y_UNIT_TEST_F(ShouldConcurrentlyAccessScopedRegistry, TEnv)
    {
        constexpr ui64 COUNT = 2'500;

        NThreading::TSynchronized<THashSet<TMetricKey>, TAdaptiveLock> keys;

        SetupRegistry({
            CreateLabel("test_project", "filestore"),
            CreateLabel("test_component", "test_fs"),
        });

        auto scopedRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("test_common_label", "test_common_value")},
            Registry);

        std::atomic<ui64> metricsCount(0);

        NTests::TScopedTasks tasks;

        for (size_t i = 0; i < 5; ++i) {
            tasks.Add(
                [index = i,
                 sr = scopedRegistry,
                 r = Registry,
                 &ks = keys,
                 &metricsCount]
                {
                    std::atomic<i64> value(1'000'000 * index);
                    for (size_t i = 0; i < COUNT / 5; ++i) {
                        const auto key = r->Register(
                            {CreateLabel("test_name", "test_value"),
                             CreateSensor(
                                 TStringBuilder()
                                 << "test_sensor_" << value.load())},
                            value);

                        const auto subKey = sr->Register(
                            {CreateLabel("test_sub_name", "test_sub_value"),
                             CreateSensor(
                                 TStringBuilder()
                                 << "test_sensor_" << value.load())},
                            value,
                            EAggregationType::AT_AVG,
                            EMetricType::MT_DERIVATIVE);

                        ks.Do(
                            [&k = key, &sk = subKey](THashSet<TMetricKey>& s)
                            {
                                UNIT_ASSERT(s.insert(k).second);
                                UNIT_ASSERT(s.insert(sk).second);
                            });

                        ++value;

                        sr->Unregister(subKey);
                        r->Unregister(key);

                        ++metricsCount;
                    }
                });
        }

        for (size_t i = 0; i < 3; ++i) {
            tasks.Add(
                [i, r = Registry, &metricsCount]
                {
                    TStringStream data;
                    auto visitor = CreateRegistryVisitor(data);
                    while (metricsCount < COUNT) {
                        r->Visit(TInstant::Now(), *visitor);
                        data.Clear();
                        sleep(i + 1);
                    }
                });
        }

        tasks.Start();
        tasks.Stop();

        UNIT_ASSERT_VALUES_EQUAL(COUNT, metricsCount.load());
        UNIT_ASSERT_VALUES_EQUAL(2 * COUNT, keys.Access()->size());
    }

    Y_UNIT_TEST_F(ShouldConcurrentlyAccessAggregatableRegistry, TEnv)
    {
        constexpr ui64 COUNT = 5'000;

        NThreading::TSynchronized<THashSet<TMetricKey>, TAdaptiveLock> keys;

        SetupRegistry({CreateLabel("test_project", "filestore")});

        auto serviceRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service")},
            Registry);
        auto serviceVolumeRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service_volume")},
            Registry);

        auto firstFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "first_fs")},
            serviceVolumeRegistry);
        auto secondFsRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("volume", "second_fs")},
            serviceVolumeRegistry);

        auto firstFsAggregatableRegistry =
            CreateScopedMetricsRegistry({}, {serviceRegistry, firstFsRegistry});
        auto secondFsAggregatableRegistry = CreateScopedMetricsRegistry(
            {},
            {serviceRegistry, secondFsRegistry});

        std::atomic<ui64> metricsCount(0);

        NTests::TScopedTasks tasks;

        for (size_t i = 0; i < 5; ++i) {
            tasks.Add(
                [r = firstFsAggregatableRegistry, &ks = keys, &metricsCount]
                {
                    for (size_t i = 0; i < COUNT / 10; ++i) {
                        std::atomic<i64> value(i);

                        const auto key = r->Register(
                            {CreateSensor("test")},
                            value,
                            EAggregationType::AT_AVG,
                            EMetricType::MT_DERIVATIVE);

                        ks.Do([&k = key](THashSet<TMetricKey>& s)
                              { UNIT_ASSERT(s.insert(k).second); });

                        value += i;

                        r->Unregister(key);

                        ++metricsCount;
                    }
                });
        }

        for (size_t i = 0; i < 5; ++i) {
            tasks.Add(
                [r = secondFsAggregatableRegistry, &ks = keys, &metricsCount]
                {
                    for (size_t i = 0; i < COUNT / 10; ++i) {
                        TAtomic value = i;

                        const auto key = r->Register(
                            {CreateSensor("test")},
                            value,
                            EAggregationType::AT_AVG,
                            EMetricType::MT_DERIVATIVE);

                        ks.Do([&k = key](THashSet<TMetricKey>& s)
                              { UNIT_ASSERT(s.insert(k).second); });

                        AtomicIncrement(value);

                        r->Unregister(key);

                        ++metricsCount;
                    }
                });
        }

        for (size_t i = 0; i < 3; ++i) {
            tasks.Add(
                [i, r = Registry, &metricsCount]
                {
                    TStringStream data;
                    auto visitor = CreateRegistryVisitor(data);
                    while (metricsCount < COUNT) {
                        r->Visit(TInstant::Now(), *visitor);
                        data.Clear();
                        sleep(i + 1);
                    }
                });
        }

        for (size_t i = 0; i < 3; ++i) {
            tasks.Add(
                [i, r = Registry, &metricsCount]
                {
                    while (metricsCount < COUNT) {
                        r->Update(TInstant::Now());
                        sleep(i + 1);
                    }
                });
        }

        tasks.Start();
        tasks.Stop();

        UNIT_ASSERT_VALUES_EQUAL(COUNT, metricsCount.load());
        UNIT_ASSERT_VALUES_EQUAL(COUNT, keys.Access()->size());
    }

    // TODO: Other registries
}

}   // namespace NCloud::NFileStore::NMetrics
