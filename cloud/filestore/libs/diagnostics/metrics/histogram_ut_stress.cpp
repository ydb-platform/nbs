#include "histogram.h"

#include "registry.h"
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
        , Registry(CreateMetricsRegistry({}, Counters))
    {}

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {}

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THistogramStressTest)
{
    Y_UNIT_TEST_F(ShouldConcurrentlyUse, TEnv)
    {
        constexpr ui64 COUNT = 5'000;
        constexpr ui64 WRITES_PER_THREAD = 500;

        NThreading::TSynchronized<THashSet<TMetricKey>, TAdaptiveLock> keys;

        auto serviceRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service")},
            Registry);
        auto serviceVolumeRegistry = CreateScopedMetricsRegistry(
            {CreateLabel("service", "service_volume")},
            Registry);

        std::atomic<ui64> metricsCount(0);
        THistogram<EHistUnit::HU_COUNT> histogram;

        NTests::TScopedTasks tasks;

        for (size_t i = 0; i < 5; ++i) {
            tasks.Add(
                [i,
                 serviceRegistry,
                 serviceVolumeRegistry,
                 &metricsCount,
                 &h = histogram,
                 &ks = keys]
                {
                    auto registry = CreateScopedMetricsRegistry(
                        {CreateLabel("volume", TStringBuilder() << "fs_" << i)},
                        serviceVolumeRegistry);

                    auto aggregatableRegistry = CreateScopedMetricsRegistry(
                        {},
                        {serviceRegistry, registry});

                    for (size_t j = 0; j < COUNT / 5; ++j) {
                        const auto key = h.Register(
                            aggregatableRegistry,
                            {CreateLabel("histogram", "Count")});

                        ks.Do([&k = key](THashSet<TMetricKey>& s)
                              { UNIT_ASSERT(s.insert(k).second); });

                        for (size_t k = 1; k <= WRITES_PER_THREAD; ++k) {
                            h.Record(j * k);
                        }

                        h.Unregister(key);

                        ++metricsCount;
                    }
                });
        }

        for (size_t i = 0; i < 3; ++i) {
            tasks.Add(
                [i, registry = Registry, &metricsCount]
                {
                    TStringStream data;
                    auto visitor = CreateRegistryVisitor(data);
                    while (metricsCount < COUNT) {
                        registry->Visit(TInstant::Now(), *visitor);
                        data.Clear();
                        sleep(i + 1);
                    }
                });
        }

        for (size_t i = 0; i < 3; ++i) {
            tasks.Add(
                [i, &registry = Registry, &metricsCount]
                {
                    while (metricsCount < COUNT) {
                        registry->Update(TInstant::Now());
                        sleep(i + 1);
                    }
                });
        }

        tasks.Start();
        tasks.Stop();

        UNIT_ASSERT_VALUES_EQUAL(COUNT, metricsCount.load());
        UNIT_ASSERT_VALUES_EQUAL(COUNT, keys.Access()->size());
    }
}

}   // namespace NCloud::NFileStore::NMetrics
