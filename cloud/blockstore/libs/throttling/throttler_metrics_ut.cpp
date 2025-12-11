#include "throttler_metrics.h"

#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TThrottlerFixture: public NUnitTest::TBaseFixture
{
    ITimerPtr Timer;
    NMonitoring::TDynamicCountersPtr RootGroup;
    NMonitoring::TDynamicCountersPtr TotalGroup;
    NMonitoring::TDynamicCountersPtr VolumeGroup;
    IThrottlerMetricsPtr Throttler;

    TThrottlerFixture()
        : Timer(std::make_shared<TTestTimer>())
        , RootGroup(new NMonitoring::TDynamicCounters())
        , TotalGroup(RootGroup->GetSubgroup("component", "server"))
        , VolumeGroup(RootGroup->GetSubgroup("component", "server_volume")
                          ->GetSubgroup("host", "cluster"))
        , Throttler(CreateThrottlerMetrics(Timer, RootGroup, "server"))
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TThrottlerBaseTests)
{
    Y_UNIT_TEST_F(ShouldRegisterCounter, TThrottlerFixture)
    {
        Throttler->Register("Disk-1", "Client-1");

        auto diskCounter = VolumeGroup->FindSubgroup("volume", "Disk-1");
        UNIT_ASSERT(diskCounter);
        auto instanceCounter =
            diskCounter->FindSubgroup("instance", "Client-1");
        UNIT_ASSERT(instanceCounter);
        UNIT_ASSERT(instanceCounter->FindCounter("UsedQuota"));
        UNIT_ASSERT(instanceCounter->FindCounter("MaxUsedQuota"));

        UNIT_ASSERT(TotalGroup->FindCounter("UsedQuota"));
        UNIT_ASSERT(TotalGroup->FindCounter("MaxUsedQuota"));

        Throttler->Register("Disk-1", "Client-2");

        diskCounter = VolumeGroup->FindSubgroup("volume", "Disk-1");
        UNIT_ASSERT(diskCounter);
        instanceCounter = diskCounter->FindSubgroup("instance", "Client-2");
        UNIT_ASSERT(instanceCounter);
        UNIT_ASSERT(instanceCounter->FindCounter("UsedQuota"));
        UNIT_ASSERT(instanceCounter->FindCounter("MaxUsedQuota"));

        Throttler->Unregister("Disk-1", "Client-1");

        diskCounter = VolumeGroup->FindSubgroup("volume", "Disk-1");
        UNIT_ASSERT(diskCounter);
        instanceCounter = diskCounter->FindSubgroup("instance", "Client-1");
        UNIT_ASSERT(instanceCounter);
        UNIT_ASSERT(!instanceCounter->FindCounter("UsedQuota"));
        UNIT_ASSERT(!instanceCounter->FindCounter("MaxUsedQuota"));

        instanceCounter = diskCounter->FindSubgroup("instance", "Client-2");
        UNIT_ASSERT(instanceCounter);
        UNIT_ASSERT(instanceCounter->FindCounter("UsedQuota"));
        UNIT_ASSERT(instanceCounter->FindCounter("MaxUsedQuota"));

        UNIT_ASSERT(TotalGroup->FindCounter("UsedQuota"));
        UNIT_ASSERT(TotalGroup->FindCounter("MaxUsedQuota"));

        Throttler->Unregister("Disk-1", "Client-2");
        UNIT_ASSERT(!instanceCounter->FindCounter("UsedQuota"));
        UNIT_ASSERT(!instanceCounter->FindCounter("MaxUsedQuota"));
        UNIT_ASSERT(!TotalGroup->FindCounter("UsedQuota"));
        UNIT_ASSERT(!TotalGroup->FindCounter("MaxUsedQuota"));
    }

    Y_UNIT_TEST_F(ShouldTrim, TThrottlerFixture)
    {
        Throttler->Register("Disk-1", "Client-1");
        auto instanceCounter = VolumeGroup->FindSubgroup("volume", "Disk-1")
                                   ->FindSubgroup("instance", "Client-1");

        Throttler->Trim(Timer->Now());
        UNIT_ASSERT(instanceCounter->FindCounter("UsedQuota"));
        UNIT_ASSERT(instanceCounter->FindCounter("MaxUsedQuota"));
        UNIT_ASSERT(TotalGroup->FindCounter("UsedQuota"));
        UNIT_ASSERT(TotalGroup->FindCounter("MaxUsedQuota"));

        Throttler->Trim(Timer->Now() + TRIM_THROTTLER_METRICS_INTERVAL);
        UNIT_ASSERT(!instanceCounter->FindCounter("UsedQuota"));
        UNIT_ASSERT(!instanceCounter->FindCounter("MaxUsedQuota"));
        UNIT_ASSERT(!TotalGroup->FindCounter("UsedQuota"));
        UNIT_ASSERT(!TotalGroup->FindCounter("MaxUsedQuota"));
    }

    Y_UNIT_TEST_F(ShouldRemountTrim, TThrottlerFixture)
    {
        Throttler->Register("Disk-1", "Client-1");
        auto instanceCounter = VolumeGroup->FindSubgroup("volume", "Disk-1")
                                   ->FindSubgroup("instance", "Client-1");

        Throttler->Trim(Timer->Now());
        UNIT_ASSERT(instanceCounter->FindCounter("UsedQuota"));
        UNIT_ASSERT(instanceCounter->FindCounter("MaxUsedQuota"));
        UNIT_ASSERT(TotalGroup->FindCounter("UsedQuota"));
        UNIT_ASSERT(TotalGroup->FindCounter("MaxUsedQuota"));

        Throttler->Trim(Timer->Now() + TRIM_THROTTLER_METRICS_INTERVAL / 2);
        UNIT_ASSERT(instanceCounter->FindCounter("UsedQuota"));
        UNIT_ASSERT(instanceCounter->FindCounter("MaxUsedQuota"));
        UNIT_ASSERT(TotalGroup->FindCounter("UsedQuota"));
        UNIT_ASSERT(TotalGroup->FindCounter("MaxUsedQuota"));

        Throttler->Register("Disk-1", "Client-1");
        Throttler->Trim(Timer->Now() + TRIM_THROTTLER_METRICS_INTERVAL / 2);
        UNIT_ASSERT(instanceCounter->FindCounter("UsedQuota"));
        UNIT_ASSERT(instanceCounter->FindCounter("MaxUsedQuota"));
        UNIT_ASSERT(TotalGroup->FindCounter("UsedQuota"));
        UNIT_ASSERT(TotalGroup->FindCounter("MaxUsedQuota"));

        Throttler->Trim(Timer->Now() + TRIM_THROTTLER_METRICS_INTERVAL);
        UNIT_ASSERT(!instanceCounter->FindCounter("UsedQuota"));
        UNIT_ASSERT(!instanceCounter->FindCounter("MaxUsedQuota"));
        UNIT_ASSERT(!TotalGroup->FindCounter("UsedQuota"));
        UNIT_ASSERT(!TotalGroup->FindCounter("MaxUsedQuota"));
    }

    Y_UNIT_TEST_F(ShouldUpdateQuota, TThrottlerFixture)
    {
        Throttler->Register("Disk-1", "Client-1");
        Throttler->Register("Disk-1", "Client-2");
        auto diskCounter = VolumeGroup->FindSubgroup("volume", "Disk-1");
        auto quotaCounter1 = diskCounter->FindSubgroup("instance", "Client-1")
                                 ->FindCounter("UsedQuota");
        auto maxQuotaCounter1 =
            diskCounter->FindSubgroup("instance", "Client-1")
                ->FindCounter("MaxUsedQuota");
        auto quotaCounter2 = diskCounter->FindSubgroup("instance", "Client-2")
                                 ->FindCounter("UsedQuota");
        auto maxQuotaCounter2 =
            diskCounter->FindSubgroup("instance", "Client-2")
                ->FindCounter("MaxUsedQuota");
        auto quotaCounterTotal = TotalGroup->FindCounter("UsedQuota");
        auto maxQuotaCounterTotal = TotalGroup->FindCounter("MaxUsedQuota");

        UNIT_ASSERT_VALUES_EQUAL(0, quotaCounter1->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, quotaCounter2->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, quotaCounterTotal->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxQuotaCounter1->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxQuotaCounter2->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxQuotaCounterTotal->Val());

        Throttler->UpdateUsedQuota(100);
        UNIT_ASSERT_VALUES_EQUAL(100, quotaCounter1->Val());
        UNIT_ASSERT_VALUES_EQUAL(100, quotaCounter2->Val());
        UNIT_ASSERT_VALUES_EQUAL(100, quotaCounterTotal->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxQuotaCounter1->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxQuotaCounter2->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxQuotaCounterTotal->Val());

        Throttler->UpdateUsedQuota(42);
        UNIT_ASSERT_VALUES_EQUAL(42, quotaCounter1->Val());
        UNIT_ASSERT_VALUES_EQUAL(42, quotaCounter2->Val());
        UNIT_ASSERT_VALUES_EQUAL(42, quotaCounterTotal->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxQuotaCounter1->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxQuotaCounter2->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxQuotaCounterTotal->Val());

        Throttler->UpdateMaxUsedQuota();
        UNIT_ASSERT_VALUES_EQUAL(42, quotaCounter1->Val());
        UNIT_ASSERT_VALUES_EQUAL(42, quotaCounter2->Val());
        UNIT_ASSERT_VALUES_EQUAL(42, quotaCounterTotal->Val());
        UNIT_ASSERT_VALUES_EQUAL(100, maxQuotaCounter1->Val());
        UNIT_ASSERT_VALUES_EQUAL(100, maxQuotaCounter2->Val());
        UNIT_ASSERT_VALUES_EQUAL(100, maxQuotaCounterTotal->Val());
    }
}

}   // namespace NCloud::NBlockStore
