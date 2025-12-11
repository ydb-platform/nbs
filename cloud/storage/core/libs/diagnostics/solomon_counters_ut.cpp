#include "solomon_counters.h"

#include "monitoring.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool FindSensor(NMonitoring::TDynamicCountersPtr subgroup, const TString& name)
{
    auto counters = subgroup->ReadSnapshot();
    for (auto c: counters) {
        if (c.first.LabelValue == name) {
            return true;
        }
    }
    return false;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCountersExpirationPolicyTest)
{
    Y_UNIT_TEST(ShouldNotCreateExpiringCounterWithZeroValue)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto group =
            monitoring->GetCounters()->GetSubgroup("counters", "blockstore");

        TSimpleCounter counter{
            TSimpleCounter::ECounterType::Generic,
            ECounterExpirationPolicy::Expiring};
        counter.Register(group, "test");

        counter.Set(0);
        counter.Publish(TInstant::Hours(1));
        counter.Publish(TInstant::Hours(3));

        UNIT_ASSERT(!FindSensor(group, "test"));
    }

    Y_UNIT_TEST(ShouldNotRemovePermanentCounterWithZeroValue)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto group =
            monitoring->GetCounters()->GetSubgroup("counters", "blockstore");

        TSimpleCounter counter{
            TSimpleCounter::ECounterType::Generic,
            ECounterExpirationPolicy::Permanent};
        counter.Register(group, "test");

        TInstant now = TInstant::Hours(1);

        counter.Set(0);
        now += TDuration::Hours(1);
        counter.Publish(now);

        counter.Increment(5);
        now += TDuration::Minutes(5);
        counter.Publish(now);

        counter.Set(0);
        now += TDuration::Hours(3);
        counter.Publish(now);

        UNIT_ASSERT(FindSensor(group, "test"));
    }

    Y_UNIT_TEST(ShouldRemoveExpiringCounterWithZeroValue)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto group =
            monitoring->GetCounters()->GetSubgroup("counters", "blockstore");

        TSimpleCounter counter{
            TSimpleCounter::ECounterType::Generic,
            ECounterExpirationPolicy::Expiring};
        counter.Register(group, "test");

        TInstant now = TInstant::Hours(1);

        counter.Set(5);
        now += TDuration::Hours(1);
        counter.Publish(now);

        UNIT_ASSERT(FindSensor(group, "test"));

        counter.Set(0);
        now += TDuration::Hours(3);
        counter.Publish(now);

        UNIT_ASSERT(!FindSensor(group, "test"));
    }

    Y_UNIT_TEST(ShouldNotRemoveExpiringCounter)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto group =
            monitoring->GetCounters()->GetSubgroup("counters", "blockstore");

        TSimpleCounter counter{
            TSimpleCounter::ECounterType::Generic,
            ECounterExpirationPolicy::Expiring};
        counter.Register(group, "test");

        TInstant now = TInstant::Hours(1);

        counter.Set(5);
        now += TDuration::Hours(1);
        counter.Publish(now);

        counter.Increment(1);
        now += TDuration::Minutes(1);
        counter.Publish(now);

        counter.Increment(10);
        now += TDuration::Minutes(5);
        counter.Publish(now);

        counter.Set(0);
        now += TDuration::Minutes(30);
        counter.Publish(now);

        UNIT_ASSERT(FindSensor(group, "test"));
    }
}

}   // namespace NCloud
