#include "request_stats.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NTesting {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestStatsTest)
{
    Y_UNIT_TEST(ShouldCalculateIndependentIntervalMaximums)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TRequestStats stats(counters);

        stats.RequestCompleted(
            ERequestType::ReadData,
            TDuration::MicroSeconds(10));
        stats.RequestCompleted(
            ERequestType::ReadData,
            TDuration::MicroSeconds(30));
        stats.RequestCompleted(
            ERequestType::ReadData,
            TDuration::MicroSeconds(20));
        stats.RequestCompleted(
            ERequestType::WriteData,
            TDuration::MicroSeconds(40));
        for (size_t i = 1; i < UpdateCountersInterval.Seconds(); ++i) {
            stats.UpdateStats(false);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters->GetSubgroup("request", "ReadData")
                ->GetCounter("MaxTime")
                ->Val());

        stats.UpdateStats(true);

        UNIT_ASSERT_VALUES_EQUAL(
            30,
            counters->GetSubgroup("request", "ReadData")
                ->GetCounter("MaxTime")
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            40,
            counters->GetSubgroup("request", "WriteData")
                ->GetCounter("MaxTime")
                ->Val());

        stats.RequestCompleted(
            ERequestType::ReadData,
            TDuration::MicroSeconds(5));
        for (size_t i = 1; i < UpdateCountersInterval.Seconds(); ++i) {
            stats.UpdateStats(false);
        }
        stats.UpdateStats(true);

        UNIT_ASSERT_VALUES_EQUAL(
            5,
            counters->GetSubgroup("request", "ReadData")
                ->GetCounter("MaxTime")
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters->GetSubgroup("request", "WriteData")
                ->GetCounter("MaxTime")
                ->Val());
    }
}

}   // namespace NCloud::NBlockStore::NTesting
