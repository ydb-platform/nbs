#include "cpu_timer.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCPUUsageTimerTest)
{
    Y_UNIT_TEST(ShouldRecordPreviousTime)
    {
        std::atomic<i64> metric;
        TCPUUsageTimer timer(metric);

        {
            TCPUUsageTimerGuard t(timer);
            Sleep(TDuration::Seconds(1));
            TCPUUsageTimerGuard t2(timer);
            Sleep(TDuration::Seconds(1));
        }

        {
            TCPUUsageTimerGuard t3(timer);
            Sleep(TDuration::Seconds(1));
        }

        UNIT_ASSERT_C(
            static_cast<ui64>(metric.load())
                >= TDuration::MilliSeconds(2900).MicroSeconds(),
            metric.load());
        UNIT_ASSERT_C(
            static_cast<ui64>(metric.load())
                < TDuration::Seconds(4).MicroSeconds(),
            metric.load());
    }
}

}   // namespace NCloud::NFileStore::NStorage
