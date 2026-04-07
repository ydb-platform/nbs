#include "device_stats.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestObserver: public IDeviceStatObserver
{
    int BrokenCount = 0;
    int RecoveredCount = 0;

    void OnDeviceBroken(const TString&, TInstant) override
    {
        ++BrokenCount;
    }

    void OnDeviceRecovered(const TString&) override
    {
        ++RecoveredCount;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceStatTest)
{
    Y_UNIT_TEST(ShouldNotifyObserverOnBrokenTransitions)
    {
        TTestObserver observer;
        TDeviceStat stat("uuid-1", &observer);

        const auto now = TInstant::Seconds(100);
        const auto maxDuration = TDuration::Seconds(10);
        const auto agentTimeout = TDuration::Seconds(60);

        stat.HandleTimeout(
            now - TDuration::Seconds(15),
            now,
            maxDuration,
            agentTimeout);
        UNIT_ASSERT_VALUES_EQUAL(1, observer.BrokenCount);
        UNIT_ASSERT_EQUAL(
            TDeviceStat::EDeviceStatus::SilentBroken,
            stat.GetDeviceStatus());

        const auto later = now + agentTimeout + TDuration::Seconds(1);
        stat.HandleTimeout(
            later - TDuration::Seconds(5),
            later,
            maxDuration,
            agentTimeout);
        UNIT_ASSERT_VALUES_EQUAL(2, observer.BrokenCount);
        UNIT_ASSERT_EQUAL(
            TDeviceStat::EDeviceStatus::Broken,
            stat.GetDeviceStatus());

        stat.HandleTimeout(
            later - TDuration::Seconds(5),
            later,
            maxDuration,
            agentTimeout);
        UNIT_ASSERT_VALUES_EQUAL(2, observer.BrokenCount);
    }

    Y_UNIT_TEST(ShouldNotifyObserverOnRecovery)
    {
        TTestObserver observer;
        TDeviceStat stat("uuid-1", &observer);

        const auto now = TInstant::Seconds(100);

        stat.MarkBroken(now, /*notifyObserver=*/true);
        UNIT_ASSERT_VALUES_EQUAL(1, observer.BrokenCount);

        stat.MarkOk(now, TDuration::MilliSeconds(5));
        UNIT_ASSERT_VALUES_EQUAL(1, observer.RecoveredCount);
        UNIT_ASSERT_EQUAL(
            TDeviceStat::EDeviceStatus::Ok,
            stat.GetDeviceStatus());

        stat.MarkOk(now, TDuration::MilliSeconds(5));
        UNIT_ASSERT_VALUES_EQUAL(1, observer.RecoveredCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
