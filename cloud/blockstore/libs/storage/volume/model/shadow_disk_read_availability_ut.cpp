#include "shadow_disk_read_availability.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TShadowDiskReadAvailabilityTest)
{
    Y_UNIT_TEST(ShouldKeepSharedTimeoutWhileFirstReadIsUnavailable)
    {
        TDevices devices;
        for (const auto* uuid: {"A", "B", "C"}) {
            auto* device = devices.Add();
            device->SetDeviceUUID(uuid);
            device->SetBlocksCount(100);
        }
        auto acquired = devices;
        acquired.DeleteSubrange(1, 1);
        TShadowDiskReadAvailability availability;
        availability.UpdateDevices(devices, acquired);
        const auto now = TInstant::Seconds(100);
        const auto timeout = TDuration::Seconds(10);
        using EStatus = TShadowDiskReadAvailability::EReadStatus;
        UNIT_ASSERT(
            availability.CheckRead(200, now, timeout) == EStatus::Available);
        UNIT_ASSERT(availability.CheckRead(0, now, timeout) == EStatus::Retry);
        UNIT_ASSERT(
            availability.CheckRead(200, now + timeout, timeout) ==
            EStatus::Available);
        UNIT_ASSERT(
            availability.CheckRead(100, now + timeout, timeout) ==
            EStatus::Error);
        UNIT_ASSERT_VALUES_EQUAL(
            "Devices [B] are unavailable",
            availability.GetErrorMessage());

        // Changing the missing device does not restart the shared timer.
        acquired = devices;
        acquired.DeleteSubrange(0, 1);
        availability.UpdateDevices(devices, acquired);
        UNIT_ASSERT(
            availability.CheckRead(0, now + timeout, timeout) ==
            EStatus::Error);
        UNIT_ASSERT(
            availability.CheckRead(100, now + timeout, timeout) ==
            EStatus::Available);

        availability.UpdateDevices(devices, devices);
        acquired.Clear();
        availability.UpdateDevices(devices, acquired);
        UNIT_ASSERT(
            availability.CheckRead(0, now + timeout, timeout) ==
            EStatus::Retry);
        UNIT_ASSERT_VALUES_EQUAL(
            "Devices [A, B, C] are unavailable",
            availability.GetErrorMessage());
    }

    Y_UNIT_TEST(ShouldRetryReadsAfterLosingTheShadowDiskSession)
    {
        TDevices devices;
        for (const auto* uuid: {"A", "B"}) {
            auto* device = devices.Add();
            device->SetDeviceUUID(uuid);
            device->SetBlocksCount(100);
        }

        TShadowDiskReadAvailability availability;
        availability.UpdateDevices(devices, devices);
        availability.UpdateDevices(devices, {});

        const auto timeout = TDuration::Seconds(10);
        const auto firstReadAt = TInstant::Seconds(100);
        using EStatus = TShadowDiskReadAvailability::EReadStatus;
        UNIT_ASSERT(
            availability.CheckRead(100, firstReadAt, timeout) ==
            EStatus::Retry);
        const auto retryAt = firstReadAt + TDuration::Seconds(9);
        UNIT_ASSERT(
            availability.CheckRead(0, retryAt, timeout) == EStatus::Retry);

        auto acquired = devices;
        acquired.DeleteSubrange(0, 1);
        availability.UpdateDevices(devices, acquired);
        UNIT_ASSERT(
            availability.CheckRead(100, firstReadAt + timeout, timeout) ==
            EStatus::Available);
        UNIT_ASSERT(
            availability.CheckRead(0, firstReadAt + timeout, timeout) ==
            EStatus::Retry);
        UNIT_ASSERT(
            availability.CheckRead(0, firstReadAt + 2 * timeout, timeout) ==
            EStatus::Error);
    }

    Y_UNIT_TEST(ShouldResetTimeoutWhenFirstReadBecomesAvailable)
    {
        TDevices devices;
        for (const auto* uuid: {"A", "B", "C", "D"}) {
            auto* device = devices.Add();
            device->SetDeviceUUID(uuid);
            device->SetBlocksCount(100);
        }

        auto acquired = devices;
        acquired.DeleteSubrange(1, 2);
        TShadowDiskReadAvailability availability;
        availability.UpdateDevices(devices, acquired);

        const auto now = TInstant::Seconds(100);
        const auto timeout = TDuration::Seconds(10);
        using EStatus = TShadowDiskReadAvailability::EReadStatus;
        UNIT_ASSERT(
            availability.CheckRead(200, now, timeout) == EStatus::Retry);

        acquired = devices;
        acquired.DeleteSubrange(1, 1);
        availability.UpdateDevices(devices, acquired);
        UNIT_ASSERT(
            availability.CheckRead(200, now + timeout, timeout) ==
            EStatus::Available);

        // B is still unavailable. Losing D starts a new retry window.
        acquired.RemoveLast();
        availability.UpdateDevices(devices, acquired);
        const auto nextFailure = now + timeout;
        UNIT_ASSERT(
            availability.CheckRead(300, nextFailure, timeout) ==
            EStatus::Retry);
        UNIT_ASSERT(
            availability.CheckRead(300, nextFailure + timeout, timeout) ==
            EStatus::Error);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
