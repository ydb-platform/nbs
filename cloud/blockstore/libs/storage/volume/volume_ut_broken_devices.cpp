#include "volume_ut.h"

#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NTestVolume;

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui64 DefaultBlockCount =
    DefaultDeviceBlockSize * DefaultDeviceBlockCount / DefaultBlockSize;

NProto::TStorageServiceConfig MakeConfig(bool enableVolumeHealth)
{
    NProto::TStorageServiceConfig config;
    config.SetAcquireNonReplicatedDevices(true);
    config.SetVolumeHealthNotificationEnabled(enableVolumeHealth);
    return config;
}

void SetupNrdVolume(TVolumeClient& volume, ui64 blockCount = DefaultBlockCount)
{
    volume.UpdateVolumeConfig(
        0,
        0,
        0,
        0,
        false,
        1,
        NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
        blockCount);
    volume.WaitReady();
}

void SendBrokenDeviceNotification(
    TVolumeClient& volume,
    const TString& uuid,
    TInstant ts = TInstant::Seconds(100))
{
    volume.SendToPipe(
        std::make_unique<
            TEvNonreplPartitionPrivate::TEvBrokenDeviceNotification>(
            TEvNonreplPartitionPrivate::TBrokenDeviceNotification(uuid, ts)));
}

void SendRecoveredDeviceNotification(TVolumeClient& volume, const TString& uuid)
{
    volume.SendToPipe(
        std::make_unique<
            TEvNonreplPartitionPrivate::TEvDeviceRecoveredNotification>(
            TEvNonreplPartitionPrivate::TDeviceRecoveredNotification(uuid)));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeBrokenDevicesTest)
{
    Y_UNIT_TEST(ShouldSendUnhealthyOnFirstBrokenDevice)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_UNHEALTHY,
            state->LastVolumeHealth);
    }

    Y_UNIT_TEST(ShouldNotSendDuplicateUnhealthy)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        SendBrokenDeviceNotification(volume, "uuid-2");
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_UNHEALTHY,
            state->LastVolumeHealth);
    }

    Y_UNIT_TEST(ShouldSendHealthyWhenAllDevicesRecover)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);

        SendBrokenDeviceNotification(volume, "uuid-1");
        SendBrokenDeviceNotification(volume, "uuid-2");
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        SendRecoveredDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, state->UpdateVolumeHealthRequests);

        SendRecoveredDeviceNotification(volume, "uuid-2");
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(2, state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_HEALTHY,
            state->LastVolumeHealth);
    }

    Y_UNIT_TEST(ShouldPersistAndRestoreBrokenDevicesAfterReboot)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, state->UpdateVolumeHealthRequests);

        volume.RebootTablet();
        volume.WaitReady();
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, state->UpdateVolumeHealthRequests);

        SendRecoveredDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(2, state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_HEALTHY,
            state->LastVolumeHealth);
    }

    Y_UNIT_TEST(ShouldSkipAllWorkWhenFlagDisabled)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/false),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(0, state->UpdateVolumeHealthRequests);

        volume.RebootTablet();
        volume.WaitReady();
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(0, state->UpdateVolumeHealthRequests);
    }

    Y_UNIT_TEST(ShouldSerializeNotifications)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);

        SendBrokenDeviceNotification(volume, "uuid-1");      // → UNHEALTHY
        SendRecoveredDeviceNotification(volume, "uuid-1");   // → HEALTHY
        SendBrokenDeviceNotification(volume, "uuid-1");      // → UNHEALTHY

        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(3, state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_UNHEALTHY,
            state->LastVolumeHealth);
    }

    Y_UNIT_TEST(ShouldCleanupStaleDevicesOnUpdateDevices)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_UNHEALTHY,
            state->LastVolumeHealth);

        volume.ReallocateDisk();
        volume.ReconnectPipe();
        volume.WaitReady();
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(2, state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_HEALTHY,
            state->LastVolumeHealth);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
