#include "volume_ut.h"

#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NTestVolume;
using namespace std::chrono_literals;

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

void SetupVolume(
    TVolumeClient& volume,
    NCloud::NProto::EStorageMediaKind mediaKind,
    ui64 blockCount)
{
    volume.UpdateVolumeConfig(0, 0, 0, 0, false, 1, mediaKind, blockCount);
    volume.WaitReady();
}

void SetupNrdVolume(TVolumeClient& volume)
{
    SetupVolume(
        volume,
        NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
        DefaultBlockCount);
}

void SendBrokenDeviceNotification(TVolumeClient& volume, const TString& uuid)
{
    volume.SendToPipe(
        std::make_unique<
            TEvNonreplPartitionPrivate::TEvBrokenDeviceNotification>(
            TEvNonreplPartitionPrivate::TBrokenDeviceNotification(
                uuid,
                TInstant::Seconds(100))));
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
        runtime->DispatchEvents({}, 1s);

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
        runtime->DispatchEvents({}, 1s);

        SendBrokenDeviceNotification(volume, "uuid-2");
        runtime->DispatchEvents({}, 1s);

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
        runtime->DispatchEvents({}, 1s);

        SendRecoveredDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(1, state->UpdateVolumeHealthRequests);

        SendRecoveredDeviceNotification(volume, "uuid-2");
        runtime->DispatchEvents({}, 1s);

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
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(1, state->UpdateVolumeHealthRequests);

        volume.RebootTablet();
        volume.WaitReady();
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(1, state->UpdateVolumeHealthRequests);

        SendRecoveredDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, 1s);

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
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(0, state->UpdateVolumeHealthRequests);

        volume.RebootTablet();
        volume.WaitReady();
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(0, state->UpdateVolumeHealthRequests);
    }

    Y_UNIT_TEST(ShouldSkipNotificationsForMirrorVolume)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupVolume(
            volume,
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2,
            DefaultBlockCount);

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(0, state->UpdateVolumeHealthRequests);

        volume.RebootTablet();
        volume.WaitReady();
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(0, state->UpdateVolumeHealthRequests);
    }

    Y_UNIT_TEST(ShouldSendNotificationOnEachHealthChange)
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

        runtime->DispatchEvents({}, 1s);

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
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(1, state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_UNHEALTHY,
            state->LastVolumeHealth);

        volume.ReallocateDisk();
        volume.ReconnectPipe();
        volume.WaitReady();
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(2, state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_HEALTHY,
            state->LastVolumeHealth);
    }

    Y_UNIT_TEST(ShouldSendIncreasingSeqNos)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(1, state->UpdateVolumeHealthRequests);
        const ui64 firstVolumeHealthSeqNo = state->LastVolumeHealthSeqNo;
        UNIT_ASSERT(firstVolumeHealthSeqNo > 0);

        SendRecoveredDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(2, state->UpdateVolumeHealthRequests);
        UNIT_ASSERT(state->LastVolumeHealthSeqNo > firstVolumeHealthSeqNo);
    }

    Y_UNIT_TEST(ShouldRetryOnError)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);

        state->VolumeHealthErrorsToReturn = 2;

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, 1s);

        // 2 errors + 1 success = 3 attempts
        UNIT_ASSERT_VALUES_EQUAL(3, state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_UNHEALTHY,
            state->LastVolumeHealth);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
