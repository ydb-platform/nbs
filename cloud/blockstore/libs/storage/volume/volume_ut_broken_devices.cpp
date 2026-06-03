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
        const ui32 requestsBefore = state->UpdateVolumeHealthRequests;

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(
            requestsBefore + 1,
            state->UpdateVolumeHealthRequests);
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
        const ui32 requestsBefore = state->UpdateVolumeHealthRequests;

        SendBrokenDeviceNotification(volume, "uuid-1");
        SendBrokenDeviceNotification(volume, "uuid-2");
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(
            requestsBefore + 1,
            state->UpdateVolumeHealthRequests);
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

        SendRecoveredDeviceNotification(volume, "uuid-2");
        runtime->DispatchEvents({}, 1s);

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
        const ui32 requestsBefore = state->UpdateVolumeHealthRequests;

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(
            requestsBefore,
            state->UpdateVolumeHealthRequests);
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
        const ui32 requestsBefore = state->UpdateVolumeHealthRequests;

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(
            requestsBefore,
            state->UpdateVolumeHealthRequests);
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
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_UNHEALTHY,
            state->LastVolumeHealth);

        volume.ReallocateDisk();
        volume.ReconnectPipe();
        volume.WaitReady();
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_HEALTHY,
            state->LastVolumeHealth);
    }

    Y_UNIT_TEST(ShouldRetryUntilDelivered)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);

        state->DropVolumeHealthResponses = true;

        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, 1s);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_UNHEALTHY,
            state->LastVolumeHealth);
        const ui32 requestsAfterFirst = state->UpdateVolumeHealthRequests;

        runtime->AdvanceCurrentTime(30s);
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_GT(state->UpdateVolumeHealthRequests, requestsAfterFirst);

        state->DropVolumeHealthResponses = false;
        runtime->AdvanceCurrentTime(30s);
        runtime->DispatchEvents({}, 1s);
        const ui32 requestsAfterAck = state->UpdateVolumeHealthRequests;

        runtime->AdvanceCurrentTime(60s);
        runtime->DispatchEvents({}, 1s);
        UNIT_ASSERT_VALUES_EQUAL(
            requestsAfterAck,
            state->UpdateVolumeHealthRequests);
    }

    Y_UNIT_TEST(ShouldStopRetryAfterTerminalResponse)
    {
        for (auto code: {E_ABORTED, S_ALREADY}) {
            auto state = MakeIntrusive<TDiskRegistryState>();
            auto runtime = PrepareTestActorRuntime(
                MakeConfig(/*enableVolumeHealth=*/true),
                state);

            TVolumeClient volume(*runtime);
            SetupNrdVolume(volume);
            const ui32 requestsBefore = state->UpdateVolumeHealthRequests;

            state->VolumeHealthForcedErrorCount = 1;
            state->VolumeHealthForcedErrorCode = code;

            SendBrokenDeviceNotification(volume, "uuid-1");
            runtime->DispatchEvents({}, 1s);
            UNIT_ASSERT_VALUES_EQUAL_C(
                requestsBefore + 1,
                state->UpdateVolumeHealthRequests,
                code);

            runtime->AdvanceCurrentTime(60s);
            runtime->DispatchEvents({}, 1s);
            UNIT_ASSERT_VALUES_EQUAL_C(
                requestsBefore + 1,
                state->UpdateVolumeHealthRequests,
                code);
        }
    }

    Y_UNIT_TEST(ShouldReplaceInflightOnNewHealthChange)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);
        const ui32 requestsBefore = state->UpdateVolumeHealthRequests;

        state->DropVolumeHealthResponses = true;
        SendBrokenDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents([&] {
            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return state->UpdateVolumeHealthRequests == requestsBefore + 1;
            };
            return options;
        }());
        UNIT_ASSERT_VALUES_EQUAL(
            requestsBefore + 1,
            state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_UNHEALTHY,
            state->LastVolumeHealth);

        state->DropVolumeHealthResponses = false;
        SendRecoveredDeviceNotification(volume, "uuid-1");
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_VALUES_EQUAL(
            requestsBefore + 2,
            state->UpdateVolumeHealthRequests);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_HEALTHY,
            state->LastVolumeHealth);

        runtime->AdvanceCurrentTime(60s);
        runtime->DispatchEvents({}, 1s);
        UNIT_ASSERT_VALUES_EQUAL(
            requestsBefore + 2,
            state->UpdateVolumeHealthRequests);
    }

    Y_UNIT_TEST(ShouldPersistBrokenDevicesAcrossTabletRestart)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(
            MakeConfig(/*enableVolumeHealth=*/true),
            state);

        TVolumeClient volume(*runtime);
        SetupNrdVolume(volume);

        SendBrokenDeviceNotification(volume, "uuid0");
        runtime->DispatchEvents({}, 1s);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_UNHEALTHY,
            state->LastVolumeHealth);
        const ui32 requestsBeforeRestart = state->UpdateVolumeHealthRequests;

        volume.RebootTablet();
        volume.WaitReady();
        runtime->DispatchEvents({}, 1s);

        UNIT_ASSERT_GT(
            state->UpdateVolumeHealthRequests,
            requestsBeforeRestart);
        UNIT_ASSERT_EQUAL(
            NProto::VOLUME_HEALTH_UNHEALTHY,
            state->LastVolumeHealth);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
