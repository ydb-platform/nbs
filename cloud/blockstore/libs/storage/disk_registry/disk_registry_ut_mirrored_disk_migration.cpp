#include "disk_registry.h"

#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_client.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldFinishReplicationForMirroredDisk)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-3", "rack-2", 10_GB),
            Device("dev-2", "uuid-4", "rack-2", 10_GB),
        });

        const auto agent3 = CreateAgentConfig("agent-3", {
            Device("dev-1", "uuid-5", "rack-3", 10_GB),
            Device("dev-2", "uuid-6", "rack-3", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1, agent2, agent3 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1, agent2, agent3 }));

        RegisterAgents(*runtime, 3);
        WaitForAgents(*runtime, 3);
        WaitForSecureErase(*runtime, {agent1, agent2, agent3 });

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1",
                20_GB,
                DefaultLogicalBlockSize,
                "", // placementGroupId
                0,  // placementPartitionIndex
                "", // cloudId
                "", // folderId
                1   // replicaCount
            );

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                msg.GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(1, msg.ReplicasSize());

            SortBy(*msg.MutableReplicas(0)->MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.GetReplicas(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                msg.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                msg.GetReplicas(0).GetDevices(1).GetDeviceUUID());
        }

        diskRegistry.ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1",
                20_GB,
                DefaultLogicalBlockSize,
                "", // placementGroupId
                0,  // placementPartitionIndex
                "", // cloudId
                "", // folderId
                1   // replicaCount
            );

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-6",
                msg.GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DeviceReplacementUUIDsSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                msg.GetDeviceReplacementUUIDs(0));
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-6",
                msg.GetDeviceReplacementUUIDs(1));

            UNIT_ASSERT_VALUES_EQUAL(1, msg.ReplicasSize());

            SortBy(*msg.MutableReplicas(0)->MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.GetReplicas(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                msg.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                msg.GetReplicas(0).GetDevices(1).GetDeviceUUID());
        }

        diskRegistry.FinishMigration("disk-1", "doesnt-matter", "uuid-5");

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1",
                20_GB,
                DefaultLogicalBlockSize,
                "", // placementGroupId
                0,  // placementPartitionIndex
                "", // cloudId
                "", // folderId
                1   // replicaCount
            );

            auto& msg = response->Record;
            SortBy(*msg.MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-6",
                msg.GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(1, msg.DeviceReplacementUUIDsSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-6",
                msg.GetDeviceReplacementUUIDs(0));

            UNIT_ASSERT_VALUES_EQUAL(1, msg.ReplicasSize());

            SortBy(*msg.MutableReplicas(0)->MutableDevices(), TByUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.GetReplicas(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                msg.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                msg.GetReplicas(0).GetDevices(1).GetDeviceUUID());
        }
    }

    void ShouldFinishMigrationForMirroredDiskImpl(bool rebootTable)
    {
        const auto agent1 = CreateAgentConfig("agent-1", {
            Device("dev-1", "uuid-1", "rack-1", 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 10_GB),
        });

        const auto agent2 = CreateAgentConfig("agent-2", {
            Device("dev-1", "uuid-3", "rack-2", 10_GB),
            Device("dev-2", "uuid-4", "rack-2", 10_GB),
        });

        const auto agent3 = CreateAgentConfig("agent-3", {
            Device("dev-1", "uuid-5", "rack-3", 10_GB),
            Device("dev-2", "uuid-6", "rack-3", 10_GB),
        });

        auto runtime = TTestRuntimeBuilder()
            .WithAgents({ agent1, agent2, agent3 })
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1, agent2, agent3 }));

        RegisterAgents(*runtime, 3);
        WaitForAgents(*runtime, 3);
        WaitForSecureErase(*runtime, {agent1, agent2, agent3 });

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1",
                20_GB,
                DefaultLogicalBlockSize,
                "", // placementGroupId
                0,  // placementPartitionIndex
                "", // cloudId
                "", // folderId
                1   // replicaCount
            );

            auto& msg = response->Record;

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                msg.GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(1, msg.ReplicasSize());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.GetReplicas(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                msg.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                msg.GetReplicas(0).GetDevices(1).GetDeviceUUID());
        }

        diskRegistry.ChangeAgentState(
            "agent-1",
            NProto::EAgentState::AGENT_STATE_WARNING);

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1",
                20_GB,
                DefaultLogicalBlockSize,
                "", // placementGroupId
                0,  // placementPartitionIndex
                "", // cloudId
                "", // folderId
                1   // replicaCount
            );

            auto& msg = response->Record;

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                msg.GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(0, msg.DeviceReplacementUUIDsSize());

            UNIT_ASSERT_VALUES_EQUAL(1, msg.ReplicasSize());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.GetReplicas(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                msg.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                msg.GetReplicas(0).GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.MigrationsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                msg.GetMigrations(0).GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                msg.GetMigrations(0).GetTargetDevice().GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                msg.GetMigrations(1).GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-6",
                msg.GetMigrations(1).GetTargetDevice().GetDeviceUUID());
        }

        diskRegistry.FinishMigration("disk-1", "uuid-1", "uuid-5");

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1",
                20_GB,
                DefaultLogicalBlockSize,
                "", // placementGroupId
                0,  // placementPartitionIndex
                "", // cloudId
                "", // folderId
                1   // replicaCount
            );

            auto& msg = response->Record;

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                msg.GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(0, msg.DeviceReplacementUUIDsSize());

            UNIT_ASSERT_VALUES_EQUAL(1, msg.ReplicasSize());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.GetReplicas(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                msg.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                msg.GetReplicas(0).GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(1, msg.MigrationsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                msg.GetMigrations(0).GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-6",
                msg.GetMigrations(0).GetTargetDevice().GetDeviceUUID());
        }

        diskRegistry.FinishMigration("disk-1", "uuid-2", "uuid-6");

        if (rebootTable) {
            diskRegistry.RebootTablet();
            diskRegistry.WaitReady();

            RegisterAgents(*runtime, 3);
            WaitForAgents(*runtime, 3);
        }

        {
            auto response = diskRegistry.AllocateDisk(
                "disk-1",
                20_GB,
                DefaultLogicalBlockSize,
                "", // placementGroupId
                0,  // placementPartitionIndex
                "", // cloudId
                "", // folderId
                1   // replicaCount
            );

            auto& msg = response->Record;

            UNIT_ASSERT_VALUES_EQUAL(2, msg.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                msg.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-6",
                msg.GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(0, msg.DeviceReplacementUUIDsSize());

            UNIT_ASSERT_VALUES_EQUAL(1, msg.ReplicasSize());

            UNIT_ASSERT_VALUES_EQUAL(2, msg.GetReplicas(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                msg.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                msg.GetReplicas(0).GetDevices(1).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(0, msg.MigrationsSize());
        }
    }

    Y_UNIT_TEST(ShouldFinishMigrationForMirroredDisk)
    {
        ShouldFinishMigrationForMirroredDiskImpl(false);
    }

    Y_UNIT_TEST(ShouldFinishMigrationForMirroredDiskAfterReboot)
    {
        ShouldFinishMigrationForMirroredDiskImpl(true);
    }

    void ShouldReplaceBrokenDevicesRateLimitedBeforeRestartImpl(
        bool limitPerRowEnabled,
        bool rateLimitWindowPassed)
    {
        const auto agent1 = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            });

        const auto agent2 = CreateAgentConfig(
            "agent-2",
            {
                Device("dev-1", "uuid-3", "rack-2", 10_GB),
                Device("dev-2", "uuid-4", "rack-2", 10_GB),
            });

        const auto agent3 = CreateAgentConfig(
            "agent-3",
            {
                Device("dev-1", "uuid-5", "rack-3", 10_GB),
                Device("dev-2", "uuid-6", "rack-3", 10_GB),
            });

        NProto::TStorageServiceConfig config = CreateDefaultStorageConfig();
        config.SetLimitMirrorDisksDeviceReplacementsPerRowEnabled(
            limitPerRowEnabled);
        config.SetMaxAutomaticDeviceReplacementsPerHour(1);
        auto runtime = TTestRuntimeBuilder()
                           .With(config)
                           .WithAgents({agent1, agent2, agent3})
                           .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1, agent2, agent3}));

        RegisterAgents(*runtime, 3);
        WaitForAgents(*runtime, 3);
        WaitForSecureErase(*runtime, {agent1, agent2, agent3});

        TSSProxyClient ss(*runtime);
        ss.CreateVolume("mirrored-vol");
        diskRegistry.AllocateDisk(
            "mirrored-vol",
            10_GB,
            DefaultLogicalBlockSize,
            "",   // placementGroupId
            0,    // placementPartitionIndex
            "",   // cloudId
            "",   // folderId
            2,    // replicaCount
            NProto::STORAGE_MEDIA_SSD_MIRROR3);

        auto deliverNotifications = [&]
        {
            runtime->AdvanceCurrentTime(TDuration::Seconds(10));
            runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        };

        auto getReplicaDevice = [&]
        {
            auto response = diskRegistry.DescribeDisk("mirrored-vol");
            const auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, r.ReplicasSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetReplicas(0).DevicesSize());
            return r.GetReplicas(0).GetDevices(0).GetDeviceUUID();
        };

        deliverNotifications();

        // The first failure is handled immediately and exhausts the
        // automatic replacement rate limit.
        diskRegistry.ChangeDeviceState("uuid-1", NProto::DEVICE_STATE_ERROR);
        deliverNotifications();

        // The second failure is not handled due to the rate limit.
        diskRegistry.ChangeDeviceState("uuid-3", NProto::DEVICE_STATE_ERROR);
        deliverNotifications();

        UNIT_ASSERT_VALUES_EQUAL("uuid-3", getReplicaDevice());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            diskRegistry.ListDisksToNotify()->DiskIds.size());

        if (rateLimitWindowPassed) {
            runtime->AdvanceCurrentTime(TDuration::Hours(2));
        }

        ui32 reallocateRequests = 0;
        runtime->SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvReallocateDiskRequest) {
                    auto* msg =
                        event->Get<TEvVolume::TEvReallocateDiskRequest>();
                    UNIT_ASSERT_VALUES_EQUAL(
                        "mirrored-vol",
                        msg->Record.GetDiskId());
                    ++reallocateRequests;
                }
                return false;
            });

        // Agents don't reregister after the restart, so nothing but the
        // restart itself can trigger the replacement and the notification.
        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        // The restart pass doesn't depend on the per-row limit flag, but it
        // respects the automatic replacement rate limit.
        if (rateLimitWindowPassed) {
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", getReplicaDevice());
            // The volume is notified about the replacement made on restart.
            UNIT_ASSERT_VALUES_EQUAL(1, reallocateRequests);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                diskRegistry.ListDisksToNotify()->DiskIds.size());
        } else {
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", getReplicaDevice());
            UNIT_ASSERT_VALUES_EQUAL(0, reallocateRequests);
        }
    }

    Y_UNIT_TEST(ShouldReplaceRateLimitedDeviceAndNotifyVolumeAfterRestart)
    {
        ShouldReplaceBrokenDevicesRateLimitedBeforeRestartImpl(true, true);
    }

    Y_UNIT_TEST(ShouldRespectReplacementRateLimitAfterRestart)
    {
        ShouldReplaceBrokenDevicesRateLimitedBeforeRestartImpl(true, false);
    }

    Y_UNIT_TEST(ShouldReplaceBrokenDevicesAfterRestartIfLimitPerRowDisabled)
    {
        ShouldReplaceBrokenDevicesRateLimitedBeforeRestartImpl(false, true);
    }

    Y_UNIT_TEST(
        ShouldRespectReplacementRateLimitAfterRestartIfLimitPerRowDisabled)
    {
        ShouldReplaceBrokenDevicesRateLimitedBeforeRestartImpl(false, false);
    }

    Y_UNIT_TEST(ShouldReplaceBrokenDevicesAfterRestart)
    {
        const auto agent1 = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            });

        const auto agent2 = CreateAgentConfig(
            "agent-2",
            {
                Device("dev-1", "uuid-3", "rack-2", 10_GB),
                Device("dev-2", "uuid-4", "rack-2", 10_GB),
            });

        const auto agent3 = CreateAgentConfig(
            "agent-3",
            {
                Device("dev-1", "uuid-5", "rack-3", 10_GB),
                Device("dev-2", "uuid-6", "rack-3", 10_GB),
            });

        NProto::TStorageServiceConfig config = CreateDefaultStorageConfig();
        config.SetLimitMirrorDisksDeviceReplacementsPerRowEnabled(true);
        auto runtime = TTestRuntimeBuilder()
                           .With(config)
                           .WithAgents({agent1, agent2, agent3})
                           .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(
            CreateRegistryConfig(0, {agent1, agent2, agent3}));

        RegisterAgents(*runtime, 3);
        WaitForAgents(*runtime, 3);
        WaitForSecureErase(*runtime, {agent1, agent2, agent3});

        TSSProxyClient ss(*runtime);
        ss.CreateVolume("mirrored-vol");
        diskRegistry.AllocateDisk(
            "mirrored-vol",
            10_GB,
            DefaultLogicalBlockSize,
            "",   // placementGroupId
            0,    // placementPartitionIndex
            "",   // cloudId
            "",   // folderId
            2,    // replicaCount
            NProto::STORAGE_MEDIA_SSD_MIRROR3);

        // Send volume reallocations.
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        IEventHandlePtr notifyEvent;

        runtime->SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistryPrivate::EvNotifyDisksResponse)
                {
                    notifyEvent = std::make_unique<IEventHandle>(
                        event->Recipient,
                        event->Sender,
                        new TEvDiskRegistryPrivate::TEvNotifyDisksResponse(
                            MakeIntrusive<TCallContext>(),
                            TVector<TDiskNotificationResult>{}));
                    return true;
                }

                return false;
            });

        diskRegistry.ChangeDeviceState("uuid-1", NProto::DEVICE_STATE_ERROR);
        diskRegistry.ChangeDeviceState("uuid-3", NProto::DEVICE_STATE_ERROR);

        // Check that "mirrored-vol" has broken device "uuid-3"
        {
            auto response = diskRegistry.DescribeDisk("mirrored-vol");
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", r.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, r.ReplicasSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetReplicas(0).DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                r.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetReplicas(1).DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                r.GetReplicas(1).GetDevices(0).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(1, r.GetDeviceReplacementUUIDs().size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                r.GetDeviceReplacementUUIDs()[0]);
        }

        runtime->SetEventFilter([](auto&, auto&) { return false; });

        UNIT_ASSERT(notifyEvent);
        runtime->Send(notifyEvent.release());

        // Restart disk registry
        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        // The second replacement should have happened on tablet start.
        {
            auto response = diskRegistry.DescribeDisk("mirrored-vol");
            auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", r.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, r.ReplicasSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetReplicas(0).DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                r.GetReplicas(0).GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, r.GetReplicas(1).DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                r.GetReplicas(1).GetDevices(0).GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(2, r.GetDeviceReplacementUUIDs().size());
            const TVector<TString> expectedReplacementUUIDs = {
                "uuid-2",
                "uuid-4"};
            ASSERT_VECTORS_EQUAL(
                expectedReplacementUUIDs,
                r.GetDeviceReplacementUUIDs());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
