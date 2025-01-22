#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/blockstore/libs/storage/volume/testlib/test_env.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NCloud::NBlockStore::NStorage::NPartition;
using namespace NCloud::NStorage;
using namespace NTestVolume;

namespace {

TVector<NProto::TDeviceConfig> MakeDeviceList(ui32 agentCount, ui32 deviceCount)
{
    TVector<NProto::TDeviceConfig> result;
    for (ui32 i = 1; i <= agentCount; i++) {
        for (ui32 j = 0; j < deviceCount; j++) {
            auto device = MakeDevice(
                Sprintf("uuid-%u.%u", i, j),
                Sprintf("dev%u", j),
                Sprintf("transport%u-%u", i, j));
            device.SetNodeId(i - 1);
            device.SetAgentId(Sprintf("agent-%u", i));
            result.push_back(std::move(device));
        }
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLaggingAgentVolumeTest)
{
    Y_UNIT_TEST(ShouldHandleDeviceTimeouted)
    {
        constexpr ui32 AgentCount = 3;
        auto diskRegistryState = MakeIntrusive<TDiskRegistryState>();
        diskRegistryState->Devices = MakeDeviceList(AgentCount, 3);
        diskRegistryState->AllocateDiskReplicasOnDifferentNodes = true;
        diskRegistryState->ReplicaCount = 2;
        TVector<TDiskAgentStatePtr> agentStates;
        for (ui32 i = 0; i < AgentCount; i++) {
            agentStates.push_back(TDiskAgentStatePtr{});
        }
        auto runtime = PrepareTestActorRuntime(
            {},
            diskRegistryState,
            {},
            {},
            std::move(agentStates));

        // Create mirror-3 volume with a size of 1 device.
        TVolumeClient volume(*runtime);
        const ui64 blockCount =
            DefaultDeviceBlockCount * DefaultDeviceBlockSize / DefaultBlockSize;
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,   // version
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
            blockCount);

        volume.WaitReady();

        auto stat = volume.StatVolume();
        const auto& devices = stat->Record.GetVolume().GetDevices();
        const auto& replicas = stat->Record.GetVolume().GetReplicas();
        UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.0", devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-1", devices[0].GetAgentId());

        UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2.0",
            replicas[0].GetDevices(0).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "agent-2",
            replicas[0].GetDevices(0).GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3.0",
            replicas[1].GetDevices(0).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "agent-3",
            replicas[1].GetDevices(0).GetAgentId());

        std::optional<TEvPartition::TAddLaggingAgentRequest>
            addLaggingAgentRequest;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartition::EvAddLaggingAgentRequest: {
                        auto* msg = event->Get<
                            TEvPartition::TEvAddLaggingAgentRequest>();
                        UNIT_ASSERT(!addLaggingAgentRequest.has_value());
                        addLaggingAgentRequest = *msg;
                        return true;
                    }
                    case TEvPartition::EvRemoveLaggingReplicaRequest: {
                        auto* msg = event->Get<
                            TEvPartition::TEvRemoveLaggingReplicaRequest>();
                        UNIT_ASSERT(addLaggingAgentRequest.has_value());
                        UNIT_ASSERT_VALUES_EQUAL(
                            msg->ReplicaIndex,
                            addLaggingAgentRequest->ReplicaIndex);
                        addLaggingAgentRequest.reset();
                        return true;
                    }
                }
                return false;
            });

        // Device in the first replica is timeouted.
        volume.DeviceTimeouted("uuid-2.0");

        UNIT_ASSERT(addLaggingAgentRequest.has_value());
        UNIT_ASSERT_VALUES_EQUAL(
            replicas[0].GetDevices(0).GetAgentId(),
            addLaggingAgentRequest->AgentId);
        UNIT_ASSERT_VALUES_EQUAL(1, addLaggingAgentRequest->ReplicaIndex);

        // Can't add more lagging devices in the same row.
        volume.SendDeviceTimeoutedRequest("uuid-3.0");
        auto response = volume.RecvDeviceTimeoutedResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            response->GetError().GetCode());

        // Agent devices are now up-to-date.
        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvSmartMigrationFinished>(
                "agent-2"));
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(!addLaggingAgentRequest.has_value());

        // Now the zeroth replica can lag.
        volume.DeviceTimeouted("uuid-1.0");
        UNIT_ASSERT(addLaggingAgentRequest.has_value());
        UNIT_ASSERT_VALUES_EQUAL(
            devices[0].GetAgentId(),
            addLaggingAgentRequest->AgentId);
        UNIT_ASSERT_VALUES_EQUAL(0, addLaggingAgentRequest->ReplicaIndex);
    }

    Y_UNIT_TEST(ShouldHandleTabletReboot)
    {
        constexpr ui32 AgentCount = 6;
        constexpr ui32 DevicePerAgentCount = 2;
        auto diskRegistryState = MakeIntrusive<TDiskRegistryState>();
        diskRegistryState->Devices =
            MakeDeviceList(AgentCount, DevicePerAgentCount);
        diskRegistryState->AllocateDiskReplicasOnDifferentNodes = true;
        diskRegistryState->ReplicaCount = 2;
        TVector<TDiskAgentStatePtr> agentStates;
        for (ui32 i = 0; i < AgentCount; i++) {
            agentStates.push_back(TDiskAgentStatePtr{});
        }
        auto runtime = PrepareTestActorRuntime(
            {},
            diskRegistryState,
            {},
            {},
            std::move(agentStates));

        TVolumeClient volume(*runtime);
        const ui64 blockCount = DefaultDeviceBlockCount *
                                DefaultDeviceBlockSize / DefaultBlockSize * 3;
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,   // version
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
            blockCount);
        volume.WaitReady();

        auto stat = volume.StatVolume();
        const auto& devices = stat->Record.GetVolume().GetDevices();
        UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.0", devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-1", devices[0].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", devices[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-1", devices[1].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL("uuid-4.0", devices[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-4", devices[2].GetAgentId());

        const auto& replicas = stat->Record.GetVolume().GetReplicas();
        UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
        const auto& replica1Devices = replicas[0].GetDevices();
        UNIT_ASSERT_VALUES_EQUAL(3, replica1Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-2.0", replica1Devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-2", replica1Devices[0].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", replica1Devices[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-2", replica1Devices[1].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL("uuid-5.0", replica1Devices[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-5", replica1Devices[2].GetAgentId());

        const auto& replica2Devices = replicas[1].GetDevices();
        UNIT_ASSERT_VALUES_EQUAL(3, replica2Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3.0", replica2Devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-3", replica2Devices[0].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3.1", replica2Devices[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-3", replica2Devices[1].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL("uuid-6.0", replica2Devices[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-6", replica2Devices[2].GetAgentId());

        std::optional<TEvPartition::TAddLaggingAgentRequest>
            addLaggingAgentRequest;
        std::optional<NProto::TAddLaggingDevicesRequest>
            addLaggingDevicesRequest;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartition::EvAddLaggingAgentRequest: {
                        auto* msg = event->Get<
                            TEvPartition::TEvAddLaggingAgentRequest>();
                        UNIT_ASSERT(!addLaggingAgentRequest.has_value());
                        addLaggingAgentRequest = *msg;
                        return true;
                    }
                    case TEvDiskRegistry::EvAddLaggingDevicesRequest: {
                        auto* msg = event->Get<
                            TEvDiskRegistry::TEvAddLaggingDevicesRequest>();
                        addLaggingDevicesRequest = msg->Record;
                        break;
                    }
                }
                return false;
            });

        // Device in the zeroth replica is timeouted.
        volume.DeviceTimeouted("uuid-1.1");

        UNIT_ASSERT(addLaggingAgentRequest.has_value());
        UNIT_ASSERT_VALUES_EQUAL(
            devices[1].GetAgentId(),
            addLaggingAgentRequest->AgentId);
        UNIT_ASSERT_VALUES_EQUAL(0, addLaggingAgentRequest->ReplicaIndex);

        {
            addLaggingAgentRequest.reset();
            // The first agent is already lagging.
            volume.SendDeviceTimeoutedRequest("uuid-1.0");
            auto response = volume.RecvDeviceTimeoutedResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, response->GetError().GetCode());
            UNIT_ASSERT(addLaggingAgentRequest.has_value());
            UNIT_ASSERT_VALUES_EQUAL(
                devices[0].GetAgentId(),
                addLaggingAgentRequest->AgentId);
        }

        {
            // 0 and 1st rows already lagging. Can't add more lagging devices on
            // these rows.
            volume.SendDeviceTimeoutedRequest("uuid-2.1");
            auto response = volume.RecvDeviceTimeoutedResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_INVALID_STATE,
                response->GetError().GetCode());
        }

        // Adding the second row to lagging.
        addLaggingAgentRequest.reset();
        volume.DeviceTimeouted("uuid-6.0");
        UNIT_ASSERT(addLaggingAgentRequest.has_value());
        UNIT_ASSERT_VALUES_EQUAL(
            replica2Devices[2].GetAgentId(),
            addLaggingAgentRequest->AgentId);

        // Rebooting the volume tablet should report lagging devices to the DR.
        UNIT_ASSERT(!addLaggingDevicesRequest.has_value());
        volume.RebootTablet();
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(addLaggingDevicesRequest.has_value());

        UNIT_ASSERT_VALUES_EQUAL("vol0", addLaggingDevicesRequest->GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            addLaggingDevicesRequest->GetLaggingDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(
            "DeviceUUID: \"uuid-1.0\"\n",
            addLaggingDevicesRequest->GetLaggingDevices(0).DebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "DeviceUUID: \"uuid-1.1\"\nRowIndex: 1\n",
            addLaggingDevicesRequest->GetLaggingDevices(1).DebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "DeviceUUID: \"uuid-6.0\"\nRowIndex: 2\n",
            addLaggingDevicesRequest->GetLaggingDevices(2).DebugString());

        // Disk Registry will remove lagging devices on reallocation.
        volume.ReallocateDisk();
        auto metaHistoryResponse = volume.ReadMetaHistory();
        UNIT_ASSERT(!metaHistoryResponse->MetaHistory.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            metaHistoryResponse->MetaHistory.back()
                .Meta.GetLaggingAgentsInfo()
                .AgentsSize());
    }

    Y_UNIT_TEST(ShouldHandleUpdateVolumeConfig)
    {
        constexpr ui32 AgentCount = 6;
        constexpr ui32 DevicePerAgentCount = 2;
        auto diskRegistryState = MakeIntrusive<TDiskRegistryState>();
        diskRegistryState->Devices =
            MakeDeviceList(AgentCount, DevicePerAgentCount);
        diskRegistryState->AllocateDiskReplicasOnDifferentNodes = true;
        diskRegistryState->ReplicaCount = 2;
        TVector<TDiskAgentStatePtr> agentStates;
        for (ui32 i = 0; i < AgentCount; i++) {
            agentStates.push_back(TDiskAgentStatePtr{});
        }
        auto runtime = PrepareTestActorRuntime(
            {},
            diskRegistryState,
            {},
            {},
            std::move(agentStates));

        TVolumeClient volume(*runtime);
        const ui64 blockCount = DefaultDeviceBlockCount *
                                DefaultDeviceBlockSize / DefaultBlockSize * 3;
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,   // version
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
            blockCount);
        volume.WaitReady();

        auto stat = volume.StatVolume();
        const auto& devices = stat->Record.GetVolume().GetDevices();
        UNIT_ASSERT_VALUES_EQUAL(3, devices.size());

        std::optional<NProto::TAddLaggingDevicesRequest>
            addLaggingDevicesRequest;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartition::EvAddLaggingAgentRequest: {
                        return true;
                    }
                    case TEvDiskRegistry::EvAddLaggingDevicesRequest: {
                        auto* msg = event->Get<
                            TEvDiskRegistry::TEvAddLaggingDevicesRequest>();
                        addLaggingDevicesRequest = msg->Record;
                        break;
                    }
                }
                return false;
            });

        // Device in the zeroth replica is timeouted.
        volume.DeviceTimeouted("uuid-1.1");

        UNIT_ASSERT(!addLaggingDevicesRequest.has_value());
        // Update volume config.
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            2,   // version
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
            blockCount);
        volume.WaitReady();
        UNIT_ASSERT(addLaggingDevicesRequest.has_value());

        auto metaHistoryResponse = volume.ReadMetaHistory();
        UNIT_ASSERT(!metaHistoryResponse->MetaHistory.empty());

        // Make sure that lagging devices are still there.
        auto historyItem = metaHistoryResponse->MetaHistory.back();
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            historyItem.Meta.GetLaggingAgentsInfo().AgentsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "agent-1",
            historyItem.Meta.GetLaggingAgentsInfo()
                .GetAgents()[0]
                .GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            historyItem.Meta.GetLaggingAgentsInfo()
                .GetAgents()[0]
                .GetDevices()
                .size());
    }

    Y_UNIT_TEST(ShouldHandleMigratingDevice)
    {
        constexpr ui32 AgentCount = 8;
        constexpr ui32 DevicePerAgentCount = 2;
        auto diskRegistryState = MakeIntrusive<TDiskRegistryState>();
        diskRegistryState->Devices =
            MakeDeviceList(AgentCount - 2, DevicePerAgentCount);
        diskRegistryState->AllocateDiskReplicasOnDifferentNodes = true;
        diskRegistryState->ReplicaCount = 2;
        diskRegistryState->MigrationMode = EMigrationMode::InProgress;

        // Add migration devices.
        {
            auto device = MakeDevice(
                "uuid-migration-1",
                "dev-migration-1",
                "transport-migration-1");
            device.SetNodeId(AgentCount - 2);
            device.SetAgentId(Sprintf("agent-%u", AgentCount - 1));
            diskRegistryState->MigrationDevices["uuid-1.0"] = device;
            diskRegistryState->Devices.push_back(device);
        }
        {
            auto device = MakeDevice(
                "uuid-migration-2",
                "dev-migration-2",
                "transport-migration-2");
            device.SetNodeId(AgentCount - 1);
            device.SetAgentId(Sprintf("agent-%u", AgentCount));
            diskRegistryState->MigrationDevices["uuid-6.0"] = device;
            diskRegistryState->Devices.push_back(device);
        }

        TVector<TDiskAgentStatePtr> agentStates;
        for (ui32 i = 0; i < AgentCount; i++) {
            agentStates.push_back(TDiskAgentStatePtr{});
        }
        auto runtime = PrepareTestActorRuntime(
            {},
            diskRegistryState,
            {},
            {},
            std::move(agentStates));

        TVolumeClient volume(*runtime);
        const ui64 blockCount = DefaultDeviceBlockCount *
                                DefaultDeviceBlockSize / DefaultBlockSize * 3;
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,   // version
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
            blockCount);
        volume.WaitReady();

        auto stat = volume.StatVolume();
        const auto& devices = stat->Record.GetVolume().GetDevices();
        UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.0", devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-1", devices[0].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", devices[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-1", devices[1].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL("uuid-4.0", devices[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-4", devices[2].GetAgentId());

        const auto& replicas = stat->Record.GetVolume().GetReplicas();
        UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
        const auto& replica1Devices = replicas[0].GetDevices();
        UNIT_ASSERT_VALUES_EQUAL(3, replica1Devices.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2.0",
            replica1Devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-2", replica1Devices[0].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2.1",
            replica1Devices[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-2", replica1Devices[1].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-5.0",
            replica1Devices[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-5", replica1Devices[2].GetAgentId());

        const auto& replica2Devices = replicas[1].GetDevices();
        UNIT_ASSERT_VALUES_EQUAL(3, replica2Devices.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3.0",
            replica2Devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-3", replica2Devices[0].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3.1",
            replica2Devices[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-3", replica2Devices[1].GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-6.0",
            replica2Devices[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("agent-6", replica2Devices[2].GetAgentId());

        const auto& migrations = stat->Record.GetVolume().GetMigrations();
        UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.0", migrations[0].GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-migration-1",
            migrations[0].GetTargetDevice().GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-6.0", migrations[1].GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-migration-2",
            migrations[1].GetTargetDevice().GetDeviceUUID());

        std::optional<TEvPartition::TAddLaggingAgentRequest>
            addLaggingAgentRequest;
        std::optional<NProto::TAddLaggingDevicesRequest>
            addLaggingDevicesRequest;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartition::EvAddLaggingAgentRequest: {
                        auto* msg = event->Get<
                            TEvPartition::TEvAddLaggingAgentRequest>();
                        UNIT_ASSERT(!addLaggingAgentRequest.has_value());
                        addLaggingAgentRequest = *msg;
                        return true;
                    }
                    case TEvDiskRegistry::EvAddLaggingDevicesRequest: {
                        auto* msg = event->Get<
                            TEvDiskRegistry::TEvAddLaggingDevicesRequest>();
                        addLaggingDevicesRequest = msg->Record;
                        break;
                    }
                }
                return false;
            });

        // Device in the zeroth replica is timeouted.
        volume.DeviceTimeouted("uuid-migration-1");

        UNIT_ASSERT(addLaggingAgentRequest.has_value());
        UNIT_ASSERT_VALUES_EQUAL("agent-7", addLaggingAgentRequest->AgentId);
        UNIT_ASSERT_VALUES_EQUAL(0, addLaggingAgentRequest->ReplicaIndex);
        addLaggingAgentRequest.reset();

        // Device in the second replica is timeouted.
        volume.DeviceTimeouted("uuid-migration-2");

        UNIT_ASSERT(addLaggingAgentRequest.has_value());
        UNIT_ASSERT_VALUES_EQUAL("agent-8", addLaggingAgentRequest->AgentId);
        UNIT_ASSERT_VALUES_EQUAL(2, addLaggingAgentRequest->ReplicaIndex);

        {
            addLaggingAgentRequest.reset();
            // The zeroth row is already lagging.
            volume.SendDeviceTimeoutedRequest("uuid-1.0");
            auto response = volume.RecvDeviceTimeoutedResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                E_INVALID_STATE,
                response->GetError().GetCode());
            UNIT_ASSERT(!addLaggingAgentRequest.has_value());
        }

        // Rebooting the volume tablet should report lagging devices to the DR.
        UNIT_ASSERT(!addLaggingDevicesRequest.has_value());
        volume.RebootTablet();
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(addLaggingDevicesRequest.has_value());

        UNIT_ASSERT_VALUES_EQUAL("vol0", addLaggingDevicesRequest->GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            addLaggingDevicesRequest->GetLaggingDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(
            "DeviceUUID: \"uuid-migration-1\"\n",
            addLaggingDevicesRequest->GetLaggingDevices(0).DebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "DeviceUUID: \"uuid-migration-2\"\nRowIndex: 2\n",
            addLaggingDevicesRequest->GetLaggingDevices(1).DebugString());

        // Disk Registry will remove lagging devices on reallocation.
        volume.ReallocateDisk();
        auto metaHistoryResponse = volume.ReadMetaHistory();
        UNIT_ASSERT(!metaHistoryResponse->MetaHistory.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            metaHistoryResponse->MetaHistory.back()
                .Meta.GetLaggingAgentsInfo()
                .AgentsSize());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
