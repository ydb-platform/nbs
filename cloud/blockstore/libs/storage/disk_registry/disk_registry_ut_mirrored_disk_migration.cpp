#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>

#include <ydb/core/testlib/basics/runtime.h>

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
}

}   // namespace NCloud::NBlockStore::NStorage
