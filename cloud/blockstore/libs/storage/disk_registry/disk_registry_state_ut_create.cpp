#include "disk_registry_state.h"

#include "disk_registry_database.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NDiskRegistryStateTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateCreateTest)
{
    Y_UNIT_TEST(ShouldCreateDiskFromDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const ui32 nativeBlockSize = 512;
        const ui32 adjustedBlockCount = 93_GB / nativeBlockSize;
        const ui32 unAdjustedBlockCount = adjustedBlockCount + 100;

        auto deviceConfig = [&] (auto name, auto uuid) {
            NProto::TDeviceConfig config;

            config.SetDeviceName(name);
            config.SetDeviceUUID(uuid);
            config.SetBlockSize(nativeBlockSize);
            config.SetBlocksCount(adjustedBlockCount);
            config.SetUnadjustedBlockCount(unAdjustedBlockCount);

            return config;
        };

        TVector agents {
            AgentConfig(1, {
                deviceConfig("dev-1", "uuid-1.1"),    // disk-1
                deviceConfig("dev-2", "uuid-1.2"),
                deviceConfig("dev-3", "uuid-1.3"),
            }),
            AgentConfig(2, {
                deviceConfig("dev-1", "uuid-2.1"),    // dirty
                deviceConfig("dev-2", "uuid-2.2"),
                deviceConfig("dev-3", "uuid-2.3"),
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithDisks({ Disk("disk-1", {"uuid-1.1"}) })
            .WithDirtyDevices({"uuid-2.1"})
            .Build();

        auto deviceByName = [] (auto agentId, auto name) {
            NProto::TDeviceConfig config;
            config.SetAgentId(agentId);
            config.SetDeviceName(name);
            return config;
        };

        auto deviceByUUID = [] (auto uuid) {
            NProto::TDeviceConfig config;
            config.SetDeviceUUID(uuid);
            return config;
        };

        // unknown device
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                Now(),
                db,
                false,  // force
                "foo",
                4_KB,
                {
                    deviceByName("agent-2", "dev-3"),
                    deviceByName("agent-3", "foo-2"),
                },
                &result);

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
            UNIT_ASSERT(error.GetMessage().Contains("not found"));

            UNIT_ASSERT(state.FindDisk("uuid-2.3").empty());
            UNIT_ASSERT(state.FindDisk("foo-2").empty());
        });

        // allocated device
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                Now(),
                db,
                true,  // force
                "foo",
                nativeBlockSize,
                {deviceByName("agent-1", "dev-1")},
                &result);

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT(error.GetMessage().Contains("is allocated for"));
        });

        // dirty device (!force)
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                Now(),
                db,
                false,  // force
                "foo",
                nativeBlockSize,
                {deviceByName("agent-2", "dev-1")},
                &result);

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT(error.GetMessage().Contains("is dirty"));
        });

        // disk already exists
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                Now(),
                db,
                true,  // force
                "disk-1",
                nativeBlockSize,
                {deviceByName("agent-2", "dev-2")},
                &result);

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT(error.GetMessage().Contains("already exists"));
        });

        // dirty device (force)
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                TInstant::FromValue(1000),
                db,
                true,  // force
                "bar",
                nativeBlockSize,
                {deviceByName("agent-2", "dev-1")},
                &result);

            UNIT_ASSERT_VALUES_EQUAL(1, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(unAdjustedBlockCount, result.Devices[0].GetBlocksCount());// ???
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("bar", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, info.State);
            UNIT_ASSERT_VALUES_EQUAL(1, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(unAdjustedBlockCount, info.Devices[0].GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", info.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(nativeBlockSize, info.LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(1000, info.StateTs.GetValue());
        });

        // regular
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                TInstant::FromValue(1000),
                db,
                false,  // force
                "baz",
                nativeBlockSize,
                {
                    deviceByName("agent-1", "dev-2"),
                    deviceByName("agent-2", "dev-2")
                },
                &result);

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", result.Devices[1].GetDeviceUUID());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("baz", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, info.State);
            UNIT_ASSERT_VALUES_EQUAL(2, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", info.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", info.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(nativeBlockSize, info.LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(1000, info.StateTs.GetValue());
        });

        // regular
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state.CreateDiskFromDevices(
                TInstant::FromValue(1000),
                db,
                false,  // force
                "nonrepl",
                nativeBlockSize,
                {
                    deviceByUUID("uuid-2.3"),
                    deviceByUUID("uuid-1.3")
                },
                &result);

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.3", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.3", result.Devices[1].GetDeviceUUID());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("nonrepl", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, info.State);
            UNIT_ASSERT_VALUES_EQUAL(2, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.3", info.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.3", info.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(nativeBlockSize, info.LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(1000, info.StateTs.GetValue());
        });
    }

    Y_UNIT_TEST(ShouldCollectMTBF)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        NProto::TAgentConfig agent1;
        agent1.SetAgentId("agent-1");
        agent1.MutableTimeBetweenFailures()->SetWorkTime(40);
        agent1.MutableTimeBetweenFailures()->SetBrokenCount(1);
        NProto::TAgentConfig agent2;
        agent2.SetAgentId("agent-2");
        agent2.MutableTimeBetweenFailures()->SetWorkTime(100);
        agent2.MutableTimeBetweenFailures()->SetBrokenCount(2);
        NProto::TAgentConfig agent3;
        agent3.SetAgentId("agent-3");

        const TVector agents{agent1, agent2, agent3};
        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(diskRegistryGroup)
            .WithKnownAgents(agents)
            .Build();

        state.PublishCounters(TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(
            diskRegistryGroup->GetCounter("MeanTimeBetweenFailures")->Val(),
            46);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
