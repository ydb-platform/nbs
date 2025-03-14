#include "disk_registry_state.h"

#include "disk_registry_database.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NDiskRegistryStateTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateTest)
{
    Y_UNIT_TEST(ShouldRegisterAgent)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const ui32 agentId = 42;

        const auto partial = AgentConfig(agentId, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        const auto complete = AgentConfig(agentId, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { complete });
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, partial));
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, complete));

            UNIT_ASSERT_VALUES_EQUAL(S_OK,
                state.UnregisterAgent(db, agentId).GetCode());

            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY,
                state.UnregisterAgent(db, agentId).GetCode());
        });
    }

    Y_UNIT_TEST(ShouldAcceptUnallowedAgent)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto config1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        const auto config2 = AgentConfig(2, {
            Device("dev-2", "uuid-2", "rack-1")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { config1 });
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config1));
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config2));
        });

        auto* agent = state.FindAgent(config2.GetAgentId());
        UNIT_ASSERT(agent);
        UNIT_ASSERT_VALUES_EQUAL(0, agent->DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, agent->UnknownDevicesSize());
    }

    Y_UNIT_TEST(ShouldAcceptAgentWithUnallowedDevice)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto config1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        const auto config2 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { config1 });
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config2));
            auto* agent = state.FindAgent(config2.GetAgentId());
            UNIT_ASSERT(agent);
            UNIT_ASSERT_VALUES_EQUAL(1, agent->DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                agent->GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, agent->UnknownDevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                agent->GetUnknownDevices(0).GetDeviceUUID());
        });
    }

    Y_UNIT_TEST(ShouldUpdateKnownAgents)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto config1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        const auto config2 = AgentConfig(1, {
            Device("dev-3", "uuid-3", "rack-1")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { config1 });

            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config1));

            {
                auto* agent = state.FindAgent(config2.GetAgentId());
                UNIT_ASSERT(agent);
                UNIT_ASSERT_VALUES_EQUAL(2, agent->DevicesSize());
                UNIT_ASSERT_VALUES_EQUAL(0, agent->UnknownDevicesSize());
            }

            UpdateConfig(state, db, { config2 });

            {
                auto* agent = state.FindAgent(config2.GetAgentId());
                UNIT_ASSERT(agent);
                UNIT_ASSERT_VALUES_EQUAL(0, agent->DevicesSize());
                UNIT_ASSERT_VALUES_EQUAL(2, agent->UnknownDevicesSize());
            }

            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config1));

            {
                auto* agent = state.FindAgent(config2.GetAgentId());
                UNIT_ASSERT(agent);
                UNIT_ASSERT_VALUES_EQUAL(0, agent->DevicesSize());
                UNIT_ASSERT_VALUES_EQUAL(2, agent->UnknownDevicesSize());
            }

            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config2));

            {
                auto* agent = state.FindAgent(config2.GetAgentId());
                UNIT_ASSERT(agent);
                UNIT_ASSERT_VALUES_EQUAL(1, agent->DevicesSize());
                UNIT_ASSERT_VALUES_EQUAL(0, agent->UnknownDevicesSize());
            }
        });

        const auto current = state.GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(1, current.KnownAgentsSize());

        const auto& agents = current.GetKnownAgents(0);

        UNIT_ASSERT_VALUES_EQUAL("agent-1", agents.GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(1, agents.DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3", agents.GetDevices(0).GetDeviceUUID());
    }

    Y_UNIT_TEST(ShouldCorrectlyReRegisterAgent)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto config1 = AgentConfig(1, {
            Device(
                "dev-1",
                "uuid-1",
                "rack-1",
                DefaultBlockSize,
                11_GB
            ),
            Device(
                "dev-2",
                "uuid-2",
                "rack-1",
                DefaultBlockSize,
                10_GB
            ),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { config1 });

            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config1));
            state.MarkDeviceAsClean(Now(), db, "uuid-1");
            state.MarkDeviceAsClean(Now(), db, "uuid-2");

            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            SortBy(devices, [&] (const auto& d) {
                return d.GetDeviceUUID();
            });
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultBlockSize,
                devices[0].GetBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultBlockSize,
                devices[1].GetBlocksCount()
            );
        });

        // simply repeating agent registration and disk allocation - device
        // sizes should not change
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config1));

            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            SortBy(devices, [&] (const auto& d) {
                return d.GetDeviceUUID();
            });
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultBlockSize,
                devices[0].GetBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultBlockSize,
                devices[1].GetBlocksCount()
            );
        });
    }

    Y_UNIT_TEST(ShouldRejectDestructiveConfig)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1"),
            Device("dev-2", "uuid-1.2", "rack-1")
        });

        const auto agent2a = AgentConfig(2, {
            Device("dev-1", "uuid-2.1", "rack-2"),
            Device("dev-2", "uuid-2.2", "rack-2")
        });

        const auto agent2b = AgentConfig(2, {
            Device("dev-1", "uuid-2.1", "rack-2"),
            Device("dev-3", "uuid-2.3", "rack-2"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2a })
            .WithDisks({
                Disk("disk-1", {"uuid-1.1", "uuid-2.1"}),
                Disk("disk-2", {"uuid-1.2", "uuid-2.2"})
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            const auto error = state.UpdateConfig(
                db,
                MakeConfig({ agent1, agent2b }),
                true,   // ignoreVersion
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", affectedDisks[0]);

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskStateUpdates().size());
        });
    }

    Y_UNIT_TEST(ShouldNotTreatAppearanceOfSerialAndPhysicalOffsetAsDestructiveChange)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDeviceConfig device;

        device.SetDeviceName("dev-1");
        device.SetDeviceUUID("uuid-1");
        device.SetRack("rack-1");
        device.SetBlockSize(4_KB);
        device.SetBlocksCount(10_GB / 4_KB);

        const auto agent1a = AgentConfig(1, {device});

        device.SetSerialNumber("xxx");
        device.SetPhysicalOffset(111);
        const auto agent1b = AgentConfig(1, {device});

        device.SetSerialNumber("yyy");
        const auto agent1c = AgentConfig(1, {device});

        device.SetSerialNumber("xxx");
        device.SetPhysicalOffset(222);
        const auto agent1d = AgentConfig(1, {device});

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1a })
            .WithDisks({
                Disk("disk-1", {"uuid-1"}),
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [r, error] = state.RegisterAgent(db, agent1b, Now());

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [r, error] = state.RegisterAgent(db, agent1c, Now());

            // error is not propagated to TRegisterAgentResponse
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", r.AffectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ERROR, update);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TString affectedDisk;
            UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                db,
                "uuid-1",
                NProto::DEVICE_STATE_ONLINE,
                Now(),
                "test",
                affectedDisk));

            UNIT_ASSERT(affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ONLINE, update);
            UNIT_ASSERT_VALUES_EQUAL("", update.State.GetStateMessage());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [r, error] = state.RegisterAgent(db, agent1d, Now());

            // error is not propagated to TRegisterAgentResponse
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", r.AffectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL(3, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ERROR, update);
        });
    }

    Y_UNIT_TEST(ShouldRejectConfigWithWrongVersion)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1a = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1"),
            Device("dev-2", "uuid-1.2", "rack-1")
        });

        const auto agent1b = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1"),
            Device("dev-2", "uuid-1.2", "rack-1"),
            Device("dev-3", "uuid-1.3", "rack-1")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig(1, { agent1a })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            const auto error = state.UpdateConfig(
                db,
                MakeConfig(42, { agent1b }),
                false,  // ignoreVersion
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            const auto error = state.UpdateConfig(
                db,
                MakeConfig(1, { agent1b }),
                false,  // ignoreVersion
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            const auto error = state.UpdateConfig(
                db,
                MakeConfig(2, { agent1a }),
                false,  // ignoreVersion
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });
    }

    Y_UNIT_TEST(ShouldRemoveDiscardedAgent)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1"),
            Device("dev-2", "uuid-1.2", "rack-1")
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-2.1", "rack-2"),
            Device("dev-2", "uuid-2.2", "rack-2")
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.UpdateAgent(agent1);
            db.UpdateAgent(agent2);
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2 })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            const auto error = state.UpdateConfig(
                db,
                MakeConfig(0, { agent1 }),
                false,  // ignoreVersion
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TAgentConfig> agents;

            db.ReadAgents(agents);
            UNIT_ASSERT_VALUES_EQUAL(2, agents.size());

            UNIT_ASSERT_VALUES_EQUAL(1, agents[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(2, agents[0].DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(2, agents[1].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(0, agents[1].DevicesSize());
        });
    }

    Y_UNIT_TEST(AllocateDiskWithDifferentBlockSize)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto device = Device("dev-1", "uuid-1", "rack-1", 512, 10_GB);
        device.SetUnadjustedBlockCount(1_TB / 512);

        const auto config1 = AgentConfig(42, {device});

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ config1 })
            .Build();

        TVector<TDeviceConfig> devices;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(42, devices.front().GetNodeId());
        });
    }

    Y_UNIT_TEST(ShouldNotReturnErrorForAlreadyAllocatedDisks)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto config1 = AgentConfig(42, {
            Device("dev-1", "uuid-1", "rack-1")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ config1 })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(42, devices.front().GetNodeId());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 9_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(42, devices.front().GetNodeId());
        });
    }

    Y_UNIT_TEST(AllocateDiskOnSingleDevice)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto device = Device("dev-1", "uuid-1", "rack-1", 512, 10_GB);
        device.SetUnadjustedBlockCount(1_TB / 512);

        const auto config1 = AgentConfig(42, {device});

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ config1 })
            .Build();

        TVector<TDeviceConfig> devices;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(42, devices.front().GetNodeId());
        });

        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            auto& devices = diskInfo.Devices;
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(42, devices.front().GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices.front().GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(DefaultLogicalBlockSize, diskInfo.LogicalBlockSize);
        }
    }

    Y_UNIT_TEST(AllocateDiskOnFewDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto config1 = AgentConfig(100, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1")
        });

        const auto config2 = AgentConfig(200, {
            Device("dev-1", "uuid-4", "rack-1"),
            Device("dev-2", "uuid-5", "rack-1"),
            Device("dev-3", "uuid-6", "rack-1"),
            Device("dev-4", "uuid-7", "rack-1")
        });

        const auto config3 = AgentConfig(300, {
            Device("dev-1", "uuid-8", "rack-1"),
            Device("dev-2", "uuid-9", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ config1, config2, config3 })
            .Build();

        TVector<TDeviceConfig> expected;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = AllocateDisk(db, state, "disk-id", {}, {}, 90_GB, expected);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(9, expected.size());
        });

        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-id", diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(
                expected.size(),
                diskInfo.Devices.size());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                diskInfo.LogicalBlockSize);

            for (size_t i = 0; i != expected.size(); ++i) {
                auto& device = diskInfo.Devices[i];

                UNIT_ASSERT_VALUES_EQUAL(
                    expected[i].GetDeviceUUID(),
                    device.GetDeviceUUID());

                UNIT_ASSERT_VALUES_EQUAL(
                    expected[i].GetBlockSize(),
                    device.GetBlockSize());

                UNIT_ASSERT_VALUES_EQUAL(
                    expected[i].GetBlocksCount(),
                    device.GetBlocksCount());
            }
        }
    }

    Y_UNIT_TEST(ShouldTakeDeviceOverridesIntoAccount)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto config1 = AgentConfig(100, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ config1 })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 28_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            SortBy(devices, [] (const auto& d) {
                return d.GetDeviceUUID();
            });

            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultBlockSize,
                devices[0].GetBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultBlockSize,
                devices[1].GetBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", devices[2].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultBlockSize,
                devices[2].GetBlocksCount()
            );
        });

        TVector<NProto::TDeviceOverride> deviceOverrides;
        deviceOverrides.emplace_back();
        deviceOverrides.back().SetDiskId("disk-1");
        deviceOverrides.back().SetDevice("uuid-1");
        deviceOverrides.back().SetBlocksCount(9_GB / DefaultBlockSize);
        deviceOverrides.emplace_back();
        deviceOverrides.back().SetDiskId("disk-1");
        deviceOverrides.back().SetDevice("uuid-2");
        deviceOverrides.back().SetBlocksCount(9_GB / DefaultBlockSize);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { config1 }, deviceOverrides);
        });

        // overrides should affect this AllocateDisk call
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 28_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            SortBy(devices, [] (const auto& d) {
                return d.GetDeviceUUID();
            });

            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                9_GB / DefaultBlockSize,
                devices[0].GetBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                9_GB / DefaultBlockSize,
                devices[1].GetBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", devices[2].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultBlockSize,
                devices[2].GetBlocksCount()
            );
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
            CleanDevices(state, db);
        });

        // overrides should only affect disk-1
        for (ui32 i = 0; i < 3; ++i) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) {
                TVector<TDeviceConfig> devices;
                auto error = AllocateDisk(db, state, "disk-2", {}, {}, 28_GB, devices);
                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_EQUAL(i ? S_ALREADY : S_OK, error.GetCode());
                UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
                SortBy(devices, [] (const auto& d) {
                    return d.GetDeviceUUID();
                });

                UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    10_GB / DefaultBlockSize,
                    devices[0].GetBlocksCount()
                );
                UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    10_GB / DefaultBlockSize,
                    devices[1].GetBlocksCount()
                );
                UNIT_ASSERT_VALUES_EQUAL("uuid-3", devices[2].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    10_GB / DefaultBlockSize,
                    devices[2].GetBlocksCount()
                );
            });
        }
    }

    Y_UNIT_TEST(CanNotAllocateTooBigDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto config1 = AgentConfig(1000, {
            Device("dev-1", "uuid-1", "rack-1")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ config1 })
            .Build();

        TVector<TDeviceConfig> devices;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = AllocateDisk(db, state, "disk-id", {}, {}, 50_GB, devices);

            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            UNIT_ASSERT(devices.empty());
        });
    }

    Y_UNIT_TEST(ReAllocateDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto config1 = AgentConfig(42, {
            Device("dev-1", "uuid-1", "rack-1")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ config1 })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 10_GB, devices);
            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            UNIT_ASSERT(devices.empty());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> dirtyDevices;
            db.ReadDirtyDevices(dirtyDevices);

            for (const auto& dd: dirtyDevices) {
                state.MarkDeviceAsClean(Now(), db, dd.Id);
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
        });
    }

    Y_UNIT_TEST(ResizeDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto config1 = AgentConfig(42, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        const auto config2 = AgentConfig(43, {
            Device("dev-1", "uuid-3", "rack-2"),
            Device("dev-2", "uuid-4", "rack-2"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ config1, config2 })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());

            Sort(devices, TByDeviceUUID());
            // Expect that the allocation of a new device will occur on the rack-1,
            // on the same rack where the disk was allocated initially.
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());

            Sort(devices, TByDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", devices[2].GetDeviceUUID());
        });
    }

    Y_UNIT_TEST(AllocateWithBlinkingAgent)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto stage1 = AgentConfig(1000, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1")
        });

        const auto stage2 = AgentConfig(1000, {
            Device("dev-2", "uuid-2", "rack-1")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { stage1 });
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, stage1));
            CleanDevices(state, db);

            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 30_GB, devices); // use all devices
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(stage1.DevicesSize(), devices.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            // lost dev-1 & dev-3
            auto [r, regError] = state.RegisterAgent(db, stage2, Now());
            UNIT_ASSERT_SUCCESS(regError);

            UNIT_ASSERT_VALUES_EQUAL(1, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", r.AffectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());

            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ERROR, update);

            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 10_GB, devices);
            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            UNIT_ASSERT(devices.empty());

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            // restore dev-1 & dev-3 but with error state
            auto [r, error] = state.RegisterAgent(db, stage1, Now());

            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
        });

        // restore dev-1 to online state
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TString affectedDisk;
            UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                db,
                "uuid-1",
                NProto::DEVICE_STATE_ONLINE,
                Now(),
                "test",
                affectedDisk));

            UNIT_ASSERT(!affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
        });

        // restore dev-3 to online state
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TString affectedDisk;
            UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                db,
                "uuid-3",
                NProto::DEVICE_STATE_ONLINE,
                Now(),
                "test",
                affectedDisk));

            UNIT_ASSERT(affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDiskStateUpdates().size());

            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ONLINE, update);
            UNIT_ASSERT_VALUES_EQUAL("", update.State.GetStateMessage());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 20_GB, devices);
            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode()); // can't use dev-1 nor dev-3
            UNIT_ASSERT(devices.empty());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 10_GB, devices);
            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode()); // can't use dev-1 nor dev-3
            UNIT_ASSERT(devices.empty());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            // lost dev-1 & dev-3
            auto [r, error] = state.RegisterAgent(db, stage2, Now());
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(1, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", r.AffectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL(3, state.GetDiskStateUpdates().size());

            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskStateUpdates().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 30_GB, devices);
            // not enough online devices
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        // restore uuid-1 & uuid-3
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, stage1));

            {
                TString affectedDisk;
                UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                    db,
                    "uuid-1",
                    NProto::DEVICE_STATE_ONLINE,
                    Now(),
                    "test",
                    affectedDisk));
            }

            {
                TString affectedDisk;
                UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                    db,
                    "uuid-3",
                    NProto::DEVICE_STATE_ONLINE,
                    Now(),
                    "test",
                    affectedDisk));
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 30_GB, devices);
            // not enough clean devices
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> dirtyDevices;
            db.ReadDirtyDevices(dirtyDevices);

            UNIT_ASSERT_VALUES_EQUAL(3, dirtyDevices.size());
        });

        UNIT_ASSERT_VALUES_EQUAL(3, state.GetDirtyDevices().size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, stage1)); // restore dev-1 & dev-3
            UNIT_ASSERT_VALUES_EQUAL(3, state.GetDirtyDevices().size());
            CleanDevices(state, db);

            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error); // use all devices
            UNIT_ASSERT_VALUES_EQUAL(stage1.DevicesSize(), devices.size());
        });
    }

    Y_UNIT_TEST(AcquireDisk)
    {
        const auto config1 = AgentConfig(1, { Device("dev-1", "uuid-1", "rack-1")});

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ config1 })
            .WithDisks({ Disk("disk-1", {"uuid-1"}) })
            .Build();

        {
            TDiskInfo diskInfo;
            auto error = state.StartAcquireDisk("disk-1", diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                diskInfo.LogicalBlockSize);
        }

        {
            TDiskInfo diskInfo;
            auto error = state.StartAcquireDisk("disk-1", diskInfo);
            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());
        }

        state.FinishAcquireDisk("disk-1");

        {
            TDiskInfo diskInfo;
            auto error = state.StartAcquireDisk("disk-1", diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                diskInfo.LogicalBlockSize);
        }

        {
            TDiskInfo diskInfo;
            auto error = state.StartAcquireDisk("disk-2", diskInfo);
            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
        }

        // TODO: test Acquire with migrations
    }

    Y_UNIT_TEST(ShouldGetDiskIds)
    {
        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithDisks({
                Disk("disk-1", {}),
                Disk("disk-2", {}),
                Disk("disk-3", {})
            })
            .Build();

        auto ids = state.GetDiskIds();

        UNIT_ASSERT_VALUES_EQUAL(ids.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ids[0], "disk-1");
        UNIT_ASSERT_VALUES_EQUAL(ids[1], "disk-2");
        UNIT_ASSERT_VALUES_EQUAL(ids[2], "disk-3");
    }

    Y_UNIT_TEST(ShouldNotAllocateDirtyDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1"),
            Device("dev-4", "uuid-4", "rack-1")
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-5", "rack-1"),
            Device("dev-2", "uuid-6", "rack-1"),
            Device("dev-3", "uuid-7", "rack-1")
        });

        TDiskRegistryState state =
            TDiskRegistryStateBuilder()
                .WithKnownAgents({agent1, agent2})
                .WithDisks({Disk("disk-1", {"uuid-1"})})
                .WithDirtyDevices(
                    {TDirtyDevice{"uuid-4", {}}, TDirtyDevice{"uuid-7", {}}})
                .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 50_GB, devices);
            // no memory: uuid-1 allocated for disk-1
            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 50_GB, devices);
            // still not enough memory: uuid-1 marked as dirty
            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> dirtyDevices;
            db.ReadDirtyDevices(dirtyDevices);

            for (const auto& dd: dirtyDevices) {
                state.MarkDeviceAsClean(Now(), db, dd.Id);
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 50_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(5, devices.size());
        });
    }

    Y_UNIT_TEST(ShouldPreserveDirtyDevicesAfterRegistration)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1"),
        });

        // same agent but without uuid-2
        const auto agent2 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1 })
            .WithDisks({ Disk("disk-1", {"uuid-1", "uuid-2"}) })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> dirtyDevices;
            db.ReadDirtyDevices(dirtyDevices);

            UNIT_ASSERT_VALUES_EQUAL(2, dirtyDevices.size());
            SortBy(dirtyDevices, [] (const auto& x) { return x.Id; });
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", dirtyDevices[0].Id);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", dirtyDevices[1].Id);
        });

        {
            auto dirtyDevices = state.GetDirtyDevices();

            UNIT_ASSERT_VALUES_EQUAL(2, dirtyDevices.size());
            Sort(dirtyDevices, TByDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", dirtyDevices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", dirtyDevices[1].GetDeviceUUID());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent2));
        });

        {
            auto dirtyDevices = state.GetDirtyDevices();

            UNIT_ASSERT_VALUES_EQUAL(2, dirtyDevices.size());
            Sort(dirtyDevices, TByDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", dirtyDevices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", dirtyDevices[1].GetDeviceUUID());
        }
    }

    Y_UNIT_TEST(ShouldGetDirtyDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, { Device("dev-1", "uuid-1", "rack-1") });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1 })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT(state.MarkDeviceAsDirty(db, "uuid-1"));
        });

        {
            auto dirtyDevices = state.GetDirtyDevices();
            UNIT_ASSERT_VALUES_EQUAL(dirtyDevices.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(dirtyDevices[0].GetDeviceUUID(), "uuid-1");
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.MarkDeviceAsClean(Now(), db, "uuid-1");
        });

        UNIT_ASSERT_VALUES_EQUAL(state.GetDirtyDevices().size(), 0);
    }

    void DoShouldUpdateCounters(bool disableFullGroupsCountCalculation)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1000, {
                Device("dev-1", "uuid-1", "rack-1"),
                Device("dev-2", "uuid-2", "rack-1")
            }),
            AgentConfig(2000, {
                Device("dev-1", "uuid-3", "rack-2"),
                Device("dev-2", "uuid-4", "rack-2")
            }),
            AgentConfig(3000, {
                Device("dev-1", "uuid-5", "rack-3")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-2", "uuid-6", "rack-3")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL)
            })
        };

        NProto::TStorageServiceConfig proto = CreateDefaultStorageConfigProto();
        proto.SetDisableFullPlacementGroupCountCalculation(
            disableFullGroupsCountCalculation);

        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(CreateStorageConfig(proto))
            .With(diskRegistryGroup)
            .WithKnownAgents(agents)
            .AddDevicePoolConfig("local-ssd", 10_GB, NProto::DEVICE_POOL_KIND_LOCAL)
            .Build();

        TDiskRegistrySelfCounters::TDevicePoolCounters defaultPool;
        TDiskRegistrySelfCounters::TDevicePoolCounters localPool;

        defaultPool.Init(diskRegistryGroup->GetSubgroup("pool", "default"));
        localPool.Init(diskRegistryGroup->GetSubgroup("pool", "local"));

        auto freeBytes = diskRegistryGroup->GetCounter("FreeBytes");
        auto totalBytes = diskRegistryGroup->GetCounter("TotalBytes");
        auto brokenBytes = diskRegistryGroup->GetCounter("BrokenBytes");
        auto decommissionedBytes = diskRegistryGroup->GetCounter("DecommissionedBytes");
        auto suspendedBytes = diskRegistryGroup->GetCounter("SuspendedBytes");
        auto dirtyBytes = diskRegistryGroup->GetCounter("DirtyBytes");
        auto allocatedBytes = diskRegistryGroup->GetCounter("AllocatedBytes");
        auto allocatedDisks = diskRegistryGroup->GetCounter("AllocatedDisks");
        auto allocatedDevices = diskRegistryGroup->GetCounter("AllocatedDevices");
        auto dirtyDevices = diskRegistryGroup->GetCounter("DirtyDevices");
        auto devicesInOnlineState = diskRegistryGroup->GetCounter("DevicesInOnlineState");
        auto devicesInWarningState = diskRegistryGroup->GetCounter("DevicesInWarningState");
        auto devicesInErrorState = diskRegistryGroup->GetCounter("DevicesInErrorState");
        auto agentsInOnlineState = diskRegistryGroup->GetCounter("AgentsInOnlineState");
        auto agentsInWarningState = diskRegistryGroup->GetCounter("AgentsInWarningState");
        auto agentsInUnavailableState = diskRegistryGroup->GetCounter("AgentsInUnavailableState");
        auto disksInOnlineState = diskRegistryGroup->GetCounter("DisksInOnlineState");
        auto disksInMigrationState = diskRegistryGroup->GetCounter("DisksInMigrationState");
        auto disksInTemporarilyUnavailableState =
            diskRegistryGroup->GetCounter("DisksInTemporarilyUnavailableState");
        auto disksInErrorState = diskRegistryGroup->GetCounter("DisksInErrorState");
        auto placementGroups = diskRegistryGroup->GetCounter("PlacementGroups");
        auto fullPlacementGroups = diskRegistryGroup->GetCounter("FullPlacementGroups");
        auto allocatedDisksInGroups = diskRegistryGroup->GetCounter("AllocatedDisksInGroups");

        auto agentCounters = diskRegistryGroup
            ->GetSubgroup("agent", "agent-1000");

        auto totalReadCount = agentCounters->GetCounter("ReadCount");
        auto totalReadBytes = agentCounters->GetCounter("ReadBytes");
        auto totalWriteCount = agentCounters->GetCounter("WriteCount");
        auto totalWriteBytes = agentCounters->GetCounter("WriteBytes");
        auto totalZeroCount = agentCounters->GetCounter("ZeroCount");
        auto totalZeroBytes = agentCounters->GetCounter("ZeroBytes");

        auto device = agentCounters->GetSubgroup("device", "agent-1000:dev-1");

        auto timePercentiles = device->GetSubgroup("percentiles", "Time");
        auto p90 = timePercentiles->GetCounter("90");

        auto readCount = device->GetCounter("ReadCount");
        auto readBytes = device->GetCounter("ReadBytes");
        auto writeCount = device->GetCounter("WriteCount");
        auto writeBytes = device->GetCounter("WriteBytes");
        auto zeroCount = device->GetCounter("ZeroCount");
        auto zeroBytes = device->GetCounter("ZeroBytes");

        UNIT_ASSERT_VALUES_EQUAL(0, p90->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalWriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalZeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, readCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, readBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, writeCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, writeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, zeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, zeroBytes->Val());

        auto makeDeviceStats = [] (
            const TString& uuid,
            const TString& deviceName,
            std::pair<ui64, ui64> r,
            std::pair<ui64, ui64> w,
            std::pair<ui64, ui64> z)
        {
            NProto::TDeviceStats stats;
            stats.SetDeviceUUID(uuid);
            stats.SetDeviceName(deviceName);

            stats.SetNumReadOps(r.first);
            stats.SetBytesRead(r.second);

            stats.SetNumWriteOps(w.first);
            stats.SetBytesWritten(w.second);

            stats.SetNumZeroOps(z.first);
            stats.SetBytesZeroed(z.second);

            {
                auto& bucket = *stats.AddHistogramBuckets();
                bucket.SetValue(1000);
                bucket.SetCount(90);
            }

            {
                auto& bucket = *stats.AddHistogramBuckets();
                bucket.SetValue(10000);
                bucket.SetCount(4);
            }

            {
                auto& bucket = *stats.AddHistogramBuckets();
                bucket.SetValue(100000);
                bucket.SetCount(5);
            }

            {
                auto& bucket = *stats.AddHistogramBuckets();
                bucket.SetValue(10000000);
                bucket.SetCount(1);
            }

            return stats;
        };

        {
            NProto::TAgentStats agentStats;
            agentStats.SetNodeId(1000);

            *agentStats.AddDeviceStats() = makeDeviceStats(
                "uuid-1", "dev-1", { 200, 10000 }, { 100, 5000 }, {10, 1000});

            *agentStats.AddDeviceStats() = makeDeviceStats(
                "uuid-2", "dev-2", { 100, 40000 }, { 20, 1000 }, {20, 2000});

            state.UpdateAgentCounters(agentStats);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, p90->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalWriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalZeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, readCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, readBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, writeCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, writeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, zeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, zeroBytes->Val());

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(1000, p90->Val());
        UNIT_ASSERT_VALUES_EQUAL(300, totalReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(50000, totalReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(120, totalWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(6000, totalWriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(30, totalZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(3000, totalZeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(200, readCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(10000, readBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(100, writeCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(5000, writeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10, zeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1000, zeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, dirtyDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(60_GB, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, brokenBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, suspendedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, allocatedDisks->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, allocatedDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(6, devicesInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, agentsInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, agentsInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, agentsInUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInTemporarilyUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, placementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, fullPlacementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, allocatedDisksInGroups->Val());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}
            ));
        });

        TString deviceToBreak;
        TString agentToBreak;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", "group-1", {}, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            deviceToBreak = devices[0].GetDeviceUUID();
            agentToBreak = devices[0].GetNodeId() == 1000 ? "agent-2000" : "agent-1000";
        });

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(40_GB, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, brokenBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, suspendedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            totalBytes->Val(),
            freeBytes->Val()
                + brokenBytes->Val()
                + decommissionedBytes->Val()
                + suspendedBytes->Val()
                + dirtyBytes->Val()
                + allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, allocatedDisks->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, allocatedDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(6, devicesInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, agentsInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, agentsInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, agentsInUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, disksInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInTemporarilyUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, placementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, fullPlacementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, allocatedDisksInGroups->Val());

        UNIT_ASSERT_VALUES_EQUAL(20_GB, defaultPool.FreeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(40_GB, defaultPool.TotalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, defaultPool.AllocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, defaultPool.AllocatedDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(4, defaultPool.DevicesInOnlineState->Val());

        UNIT_ASSERT_VALUES_EQUAL(20_GB, localPool.FreeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, localPool.TotalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localPool.AllocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localPool.AllocatedDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, localPool.DevicesInOnlineState->Val());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
        });

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(30_GB, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, brokenBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, suspendedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(30_GB, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            totalBytes->Val(),
            freeBytes->Val()
                + brokenBytes->Val()
                + decommissionedBytes->Val()
                + suspendedBytes->Val()
                + dirtyBytes->Val()
                + allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, allocatedDisks->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, allocatedDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(6, devicesInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, agentsInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, agentsInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, agentsInUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, disksInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInTemporarilyUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, placementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            disableFullGroupsCountCalculation ? 0 : 1,
            fullPlacementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, allocatedDisksInGroups->Val());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            auto error = state.UpdateDeviceState(
                db,
                deviceToBreak,
                NProto::DEVICE_STATE_ERROR,
                Now(),
                "test",
                affectedDisk
            );

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UpdateAgentState(state, db, agentToBreak, NProto::AGENT_STATE_WARNING);
        });

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(20_GB, freeBytes->Val()); // the agent is in a warning state
        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, brokenBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, suspendedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            totalBytes->Val(),
            freeBytes->Val()
                + brokenBytes->Val()
                + decommissionedBytes->Val()
                + suspendedBytes->Val()
                + dirtyBytes->Val()
                + allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, allocatedDisks->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, allocatedDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(5, devicesInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, devicesInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, agentsInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, agentsInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, agentsInUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInTemporarilyUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, disksInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, placementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            disableFullGroupsCountCalculation ? 0 : 1,
            fullPlacementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, allocatedDisksInGroups->Val());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto error = state.SuspendDevice(db, deviceToBreak);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(20_GB, freeBytes->Val()); // the agent is in a warning state
        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, brokenBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, suspendedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            totalBytes->Val(),
            freeBytes->Val()
                + brokenBytes->Val()
                + decommissionedBytes->Val()
                + suspendedBytes->Val()
                + dirtyBytes->Val()
                + allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, allocatedDisks->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, allocatedDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(5, devicesInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, devicesInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, agentsInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, agentsInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, agentsInUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInTemporarilyUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, disksInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, placementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            disableFullGroupsCountCalculation ? 0 : 1,
            fullPlacementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, allocatedDisksInGroups->Val());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT(state.MarkDeviceAsDirty(db, "uuid-5"));
        });

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(10_GB, freeBytes->Val()); // the agent is in a warning state
        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, brokenBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, suspendedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, dirtyBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            totalBytes->Val(),
            freeBytes->Val()
                + brokenBytes->Val()
                + decommissionedBytes->Val()
                + suspendedBytes->Val()
                + dirtyBytes->Val()
                + allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, allocatedDisks->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, allocatedDevices->Val());
        UNIT_ASSERT_VALUES_EQUAL(5, devicesInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, devicesInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, agentsInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, agentsInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, agentsInUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInOnlineState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInTemporarilyUnavailableState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, disksInErrorState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, placementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            disableFullGroupsCountCalculation ? 0 : 1,
            fullPlacementGroups->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, allocatedDisksInGroups->Val());

        UNIT_ASSERT_VALUES_EQUAL(10_GB, localPool.FreeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, localPool.TotalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localPool.AllocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localPool.BrokenBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localPool.DecommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, localPool.DirtyBytes->Val());
    }

    Y_UNIT_TEST(ShouldUpdateCounters)
    {
        DoShouldUpdateCounters(false);
    }

    Y_UNIT_TEST(ShouldUpdateCountersWithDisabledFullGroupsCalculation)
    {
        DoShouldUpdateCounters(true);
    }

    Y_UNIT_TEST(ShouldRejectBrokenStats)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto config1 = AgentConfig(1000, {
            Device("dev-1", "uuid-1.1"),
        });
        auto config2 = AgentConfig(1001, {
            Device("dev-1", "uuid-2.1"),
            Device("dev-3", "uuid-2.3"),
        });

        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(diskRegistryGroup)
            .WithConfig({ config1, config2 })
            .Build();

        config2 = AgentConfig(1001, {
            Device("dev-1", "uuid-2.1"),
            // add an unknown device to #1001
            Device("dev-2", "uuid-2.2"),
            Device("dev-3", "uuid-2.3"),
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config1));
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config2));
        });

        state.PublishCounters(Now());

        {
            NProto::TAgentStats stats;
            stats.SetNodeId(1001);
            auto* d = stats.AddDeviceStats();
            d->SetDeviceUUID("uuid-1.1"); // uuid-1.1 belongs to agent-1000

            auto error = state.UpdateAgentCounters(stats);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        }

        {
            NProto::TAgentStats stats;
            stats.SetNodeId(1001);

            {
                auto* d = stats.AddDeviceStats();
                d->SetDeviceUUID("uuid-2.1");
                d->SetDeviceName("dev-1");
                d->SetBytesRead(4_KB);
                d->SetNumReadOps(1);
            }

            {
                auto* d = stats.AddDeviceStats();
                d->SetDeviceUUID("uuid-2.2");
                d->SetDeviceName("dev-2");
                d->SetBytesRead(8_KB);
                d->SetNumReadOps(2);
            }

            {
                auto* d = stats.AddDeviceStats();
                d->SetDeviceUUID("uuid-2.3");
                d->SetDeviceName("dev-3");
                d->SetBytesRead(12_KB);
                d->SetNumReadOps(3);
            }

            auto error = state.UpdateAgentCounters(stats);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        }

        state.PublishCounters(Now());

        UNIT_ASSERT(diskRegistryGroup->FindSubgroup("agent", "agent-1000"));

        auto counters = diskRegistryGroup->FindSubgroup("agent", "agent-1001");
        UNIT_ASSERT(counters);

        auto totalReadCount = counters->FindCounter("ReadCount");
        UNIT_ASSERT(totalReadCount);
        UNIT_ASSERT_VALUES_EQUAL(4, totalReadCount->Val());

        auto totalReadBytes = counters->FindCounter("ReadBytes");
        UNIT_ASSERT(totalReadBytes);
        UNIT_ASSERT_VALUES_EQUAL(16_KB, totalReadBytes->Val());

        {
            auto device = counters->FindSubgroup("device", "agent-1001:dev-1");
            UNIT_ASSERT(device);

            auto readCount = device->FindCounter("ReadCount");
            UNIT_ASSERT(readCount);
            UNIT_ASSERT_VALUES_EQUAL(1, readCount->Val());

            auto readBytes = device->FindCounter("ReadBytes");
            UNIT_ASSERT(readBytes);
            UNIT_ASSERT_VALUES_EQUAL(4_KB, readBytes->Val());
        }

        // no metrics for the unknown device
        UNIT_ASSERT(!counters->FindSubgroup("device", "agent-1001:dev-2"));

        {
            auto device = counters->FindSubgroup("device", "agent-1001:dev-3");
            UNIT_ASSERT(device);

            auto readCount = device->FindCounter("ReadCount");
            UNIT_ASSERT(readCount);
            UNIT_ASSERT_VALUES_EQUAL(3, readCount->Val());

            auto readBytes = device->FindCounter("ReadBytes");
            UNIT_ASSERT(readBytes);
            UNIT_ASSERT_VALUES_EQUAL(12_KB, readBytes->Val());
        }
    }

    Y_UNIT_TEST(ShouldRemoveAgentWithSameId)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto config1 = AgentConfig(1121, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        auto config2 = config1;
        config2.SetNodeId(1400);

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { config1 });
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config1));
        });

        {
            const auto agents = state.GetAgents();

            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_VALUES_EQUAL(1121, agents[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("agent-1121", agents[0].GetAgentId());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, config2));
        });

        {
            const auto agents = state.GetAgents();

            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_VALUES_EQUAL(1400, agents[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("agent-1121", agents[0].GetAgentId());
        }

        {
            const auto device = state.GetDevice("uuid-1");

            UNIT_ASSERT_VALUES_EQUAL("uuid-1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1400, device.GetNodeId());
        }
    }

    Y_UNIT_TEST(ShouldRejectUnknownDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig = AgentConfig(1121, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { agentConfig });

            UNIT_ASSERT(state.MarkDeviceAsDirty(db, "uuid-1"));
            UNIT_ASSERT(!state.MarkDeviceAsDirty(db, "unknown"));
        });

        UNIT_ASSERT(state.GetDirtyDevices().empty());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            RegisterAgent(state, db, agentConfig);
        });

        const auto& devices = state.GetDirtyDevices();

        UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(1121, devices[0].GetNodeId());
    }

    Y_UNIT_TEST(ShouldSupportPlacementGroups)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1"),
        });

        auto agentConfig2 = AgentConfig(2, {
            Device("dev-4", "uuid-4", "rack-2"),
            Device("dev-5", "uuid-5", "rack-2"),
            Device("dev-6", "uuid-6", "rack-2"),
        });

        auto agentConfig3 = AgentConfig(3, {
            Device("dev-7", "uuid-7", "rack-3"),
            Device("dev-8", "uuid-8", "rack-3"),
            Device("dev-9", "uuid-9", "rack-3"),
        });

        auto agentConfig4 = AgentConfig(4, {
            Device("dev-10", "uuid-10", "rack-4"),
            Device("dev-11", "uuid-11", "rack-4"),
            Device("dev-12", "uuid-12", "rack-4"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3,
                agentConfig4
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}
            ));
        });

        const auto& groups = state.GetPlacementGroups();
        UNIT_ASSERT_VALUES_EQUAL(1, groups.size());
        UNIT_ASSERT_VALUES_EQUAL("group-1", groups.begin()->first);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-5", devices[1].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-3", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-4", {}, {}, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-10", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-11", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-12", devices[2].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-5", {}, {}, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-3", devices[1].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-6", {}, {}, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-8", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-9", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-6", devices[2].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-1", "disk-5", "disk-3"};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                1,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL(E_PRECONDITION_FAILED, error.GetCode());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"disk-5"},
                disksToAdd
            );

            disksToAdd = {"disk-1"};
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                1,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_SUCCESS(error);
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, disksToAdd);

            disksToAdd = {"disk-5"};
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                2,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL(E_PRECONDITION_FAILED, error.GetCode());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"disk-5"},
                disksToAdd
            );

            disksToAdd = {"disk-3"};
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                1,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, error.GetCode());

            disksToAdd = {"disk-3"};
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                2,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_SUCCESS(error);
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, disksToAdd);

            disksToAdd = {"disk-4", "disk-5"};
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                3,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_PRECONDITION_FAILED,
                error.GetCode(),
                error.GetMessage()
            );
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"disk-5"},
                disksToAdd
            );

            disksToAdd = {"disk-4"};
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                3,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_SUCCESS(error);
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, disksToAdd);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-2"};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                4,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL(E_BS_RESOURCE_EXHAUSTED, error.GetCode());
            UNIT_ASSERT_C(
                HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                error.GetMessage());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"disk-2"},
                disksToAdd
            );

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDisks().size());
        });

        auto* group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(3, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-3", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-4", group->GetDisks(2).GetDiskId());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-2"};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                4,
                disksToAdd,
                {"disk-3"}
            );
            UNIT_ASSERT_SUCCESS(error);
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, disksToAdd);
        });

        group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(3, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-4", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(2).GetDiskId());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-4"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-4"));
        });

        group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(2, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(1).GetDiskId());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                6,
                disksToAdd,
                {"disk-1"}
            );
            group = state.FindPlacementGroup("group-1");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, group->DisksSize());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                7,
                disksToAdd,
                {"disk-2"}
            );
            group = state.FindPlacementGroup("group-1");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(0, group->DisksSize());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.DestroyPlacementGroup(db, "group-1", affectedDisks));
        });

        UNIT_ASSERT_VALUES_EQUAL(0, groups.size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
        });
    }

    Y_UNIT_TEST(ShouldSupportPartitionPlacementGroups)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1"),
        });

        auto agentConfig2 = AgentConfig(2, {
            Device("dev-4", "uuid-4", "rack-2"),
            Device("dev-5", "uuid-5", "rack-2"),
            Device("dev-6", "uuid-6", "rack-2"),
        });

        auto agentConfig3 = AgentConfig(3, {
            Device("dev-7", "uuid-7", "rack-3"),
            Device("dev-8", "uuid-8", "rack-3"),
            Device("dev-9", "uuid-9", "rack-3"),
        });

        auto agentConfig4 = AgentConfig(4, {
            Device("dev-10", "uuid-10", "rack-4"),
            Device("dev-11", "uuid-11", "rack-4"),
            Device("dev-12", "uuid-12", "rack-4"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3,
                agentConfig4
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_PARTITION,
                2));
        });

        const auto& groups = state.GetPlacementGroups();
        UNIT_ASSERT_VALUES_EQUAL(1, groups.size());
        UNIT_ASSERT_VALUES_EQUAL("group-1", groups.begin()->first);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-5", devices[1].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-3", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-4", {}, {}, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-10", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-11", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-12", devices[2].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-5", {}, {}, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-3", devices[1].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-6", {}, {}, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-8", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-9", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-6", devices[2].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-1", "disk-4"};
            // Error due to missed partition name.
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                1,
                disksToAdd,
                {});
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());

            // part-1 occupied rack-1 and rack-2.
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                1,
                1,
                disksToAdd,
                {});
            UNIT_ASSERT_SUCCESS(error);
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, disksToAdd);

            // part-2 occupied rack-2 and rack-3.
            disksToAdd = {"disk-2", "disk-3"};
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                2,
                2,
                disksToAdd,
                {});
            UNIT_ASSERT_SUCCESS(error);
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, disksToAdd);

            // Can't add disk-5(rack-1) to part-2 because there is an
            // intersection on rack-1 since it belongs to part-1
            disksToAdd = {"disk-5"};
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                2,
                3,
                disksToAdd,
                {});
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_PRECONDITION_FAILED,
                error.GetCode(),
                error.GetMessage());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"disk-5"},
                disksToAdd);

            // Can't add disk-5(rack-1) and disk-6(rack-2 & rack-3) to part-1
            // because there is an intersection on rack-2 & rack-3 since it
            // belongs to part-2
            disksToAdd = {"disk-6", "disk-5"};
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                1,
                3,
                disksToAdd,
                {});
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_PRECONDITION_FAILED,
                error.GetCode(),
                error.GetMessage());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"disk-6"},
                disksToAdd);

            // OK to add disk-5 to part-1.
            disksToAdd = {"disk-5"};
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                1,
                3,
                disksToAdd,
                {});
            UNIT_ASSERT_SUCCESS(error);
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, disksToAdd);

            // OK to add disk-6 to part-2.
            disksToAdd = {"disk-6"};
            error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                2,
                4,
                disksToAdd,
                {});
            UNIT_ASSERT_SUCCESS(error);
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, disksToAdd);
        });

        auto* group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(6, group->DisksSize());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            // Remove disk-3 from group. When remove disk, we may not specify a partition.
            TVector<TString> disksToAdd = {};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                5,
                disksToAdd,
                {"disk-3"});
            UNIT_ASSERT_SUCCESS(error);
        });

        group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(5, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-4", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(1, group->GetDisks(0).GetPlacementPartitionIndex());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(2).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-5", group->GetDisks(3).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-6", group->GetDisks(4).GetDiskId());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            // OK to deallocate disk from group.
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-4"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-4"));
        });

        group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(4, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-5", group->GetDisks(2).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-6", group->GetDisks(3).GetDiskId());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            // Remove disk-6 and add disk-3 to part-2.
            TVector<TString> disksToAdd = {"disk-3"};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                2,
                7,
                disksToAdd,
                {"disk-6"});
            group = state.FindPlacementGroup("group-1");
            UNIT_ASSERT_SUCCESS(error);
        });

        group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(4, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-5", group->GetDisks(2).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-3", group->GetDisks(3).GetDiskId());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            // Remove some disks. Let disk-3 remain in the group.
            TVector<TString> disksToAdd = {};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                8,
                disksToAdd,
                {"disk-1", "disk-2", "disk-5"});
            group = state.FindPlacementGroup("group-1");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, group->DisksSize());
        });
        group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(1, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-3", group->GetDisks(0).GetDiskId());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            // OK to destroy placement group with disks.
            UNIT_ASSERT_SUCCESS(
                state.DestroyPlacementGroup(db, "group-1", affectedDisks));
        });

        UNIT_ASSERT_VALUES_EQUAL(0, groups.size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-3"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-3"));
        });
    }

    Y_UNIT_TEST(ShouldAllocateInPlacementGroup)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1"),
        });

        auto agentConfig2 = AgentConfig(2, {
            Device("dev-4", "uuid-4", "rack-2"),
            Device("dev-5", "uuid-5", "rack-2"),
            Device("dev-6", "uuid-6", "rack-2"),
        });

        auto agentConfig3 = AgentConfig(3, {
            Device("dev-7", "uuid-7", "rack-3"),
            Device("dev-8", "uuid-8", "rack-3"),
            Device("dev-9", "uuid-9", "rack-3"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            // Unknown placement group name.
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-x", "group-2", 0, 10_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            // Can't specify partition index for PLACEMENT_STRATEGY_SPREAD.
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-x", "group-1", 1, 10_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", "group-1", 0, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", "group-1", 0, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-5", devices[1].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-3", "group-1", 0, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-4", "group-1", 0, 10_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_RESOURCE_EXHAUSTED, error.GetCode());
            UNIT_ASSERT_C(
                HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                error.GetMessage());
        });

        auto* group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(3, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-3", group->GetDisks(2).GetDiskId());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-3", "group-1", 0, 40_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            // group hint should be ignored since we already know this disk's
            // actual group
            auto error = AllocateDisk(db, state, "disk-3", "group-2", 0, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-8", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-9", devices[2].GetDeviceName());
        });

        group = state.FindPlacementGroup("group-2");
        UNIT_ASSERT(!group);

        group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(3, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-3", group->GetDisks(2).GetDiskId());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-3"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-3"));

            for (const auto& d: state.GetDirtyDevices()) {
                state.MarkDeviceAsClean(Now(), db, d.GetDeviceUUID());
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-4", "group-1", 0, 40_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-4", "group-1", 0, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-8", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-9", devices[2].GetDeviceName());
        });

        group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(3, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-4", group->GetDisks(2).GetDiskId());

        auto racksInfo = state.GatherRacksInfo("" /* poolName */);
        UNIT_ASSERT_VALUES_EQUAL(3, racksInfo.size());

        UNIT_ASSERT_VALUES_EQUAL(1, racksInfo[0].AgentInfos.size());
        UNIT_ASSERT_VALUES_EQUAL(
            agentConfig1.GetAgentId(),
            racksInfo[0].AgentInfos[0].AgentId
        );
        UNIT_ASSERT_VALUES_EQUAL(2, racksInfo[0].AgentInfos[0].FreeDevices);
        UNIT_ASSERT_VALUES_EQUAL(1, racksInfo[0].AgentInfos[0].AllocatedDevices);
        UNIT_ASSERT_VALUES_EQUAL(0, racksInfo[0].AgentInfos[0].BrokenDevices);

        UNIT_ASSERT_VALUES_EQUAL(1, racksInfo[1].AgentInfos.size());
        UNIT_ASSERT_VALUES_EQUAL(
            agentConfig2.GetAgentId(),
            racksInfo[1].AgentInfos[0].AgentId
        );
        UNIT_ASSERT_VALUES_EQUAL(1, racksInfo[1].AgentInfos[0].FreeDevices);
        UNIT_ASSERT_VALUES_EQUAL(2, racksInfo[1].AgentInfos[0].AllocatedDevices);
        UNIT_ASSERT_VALUES_EQUAL(0, racksInfo[1].AgentInfos[0].BrokenDevices);

        UNIT_ASSERT_VALUES_EQUAL(1, racksInfo[2].AgentInfos.size());
        UNIT_ASSERT_VALUES_EQUAL(
            agentConfig3.GetAgentId(),
            racksInfo[2].AgentInfos[0].AgentId
        );
        UNIT_ASSERT_VALUES_EQUAL(0, racksInfo[2].AgentInfos[0].FreeDevices);
        UNIT_ASSERT_VALUES_EQUAL(3, racksInfo[2].AgentInfos[0].AllocatedDevices);
        UNIT_ASSERT_VALUES_EQUAL(0, racksInfo[2].AgentInfos[0].BrokenDevices);
    }

    Y_UNIT_TEST(ShouldAllocateInPartitionPlacementGroup)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1"),
        });

        auto agentConfig2 = AgentConfig(2, {
            Device("dev-4", "uuid-4", "rack-2"),
            Device("dev-5", "uuid-5", "rack-2"),
            Device("dev-6", "uuid-6", "rack-2"),
        });

        auto agentConfig3 = AgentConfig(3, {
            Device("dev-7", "uuid-7", "rack-3"),
            Device("dev-8", "uuid-8", "rack-3"),
            Device("dev-9", "uuid-9", "rack-3"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_PARTITION,
                3));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            // Placement partition index out of range.
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-0", "group-1", 10, 10_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", "group-1", 1, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", "group-1", 1, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-3", devices[1].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-3", "group-1", 1, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-4", "group-1", 2, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-5", "group-1", 3, 10_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-5", "group-1", 2, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-8", devices[0].GetDeviceName());
        });

        auto* group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(5, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(1, group->GetDisks(0).GetPlacementPartitionIndex());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(1, group->GetDisks(1).GetPlacementPartitionIndex());
        UNIT_ASSERT_VALUES_EQUAL("disk-3", group->GetDisks(2).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(1, group->GetDisks(2).GetPlacementPartitionIndex());
        UNIT_ASSERT_VALUES_EQUAL("disk-4", group->GetDisks(3).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(2, group->GetDisks(3).GetPlacementPartitionIndex());
        UNIT_ASSERT_VALUES_EQUAL("disk-5", group->GetDisks(4).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(2, group->GetDisks(4).GetPlacementPartitionIndex());


        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-3", "group-1", 1, 40_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            // group and partition hint should be ignored since we already know this disk's
            // actual group and placement partition index
            auto error = AllocateDisk(db, state, "disk-3", "group-2", 3, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-5", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-6", devices[2].GetDeviceName());
        });

        group = state.FindPlacementGroup("group-2");
        UNIT_ASSERT(!group);

        group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(5, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(1, group->GetDisks(0).GetPlacementPartitionIndex());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(1, group->GetDisks(1).GetPlacementPartitionIndex());
        UNIT_ASSERT_VALUES_EQUAL("disk-3", group->GetDisks(2).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(1, group->GetDisks(2).GetPlacementPartitionIndex());
        UNIT_ASSERT_VALUES_EQUAL("disk-4", group->GetDisks(3).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(2, group->GetDisks(3).GetPlacementPartitionIndex());
        UNIT_ASSERT_VALUES_EQUAL("disk-5", group->GetDisks(4).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(2, group->GetDisks(4).GetPlacementPartitionIndex());


        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-3"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-3"));

            for (const auto& d: state.GetDirtyDevices()) {
                state.MarkDeviceAsClean(Now(), db, d.GetDeviceUUID());
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-6",
                "group-1",
                1,
                40_GB,
                devices);
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-6",
                "group-1",
                1,
                30_GB,
                devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-5", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-6", devices[2].GetDeviceName());
        });

        group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(5, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-4", group->GetDisks(2).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-5", group->GetDisks(3).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-6", group->GetDisks(4).GetDiskId());

        auto racksInfo = state.GatherRacksInfo("" /* poolName */);
        UNIT_ASSERT_VALUES_EQUAL(3, racksInfo.size());

        UNIT_ASSERT_VALUES_EQUAL(1, racksInfo[0].AgentInfos.size());
        UNIT_ASSERT_VALUES_EQUAL(
            agentConfig1.GetAgentId(),
            racksInfo[0].AgentInfos[0].AgentId
        );
        UNIT_ASSERT_VALUES_EQUAL(0, racksInfo[0].AgentInfos[0].FreeDevices);
        UNIT_ASSERT_VALUES_EQUAL(3, racksInfo[0].AgentInfos[0].AllocatedDevices);
        UNIT_ASSERT_VALUES_EQUAL(0, racksInfo[0].AgentInfos[0].BrokenDevices);

        UNIT_ASSERT_VALUES_EQUAL(1, racksInfo[1].AgentInfos.size());
        UNIT_ASSERT_VALUES_EQUAL(
            agentConfig2.GetAgentId(),
            racksInfo[1].AgentInfos[0].AgentId
        );
        UNIT_ASSERT_VALUES_EQUAL(0, racksInfo[1].AgentInfos[0].FreeDevices);
        UNIT_ASSERT_VALUES_EQUAL(3, racksInfo[1].AgentInfos[0].AllocatedDevices);
        UNIT_ASSERT_VALUES_EQUAL(0, racksInfo[1].AgentInfos[0].BrokenDevices);

        UNIT_ASSERT_VALUES_EQUAL(1, racksInfo[2].AgentInfos.size());
        UNIT_ASSERT_VALUES_EQUAL(
            agentConfig3.GetAgentId(),
            racksInfo[2].AgentInfos[0].AgentId
        );
        UNIT_ASSERT_VALUES_EQUAL(1, racksInfo[2].AgentInfos[0].FreeDevices);
        UNIT_ASSERT_VALUES_EQUAL(2, racksInfo[2].AgentInfos[0].AllocatedDevices);
        UNIT_ASSERT_VALUES_EQUAL(0, racksInfo[2].AgentInfos[0].BrokenDevices);
    }

    Y_UNIT_TEST(ShouldNotAllocateMoreThanLimitInPlacementGroup)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        auto agentConfig2 = AgentConfig(2, {
            Device("dev-2", "uuid-2", "rack-2"),
        });

        auto agentConfig3 = AgentConfig(3, {
            Device("dev-3", "uuid-3", "rack-3"),
        });

        auto agentConfig4 = AgentConfig(4, {
            Device("dev-4", "uuid-4", "rack-4"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3,
                agentConfig4
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", "group-1", 0, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", "group-1", 0, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-3", "group-1", 0, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-3", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-4",
                "group-1",
                0,
                10_GB,
                devices,
                TInstant::Seconds(100)
            );
            UNIT_ASSERT_VALUES_EQUAL(E_BS_RESOURCE_EXHAUSTED, error.GetCode());
            UNIT_ASSERT_C(
                HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetBrokenDisks().size());
            UNIT_ASSERT_VALUES_EQUAL("disk-4", state.GetBrokenDisks()[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(105),
                state.GetBrokenDisks()[0].TsToDestroy
            );

            error = AllocateDisk(
                db,
                state,
                "disk-5",
                "group-1",
                0,
                10_GB,
                devices,
                TInstant::Seconds(101)
            );
            UNIT_ASSERT_VALUES_EQUAL(E_BS_RESOURCE_EXHAUSTED, error.GetCode());
            UNIT_ASSERT_C(
                HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                error.GetMessage());

            auto brokenDisks = state.GetBrokenDisks();
            SortBy(brokenDisks, [] (const auto& info) {
                return info.DiskId;
            });

            UNIT_ASSERT_VALUES_EQUAL(2, brokenDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-4", brokenDisks[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(105),
                brokenDisks[0].TsToDestroy
            );
            UNIT_ASSERT_VALUES_EQUAL("disk-5", brokenDisks[1].DiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(106),
                brokenDisks[1].TsToDestroy
            );
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TBrokenDiskInfo> diskInfos;
            UNIT_ASSERT(db.ReadBrokenDisks(diskInfos));
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfos.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-4", diskInfos[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("disk-5", diskInfos[1].DiskId);

            state.DeleteBrokenDisks(db, {"disk-4"});
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetBrokenDisks().size());
            UNIT_ASSERT_VALUES_EQUAL("disk-5", state.GetBrokenDisks()[0].DiskId);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TBrokenDiskInfo> diskInfos;
            UNIT_ASSERT(db.ReadBrokenDisks(diskInfos));
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfos.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-5", diskInfos[0].DiskId);

            state.DeleteBrokenDisks(db, {"unknown-1", "unknown-2"});
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetBrokenDisks().size());
            UNIT_ASSERT_VALUES_EQUAL("disk-5", state.GetBrokenDisks()[0].DiskId);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TBrokenDiskInfo> diskInfos;
            UNIT_ASSERT(db.ReadBrokenDisks(diskInfos));
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfos.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-5", diskInfos[0].DiskId);

            state.DeleteBrokenDisks(db, {"disk-5"});
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDisks().size());
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TBrokenDiskInfo> diskInfos;
            UNIT_ASSERT(db.ReadBrokenDisks(diskInfos));
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfos.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-4", "", 0, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", devices[0].GetDeviceName());
        });

        auto* group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(3, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-3", group->GetDisks(2).GetDiskId());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-4"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-4"));
            CleanDevices(state, db);

            NProto::TPlacementGroupSettings settings;
            settings.SetMaxDisksInGroup(4);
            state.UpdatePlacementGroupSettings(db, "group-1", 4, std::move(settings));

            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-4",
                "group-1",
                0,
                10_GB,
                devices,
                TInstant::Seconds(102)
            );

            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", devices[0].GetDeviceName());

            error = AllocateDisk(
                db,
                state,
                "disk-5",
                "group-1",
                0,
                10_GB,
                devices,
                TInstant::Seconds(103)
            );
            UNIT_ASSERT_VALUES_EQUAL(E_BS_RESOURCE_EXHAUSTED, error.GetCode());
            UNIT_ASSERT_C(
                HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
                error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetBrokenDisks().size());
            UNIT_ASSERT_VALUES_EQUAL("disk-5", state.GetBrokenDisks()[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(108),
                state.GetBrokenDisks()[0].TsToDestroy
            );
        });

        UNIT_ASSERT_VALUES_EQUAL("group-1", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(4, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-3", group->GetDisks(2).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-4", group->GetDisks(3).GetDiskId());
    }

    Y_UNIT_TEST(ShouldNotAddOneDiskToTwoGroups)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agentConfig1 })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-2",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-1",
                "group-1",
                0,
                10_GB,
                devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-1"};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                2,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-1"};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-2",
                0,
                1,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL(E_PRECONDITION_FAILED, error.GetCode());
        });
    }

    Y_UNIT_TEST(ShouldIgnoreAlteringAlreadyLocatedDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agentConfig1 })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-2",
                NProto::PLACEMENT_STRATEGY_PARTITION,
                2));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-1",
                "group-1",
                0,
                10_GB,
                devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-1"};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                2,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-2",
                "group-2",
                1,
                10_GB,
                devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", devices[0].GetDeviceName());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-2"};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-2",
                1,
                2,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });
    }

    Y_UNIT_TEST(ShouldMoveDiskBetweenGroups)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agentConfig1 })
            .Build();

        // Make two groups with different placement strategy.
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-2",
                NProto::PLACEMENT_STRATEGY_PARTITION,
                2));
        });

        // Allocate disk in first group.
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-1",
                "group-1",
                0,
                10_GB,
                devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
        });

        // Remove disk from group.
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                2,
                disksToAdd,
                {"disk-1"}
            );
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        // Move disk to second group.
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-1"};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-2",
                1,
                1,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        // Remove disk from group.
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-2",
                0,
                2,
                disksToAdd,
                {"disk-1"}
            );
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        // Move disk to first group.
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-1"};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                3,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });
    }

    Y_UNIT_TEST(ShouldMoveDiskBetweenPartitions)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agentConfig1 })
            .Build();

        // Make group with partitions.
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_PARTITION,
                2));
        });

        // Allocate disk in first partition
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-1",
                "group-1",
                1,
                10_GB,
                devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
        });

        // Remove disk from group
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                0,
                2,
                disksToAdd,
                {"disk-1"}
            );
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        // Move disk to second partition.
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-1"};
            auto error = state.AlterPlacementGroupMembership(
                db,
                "group-1",
                2,
                3,
                disksToAdd,
                {}
            );
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });
    }

    Y_UNIT_TEST(ShouldConsistentlyUpdateDeviceRacksOnAgentUpdate)
    {
        const int AgentNodeId = 1;
        const auto config = AgentConfig(AgentNodeId, {
            Device("dev-1", "uuid-1", "rack-1", 512, 10_GB),
            Device("dev-2", "uuid-2", "rack-1", 512, 10_GB),
        });
        const auto configUpdated = AgentConfig(AgentNodeId, {
            Device("dev-1", "uuid-1", "rack-1-updated", 512, 10_GB),
            Device("dev-2", "uuid-2", "rack-1-updated", 512, 10_GB),
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({config})
            .WithSpreadPlacementGroups({"group-1"})
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {"group-1"}, {}, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
        });

        auto group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL(1, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL(1, group->GetDisks(0).DeviceRacksSize());
        UNIT_ASSERT_VALUES_EQUAL("rack-1", group->GetDisks(0).GetDeviceRacks(0));

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, configUpdated));
        });

        group = state.FindPlacementGroup("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL(1, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL(1, group->GetDisks(0).DeviceRacksSize());
        UNIT_ASSERT_VALUES_EQUAL("rack-1-updated", group->GetDisks(0).GetDeviceRacks(0));
    }

    Y_UNIT_TEST(ShouldUpdateAgentState)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-3", "rack-2"),
            Device("dev-2", "uuid-4", "rack-2"),
            Device("dev-3", "uuid-5", "rack-2")
        });

        const auto agent3 = AgentConfig(3, {
            Device("dev-1", "uuid-6", "rack-3"),
            Device("dev-2", "uuid-7", "rack-3")
        });

        ui64 lastSeqNo = 0;

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2, agent3 })
            .WithDisks({
                Disk("disk-1", {"uuid-1"}),
                Disk("disk-2", {"uuid-3"}),
                Disk("disk-3", {"uuid-2", "uuid-4"}),
            })
            .With(lastSeqNo)
            .Build();

        // #1 : online -> warning
        // disk-1 : online -> migration
        // disk-3 : online -> migration
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(2, affectedDisks.size());
            Sort(affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisks[1]);

            auto updates = state.GetDiskStateUpdates();
            UNIT_ASSERT_VALUES_EQUAL(2, updates.size());

            Sort(updates, TByDiskId());

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_WARNING, updates[0]);
            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_WARNING, updates[1]);

            UNIT_ASSERT_VALUES_UNEQUAL(updates[0].SeqNo, updates[1].SeqNo);
            lastSeqNo = std::max(updates[0].SeqNo, updates[1].SeqNo);
        });

        UNIT_ASSERT_VALUES_EQUAL(1, lastSeqNo);

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        // #1 : warning -> unavailable
        // disk-1 : online -> unavailable
        // disk-3 : online -> unavailable
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(2, affectedDisks.size());
            Sort(affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisks[1]);

            UNIT_ASSERT_VALUES_EQUAL(4, state.GetDiskStateUpdates().size());

            TVector updates {
                state.GetDiskStateUpdates()[2],
                state.GetDiskStateUpdates()[3],
            };

            Sort(updates, TByDiskId());

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_TEMPORARILY_UNAVAILABLE, updates[0]);
            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_TEMPORARILY_UNAVAILABLE, updates[1]);

            UNIT_ASSERT_GT(updates[0].SeqNo, lastSeqNo);
            UNIT_ASSERT_VALUES_UNEQUAL(updates[0].SeqNo, updates[1].SeqNo);
            lastSeqNo = std::max(updates[0].SeqNo, updates[1].SeqNo);
        });

        UNIT_ASSERT_VALUES_EQUAL(3, lastSeqNo);

        // #3 : online -> unavailable
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent3.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        // #2 : online -> online
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent2.GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        // #2 : online -> unavailable
        // disk-2 : online -> unavailable
        // disk-3 : already in unavailable
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent2.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL(5, state.GetDiskStateUpdates().size());

            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-2", DISK_STATE_TEMPORARILY_UNAVAILABLE, update);
            lastSeqNo = update.SeqNo;
        });

        UNIT_ASSERT_VALUES_EQUAL(4, lastSeqNo);

        // #1 : unavailable -> online
        // disk-1 : unavailable -> online
        // disk-3 : #2 still in unavailable
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL(6, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ONLINE, update);
            lastSeqNo = update.SeqNo;
        });

        UNIT_ASSERT_VALUES_EQUAL(5, lastSeqNo);

        // #2 : unavailable -> online
        // disk-2 : unavailable -> online
        // disk-3 : unavailable -> online
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent2.GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(2, affectedDisks.size());
            Sort(affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL("disk-2", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisks[1]);

            UNIT_ASSERT_VALUES_EQUAL(8, state.GetDiskStateUpdates().size());
            TVector updates {
                state.GetDiskStateUpdates()[6],
                state.GetDiskStateUpdates()[7]
            };

            Sort(updates, TByDiskId());

            UNIT_ASSERT_DISK_STATE("disk-2", DISK_STATE_ONLINE, updates[0]);
            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_ONLINE, updates[1]);

            UNIT_ASSERT_GT(updates[0].SeqNo, lastSeqNo);
            UNIT_ASSERT_VALUES_UNEQUAL(updates[0].SeqNo, updates[1].SeqNo);
            lastSeqNo = std::max(updates[0].SeqNo, updates[1].SeqNo);
        });

        UNIT_ASSERT_VALUES_EQUAL(7, lastSeqNo);

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TAgentConfig> agents;
            TVector<NProto::TDiskConfig> disks;
            ui64 seqNo = 0;

            db.ReadLastDiskStateSeqNo(seqNo);
            UNIT_ASSERT_VALUES_EQUAL(lastSeqNo + 1, seqNo);

            db.ReadAgents(agents);
            UNIT_ASSERT_VALUES_EQUAL(3, agents.size());
            Sort(agents, TByNodeId());

            UNIT_ASSERT_VALUES_EQUAL(1, agents[0].GetNodeId());
            UNIT_ASSERT_EQUAL(NProto::AGENT_STATE_ONLINE, agents[0].GetState());

            UNIT_ASSERT_VALUES_EQUAL(2, agents[1].GetNodeId());
            UNIT_ASSERT_EQUAL(NProto::AGENT_STATE_ONLINE, agents[1].GetState());

            UNIT_ASSERT_VALUES_EQUAL(3, agents[2].GetNodeId());
            UNIT_ASSERT_EQUAL(NProto::AGENT_STATE_UNAVAILABLE, agents[2].GetState());

            db.ReadDisks(disks);
            UNIT_ASSERT_VALUES_EQUAL(3, disks.size());
            Sort(disks, TByDiskId());

            UNIT_ASSERT_VALUES_EQUAL("disk-1", disks[0].GetDiskId());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, disks[0].GetState());

            UNIT_ASSERT_VALUES_EQUAL("disk-2", disks[1].GetDiskId());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, disks[1].GetState());

            UNIT_ASSERT_VALUES_EQUAL("disk-3", disks[2].GetDiskId());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, disks[2].GetState());
        });
    }

    Y_UNIT_TEST(ShouldUpdateDeviceState)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-3", "rack-2"),
            Device("dev-2", "uuid-4", "rack-2"),
            Device("dev-3", "uuid-5", "rack-2")
        });

        const auto agent3 = AgentConfig(3, {
            Device("dev-1", "uuid-6", "rack-3"),
            Device("dev-2", "uuid-7", "rack-3"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2, agent3 })
            .WithDisks({
                Disk("disk-1", {"uuid-1"}),
                Disk("disk-2", {"uuid-3"}),
                Disk("disk-3", {"uuid-2", "uuid-4"})
            })
            .Build();

        // uuid-1 : online -> warning
        // disk-1 : online -> migration
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-1",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_WARNING, update);
            UNIT_ASSERT_VALUES_EQUAL(0, update.SeqNo);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-1",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            UNIT_ASSERT(affectedDisk.empty());
        });

        // uuid-5 : online -> warning

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-5",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(affectedDisk.empty());
        });

        // uuid-7 : online -> warning

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-7",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(affectedDisk.empty());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-7",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            UNIT_ASSERT(affectedDisk.empty());
        });

        // uuid-2 : online -> warning
        // disk-3 : online -> migration

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-2",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_WARNING, update);
            UNIT_ASSERT_VALUES_EQUAL(1, update.SeqNo);
        });

        // uuid-4 : online -> warning
        // disk-3 : already in migration

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-4",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(affectedDisk.empty());
        });

        // uuid-4 : warning -> error
        // disk-3 : migration -> error

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-4",
                NProto::DEVICE_STATE_ERROR,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(3, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_ERROR, update);
            UNIT_ASSERT_VALUES_EQUAL(2, update.SeqNo);
        });

        // uuid-4 : error -> online
        // disk-3 : error -> migration

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-4",
                NProto::DEVICE_STATE_ONLINE,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(4, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_WARNING, update);
            UNIT_ASSERT_VALUES_EQUAL(3, update.SeqNo);
        });

        // uuid-2 : warning -> online
        // disk-3 : migration -> online

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-2",
                NProto::DEVICE_STATE_ONLINE,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(5, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_ONLINE, update);
            UNIT_ASSERT_VALUES_EQUAL(4, update.SeqNo);
        });
    }

    Y_UNIT_TEST(ShouldNotAllocateDiskWithBrokenDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-3", "rack-2"),
            Device("dev-2", "uuid-4", "rack-2"),
            Device("dev-3", "uuid-5", "rack-2")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2 })
            .Build();

        // #2 : online -> warning
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent2.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        // uuid-1 : online -> warning
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-1",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(affectedDisk.empty());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;

            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 30_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        // #2 : warning -> online
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent2.GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;

            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 30_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;

            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 20_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        });

        // uuid-1 : warning -> online
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-1",
                NProto::DEVICE_STATE_ONLINE,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(affectedDisk.empty());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;

            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 20_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
        });
    }

    Y_UNIT_TEST(ShouldUpdateDeviceAndAgentState)
    {
        const auto genAgentConfig = [] (ui32 nodeId, int deviceCount) {
            NProto::TAgentConfig agent;
            agent.SetNodeId(nodeId);
            agent.SetAgentId("agent-" + ToString(nodeId));

            auto& devices = *agent.MutableDevices();
            devices.Reserve(deviceCount);

            for (int i = 0; i != deviceCount; ++i) {
                *devices.Add() = Device(
                    Sprintf("dev-%d", i + 1),
                    Sprintf("uuid-%d.%d", nodeId, i + 1),
                    Sprintf("rack-%d", nodeId));
            }

            return agent;
        };

        const auto agent1 = genAgentConfig(1, 3);
        const auto agent2 = genAgentConfig(2, 3);

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2 })
            .WithDisks({
                Disk("disk-1", {"uuid-1.1"}),
                Disk("disk-2", {"uuid-2.1"}),
                Disk("disk-3", {"uuid-1.2", "uuid-2.2"}),
                Disk("disk-4", {"uuid-1.3", "uuid-2.3"})
            })
            .Build();

        // #2 : online -> warning
        // disk-2 : online -> migration
        // disk-3 : online -> migration
        // disk-4 : online -> migration
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent2.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(3, affectedDisks.size());
            Sort(affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL("disk-2", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisks[1]);
            UNIT_ASSERT_VALUES_EQUAL("disk-4", affectedDisks[2]);

            auto updates = state.GetDiskStateUpdates();
            UNIT_ASSERT_VALUES_EQUAL(3, updates.size());
            Sort(updates, TByDiskId());

            UNIT_ASSERT_DISK_STATE("disk-2", DISK_STATE_WARNING, updates[0]);
            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_WARNING, updates[1]);
            UNIT_ASSERT_DISK_STATE("disk-4", DISK_STATE_WARNING, updates[2]);
        });

        // uuid-2.2 : online -> error
        // disk-3 : migration -> error
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-2.2",
                NProto::DEVICE_STATE_ERROR,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(4, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_ERROR, update);
        });

        // uuid-1.3 : online -> error
        // disk-4 : migration -> error
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-1.3",
                NProto::DEVICE_STATE_ERROR,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("disk-4", affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(5, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-4", DISK_STATE_ERROR, update);
        });

        // #2 : warning -> online
        // disk-2 : migration -> online
        // disk-3 : still in error state
        // disk-4 : still in error state
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent2.GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());

            UNIT_ASSERT_VALUES_EQUAL("disk-2", affectedDisks[0]);

            UNIT_ASSERT_VALUES_EQUAL(6, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-2", DISK_STATE_ONLINE, update);
        });
    }

    Y_UNIT_TEST(ShouldUpdateDiskAtAgentRegistration)
    {
        const auto agent1a = AgentConfig(1, "host-1.cloud.yandex.net", {
            Device("dev-1", "uuid-1.1", "rack-1"),
            Device("dev-2", "uuid-1.2", "rack-1"),
            Device("dev-3", "uuid-1.3", "rack-1"),
        });

        const auto agent1b = AgentConfig(1, "host-1.cloud.yandex.net", {
            Device("dev-2", "uuid-1.2", "rack-1"),
            Device("dev-3", "uuid-1.3", "rack-1"),
        });

        const auto agent2a = AgentConfig(2, "host-2.cloud.yandex.net", {
            Device("dev-1", "uuid-2.1", "rack-2"),
            Device("dev-2", "uuid-2.2", "rack-2"),
            Device("dev-3", "uuid-2.3", "rack-2"),
        });

        const auto agent2b = AgentConfig(2, "host-2.cloud.yandex.net", {
            Device("dev-1", "uuid-2.1", "rack-2"),
        });

        const auto agent2c = AgentConfig(42, "host-2.cloud.yandex.net", {
            Device("dev-1", "uuid-2.1", "rack-2"),
            Device("dev-2", "uuid-2.2", "rack-2"),
            Device("dev-3", "uuid-2.3", "rack-2"),
        });

        const auto agent3a = AgentConfig(3, "host-3.cloud.yandex.net", {
            Device("dev-1", "uuid-3.1", "rack-3"),
            Device("dev-2", "uuid-3.2", "rack-3"),
            Device("dev-3", "uuid-3.3", "rack-3"),
        });

        const auto agent4a = AgentConfig(4, "host-4.cloud.yandex.net", {
            Device("dev-1", "uuid-4.1", "rack-4"),
            Device("dev-2", "uuid-4.2", "rack-4"),
            Device("dev-3", "uuid-4.3", "rack-4"),
            Device("dev-4", "uuid-4.4", "rack-4"),
            Device("dev-5", "uuid-4.5", "rack-4"),
            Device("dev-6", "uuid-4.6", "rack-4"),
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1a, agent2a, agent3a, agent4a })
            .WithDisks({
                Disk("disk-1", {"uuid-1.1"}),
                Disk("disk-2", {"uuid-2.1"}),
                Disk("disk-3", {"uuid-1.2", "uuid-2.2"}),
                Disk("disk-4", {"uuid-1.3", "uuid-2.3"})
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            // drop uuid-1.1
            auto [r, error] = state.RegisterAgent(db, agent1b, Now());
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(1, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", r.AffectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());

            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ERROR, update);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            // drop uuid-2.2 & uuid-2.3
            auto [r, error] = state.RegisterAgent(db, agent2b, Now());
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(2, r.AffectedDisks.size());
            Sort(r.AffectedDisks);

            UNIT_ASSERT_VALUES_EQUAL("disk-3", r.AffectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-4", r.AffectedDisks[1]);

            UNIT_ASSERT_VALUES_EQUAL(3, state.GetDiskStateUpdates().size());

            TVector updates {
                state.GetDiskStateUpdates()[1],
                state.GetDiskStateUpdates()[2]
            };
            Sort(updates, TByDiskId());

            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_ERROR, updates[0]);
            UNIT_ASSERT_DISK_STATE("disk-4", DISK_STATE_ERROR, updates[1]);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            // restore uuid-2.2 & uuid-2.3 but with error state
            auto [r, error] = state.RegisterAgent(db, agent2c, Now());
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
        });

        // restore uuid-2.2 to online
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
                TString affectedDisk;

                UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                    db,
                    "uuid-2.2",
                    NProto::DEVICE_STATE_ONLINE,
                    Now(),
                    "test",
                    affectedDisk));

                UNIT_ASSERT(affectedDisk);
                UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisk);
                UNIT_ASSERT_VALUES_EQUAL(4, state.GetDiskStateUpdates().size());
                const auto& update = state.GetDiskStateUpdates().back();

                UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_ONLINE, update);
                UNIT_ASSERT_VALUES_EQUAL("", update.State.GetStateMessage());
        });

        // restore uuid-2.3 to online
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;

            UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                db,
                "uuid-2.3",
                NProto::DEVICE_STATE_ONLINE,
                Now(),
                "test",
                affectedDisk));

            UNIT_ASSERT(affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL("disk-4", affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL(5, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-4", DISK_STATE_ONLINE, update);
            UNIT_ASSERT_VALUES_EQUAL("", update.State.GetStateMessage());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [r, error] = state.RegisterAgent(db, agent3a, Now());
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [r, error] = state.RegisterAgent(db, agent4a, Now());
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
        });

        // agent2 -> unavailable

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                db,
                agent2c.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                Now(),
                "state message",
                affectedDisks));

            Sort(affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(3, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisks[1]);
            UNIT_ASSERT_VALUES_EQUAL("disk-4", affectedDisks[2]);

            UNIT_ASSERT_VALUES_EQUAL(8, state.GetDiskStateUpdates().size());

            TVector updates {
                state.GetDiskStateUpdates()[5],
                state.GetDiskStateUpdates()[6],
                state.GetDiskStateUpdates()[7]
            };
            Sort(updates, TByDiskId());

            UNIT_ASSERT_DISK_STATE("disk-2", DISK_STATE_TEMPORARILY_UNAVAILABLE, updates[0]);
            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_TEMPORARILY_UNAVAILABLE, updates[1]);
            UNIT_ASSERT_DISK_STATE("disk-4", DISK_STATE_TEMPORARILY_UNAVAILABLE, updates[2]);
        });

        // agent2 -> ...

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [r, error] = state.RegisterAgent(db, agent2a, Now());
            UNIT_ASSERT_SUCCESS(error);

            Sort(r.AffectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(3, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", r.AffectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", r.AffectedDisks[1]);
            UNIT_ASSERT_VALUES_EQUAL("disk-4", r.AffectedDisks[2]);

            UNIT_ASSERT_VALUES_EQUAL(11, state.GetDiskStateUpdates().size());

            TVector updates {
                state.GetDiskStateUpdates()[8],
                state.GetDiskStateUpdates()[9],
                state.GetDiskStateUpdates()[10]
            };
            Sort(updates, TByDiskId());

            UNIT_ASSERT_DISK_STATE("disk-2", DISK_STATE_WARNING, updates[0]);
            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_WARNING, updates[1]);
            UNIT_ASSERT_DISK_STATE("disk-4", DISK_STATE_WARNING, updates[2]);
        });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(3, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", migrations[0].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", migrations[1].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", migrations[1].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-4", migrations[2].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.3", migrations[2].SourceDeviceId);
        }

        // agent2 -> online

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent2a.GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(3, affectedDisks.size());

            Sort(affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL("disk-2", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisks[1]);
            UNIT_ASSERT_VALUES_EQUAL("disk-4", affectedDisks[2]);

            UNIT_ASSERT_VALUES_EQUAL(14, state.GetDiskStateUpdates().size());

            TVector updates {
                state.GetDiskStateUpdates()[11],
                state.GetDiskStateUpdates()[12],
                state.GetDiskStateUpdates()[13]
            };
            Sort(updates, TByDiskId());

            UNIT_ASSERT_DISK_STATE("disk-2", DISK_STATE_ONLINE, updates[0]);
            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_ONLINE, updates[1]);
            UNIT_ASSERT_DISK_STATE("disk-4", DISK_STATE_ONLINE, updates[2]);
        });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TAgentConfig> agents;
            db.ReadAgents(agents);

            UNIT_ASSERT_VALUES_EQUAL(4, agents.size());
        });
    }

    Y_UNIT_TEST(ShouldUpdateDeviceConfigAtAgentRegistration)
    {
        const auto agent1a = AgentConfig(1, "host-1.cloud.yandex.net", {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB, "#1"),
            Device("dev-3", "uuid-1.3", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-4", "uuid-1.4", "rack-1", DefaultBlockSize, 10_GB)
        });

        const auto agent1b = AgentConfig(1, "host-1.cloud.yandex.net", {
            // dev-1 - lost
            // dev-2 has new transport id
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB, "#2"),
            // dev-3 has new size (bigger)
            Device("dev-3", "uuid-1.3", "rack-1", DefaultBlockSize, 20_GB),
            // dev-4 has new size (smaller)
            Device("dev-4", "uuid-1.4", "rack-1", DefaultBlockSize, 9_GB)
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1a })
            .Build();

        // check initial configuration
        {
            const auto dev1 = state.GetDevice("uuid-1.1");
            UNIT_ASSERT_VALUES_EQUAL(10_GB / DefaultBlockSize, dev1.GetBlocksCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev1.GetState());

            const auto dev2 = state.GetDevice("uuid-1.2");
            UNIT_ASSERT_VALUES_EQUAL(10_GB / DefaultBlockSize, dev2.GetBlocksCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev2.GetState());
            UNIT_ASSERT_EQUAL("#1", dev2.GetTransportId());

            const auto dev3 = state.GetDevice("uuid-1.3");
            UNIT_ASSERT_VALUES_EQUAL(10_GB / DefaultBlockSize, dev3.GetBlocksCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev3.GetState());

            const auto dev4 = state.GetDevice("uuid-1.4");
            UNIT_ASSERT_VALUES_EQUAL(10_GB / DefaultBlockSize, dev4.GetBlocksCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev4.GetState());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [r, error] = state.RegisterAgent(db, agent1b, Now());
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
        });

        {
            const auto dev1 = state.GetDevice("uuid-1.1");
            UNIT_ASSERT_VALUES_EQUAL(10_GB / DefaultBlockSize, dev1.GetBlocksCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ERROR, dev1.GetState());
            UNIT_ASSERT_VALUES_EQUAL("lost", dev1.GetStateMessage());

            const auto dev2 = state.GetDevice("uuid-1.2");
            UNIT_ASSERT_VALUES_EQUAL(10_GB / DefaultBlockSize, dev2.GetBlocksCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev2.GetState());
            UNIT_ASSERT_EQUAL("#2", dev2.GetTransportId());

            const auto dev3 = state.GetDevice("uuid-1.3");
            UNIT_ASSERT_VALUES_EQUAL(10_GB / DefaultBlockSize, dev3.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(20_GB / DefaultBlockSize, dev3.GetUnadjustedBlockCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev3.GetState());

            const auto dev4 = state.GetDevice("uuid-1.4");
            UNIT_ASSERT_VALUES_EQUAL(10_GB / DefaultBlockSize, dev4.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(9_GB / DefaultBlockSize, dev4.GetUnadjustedBlockCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ERROR, dev4.GetState());
        }
    }

    Y_UNIT_TEST(ShouldRestoreDevicePool)
    {
        const auto agent = AgentConfig(1, "host.cloud.yandex.net", {
            Device("dev-1", "uuid-1.1", "rack-1"),
            Device("dev-2", "uuid-1.2", "rack-1"),
            Device("dev-3", "uuid-1.3", "rack-1")
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                db,
                agent.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                Now(),
                "state message",
                affectedDisks));
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 30_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 30_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent.GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 30_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
        });
    }

    Y_UNIT_TEST(ShouldFindDisksToNotify)
    {
        const auto agent1 = AgentConfig(1, "host-1.cloud.yandex.net", {
            Device("dev-1", "uuid-1", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-2", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-3", "uuid-3", "rack-1", DefaultBlockSize, 10_GB)
        });

        const auto agent2 = AgentConfig(2, "host-2.cloud.yandex.net", {
            Device("dev-1", "uuid-4", "rack-2", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-5", "rack-2", DefaultBlockSize, 10_GB),
            Device("dev-3", "uuid-6", "rack-2", DefaultBlockSize, 10_GB)
        });

        const auto agent3 = AgentConfig(3, "host-3.cloud.yandex.net", {
            Device("dev-1", "uuid-7", "rack-3", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-8", "rack-3", DefaultBlockSize, 10_GB),
            Device("dev-3", "uuid-9", "rack-3", DefaultBlockSize, 10_GB)
        });

        const auto agent1a = AgentConfig(4, "host-1.cloud.yandex.net", {
            Device("dev-1", "uuid-1", "rack-2", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-2", "rack-2", DefaultBlockSize, 10_GB),
            Device("dev-3", "uuid-3", "rack-2", DefaultBlockSize, 10_GB)
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2, agent3 })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, devices[0].GetNodeId());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, devices[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, devices[1].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("uuid-6", devices[2].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, devices[2].GetNodeId());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-3", {}, {}, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(3, devices[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(3, devices[1].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("uuid-9", devices[2].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(3, devices[2].GetNodeId());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-4", {}, {}, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, devices[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, devices[1].GetNodeId());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [r, error] = state.RegisterAgent(db, agent1a, Now());
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(2, r.DisksToReallocate.size());
            Sort(r.DisksToReallocate);
            UNIT_ASSERT_VALUES_EQUAL("disk-1", r.DisksToReallocate[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-4", r.DisksToReallocate[1]);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-4", {}, {}, 20_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(4, devices[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(4, devices[1].GetNodeId());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(4, devices[0].GetNodeId());
        });
    }

    Y_UNIT_TEST(ShouldReplaceDevice)
    {
        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1"),
            Device("dev-2", "uuid-1.2", "rack-1")
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-2.1", "rack-2")
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2 })
            .WithDisks({ Disk("disk-1", { "uuid-1.1", "uuid-2.1" }) })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-2.1",
                NProto::DEVICE_STATE_ERROR,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ERROR, update);
            UNIT_ASSERT_VALUES_EQUAL(0, update.SeqNo);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;
            const auto error = state.ReplaceDevice(
                db,
                "disk-1",
                "uuid-2.1",
                "",     // no replacement device
                Now(),
                "",     // message
                true,   // manual
                &updated);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(updated);

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_VALUES_EQUAL("disk-1", update.State.GetDiskId());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, update.State.GetState());
            UNIT_ASSERT_VALUES_EQUAL(1, update.SeqNo);
        });

        {
            TVector<TDeviceConfig> devices;
            const auto error = state.GetDiskDevices("disk-1", devices);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            Sort(devices, TByDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", devices[1].GetDeviceUUID());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", info));
            // device replacement list should be empty for non-mirrored disks
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, info.DeviceReplacementIds);
        }

        {
            const auto device = state.GetDevice("uuid-2.1");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ERROR, device.GetState());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 10_GB, devices);

            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, devices.size());
        });
    }

    Y_UNIT_TEST(ShouldReplaceDeviceInPlacementGroup)
    {
        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1")
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-2.1", "rack-2"),
            Device("dev-2", "uuid-2.2", "rack-2")
        });

        const auto agent3 = AgentConfig(3, {
            Device("dev-1", "uuid-3.1", "rack-3")
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2 })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}));
        });

        TString device;

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TDeviceConfig> devices;

            UNIT_ASSERT_SUCCESS(AllocateDisk(
                db, state, "disk-1", "group-1", {}, 10_GB, devices));
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("rack-2", devices[0].GetRack());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TDeviceConfig> devices;

            UNIT_ASSERT_SUCCESS(AllocateDisk(
                db, state, "disk-2", "group-1", {}, 10_GB, devices));
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("rack-1", devices[0].GetRack());

            device = devices[0].GetDeviceUUID();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;
            const auto error = state.ReplaceDevice(
                db,
                "disk-2",
                device,
                "",     // no replacement device
                Now(),
                "",     // message
                true,   // manual
                &updated);

            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            UNIT_ASSERT(!updated);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskStateUpdates().size());
        });

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { agent1, agent2, agent3 });
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent3));
            CleanDevices(state, db);

            bool updated = false;
            const auto error = state.ReplaceDevice(
                db,
                "disk-2",
                device,
                "",     // no replacement device
                Now(),
                "",     // message
                true,   // manual
                &updated);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(!updated);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskStateUpdates().size());
        });

        {
            auto* group = state.FindPlacementGroup("group-1");
            UNIT_ASSERT(group);

            for (auto& info: group->GetDisks()) {
                if (info.GetDiskId() == "disk-2") {
                    UNIT_ASSERT_VALUES_EQUAL(1, info.DeviceRacksSize());
                    UNIT_ASSERT_VALUES_EQUAL("rack-3", info.GetDeviceRacks(0));
                }
            }
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TPlacementGroupConfig> groups;

            db.ReadPlacementGroups(groups);
            UNIT_ASSERT_VALUES_EQUAL(1, groups.size());

            for (auto& group: groups) {
                UNIT_ASSERT_VALUES_EQUAL(2, group.DisksSize());
                for (auto& info: group.GetDisks()) {
                    if (info.GetDiskId() == "disk-1") {
                        UNIT_ASSERT_VALUES_EQUAL(1, info.DeviceRacksSize());
                        UNIT_ASSERT_VALUES_EQUAL("rack-2", info.GetDeviceRacks(0));
                    }
                    if (info.GetDiskId() == "disk-2") {
                        UNIT_ASSERT_VALUES_EQUAL(1, info.DeviceRacksSize());
                        UNIT_ASSERT_VALUES_EQUAL("rack-3", info.GetDeviceRacks(0));
                    }
                }
            }
        });
    }

    Y_UNIT_TEST(ShouldNotReplaceWithSeveralDevices)
    {
        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 40_GB),
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 40_GB),
            Device("dev-3", "uuid-1.3", "rack-1", DefaultBlockSize, 40_GB)
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-2.1", "rack-2", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-2.2", "rack-2", DefaultBlockSize, 10_GB),
            Device("dev-3", "uuid-2.3", "rack-2", DefaultBlockSize, 10_GB),
            Device("dev-4", "uuid-2.4", "rack-2", DefaultBlockSize, 10_GB)
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2 })
            .WithDisks({
                Disk("disk-1", { "uuid-1.1", "uuid-1.2", "uuid-1.3" })
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-1.2",
                NProto::DEVICE_STATE_ERROR,
                Now(),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates()[0];

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ERROR, update);
            UNIT_ASSERT_VALUES_EQUAL(0, update.SeqNo);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;
            const auto error = state.ReplaceDevice(
                db,
                "disk-1",
                "uuid-1.2",
                "",     // no replacement device
                Now(),
                "",     // message
                true,   // manual
                &updated);

            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            UNIT_ASSERT(!updated);
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
        });
    }

    Y_UNIT_TEST(ShouldNotReplaceDeviceWithWrongBlockCount)
    {
        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-3", "uuid-1.3", "rack-1", DefaultBlockSize, 10_GB)
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-2.1", "rack-2", DefaultBlockSize, 9_GB),
            Device("dev-2", "uuid-2.2", "rack-2", DefaultBlockSize, 9_GB)
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2 })
            .WithDisks({
                Disk("disk-1", { "uuid-1.1", "uuid-1.2", "uuid-1.3" })
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;
            const auto error = state.ReplaceDevice(
                db,
                "disk-1",
                "uuid-1.2",
                "",     // no replacement device
                Now(),
                "",     // message
                true,   // manual
                &updated);

            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            UNIT_ASSERT(!updated);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskStateUpdates().size());
        });
    }

    Y_UNIT_TEST(ShouldUpdateDeviceStateAtRegistration)
    {
        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-3", "uuid-1.3", "rack-1", DefaultBlockSize, 10_GB)
        });

        // uuid-1.1 -> online
        // uuid-1.2 -> online
        // uuid-1.3 -> N/A
        const auto agent1a = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB)
        });

        // uuid-1.1 -> error
        // uuid-1.2 -> online
        // uuid-1.3 -> online
        const auto agent1b = AgentConfig(1, {
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-3", "uuid-1.3", "rack-1", DefaultBlockSize, 10_GB)
        });

        const auto ts1 = TInstant::MicroSeconds(10);
        const auto ts2 = TInstant::MicroSeconds(20);
        const auto warningTs = TInstant::MicroSeconds(30);
        const auto ts3 = TInstant::MicroSeconds(40);

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { agent1 });
        });

        // uuid-1.1 : online
        // uuid-1.2 : online
        // uuid-1.3 : N/A
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent1a, ts1));

            const auto& agents = state.GetAgents();
            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_EQUAL(NProto::AGENT_STATE_ONLINE, agents[0].GetState());
            UNIT_ASSERT_VALUES_EQUAL(ts1.MicroSeconds(), agents[0].GetStateTs());

            auto dev1 = state.GetDevice("uuid-1.1");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev1.GetState());
            UNIT_ASSERT_VALUES_EQUAL(ts1.MicroSeconds(), dev1.GetStateTs());

            auto dev2 = state.GetDevice("uuid-1.2");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev2.GetState());
            UNIT_ASSERT_VALUES_EQUAL(ts1.MicroSeconds(), dev2.GetStateTs());

            auto dev3 = state.GetDevice("uuid-1.3");
            // N/A
            UNIT_ASSERT_VALUES_EQUAL(0, dev3.GetStateTs());
        });

        // uuid-1.1 : error
        // uuid-1.2 : online
        // uuid-1.3 : online
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent1b, ts2));

            const auto& agents = state.GetAgents();
            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_EQUAL(NProto::AGENT_STATE_ONLINE, agents[0].GetState());
            UNIT_ASSERT_VALUES_EQUAL(ts1.MicroSeconds(), agents[0].GetStateTs());

            auto dev1 = state.GetDevice("uuid-1.1");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ERROR, dev1.GetState());
            UNIT_ASSERT_VALUES_EQUAL(ts2.MicroSeconds(), dev1.GetStateTs());

            auto dev2 = state.GetDevice("uuid-1.2");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev2.GetState());
            UNIT_ASSERT_VALUES_EQUAL(ts1.MicroSeconds(), dev2.GetStateTs());

            auto dev3 = state.GetDevice("uuid-1.3");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev3.GetState());
            UNIT_ASSERT_VALUES_EQUAL(ts2.MicroSeconds(), dev3.GetStateTs());
        });

        // uuid-1.2 -> warn
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-1.2",
                NProto::DEVICE_STATE_WARNING,
                warningTs,
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(affectedDisk.empty());
        });

        // uuid-1.1 : error
        // uuid-1.2 : warn
        // uuid-1.3 : online
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent1b, ts3));

            const auto& agents = state.GetAgents();
            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_EQUAL(NProto::AGENT_STATE_ONLINE, agents[0].GetState());
            UNIT_ASSERT_VALUES_EQUAL(ts1.MicroSeconds(), agents[0].GetStateTs());

            auto dev1 = state.GetDevice("uuid-1.1");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ERROR, dev1.GetState());
            UNIT_ASSERT_VALUES_EQUAL(ts2.MicroSeconds(), dev1.GetStateTs());

            auto dev2 = state.GetDevice("uuid-1.2");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_WARNING, dev2.GetState());
            UNIT_ASSERT_VALUES_EQUAL(warningTs.MicroSeconds(), dev2.GetStateTs());

            auto dev3 = state.GetDevice("uuid-1.3");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev3.GetState());
            UNIT_ASSERT_VALUES_EQUAL(ts2.MicroSeconds(), dev3.GetStateTs());
        });
    }

    Y_UNIT_TEST(ShouldNotAllocateDiskAtAgentWithWarningState)
    {
        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 10_GB)
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { agent1 });
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent1));
            CleanDevices(state, db);

            TVector<TDeviceConfig> devices;
            UNIT_ASSERT_SUCCESS(AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices));
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            const auto error = AllocateDisk(db, state, "disk-1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });
    }

    Y_UNIT_TEST(ShouldPreserveDiskState)
    {
        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB)
        });

        const auto registerTs = TInstant::MicroSeconds(5);
        const auto createTs = TInstant::MicroSeconds(10);
        const auto resizeTs = TInstant::MicroSeconds(20);
        const auto errorTs = TInstant::MicroSeconds(30);
        const auto updateTs = TInstant::MicroSeconds(40);

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, { agent1 });
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent1, registerTs));
            CleanDevices(state, db);
        });

        // create
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};

            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                createTs,
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "disk-1",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = 10_GB / DefaultLogicalBlockSize
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(1, result.Devices.size());
            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_OK, result.IOMode);
            UNIT_ASSERT_UNEQUAL(TInstant::Zero(), result.IOModeTs);
            UNIT_ASSERT_VALUES_EQUAL(0, result.Migrations.size());
        });

        {
            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, info.State);
            UNIT_ASSERT_VALUES_EQUAL(createTs, info.StateTs);
            UNIT_ASSERT_VALUES_EQUAL(1, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(DefaultLogicalBlockSize, info.LogicalBlockSize);
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TDiskConfig> disks;

            db.ReadDisks(disks);
            UNIT_ASSERT_VALUES_EQUAL(1, disks.size());
            const auto& disk = disks[0];
            UNIT_ASSERT_VALUES_EQUAL("disk-1", disk.GetDiskId());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, disk.GetState());
            UNIT_ASSERT_VALUES_EQUAL(createTs.MicroSeconds(), disk.GetStateTs());
        });

        // resize
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};

            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                resizeTs,
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "disk-1",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = 20_GB / DefaultLogicalBlockSize
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_OK, result.IOMode);
            UNIT_ASSERT_UNEQUAL(TInstant::Zero(), result.IOModeTs);
            UNIT_ASSERT_VALUES_EQUAL(0, result.Migrations.size());
        });

        {
            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, info.State);
            UNIT_ASSERT_VALUES_EQUAL(createTs, info.StateTs);
            UNIT_ASSERT_VALUES_EQUAL(2, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(DefaultLogicalBlockSize, info.LogicalBlockSize);
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TDiskConfig> disks;

            db.ReadDisks(disks);
            UNIT_ASSERT_VALUES_EQUAL(1, disks.size());
            const auto& disk = disks[0];
            UNIT_ASSERT_VALUES_EQUAL("disk-1", disk.GetDiskId());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, disk.GetState());
            UNIT_ASSERT_VALUES_EQUAL(createTs.MicroSeconds(), disk.GetStateTs());
        });

        // uuid-1.1 : online -> error
        // disk-1   : online -> error
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                db,
                "uuid-1.1",
                NProto::DEVICE_STATE_ERROR,
                errorTs,
                "test",
                affectedDisk));

            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ERROR, update);
        });

        {
            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ERROR, info.State);
            UNIT_ASSERT_VALUES_EQUAL(errorTs, info.StateTs);
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TDiskConfig> disks;

            db.ReadDisks(disks);
            UNIT_ASSERT_VALUES_EQUAL(1, disks.size());
            const auto& disk = disks[0];
            UNIT_ASSERT_VALUES_EQUAL("disk-1", disk.GetDiskId());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ERROR, disk.GetState());
            UNIT_ASSERT_VALUES_EQUAL(errorTs.MicroSeconds(), disk.GetStateTs());
        });

        // update
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};

            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                updateTs,
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "disk-1",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = 20_GB / DefaultLogicalBlockSize
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_ERROR_READ_ONLY, result.IOMode);
            UNIT_ASSERT_UNEQUAL(TInstant::Zero(), result.IOModeTs);
            UNIT_ASSERT_VALUES_EQUAL(0, result.Migrations.size());
        });

        {
            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", info));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ERROR, info.State);
            UNIT_ASSERT_VALUES_EQUAL(errorTs, info.StateTs);
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TDiskConfig> disks;

            db.ReadDisks(disks);
            UNIT_ASSERT_VALUES_EQUAL(1, disks.size());
            const auto& disk = disks[0];
            UNIT_ASSERT_VALUES_EQUAL("disk-1", disk.GetDiskId());
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ERROR, disk.GetState());
            UNIT_ASSERT_VALUES_EQUAL(errorTs.MicroSeconds(), disk.GetStateTs());
        });
    }

    Y_UNIT_TEST(ShouldNotifyDisks)
    {
        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 10_GB),
        });

        const auto agent2a = AgentConfig(2, {
            Device("dev-1", "uuid-2.1", "rack-2", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-2.2", "rack-2", DefaultBlockSize, 10_GB)
        });

        const auto agent2b = [&] {
            NProto::TAgentConfig config = agent2a;
            config.SetNodeId(42);

            return config;
        }();

        const TInstant errorTs = TInstant::MicroSeconds(10);
        const TInstant regTs = TInstant::MicroSeconds(20);

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2a })
            .WithDisks({
                Disk("disk-1", { "uuid-1.1", "uuid-2.1" })
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TString affectedDisk;
            UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                db,
                "uuid-1.1",
                NProto::DEVICE_STATE_ERROR,
                errorTs,
                "test",
                affectedDisk));

            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ERROR, update);
        });

        ui64 seqNo1 = 0;
        ui64 seqNo2 = 0;

        {
            const auto& disks = state.GetDisksToReallocate();
            UNIT_ASSERT_VALUES_EQUAL(1, disks.size());

            seqNo1 = disks.at("disk-1");
            UNIT_ASSERT_VALUES_UNEQUAL(0, seqNo1);
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [r, error] = state.RegisterAgent(db, agent2b, regTs);
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, r.DisksToReallocate.size());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDisksToReallocate().size());

            seqNo2 = state.GetDisksToReallocate().at("disk-1");

            UNIT_ASSERT_VALUES_UNEQUAL(0, seqNo2);
            UNIT_ASSERT_VALUES_UNEQUAL(seqNo1, seqNo2);
        });

        {
            const auto& disks = state.GetDisksToReallocate();
            UNIT_ASSERT_VALUES_EQUAL(1, disks.size());
            UNIT_ASSERT_VALUES_EQUAL(seqNo2, disks.at("disk-1"));
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeleteDiskToReallocate(db, "disk-1", seqNo1);
        });

        {
            const auto& disks = state.GetDisksToReallocate();
            UNIT_ASSERT_VALUES_EQUAL(1, disks.size());
            UNIT_ASSERT_VALUES_EQUAL(seqNo2, disks.at("disk-1"));
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeleteDiskToReallocate(db, "disk-1", seqNo2);
        });

        {
            const auto& disks = state.GetDisksToReallocate();
            UNIT_ASSERT_VALUES_EQUAL(0, disks.size());
        }
    }

    Y_UNIT_TEST(ShouldHandleCmsRequestsForHosts)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        ui64 lastSeqNo = 0;
        NProto::TStorageServiceConfig proto;
        proto.SetNonReplicatedInfraTimeout(TDuration::Days(1).MilliSeconds());
        proto.SetNonReplicatedInfraUnavailableAgentTimeout(
            TDuration::Hours(1).MilliSeconds());
        auto storageConfig = CreateStorageConfig(proto);

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(storageConfig)
            .WithKnownAgents({ agent1 })
            .WithDisks({
                Disk("disk-1", {"uuid-1"}),
                Disk("disk-3", {"uuid-2"}),
            })
            .With(lastSeqNo)
            .Build();

        auto ts = Now();
        TDuration cmsTimeout;

        // agent-1: online -> warning
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateCmsHostState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts,
                false, // dryRun
                affectedDisks,
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                storageConfig->GetNonReplicatedInfraTimeout(),
                cmsTimeout);

            Sort(affectedDisks);
            UNIT_ASSERT_VALUES_EQUAL(2, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisks[1]);

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDiskStateUpdates().size());
            auto updates = state.GetDiskStateUpdates();
            Sort(updates, TByDiskId());

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_WARNING, updates[0]);
            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_WARNING, updates[1]);

            UNIT_ASSERT_VALUES_UNEQUAL(updates[0].SeqNo, updates[1].SeqNo);
            lastSeqNo = std::max(updates[0].SeqNo, updates[1].SeqNo);
        });

        UNIT_ASSERT_VALUES_EQUAL(1, lastSeqNo);

        // agent-1: warning -> warning
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts + TDuration::Seconds(10),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());

            {
                auto res = state.GetAgentCmsTs(agent1.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(true, res.Defined());
                UNIT_ASSERT_VALUES_EQUAL(ts, *res);
            }

            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDiskStateUpdates().size());
        });

        // agent-1: warning -> unavailable
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                ts + TDuration::Seconds(20),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            {
                auto res = state.GetAgentCmsTs(agent1.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(true, res.Defined());
                UNIT_ASSERT_VALUES_EQUAL(ts, *res);
            }

            UNIT_ASSERT_VALUES_EQUAL(2, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(4, state.GetDiskStateUpdates().size());
        });

        ts += storageConfig->GetNonReplicatedInfraUnavailableAgentTimeout() / 2;

        // cms comes and requests host removal
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateCmsHostState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts + TDuration::Seconds(10),
                false, // dryRun
                affectedDisks,
                timeout);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                storageConfig->GetNonReplicatedInfraUnavailableAgentTimeout() / 2
                    - TDuration::Seconds(10),
                timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(4, state.GetDiskStateUpdates().size());
        });

        ts += storageConfig->GetNonReplicatedInfraUnavailableAgentTimeout() / 2
            + TDuration::Seconds(1);

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateCmsHostState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts + TDuration::Seconds(10),
                false, // dryRun
                affectedDisks,
                timeout);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration(), timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(4, state.GetDiskStateUpdates().size());
        });

        // mark agent as unavailable
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                ts + TDuration::Seconds(20),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            {
                auto res = state.GetAgentCmsTs(agent1.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(true, res.Defined());
                UNIT_ASSERT_VALUES_EQUAL(TInstant(), *res);
            }

            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(4, state.GetDiskStateUpdates().size());
        });

        // mark agent as online
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                ts + TDuration::Seconds(30),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            {
                auto res = state.GetAgentCmsTs(agent1.GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(true, res.Defined());
                UNIT_ASSERT_VALUES_EQUAL(TInstant(), *res);
            }

            Sort(affectedDisks);
            UNIT_ASSERT_VALUES_EQUAL(2, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisks[1]);

            UNIT_ASSERT_VALUES_EQUAL(6, state.GetDiskStateUpdates().size());
            TVector updates {
                state.GetDiskStateUpdates()[4],
                state.GetDiskStateUpdates()[5]
            };
            Sort(updates, TByDiskId());

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ONLINE, updates[0]);
            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_ONLINE, updates[1]);
        });

        ts += TDuration::Seconds(40);

        // cms comes and requests host removal
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateCmsHostState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts,
                false, // dryRun
                affectedDisks,
                timeout);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                storageConfig->GetNonReplicatedInfraTimeout(),
                timeout);

            Sort(affectedDisks);
            UNIT_ASSERT_VALUES_EQUAL(2, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisks[1]);

            UNIT_ASSERT_VALUES_EQUAL(8, state.GetDiskStateUpdates().size());
            TVector updates {
                state.GetDiskStateUpdates()[6],
                state.GetDiskStateUpdates()[7]
            };
            Sort(updates, TByDiskId());

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_WARNING, updates[0]);
            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_WARNING, updates[1]);
        });

        ts += storageConfig->GetNonReplicatedInfraTimeout() / 2;

        // cms comes and requests host removal
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateCmsHostState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts,
                false, // dryRun
                affectedDisks,
                timeout);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                storageConfig->GetNonReplicatedInfraTimeout() / 2,
                timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        ts += storageConfig->GetNonReplicatedInfraTimeout() / 2
            + TDuration::Seconds(1);

        // timer should be restarted
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateCmsHostState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts + TDuration::Seconds(10),
                false, // dryRun
                affectedDisks,
                timeout);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                storageConfig->GetNonReplicatedInfraTimeout(),
                timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });
    }

    Y_UNIT_TEST(ShouldHandleCmsRequestsForDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        ui64 lastSeqNo = 0;
        auto storageConfig = CreateStorageConfig();

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(storageConfig)
            .WithKnownAgents({ agent1 })
            .WithDisks({
                Disk("disk-1", {"uuid-1"}),
                Disk("disk-3", {"uuid-2"}),
            })
            .With(lastSeqNo)
            .Build();

        auto ts = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto result = state.UpdateCmsDeviceState(
                db,
                agent1.GetAgentId(),
                "dev-2",
                NProto::DEVICE_STATE_WARNING,
                ts,
                false,   // shouldResumeDevice
                false);  // dryRun

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, result.Error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                storageConfig->GetNonReplicatedInfraTimeout(),
                result.Timeout);
            ASSERT_VECTORS_EQUAL(TVector{"disk-3"}, result.AffectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_WARNING, update);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            auto error = state.UpdateDeviceState(
                db,
                "uuid-2",
                NProto::DEVICE_STATE_WARNING,
                ts,
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("", affectedDisk);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            auto error = state.UpdateDeviceState(
                db,
                "uuid-2",
                NProto::DEVICE_STATE_ERROR,
                ts,
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("disk-3", affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-3", DISK_STATE_ERROR, update);
        });

        ts = ts + storageConfig->GetNonReplicatedInfraTimeout();

        // cms comes and request host removal
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto result = state.UpdateCmsDeviceState(
                db,
                agent1.GetAgentId(),
                "dev-2",
                NProto::DEVICE_STATE_WARNING,
                ts + TDuration::Seconds(10),
                false,  // shouldResumeDevice
                false); // dryRun

            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.Error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration(),
                result.Timeout);
            ASSERT_VECTORS_EQUAL(TVector<TString>(), result.AffectedDisks);
        });

        // mark agent is unavailable
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            auto error = state.UpdateDeviceState(
                db,
                "uuid-2",
                NProto::DEVICE_STATE_ERROR,
                ts + TDuration::Seconds(20),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            UNIT_ASSERT(affectedDisk.empty());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            auto error = state.UpdateDeviceState(
                db,
                "uuid-2",
                NProto::DEVICE_STATE_ONLINE,
                ts + TDuration::Seconds(20),
                "test",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });
    }

    Y_UNIT_TEST(ShouldStoreMediaKindInDB)
    {
        const TVector agents {
            AgentConfig(1000, {
                Device("dev-1", "uuid-1.1", "rack-1", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(2000, {
                Device("dev-1", "uuid-2.1", "rack-1", 4_KB, 100_GB),
            }),
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .AddDevicePoolConfig("", 100_GB, NProto::DEVICE_POOL_KIND_DEFAULT)
            .AddDevicePoolConfig("rot", 100_GB, NProto::DEVICE_POOL_KIND_GLOBAL)
            .Build();

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto ts = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-1",
                {}, // placementGroupId
                {}, // placementPartitionIndex
                100_GB,
                devices,
                ts,
                NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
                "rot");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", devices[0].GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-2",
                {}, // placementGroupId
                {}, // placementPartitionIndex
                100_GB,
                devices,
                ts,
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", devices[0].GetDeviceUUID());
        });

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<NProto::TDiskConfig> items;
            db.ReadDisks(items);

            UNIT_ASSERT_VALUES_EQUAL(2, items.size());

            const auto& disk1 = items[0];

            UNIT_ASSERT_VALUES_EQUAL("disk-1", disk1.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(1, disk1.DeviceUUIDsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(NProto::STORAGE_MEDIA_HDD_NONREPLICATED),
                static_cast<int>(disk1.GetStorageMediaKind()));

            const auto& disk2 = items[1];

            UNIT_ASSERT_VALUES_EQUAL("disk-2", disk2.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(1, disk2.DeviceUUIDsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(NProto::STORAGE_MEDIA_SSD_NONREPLICATED),
                static_cast<int>(disk2.GetStorageMediaKind()));
        });
    }

    Y_UNIT_TEST(ShouldGuessHddMediaKindByDevicePoolNames)
    {
        const TVector agents {
            AgentConfig(1000, {
                Device("dev-1", "uuid-1.1", "rack-1", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .AddDevicePoolConfig("rot", 100_GB, NProto::DEVICE_POOL_KIND_GLOBAL)
            .WithDisks({
                Disk("disk-1", {"uuid-1.1"}),
            })
            .Build();

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskInfo info;
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", info));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::STORAGE_MEDIA_HDD_NONREPLICATED),
            static_cast<int>(info.MediaKind));
    }

    Y_UNIT_TEST(ShouldHandleCmsRequestsForHostsWithDependentHddDisks)
    {
        /*
         *  Configuring 6 agents in 3 different racks to be able to create
         *  placement groups with 3 partitions with a disk spanning 2 agents
         *  in each of the partitions.
         */

        const TVector agents {
            AgentConfig(1000, {
                Device("dev-1", "uuid-1.1", "rack-1", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(1001, {
                Device("dev-1", "uuid-1.2", "rack-1", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(2000, {
                Device("dev-1", "uuid-2.1", "rack-2", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(2001, {
                Device("dev-1", "uuid-2.2", "rack-2", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(3000, {
                Device("dev-1", "uuid-3.1", "rack-3", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(3001, {
                Device("dev-1", "uuid-3.2", "rack-3", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
        };

        NProto::TStorageServiceConfig proto;
        proto.SetNonReplicatedInfraTimeout(TDuration::Days(1).MilliSeconds());
        auto storageConfig = CreateStorageConfig(proto);

        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");
        auto disksInWarningState = diskRegistryGroup->GetCounter("DisksInWarningState");
        auto maxWarningTime = diskRegistryGroup->GetCounter("MaxWarningTime");

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(diskRegistryGroup)
            .With(storageConfig)
            .WithKnownAgents(agents)
            .AddDevicePoolConfig("rot", 100_GB, NProto::DEVICE_POOL_KIND_GLOBAL)
            .Build();

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        /*
         *  Creating our placement group and creating a disk in each partition.
         */

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_PARTITION,
                3));
        });

        auto ts = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-1",
                "group-1",
                1,
                200_GB,
                devices,
                ts,
                NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
                "rot");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", devices[1].GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-2",
                "group-1",
                2,
                200_GB,
                devices,
                ts,
                NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
                "rot");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", devices[1].GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-3",
                "group-1",
                3,
                200_GB,
                devices,
                ts,
                NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
                "rot");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3.1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3.2", devices[1].GetDeviceUUID());
        });

        const TDuration h = TDuration::Hours(1);
        state.PublishCounters(ts + h);
        UNIT_ASSERT_VALUES_EQUAL(0, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxWarningTime->Val());

        /*
         *  Trying to start maintenance for one of the agents - maintenance
         *  should succeed since there will be no groups with more than 1
         *  broken partition after this.
         */

        TDuration cmsTimeout;

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateCmsHostState(
                db,
                agents[0].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts,
                false, // dryRun
                affectedDisks,
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);

            /*
             * migrations shouldn't start for STORAGE_MEDIA_HDD_NONREPLICATED
             * disks
             */

            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
        });

        state.PublishCounters(ts + h);
        UNIT_ASSERT_VALUES_EQUAL(1, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(h.Seconds(), maxWarningTime->Val());

        /*
         *  Trying to start maintenance for another agent in the same rack -
         *  maintenance should succeed since it doesn't break another partition.
         */

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateCmsHostState(
                db,
                agents[1].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts,
                false, // dryRun
                affectedDisks,
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());

            /*
             * migrations shouldn't start for STORAGE_MEDIA_HDD_NONREPLICATED
             * disks
             */

            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
        });

        /*
         *  Trying to start maintenance for another agent - maintenance should
         *  not be allowed since group-1 will lose at least 2 partitions after
         *  that.
         */

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateCmsHostState(
                db,
                agents[2].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts,
                false, // dryRun
                affectedDisks,
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                storageConfig->GetNonReplicatedInfraTimeout(),
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", affectedDisks[0]);

            /*
             * migrations shouldn't start for STORAGE_MEDIA_HDD_NONREPLICATED
             * disks
             */

            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
        });

        ts += h;

        state.PublishCounters(ts + h);
        UNIT_ASSERT_VALUES_EQUAL(2, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL((h + h).Seconds(), maxWarningTime->Val());

        /*
         * What if agent 0 goes from WARNING to UNAVAILABLE?
         */

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agents[0].GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                ts,
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);
        });

        state.PublishCounters(ts + h);
        UNIT_ASSERT_VALUES_EQUAL(1, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL((h + h).Seconds(), maxWarningTime->Val());

        /*
         *  Maintenance should still be unallowed for other agents.
         */

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateCmsHostState(
                db,
                agents[2].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts,
                false, // dryRun
                affectedDisks,
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                storageConfig->GetNonReplicatedInfraTimeout()
                    - TDuration::Hours(1),
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        state.PublishCounters(ts + h);
        UNIT_ASSERT_VALUES_EQUAL(1, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL((h + h).Seconds(), maxWarningTime->Val());

        /*
         *  Maintenance should still be unallowed for other agents even after
         *  the timeout.
         */

        ts += storageConfig->GetNonReplicatedInfraTimeout();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateCmsHostState(
                db,
                agents[2].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts,
                false, // dryRun
                affectedDisks,
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            // timeout should just wrap
            UNIT_ASSERT_VALUES_EQUAL(
                storageConfig->GetNonReplicatedInfraTimeout(),
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        state.PublishCounters(ts);
        UNIT_ASSERT_VALUES_EQUAL(1, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            (h + storageConfig->GetNonReplicatedInfraTimeout()).Seconds(),
            maxWarningTime->Val());

        /*
         *  Finally, after disk-1 gets deleted, maintenance should be allowed.
         */

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateCmsHostState(
                db,
                agents[2].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts,
                false, // dryRun
                affectedDisks,
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        state.PublishCounters(ts);
        UNIT_ASSERT_VALUES_EQUAL(1, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            (h + storageConfig->GetNonReplicatedInfraTimeout()).Seconds(),
            maxWarningTime->Val());

        /*
         * What if agent 2 goes from WARNING to UNAVAILABLE?
         */

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agents[2].GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                ts,
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", affectedDisks[0]);
        });

        state.PublishCounters(ts);
        UNIT_ASSERT_VALUES_EQUAL(0, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxWarningTime->Val());
    }

    Y_UNIT_TEST(ShouldNotBlockMaintenanceIfIrrelevantPlacementGroupIsBroken)
    {
        /*
         *  Configuring 6 agents in 6 different racks to be able to create
         *  placement groups with 3 partitions.
         */

        const TVector agents {
            AgentConfig(1000, {
                Device("dev-1", "uuid-1.1", "rack-1", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(2000, {
                Device("dev-1", "uuid-2.1", "rack-2", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(3000, {
                Device("dev-1", "uuid-3.1", "rack-3", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(4000, {
                Device("dev-1", "uuid-4.1", "rack-4", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(5000, {
                Device("dev-1", "uuid-5.1", "rack-5", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(6000, {
                Device("dev-1", "uuid-6.1", "rack-6", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
        };

        NProto::TStorageServiceConfig proto;
        proto.SetNonReplicatedInfraTimeout(TDuration::Days(1).MilliSeconds());
        auto storageConfig = CreateStorageConfig(proto);

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(storageConfig)
            .WithKnownAgents(agents)
            .AddDevicePoolConfig("rot", 100_GB, NProto::DEVICE_POOL_KIND_GLOBAL)
            .Build();

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        /*
         *  Creating our placement groups and creating a disk in each partition.
         */

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_PARTITION,
                3));

            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-2",
                NProto::PLACEMENT_STRATEGY_PARTITION,
                3));
        });

        auto ts = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-1",
                "group-1",
                1,
                100_GB,
                devices,
                ts,
                NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
                "rot");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", devices[0].GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-2",
                "group-1",
                2,
                100_GB,
                devices,
                ts,
                NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
                "rot");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", devices[0].GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-3",
                "group-1",
                3,
                100_GB,
                devices,
                ts,
                NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
                "rot");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3.1", devices[0].GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-4",
                "group-2",
                1,
                100_GB,
                devices,
                ts,
                NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
                "rot");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4.1", devices[0].GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-5",
                "group-2",
                2,
                100_GB,
                devices,
                ts,
                NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
                "rot");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5.1", devices[0].GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(
                db,
                state,
                "disk-6",
                "group-2",
                3,
                100_GB,
                devices,
                ts,
                NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
                "rot");
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-6.1", devices[0].GetDeviceUUID());
        });

        /*
         *  Breaking group-2.
         */

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agents[3].GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                ts,
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-4", affectedDisks[0]);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agents[4].GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                ts,
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-5", affectedDisks[0]);
        });

        /*
         *  Trying to start maintenance for one of the agents - maintenance
         *  should succeed since the groups that depend on this agent won't
         *  have 2 or more broken partitions.
         */

        TDuration cmsTimeout;

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateCmsHostState(
                db,
                agents[0].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts,
                false, // dryRun
                affectedDisks,
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);

            /*
             * migrations shouldn't start for STORAGE_MEDIA_HDD_NONREPLICATED
             * disks
             */

            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
        });

        /*
         *  Trying to start maintenance for another agent - maintenance should
         *  not be allowed since group-1 will lose at least 2 partitions after
         *  that.
         */

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateCmsHostState(
                db,
                agents[1].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                ts,
                false, // dryRun
                affectedDisks,
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                storageConfig->GetNonReplicatedInfraTimeout(),
                cmsTimeout);

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", affectedDisks[0]);

            /*
             * migrations shouldn't start for STORAGE_MEDIA_HDD_NONREPLICATED
             * disks
             */

            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
        });
    }

    Y_UNIT_TEST(ShouldReturnDependentDisks)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1"),
            Device("dev-4", "uuid-4", "rack-1"),
            Device("dev-5", "uuid-5", "rack-1"),
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-6", "rack-1"),
            Device("dev-2", "uuid-7", "rack-1"),
            Device("dev-3", "uuid-8", "rack-1"),
            Device("dev-4", "uuid-9", "rack-1"),
            Device("dev-5", "uuid-10", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2 })
            .WithDisks({
                Disk("disk-1", {"uuid-1", "uuid-2"}),
                Disk("disk-2", {"uuid-3"}),
                Disk("disk-3", {"uuid-5"}),
            })
            .Build();

        TVector<TString> diskIds;
        auto error =
            state.GetDependentDisks(agent1.GetAgentId(), {}, false, &diskIds);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "disk-1,disk-2,disk-3",
            JoinSeq(",", diskIds));

        error =
            state.GetDependentDisks(agent2.GetAgentId(), {}, false, &diskIds);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("", JoinSeq(",", diskIds));

        error = state.GetDependentDisks("no-such-agent", {}, false, &diskIds);

        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("", JoinSeq(",", diskIds));
    }

    Y_UNIT_TEST(ShouldReturnDependentDisksForMirroredDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1"),
            Device("dev-2", "uuid-1.2", "rack-1"),
            Device("dev-3", "uuid-1.3", "rack-1"),
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-2.1", "rack-1"),
            Device("dev-2", "uuid-2.2", "rack-1"),
            Device("dev-3", "uuid-2.3", "rack-1"),
        });

        const auto agent3 = AgentConfig(3, {
            Device("dev-1", "uuid-3.1", "rack-1"),
            Device("dev-2", "uuid-3.2", "rack-1"),
            Device("dev-3", "uuid-3.3", "rack-1"),
        });

        auto disks = MirrorDisk(
            "mirrored-disk-1",
            {
                {"uuid-1.1", "uuid-1.2"},
                {"uuid-2.1", "uuid-2.2"},
            });
        disks.push_back(Disk("disk-1", {"uuid-2.3", "uuid-3.3"}));

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2, agent3 })
            .WithDisks(disks)
            .Build();

        TVector<TString> agentsIds = {
            agent1.GetAgentId(),
            agent2.GetAgentId(),
            agent3.GetAgentId()};

        THashMap<TString, TString> expectedDisksAll = {
            {agent1.GetAgentId(), "mirrored-disk-1"},
            {agent2.GetAgentId(), "mirrored-disk-1,disk-1"},
            {agent3.GetAgentId(), "disk-1"}};

        THashMap<TString, TString> expectedDisksIgnoreReplicated = {
            {agent1.GetAgentId(), ""},
            {agent2.GetAgentId(), "disk-1"},
            {agent3.GetAgentId(), "disk-1"}};

        for (const bool ignoreReplicated: {false, true}) {
            for (const auto& agentId: agentsIds) {
                TVector<TString> diskIds;
                auto error = state.GetDependentDisks(
                    agentId,
                    {}, // path
                    ignoreReplicated,
                    &diskIds);
                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                UNIT_ASSERT_VALUES_EQUAL(
                    ignoreReplicated
                        ? expectedDisksIgnoreReplicated[agentId]
                        : expectedDisksAll[agentId],
                    JoinSeq(",", diskIds));
            }
        }
    }

    Y_UNIT_TEST(ShouldReturnDependentDisksForShadowDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const auto agent1 =
            AgentConfig(1, {Device("dev-1", "uuid-1", "rack-1")});

        const auto agent2 =
            AgentConfig(2, {Device("dev-2", "uuid-2", "rack-1")});

        TDiskRegistryState state =
            TDiskRegistryStateBuilder()
                .WithKnownAgents({agent1, agent2})
                .WithDisks({
                    Disk("disk-1", {"uuid-1"}),
                    ShadowDisk("disk-1", "cp-1", {"uuid-2"}),
                })
                .Build();

        TVector<TString> diskIds;
        auto error =
            state.GetDependentDisks(agent2.GetAgentId(), {}, false, &diskIds);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, diskIds.size());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", diskIds[0]);
    }

    Y_UNIT_TEST(ShouldNotRestoreOnlineStateAutomatically)
    {
        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 10_GB),
        });

        const TInstant errorTs = TInstant::MicroSeconds(10);
        const TInstant regTs = TInstant::MicroSeconds(20);
        const TInstant errorTs2 = TInstant::MicroSeconds(30);

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1 })
            .Build();

        {
            auto agents = state.GetAgents();
            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_EQUAL(NProto::AGENT_STATE_ONLINE, agents[0].GetState());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                errorTs,
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        {
            auto agents = state.GetAgents();
            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_EQUAL(
                NProto::AGENT_STATE_UNAVAILABLE,
                agents[0].GetState()
            );
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TAgentConfig> agents;
            db.ReadAgents(agents);
            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_EQUAL(
                NProto::AGENT_STATE_UNAVAILABLE,
                agents[0].GetState()
            );
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [r, error] = state.RegisterAgent(db, agent1, regTs);
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, r.DisksToReallocate.size());
        });

        {
            auto agents = state.GetAgents();
            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_EQUAL(
                NProto::AGENT_STATE_WARNING,
                agents[0].GetState()
            );
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TAgentConfig> agents;
            db.ReadAgents(agents);
            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_EQUAL(
                NProto::AGENT_STATE_WARNING,
                agents[0].GetState()
            );
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                errorTs2,
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage()
            );
        });

        {
            auto agents = state.GetAgents();
            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_EQUAL(
                NProto::AGENT_STATE_ONLINE,
                agents[0].GetState()
            );
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TAgentConfig> agents;
            db.ReadAgents(agents);
            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_EQUAL(
                NProto::AGENT_STATE_ONLINE,
                agents[0].GetState()
            );
        });
    }

    Y_UNIT_TEST(ShouldHandleDeviceSizeMismatch)
    {
        const auto agent1a = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-3", "uuid-1.3", "rack-1", DefaultBlockSize, 10_GB)
        });

        const auto agent1b = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1", 4_KB, 16_GB),
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-3", "uuid-1.3", "rack-1", 4_KB, 8_GB)
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1a })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;

            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 30_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [r, error] = state.RegisterAgent(db, agent1b, Now());
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(1, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", r.AffectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());

            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ERROR, update);

            UNIT_ASSERT_EQUAL(
                NProto::DEVICE_STATE_ERROR,
                state.GetDevice("uuid-1.1").GetState()
            );

            UNIT_ASSERT_EQUAL(
                NProto::DEVICE_STATE_ONLINE,
                state.GetDevice("uuid-1.2").GetState()
            );

            UNIT_ASSERT_EQUAL(
                NProto::DEVICE_STATE_ERROR,
                state.GetDevice("uuid-1.3").GetState()
            );
        });
    }

    Y_UNIT_TEST(ShouldVerifyConfig)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;

            NProto::TDiskRegistryConfig config;
            auto& agent = *config.AddKnownAgents();
            agent.SetAgentId("foo");
            agent.AddDevices()->SetDeviceUUID("uuid-1");
            agent.AddDevices()->SetDeviceUUID("uuid-2");
            agent.AddDevices()->SetDeviceUUID("uuid-2");

            auto error = state.UpdateConfig(
                db,
                config,
                false, // ignoreVersion
                affectedDisks
            );

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;

            NProto::TDiskRegistryConfig config;
            {
                auto& agent = *config.AddKnownAgents();
                agent.SetAgentId("foo");
                agent.AddDevices()->SetDeviceUUID("uuid-1");
                agent.AddDevices()->SetDeviceUUID("uuid-2");
            }

            {
                auto& agent = *config.AddKnownAgents();
                agent.SetAgentId("bar");
                agent.AddDevices()->SetDeviceUUID("uuid-3");
                agent.AddDevices()->SetDeviceUUID("uuid-2");
            }

            auto error = state.UpdateConfig(
                db,
                config,
                false, // ignoreVersion
                affectedDisks
            );

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;

            NProto::TDiskRegistryConfig config;
            {
                auto& agent = *config.AddKnownAgents();
                agent.SetAgentId("foo");
                agent.AddDevices()->SetDeviceUUID("uuid-1");
                agent.AddDevices()->SetDeviceUUID("uuid-2");
            }

            {
                auto& agent = *config.AddKnownAgents();
                agent.SetAgentId("foo");
                agent.AddDevices()->SetDeviceUUID("uuid-3");
                agent.AddDevices()->SetDeviceUUID("uuid-4");
            }

            auto error = state.UpdateConfig(
                db,
                config,
                false, // ignoreVersion
                affectedDisks
            );

            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        });
    }

    Y_UNIT_TEST(ShouldReturnOkForDeviceCmsRequestIfNoUserDisks)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        ui64 lastSeqNo = 0;
        auto storageConfig = CreateStorageConfig();

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(storageConfig)
            .WithKnownAgents({ agent1 })
            .With(lastSeqNo)
            .Build();

        auto ts = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto result = state.UpdateCmsDeviceState(
                db,
                agent1.GetAgentId(),
                "dev-2",
                NProto::DEVICE_STATE_WARNING,
                ts,
                false,  // shouldResumeDevice
                false); // dryRun

            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.Error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration(), result.Timeout);
            ASSERT_VECTORS_EQUAL(TVector<TString>(), result.AffectedDisks);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto result = state.UpdateCmsDeviceState(
                db,
                agent1.GetAgentId(),
                "dev-2",
                NProto::DEVICE_STATE_WARNING,
                ts + TDuration::Seconds(10),
                false,   // shouldResumeDevice
                true);   // dryRun

            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.Error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration(), result.Timeout);
            ASSERT_VECTORS_EQUAL(TVector<TString>(), result.AffectedDisks);
        });
    }

    Y_UNIT_TEST(ShouldReturnOkForDeviceCmsRequestIfNoUserDisksAfterDeallocation)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        ui64 lastSeqNo = 0;
        auto storageConfig = CreateStorageConfig();

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(storageConfig)
            .WithKnownAgents({ agent1 })
            .With(lastSeqNo)
            .Build();

        auto updateDevice = [&] (auto db, auto desiredState) {
            return state.UpdateCmsDeviceState(
                db,
                agent1.GetAgentId(),
                "dev-2",
                desiredState,
                Now(),
                false,    // shouldResumeDevice
                false);   // dryRun
        };

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TDiskRegistryState::TAllocateDiskResult result;

            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                Now(),
                db,
                {
                    .DiskId = "disk-1",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = 20_GB / DefaultLogicalBlockSize
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto r = updateDevice(db, NProto::DEVICE_STATE_WARNING);

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, r.Error.GetCode());
            UNIT_ASSERT_VALUES_UNEQUAL(TDuration{}, r.Timeout);
            ASSERT_VECTORS_EQUAL(TVector<TString>{"disk-1"}, r.AffectedDisks);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto r = updateDevice(db, NProto::DEVICE_STATE_WARNING);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, r.Error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration{}, r.Timeout);
            ASSERT_VECTORS_EQUAL(TVector<TString>(), r.AffectedDisks);
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto r = updateDevice(db, NProto::DEVICE_STATE_WARNING);

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, r.Error.GetCode(), r.Error);
            UNIT_ASSERT_VALUES_EQUAL(TDuration{}, r.Timeout);
            ASSERT_VECTORS_EQUAL(TVector<TString>(), r.AffectedDisks);
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto r = updateDevice(db, NProto::DEVICE_STATE_ONLINE);

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, r.Error.GetCode(), r.Error);
            UNIT_ASSERT_VALUES_EQUAL(TDuration{}, r.Timeout);
            ASSERT_VECTORS_EQUAL(TVector<TString>(), r.AffectedDisks);
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());
        });
    }

    Y_UNIT_TEST(ShouldReturnOkForAgentCmsRequestIfNoUserDisks)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        ui64 lastSeqNo = 0;
        auto storageConfig = CreateStorageConfig();

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(storageConfig)
            .WithKnownAgents({ agent1 })
            .With(lastSeqNo)
            .Build();

        auto ts = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateCmsHostState(
                db,
                "agent-1",
                NProto::AGENT_STATE_WARNING,
                ts,
                false,
                affectedDisks,
                timeout);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateCmsHostState(
                db,
                "agent-1",
                NProto::AGENT_STATE_WARNING,
                ts + TDuration::Seconds(10),
                true,
                affectedDisks,
                timeout);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });
    }

    Y_UNIT_TEST(ShouldRemoveOutdatedAgent)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agent1 = AgentConfig(1000, "agent-1", {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        auto agent2 = AgentConfig(1000, "agent-2", {
            Device("dev-1", "uuid-2", "rack-2"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, {agent1, agent2});
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent1));
        });

        {
            const auto agents = state.GetAgents();

            UNIT_ASSERT_VALUES_EQUAL(1, agents.size());
            UNIT_ASSERT_VALUES_EQUAL(1000, agents[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("agent-1", agents[0].GetAgentId());

            const auto device = state.GetDevice("uuid-1");
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1000, device.GetNodeId());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto error = RegisterAgent(state, db, agent2);
            UNIT_ASSERT_VALUES_EQUAL(
                E_INVALID_STATE,
                RegisterAgent(state, db, agent2).GetCode());

            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(
                state.UpdateAgentState(
                    db,
                    "agent-1",
                    NProto::AGENT_STATE_UNAVAILABLE,
                    Now(),
                    "state message",
                    affectedDisks));

            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent2));
        });

        {
            auto agents = state.GetAgents();
            Sort(agents, TByNodeId());

            UNIT_ASSERT_VALUES_EQUAL(2, agents.size());
            UNIT_ASSERT_VALUES_EQUAL(0, agents[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("agent-1", agents[0].GetAgentId());

            UNIT_ASSERT_VALUES_EQUAL(1000, agents[1].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("agent-2", agents[1].GetAgentId());

            auto d1 = state.GetDevice("uuid-1");
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", d1.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, d1.GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("agent-1", d1.GetAgentId());

            auto d2 = state.GetDevice("uuid-2");
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", d2.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1000, d2.GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("agent-2", d2.GetAgentId());
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<NProto::TAgentConfig> agents;

            db.ReadAgents(agents);
            Sort(agents, TByNodeId());

            UNIT_ASSERT_VALUES_EQUAL(2, agents.size());

            UNIT_ASSERT_VALUES_EQUAL(0, agents[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("agent-1", agents[0].GetAgentId());

            UNIT_ASSERT_VALUES_EQUAL(1000, agents[1].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("agent-2", agents[1].GetAgentId());
        });
    }

    Y_UNIT_TEST(ShouldBackupState)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                AgentConfig(1, {
                    Device("dev-1", "uuid-1.1", "rack-1"),
                    Device("dev-2", "uuid-1.2", "rack-1"),
                    Device("dev-3", "uuid-1.3", "rack-1"),
                    Device("dev-4", "uuid-1.4", "rack-1"),
                }),
                AgentConfig(2, {
                    Device("dev-1", "uuid-2.1", "rack-2"),
                    Device("dev-2", "uuid-2.2", "rack-2"),
                    Device("dev-3", "uuid-2.3", "rack-2"),
                    Device("dev-4", "uuid-2.4", "rack-2")
                }),
                AgentConfig(3, {
                    Device("dev-1", "uuid-3.1", "rack-3"),
                    Device("dev-2", "uuid-3.2", "rack-3"),
                    Device("dev-3", "uuid-3.3", "rack-3"),
                    Device("dev-4", "uuid-3.4", "rack-3")
                }),
                AgentConfig(4, {
                    Device("dev-1", "uuid-4.1", "rack-4"),
                    Device("dev-2", "uuid-4.2", "rack-4"),
                    Device("dev-3", "uuid-4.3", "rack-4"),
                    Device("dev-4", "uuid-4.4", "rack-4")
                })
            })
            .Build();

        TVector<TVector<TDeviceConfig>> diskDevices(5);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}));
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-2",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}));
            UNIT_ASSERT_SUCCESS(
                AllocateDisk(db, state, "disk-1", "group-1", {}, 20_GB, diskDevices[0]));
            UNIT_ASSERT_SUCCESS(
                AllocateDisk(db, state, "disk-2", "group-1", {}, 20_GB, diskDevices[1]));
            UNIT_ASSERT_SUCCESS(
                AllocateDisk(db, state, "disk-3", "group-2", {}, 20_GB, diskDevices[2]));
            UNIT_ASSERT_SUCCESS(
                AllocateDisk(db, state, "disk-4", "group-2", {}, 20_GB, diskDevices[3]));
            UNIT_ASSERT_SUCCESS(
                AllocateDisk(db, state, "disk-5", {}, {}, 40_GB, diskDevices[4]));
        });

        auto snapshot = state.BackupState();
        UNIT_ASSERT_VALUES_EQUAL(5, snapshot.DisksSize());
        UNIT_ASSERT_VALUES_EQUAL(2, snapshot.PlacementGroupsSize());
        UNIT_ASSERT_VALUES_EQUAL(4, snapshot.AgentsSize());

        Sort(*snapshot.MutableDisks(), TByDiskId());

        SortBy(*snapshot.MutablePlacementGroups(), [] (auto& x) {
            return x.GetGroupId();
        });

        auto equalDevices = [] (auto& disk, const auto& devices) {
            UNIT_ASSERT(std::equal(
                disk.GetDeviceUUIDs().begin(),
                disk.GetDeviceUUIDs().end(),
                devices.begin(),
                devices.end(),
                [] (auto& x, auto& y) {
                    return x == y.GetDeviceUUID();
                }));
        };

        for (size_t i = 0; i != snapshot.DisksSize(); ++i) {
            auto& disk = snapshot.GetDisks(i);
            UNIT_ASSERT_VALUES_EQUAL("disk-" + ToString(i + 1), disk.GetDiskId());
            equalDevices(disk, diskDevices[i]);
        }

        {
            auto& pg = *snapshot.MutablePlacementGroups(0);
            UNIT_ASSERT_VALUES_EQUAL("group-1", pg.GetGroupId());
            UNIT_ASSERT_VALUES_EQUAL(2, pg.DisksSize());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", pg.GetDisks(0).GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(1, pg.GetDisks(0).DeviceRacksSize());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", pg.GetDisks(1).GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(1, pg.GetDisks(1).DeviceRacksSize());
        }

        {
            auto& pg = *snapshot.MutablePlacementGroups(1);
            UNIT_ASSERT_VALUES_EQUAL("group-2", pg.GetGroupId());
            UNIT_ASSERT_VALUES_EQUAL(2, pg.DisksSize());
            UNIT_ASSERT_VALUES_EQUAL("disk-3", pg.GetDisks(0).GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(1, pg.GetDisks(0).DeviceRacksSize());
            UNIT_ASSERT_VALUES_EQUAL("disk-4", pg.GetDisks(1).GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(1, pg.GetDisks(1).DeviceRacksSize());
        }
    }

    Y_UNIT_TEST(ShouldMigrateDiskDevice)
    {
        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1")
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1", "rack-2")
            })
        };

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithDisks({Disk("disk-1", {"uuid-2.1", "uuid-1.1"})})
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};

            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "disk-1",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = 20_GB / DefaultLogicalBlockSize
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", result.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_OK, result.IOMode);
            UNIT_ASSERT_UNEQUAL(TInstant::Zero(), result.IOModeTs);
            UNIT_ASSERT_VALUES_EQUAL(0, result.Migrations.size());
        });

        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-2.1",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                "test",
                affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_WARNING, update);
            UNIT_ASSERT_VALUES_EQUAL(0, update.SeqNo);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};

            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "disk-1",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = 20_GB / DefaultLogicalBlockSize
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", result.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_OK, result.IOMode);
            UNIT_ASSERT_UNEQUAL(TInstant::Zero(), result.IOModeTs);
            UNIT_ASSERT_VALUES_EQUAL(0, result.Migrations.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            const auto m = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(1, m.size());
            auto it = m.begin();
            UNIT_ASSERT_VALUES_EQUAL("disk-1", it->DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", it->SourceDeviceId);
            auto target = state.StartDeviceMigration(Now(), db, it->DiskId, it->SourceDeviceId);
            UNIT_ASSERT_SUCCESS(target.GetError());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", target.GetResult().GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};

            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "disk-1",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = 20_GB / DefaultLogicalBlockSize
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", result.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_OK, result.IOMode);
            UNIT_ASSERT_UNEQUAL(TInstant::Zero(), result.IOModeTs);
            UNIT_ASSERT_VALUES_EQUAL(1, result.Migrations.size());
            const auto& m = result.Migrations[0];
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", m.GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", m.GetTargetDevice().GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;

            const auto error = state.FinishDeviceMigration(
                db,
                "disk-1",
                "uuid-2.1",
                "uuid-1.2",
                Now(),
                &updated);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(updated);

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ONLINE, update);
            UNIT_ASSERT_VALUES_EQUAL(1, update.SeqNo);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};

            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "disk-1",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = 20_GB / DefaultLogicalBlockSize
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", result.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_OK, result.IOMode);
            UNIT_ASSERT_UNEQUAL(TInstant::Zero(), result.IOModeTs);
            UNIT_ASSERT_VALUES_EQUAL(0, result.Migrations.size());
        });
    }

    Y_UNIT_TEST(ShouldHandleBrokenDevices)
    {
        const auto agentA = AgentConfig(1111, {
            Device("dev-1", "uuid-1", "rack-1", DefaultBlockSize, 93_GB),
            Device("dev-2", "uuid-2", "rack-1", DefaultBlockSize, 93_GB),
            Device("dev-3", "uuid-3", "rack-1", DefaultBlockSize, 93_GB),
            Device("dev-4", "uuid-4", "rack-1", DefaultBlockSize, 93_GB),
        });

        auto agentB = AgentConfig(1111, {
            Device("dev-1", "uuid-1", "rack-2", DefaultBlockSize, 94_GB),
            Device("dev-2", "uuid-2", "rack-2", DefaultBlockSize, 94_GB),
            Device("dev-3", "uuid-3", "rack-2", DefaultBlockSize, 93_GB),
            Device("dev-4", "uuid-4", "rack-2", DefaultBlockSize, 94_GB),
        });
        agentB.SetNodeId(42);

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agentA })
            .WithStorageConfig([] {
                auto config = CreateDefaultStorageConfigProto();
                config.SetAllocationUnitNonReplicatedSSD(93);
                return config;
            }())
            .WithDisks({
                [] {
                    NProto::TDiskConfig disk;

                    disk.SetDiskId("vol0");
                    disk.SetBlockSize(4_KB);
                    disk.AddDeviceUUIDs("uuid-4");

                    return disk;
                } ()
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [r, error] = state.RegisterAgent(db, agentB, Now());
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(1, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("vol0", r.AffectedDisks[0]);
        });

        UNIT_ASSERT(state.FindAgent(1) == nullptr);
        UNIT_ASSERT(state.FindAgent(42) != nullptr);

        for (size_t i = 1; i != agentB.DevicesSize() + 1; ++i) {
            const TString uuid = Sprintf("uuid-%lu", i);
            const auto device = state.GetDevice(uuid);
            UNIT_ASSERT_VALUES_EQUAL(Sprintf("dev-%lu", i), device.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, device.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(42, device.GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL("rack-2", device.GetRack());
            UNIT_ASSERT_VALUES_EQUAL(agentB.GetAgentId(), device.GetAgentId());

            const auto expectedState = i == 4
                ? NProto::DEVICE_STATE_ERROR
                : NProto::DEVICE_STATE_ONLINE;

            UNIT_ASSERT_EQUAL_C(expectedState, device.GetState(), uuid);
            UNIT_ASSERT_VALUES_EQUAL_C(
                93_GB / DefaultBlockSize,
                device.GetBlocksCount(),
                uuid);
        }
    }

    Y_UNIT_TEST(ShouldTrackMaxWarningTime)
    {
        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1"),
                Device("dev-3", "uuid-1.3", "rack-1"),
                Device("dev-4", "uuid-1.4", "rack-1"),
                Device("dev-5", "uuid-1.5", "rack-1"),
                Device("dev-6", "uuid-1.6", "rack-1"),
                Device("dev-7", "uuid-1.7", "rack-1"),
                Device("dev-8", "uuid-1.8", "rack-1"),
            })
        };

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");
        auto disksInWarningState = diskRegistryGroup->GetCounter("DisksInWarningState");
        auto disksInMigrationState = diskRegistryGroup->GetCounter("DisksInMigrationState");
        auto devicesInMigrationState = diskRegistryGroup->GetCounter("DevicesInMigrationState");
        auto maxWarningTime = diskRegistryGroup->GetCounter("MaxWarningTime");
        auto maxMigrationTime = diskRegistryGroup->GetCounter("MaxMigrationTime");

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(diskRegistryGroup)
            .WithKnownAgents(agents)
            .WithDisks({
                Disk("disk-1", {"uuid-1.1"}),
                Disk("disk-2", {"uuid-1.2"}),
                Disk("disk-3", {"uuid-1.3"}),
                Disk("disk-4", {"uuid-1.4"}),
                Disk("disk-5", {"uuid-1.5"}),
                Disk("disk-6", {"uuid-1.6"}),
            })
            .Build();

        auto kickDevice = [&] (
            int i,
            TInstant timestamp,
            NProto::EDeviceState newState,
            NProto::EDiskState expectedDiskState)
        {
            TString affectedDisk;

            executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
                const auto error = state.UpdateDeviceState(
                    db,
                    Sprintf("uuid-1.%d", i),
                    newState,
                    timestamp,
                    "test",
                    affectedDisk);
                UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
            });

            const auto expectedId = Sprintf("disk-%d", i);

            UNIT_ASSERT_VALUES_EQUAL(expectedId, affectedDisk);
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_VALUES_EQUAL(expectedId, update.State.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::EDiskState_Name(expectedDiskState),
                NProto::EDiskState_Name(update.State.GetState()));
        };

        auto warnDevice = [&] (int i, TInstant timestamp) {
            kickDevice(i, timestamp, NProto::DEVICE_STATE_WARNING, NProto::DISK_STATE_WARNING);
        };

        auto errorDevice = [&] (int i, TInstant timestamp) {
            kickDevice(i, timestamp, NProto::DEVICE_STATE_ERROR, NProto::DISK_STATE_ERROR);
        };

        auto onlineDevice = [&] (int i, TInstant timestamp) {
            kickDevice(i, timestamp, NProto::DEVICE_STATE_ONLINE, NProto::DISK_STATE_ONLINE);
        };

        state.PublishCounters(TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxWarningTime->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxMigrationTime->Val());

        auto disk1WarningTs = TInstant::Seconds(100);
        auto disk2WarningTs = TInstant::Seconds(200);
        auto disk3WarningTs = TInstant::Seconds(300);

        warnDevice(1, disk1WarningTs);
        warnDevice(2, disk2WarningTs);
        warnDevice(3, disk3WarningTs);

        auto now1 = TInstant::Seconds(1000);
        state.PublishCounters(now1);
        UNIT_ASSERT_VALUES_EQUAL(3, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, devicesInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            (now1 - disk1WarningTs).Seconds(),
            maxWarningTime->Val());

        // there is no any migration yet
        UNIT_ASSERT_VALUES_EQUAL(0, maxMigrationTime->Val());

        auto disk1StartMigrationTs = TInstant::Seconds(1500);

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [target, error] = state.StartDeviceMigration(
                disk1StartMigrationTs,
                db,
                "disk-1",
                "uuid-1.1",
                "uuid-1.7"
            );

            UNIT_ASSERT_SUCCESS(error);
        });

        auto disk2StartMigrationTs = TInstant::Seconds(1600);

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [target, error] = state.StartDeviceMigration(
                disk2StartMigrationTs,
                db,
                "disk-2",
                "uuid-1.2",
                "uuid-1.8"
            );

            UNIT_ASSERT_SUCCESS(error);
        });

        // cancel migration for disk-3
        errorDevice(3, TInstant::Seconds(2100));

        auto now2 = TInstant::Seconds(3000);
        state.PublishCounters(now2);
        UNIT_ASSERT_VALUES_EQUAL(2, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, devicesInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            (now2 - disk1WarningTs).Seconds(),
            maxWarningTime->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            (now2 - disk1StartMigrationTs).Seconds(),
            maxMigrationTime->Val());

        // restart migration for disk-1
        disk1WarningTs = TInstant::Seconds(3300);
        errorDevice(1, disk1WarningTs);
        onlineDevice(1, disk1WarningTs);
        warnDevice(1, disk1WarningTs);

        auto now3 = TInstant::Seconds(4000);
        state.PublishCounters(now3);
        UNIT_ASSERT_VALUES_EQUAL(2, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, devicesInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            (now3 - disk2WarningTs).Seconds(),
            maxWarningTime->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            (now3 - disk2StartMigrationTs).Seconds(),
            maxMigrationTime->Val());

        // cancel migration for disk-2
        errorDevice(2, TInstant::Seconds(4100));

        auto now4 = TInstant::Seconds(5000);
        state.PublishCounters(now4);
        UNIT_ASSERT_VALUES_EQUAL(1, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, devicesInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            (now4 - disk1WarningTs).Seconds(),
            maxWarningTime->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxMigrationTime->Val());

        // cancel migration for disk-1
        errorDevice(1, TInstant::Seconds(5100));
        state.PublishCounters(TInstant::Seconds(6000));
        UNIT_ASSERT_VALUES_EQUAL(0, disksInWarningState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, disksInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, devicesInMigrationState->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxWarningTime->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxMigrationTime->Val());
    }

    Y_UNIT_TEST(ShouldGatherRacksInfo)
    {
        const TVector agents {
            AgentConfig(1000, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1"),
            }),
            AgentConfig(2000, {
                Device("dev-1", "uuid-2.1", "rack-2"),
                Device("dev-2", "uuid-2.2", "rack-2"),
            }),
            AgentConfig(3000, {
                Device("dev-1", "uuid-3.1", "rack-2"),
                Device("dev-2", "uuid-3.2", "rack-2"),
                Device("dev-3", "uuid-3.3", "rack-2"),
                Device("dev-4", "uuid-3.4", "rack-2", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
                Device("dev-5", "uuid-3.5", "rack-2", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
                Device("dev-6", "uuid-3.6", "rack-2", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
                Device("dev-7", "uuid-3.7", "rack-2", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
            AgentConfig(4000, {
                Device("dev-1", "uuid-4.1", "rack-3")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("dev-2", "uuid-4.2", "rack-3")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
            }),
            AgentConfig(5000, {
                Device("dev-1", "uuid-5.1", "rack-3", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
                Device("dev-2", "uuid-5.2", "rack-3", 4_KB, 100_GB)
                    | WithPool("rot", NProto::DEVICE_POOL_KIND_GLOBAL),
            }),
        };

        TDiskRegistryState state =
            TDiskRegistryStateBuilder()
                .WithKnownAgents(agents)
                .WithDisks({
                    Disk("disk-1", {"uuid-1.1"}),
                    Disk("disk-2", {"uuid-2.2"}),
                    Disk("disk-3", {"uuid-3.4", "uuid-3.5"}),
                    Disk("disk-4", {"uuid-4.1"}),
                })
                .WithDirtyDevices(
                    {TDirtyDevice{"uuid-2.1", {}},
                     TDirtyDevice{"uuid-3.3", {}},
                     TDirtyDevice{"uuid-3.6", {}},
                     TDirtyDevice{"uuid-5.2", {}}})
                .AddDevicePoolConfig(
                    "local-ssd",
                    10_GB,
                    NProto::DEVICE_POOL_KIND_LOCAL)
                .AddDevicePoolConfig(
                    "rot",
                    10_GB,
                    NProto::DEVICE_POOL_KIND_GLOBAL)
                .Build();

        const auto poolNames = state.GetPoolNames();
        ASSERT_VECTORS_EQUAL(
            TVector<TString>({"", "local-ssd", "rot"}),
            poolNames);

        // default pool

        auto info = state.GatherRacksInfo("");

        UNIT_ASSERT_VALUES_EQUAL(2, info.size());
        SortBy(info, [] (auto& x) { return x.Name; });

        {
            auto& rack = info[0];
            UNIT_ASSERT_VALUES_EQUAL("rack-1", rack.Name);
            UNIT_ASSERT_VALUES_EQUAL(10_GB, rack.FreeBytes);
            UNIT_ASSERT_VALUES_EQUAL(20_GB, rack.TotalBytes);
            UNIT_ASSERT_VALUES_EQUAL(1, rack.AgentInfos.size());

            auto& agent = rack.AgentInfos[0];
            UNIT_ASSERT_VALUES_EQUAL("agent-1000", agent.AgentId);
            UNIT_ASSERT_VALUES_EQUAL(1000, agent.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(1, agent.FreeDevices);
            UNIT_ASSERT_VALUES_EQUAL(1, agent.AllocatedDevices);
            UNIT_ASSERT_VALUES_EQUAL(0, agent.BrokenDevices);
            UNIT_ASSERT_VALUES_EQUAL(2, agent.TotalDevices);
        }

        {
            auto& rack = info[1];
            UNIT_ASSERT_VALUES_EQUAL("rack-2", rack.Name);
            UNIT_ASSERT_VALUES_EQUAL(20_GB, rack.FreeBytes);
            UNIT_ASSERT_VALUES_EQUAL(50_GB, rack.TotalBytes);
            UNIT_ASSERT_VALUES_EQUAL(2, rack.AgentInfos.size());
            SortBy(rack.AgentInfos, [] (auto& x) { return x.NodeId; });

            auto& agent1 = rack.AgentInfos[0];
            UNIT_ASSERT_VALUES_EQUAL("agent-2000", agent1.AgentId);
            UNIT_ASSERT_VALUES_EQUAL(2000, agent1.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(0, agent1.FreeDevices);
            UNIT_ASSERT_VALUES_EQUAL(1, agent1.AllocatedDevices);
            UNIT_ASSERT_VALUES_EQUAL(0, agent1.BrokenDevices);
            UNIT_ASSERT_VALUES_EQUAL(2, agent1.TotalDevices);

            auto& agent2 = rack.AgentInfos[1];
            UNIT_ASSERT_VALUES_EQUAL("agent-3000", agent2.AgentId);
            UNIT_ASSERT_VALUES_EQUAL(3000, agent2.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(2, agent2.FreeDevices);
            UNIT_ASSERT_VALUES_EQUAL(0, agent2.AllocatedDevices);
            UNIT_ASSERT_VALUES_EQUAL(0, agent2.BrokenDevices);
            UNIT_ASSERT_VALUES_EQUAL(3, agent2.TotalDevices);
        }

        // local-ssd pool

        info = state.GatherRacksInfo("local-ssd");

        UNIT_ASSERT_VALUES_EQUAL(1, info.size());

        {
            auto& rack = info[0];
            UNIT_ASSERT_VALUES_EQUAL("rack-3", rack.Name);
            UNIT_ASSERT_VALUES_EQUAL(10_GB, rack.FreeBytes);
            UNIT_ASSERT_VALUES_EQUAL(20_GB, rack.TotalBytes);
            UNIT_ASSERT_VALUES_EQUAL(1, rack.AgentInfos.size());

            auto& agent = rack.AgentInfos[0];
            UNIT_ASSERT_VALUES_EQUAL("agent-4000", agent.AgentId);
            UNIT_ASSERT_VALUES_EQUAL(4000, agent.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(1, agent.FreeDevices);
            UNIT_ASSERT_VALUES_EQUAL(1, agent.AllocatedDevices);
            UNIT_ASSERT_VALUES_EQUAL(0, agent.BrokenDevices);
            UNIT_ASSERT_VALUES_EQUAL(2, agent.TotalDevices);
        }

        // rot pool

        info = state.GatherRacksInfo("rot");

        UNIT_ASSERT_VALUES_EQUAL(2, info.size());
        SortBy(info, [] (auto& x) { return x.Name; });

        {
            auto& rack = info[0];
            UNIT_ASSERT_VALUES_EQUAL("rack-2", rack.Name);
            UNIT_ASSERT_VALUES_EQUAL(100_GB, rack.FreeBytes);
            UNIT_ASSERT_VALUES_EQUAL(400_GB, rack.TotalBytes);
            UNIT_ASSERT_VALUES_EQUAL(1, rack.AgentInfos.size());

            auto& agent = rack.AgentInfos[0];
            UNIT_ASSERT_VALUES_EQUAL("agent-3000", agent.AgentId);
            UNIT_ASSERT_VALUES_EQUAL(3000, agent.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(1, agent.FreeDevices);
            UNIT_ASSERT_VALUES_EQUAL(2, agent.AllocatedDevices);
            UNIT_ASSERT_VALUES_EQUAL(0, agent.BrokenDevices);
            UNIT_ASSERT_VALUES_EQUAL(4, agent.TotalDevices);
        }

        {
            auto& rack = info[1];
            UNIT_ASSERT_VALUES_EQUAL("rack-3", rack.Name);
            UNIT_ASSERT_VALUES_EQUAL(100_GB, rack.FreeBytes);
            UNIT_ASSERT_VALUES_EQUAL(200_GB, rack.TotalBytes);
            UNIT_ASSERT_VALUES_EQUAL(1, rack.AgentInfos.size());

            auto& agent = rack.AgentInfos[0];
            UNIT_ASSERT_VALUES_EQUAL("agent-5000", agent.AgentId);
            UNIT_ASSERT_VALUES_EQUAL(5000, agent.NodeId);
            UNIT_ASSERT_VALUES_EQUAL(1, agent.FreeDevices);
            UNIT_ASSERT_VALUES_EQUAL(0, agent.AllocatedDevices);
            UNIT_ASSERT_VALUES_EQUAL(0, agent.BrokenDevices);
            UNIT_ASSERT_VALUES_EQUAL(2, agent.TotalDevices);
        }
    }

    Y_UNIT_TEST(ShouldCountBrokenPlacementGroupsWithSpreadStrategy)
    {
        const TVector agents {
            AgentConfig(1000, {Device("dev-1", "uuid-1", "rack-1")}),
            AgentConfig(2000, {Device("dev-1", "uuid-2", "rack-2")}),
            AgentConfig(3000, {Device("dev-1", "uuid-3", "rack-3")}),
            AgentConfig(4000, {Device("dev-1", "uuid-4", "rack-4")}),
            AgentConfig(5000, {Device("dev-1", "uuid-5", "rack-5")}),
            AgentConfig(6000, {Device("dev-1", "uuid-6", "rack-6")}),
            AgentConfig(7000, {Device("dev-1", "uuid-7", "rack-7")}),
            AgentConfig(8000, {Device("dev-1", "uuid-8", "rack-8")}),
            AgentConfig(9000, {Device("dev-1", "uuid-9", "rack-9")}),
        };

        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .With(diskRegistryGroup)
            .WithStorageConfig([] {
                auto config = CreateDefaultStorageConfigProto();
                config.SetPlacementGroupAlertPeriod(TDuration::Days(1).MilliSeconds());
                return config;
            }())
            .WithDisks({
                Disk("disk-1", {"uuid-1"}),
                Disk("disk-2", {"uuid-2"}),
                Disk("disk-3", {"uuid-3"}),
                Disk("disk-4", {"uuid-4"}),
                Disk("disk-5", {"uuid-5"}),
                Disk("disk-6", {"uuid-6"}),
                Disk("disk-7", {"uuid-7"}),
                Disk("disk-8", {"uuid-8"}),
                Disk("disk-9", {"uuid-9"}),
            })
            .WithPlacementGroups({
                SpreadPlacementGroup("group-1", {"disk-1", "disk-2", "disk-3"}),
                SpreadPlacementGroup("group-2", {"disk-4", "disk-5", "disk-6"}),
                SpreadPlacementGroup("group-3", {"disk-7", "disk-8", "disk-9"}),
            })
            .Build();

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto recentlySingle = diskRegistryGroup->GetCounter(
            "PlacementGroupsWithRecentlyBrokenSinglePartition");
        auto recentlyTwoOrMore = diskRegistryGroup->GetCounter(
            "PlacementGroupsWithRecentlyBrokenTwoOrMorePartitions");
        auto totalSingle = diskRegistryGroup->GetCounter(
            "PlacementGroupsWithBrokenSinglePartition");
        auto totalTwoOrMore = diskRegistryGroup->GetCounter(
            "PlacementGroupsWithBrokenTwoOrMorePartitions");

        state.PublishCounters(TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(0, recentlySingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, recentlyTwoOrMore->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalSingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalTwoOrMore->Val());

        auto breakDisk = [&] (int i, auto ts) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) {
                TString affectedDisk;
                UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                    db,
                    Sprintf("uuid-%d", i),
                    NProto::DEVICE_STATE_ERROR,
                    ts,
                    "test",
                    affectedDisk));

                const auto expectedId = Sprintf("disk-%d", i);

                UNIT_ASSERT_VALUES_EQUAL(expectedId, affectedDisk);
                UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
                const auto& update = state.GetDiskStateUpdates().back();
                UNIT_ASSERT_DISK_STATE(expectedId, DISK_STATE_ERROR, update);
            });
        };

        breakDisk(1, TInstant::Hours(2));
        breakDisk(2, TInstant::Hours(4));
        breakDisk(4, TInstant::Hours(6));
        breakDisk(7, TInstant::Hours(8));
        breakDisk(8, TInstant::Hours(10));
        breakDisk(9, TInstant::Hours(12));

        state.PublishCounters(TInstant::Hours(13));

        // group-1: [disk-1(r) disk-2(r) disk-3   ]  2/2
        // group-2: [disk-4(r) disk-5    disk-6   ]  1/1
        // group-3: [disk-7(r) disk-8(r) disk-9(r)]  3/3
        UNIT_ASSERT_VALUES_EQUAL(1, recentlySingle->Val());    // group-2
        UNIT_ASSERT_VALUES_EQUAL(2, recentlyTwoOrMore->Val()); // group-1 group-3
        UNIT_ASSERT_VALUES_EQUAL(1, totalSingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, totalTwoOrMore->Val());

        state.PublishCounters(TInstant::Hours(27));

        // group-1: [disk-1(d) disk-2(r) disk-3   ]  1/2
        // group-2: [disk-4(r) disk-5    disk-6   ]  1/1
        // group-3: [disk-7(r) disk-8(r) disk-9(r)]  3/3
        UNIT_ASSERT_VALUES_EQUAL(2, recentlySingle->Val());    // group-1 group-2
        UNIT_ASSERT_VALUES_EQUAL(1, recentlyTwoOrMore->Val()); // group-3
        UNIT_ASSERT_VALUES_EQUAL(1, totalSingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, totalTwoOrMore->Val());

        state.PublishCounters(TInstant::Hours(31));

        // group-1: [disk-1(d) disk-2(d) disk-3   ]  0/2
        // group-2: [disk-4(d) disk-5    disk-6   ]  0/1
        // group-3: [disk-7(r) disk-8(r) disk-9(r)]  3/3
        UNIT_ASSERT_VALUES_EQUAL(0, recentlySingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, recentlyTwoOrMore->Val()); // group-3
        UNIT_ASSERT_VALUES_EQUAL(1, totalSingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, totalTwoOrMore->Val());

        state.PublishCounters(TInstant::Hours(35));

        // group-1: [disk-1(d) disk-2(d) disk-3   ]  0/2
        // group-2: [disk-4(d) disk-5    disk-6   ]  0/1
        // group-3: [disk-7(d) disk-8(d) disk-9(r)]  1/3
        UNIT_ASSERT_VALUES_EQUAL(1, recentlySingle->Val()); // group-3
        UNIT_ASSERT_VALUES_EQUAL(0, recentlyTwoOrMore->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, totalSingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, totalTwoOrMore->Val());

        state.PublishCounters(TInstant::Hours(37));

        // group-1: [disk-1(d) disk-2(d) disk-3   ]  0/2
        // group-2: [disk-4(d) disk-5    disk-6   ]  0/1
        // group-3: [disk-7(d) disk-8(d) disk-9(d)]  0/3
        UNIT_ASSERT_VALUES_EQUAL(0, recentlySingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, recentlyTwoOrMore->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, totalSingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, totalTwoOrMore->Val());
    }

    Y_UNIT_TEST(ShouldCountBrokenPlacementGroupsWithPartitionStrategy)
    {
        const TVector agents {
            AgentConfig(1000, {Device("dev-1", "uuid-1", "rack-1")}),
            AgentConfig(2000, {Device("dev-1", "uuid-2", "rack-2")}),
            AgentConfig(3000, {Device("dev-1", "uuid-3", "rack-3")}),
            AgentConfig(4000, {Device("dev-1", "uuid-4", "rack-4")}),
            AgentConfig(5000, {Device("dev-1", "uuid-5", "rack-5")}),
            AgentConfig(6000, {Device("dev-1", "uuid-6", "rack-6")}),
            AgentConfig(7000, {Device("dev-1", "uuid-7", "rack-7")}),
            AgentConfig(8000, {Device("dev-1", "uuid-8", "rack-8")}),
            AgentConfig(9000, {Device("dev-1", "uuid-9", "rack-9")}),
        };

        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .With(diskRegistryGroup)
            .WithStorageConfig([] {
                auto config = CreateDefaultStorageConfigProto();
                config.SetPlacementGroupAlertPeriod(TDuration::Days(1).MilliSeconds());
                return config;
            }())
            .WithDisks({
                Disk("disk-1", {"uuid-1"}),
                Disk("disk-2", {"uuid-2"}),
                Disk("disk-3", {"uuid-3"}),
                Disk("disk-4", {"uuid-4"}),
                Disk("disk-5", {"uuid-5"}),
                Disk("disk-6", {"uuid-6"}),
                Disk("disk-7", {"uuid-7"}),
                Disk("disk-8", {"uuid-8"}),
                Disk("disk-9", {"uuid-9"}),
            })
            .WithPlacementGroups({
                PartitionPlacementGroup("group-1", {
                    {"disk-1", "disk-2", "disk-3"},
                    {"disk-4", "disk-5", "disk-6"}
                }),
                PartitionPlacementGroup("group-2", {
                    {"disk-7", "disk-8", "disk-9"}
                }),
            })
            .Build();


        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto recentlySingle = diskRegistryGroup->GetCounter(
            "PlacementGroupsWithRecentlyBrokenSinglePartition");
        auto recentlyTwoOrMore = diskRegistryGroup->GetCounter(
            "PlacementGroupsWithRecentlyBrokenTwoOrMorePartitions");
        auto totalSingle = diskRegistryGroup->GetCounter(
            "PlacementGroupsWithBrokenSinglePartition");
        auto totalTwoOrMore = diskRegistryGroup->GetCounter(
            "PlacementGroupsWithBrokenTwoOrMorePartitions");

        state.PublishCounters(TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(0, recentlySingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, recentlyTwoOrMore->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalSingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, totalTwoOrMore->Val());

        auto breakDisk = [&] (int i, auto ts) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) {
                TString affectedDisk;
                UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                    db,
                    Sprintf("uuid-%d", i),
                    NProto::DEVICE_STATE_ERROR,
                    ts,
                    "test",
                    affectedDisk));

                const auto expectedId = Sprintf("disk-%d", i);

                UNIT_ASSERT_VALUES_EQUAL(expectedId, affectedDisk);
                UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
                const auto& update = state.GetDiskStateUpdates().back();
                UNIT_ASSERT_DISK_STATE(expectedId, DISK_STATE_ERROR, update);
            });
        };

        breakDisk(1, TInstant::Hours(2));
        breakDisk(2, TInstant::Hours(4));
        breakDisk(4, TInstant::Hours(6));
        breakDisk(7, TInstant::Hours(8));
        breakDisk(8, TInstant::Hours(10));
        breakDisk(9, TInstant::Hours(12));

        state.PublishCounters(TInstant::Hours(13));

        // group-1 part-1: [disk-1(r) disk-2(r) disk-3   ]
        //         part-2: [disk-4(r) disk-5    disk-6   ]
        // group-2 part-1: [disk-7(r) disk-8(r) disk-9(r)]
        UNIT_ASSERT_VALUES_EQUAL(1, recentlySingle->Val());    // group-2
        UNIT_ASSERT_VALUES_EQUAL(1, recentlyTwoOrMore->Val()); // group-1
        UNIT_ASSERT_VALUES_EQUAL(1, totalSingle->Val());       // group-2
        UNIT_ASSERT_VALUES_EQUAL(1, totalTwoOrMore->Val());    // group-1

        state.PublishCounters(TInstant::Hours(27));

        // group-1 part-1: [disk-1(d) disk-2(r) disk-3   ]
        //         part-2: [disk-4(r) disk-5    disk-6   ]
        // group-2 part-1: [disk-7(r) disk-8(r) disk-9(r)]
        UNIT_ASSERT_VALUES_EQUAL(1, recentlySingle->Val());    // group-2
        UNIT_ASSERT_VALUES_EQUAL(1, recentlyTwoOrMore->Val()); // group-1
        UNIT_ASSERT_VALUES_EQUAL(1, totalSingle->Val());       // group-2
        UNIT_ASSERT_VALUES_EQUAL(1, totalTwoOrMore->Val());    // group-1

        state.PublishCounters(TInstant::Hours(29));

        // group-1 part-1: [disk-1(d) disk-2(d) disk-3   ]
        //         part-2: [disk-4(r) disk-5    disk-6   ]
        // group-2 part-1: [disk-7(r) disk-8(r) disk-9(r)]
        UNIT_ASSERT_VALUES_EQUAL(2, recentlySingle->Val());    // group-1 group-2
        UNIT_ASSERT_VALUES_EQUAL(0, recentlyTwoOrMore->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, totalSingle->Val());       // group-2
        UNIT_ASSERT_VALUES_EQUAL(1, totalTwoOrMore->Val());    // group-2

        state.PublishCounters(TInstant::Hours(35));

        // group-1 part-1: [disk-1(d) disk-2(d) disk-3   ]
        //         part-2: [disk-4(d) disk-5    disk-6   ]
        // group-2 part-1: [disk-7(d) disk-8(d) disk-9(r)]
        UNIT_ASSERT_VALUES_EQUAL(1, recentlySingle->Val());    // group-2
        UNIT_ASSERT_VALUES_EQUAL(0, recentlyTwoOrMore->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, totalSingle->Val());       // group-2
        UNIT_ASSERT_VALUES_EQUAL(1, totalTwoOrMore->Val());    // group-1

        state.PublishCounters(TInstant::Hours(37));

        // group-1 part-1: [disk-1(d) disk-2(d) disk-3   ]
        //         part-2: [disk-4(d) disk-5    disk-6   ]
        // group-3 part-1: [disk-7(d) disk-8(d) disk-9(d)]
        UNIT_ASSERT_VALUES_EQUAL(0, recentlySingle->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, recentlyTwoOrMore->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, totalSingle->Val());       // group-2
        UNIT_ASSERT_VALUES_EQUAL(1, totalTwoOrMore->Val());    // group-1
    }

    Y_UNIT_TEST(ShouldCountFreeBytes)
    {
        const TVector agents {
            AgentConfig(1000, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1"),
            }),
            AgentConfig(2000, {
                Device("dev-1", "uuid-2.1", "rack-2"),
                Device("dev-2", "uuid-2.2", "rack-2"),
            }),
            AgentConfig(3000, {
                Device("dev-1", "uuid-3.1", "rack-3"),
                Device("dev-2", "uuid-3.2", "rack-3"),
            }),
        };

        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");
        auto freeBytes = diskRegistryGroup->GetCounter("FreeBytes");

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .With(diskRegistryGroup)
            .WithDisks({ Disk("disk-1", {"uuid-1.1"}) })
            .Build();

        state.PublishCounters(TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(10_GB * 5, freeBytes->Val());

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TString affectedDisk;
            UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                db,
                "uuid-2.2",
                NProto::DEVICE_STATE_ERROR,
                TInstant::Zero(),
                "test",
                affectedDisk));
            UNIT_ASSERT(!affectedDisk);
        });

        state.PublishCounters(TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(10_GB * 4, freeBytes->Val());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                "agent-3000",
                NProto::AGENT_STATE_WARNING,
                TInstant::Zero(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(affectedDisks.empty());
        });

        state.PublishCounters(TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(10_GB * 2, freeBytes->Val());
    }

    Y_UNIT_TEST(ShouldRejectDevicesWithBadRack)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1a = AgentConfig(1000, {
            Device("dev-1", "uuid-1.1", ""),
            Device("dev-2", "uuid-1.2", ""),
        });

        const auto agent1b = AgentConfig(1000, {
            Device("dev-2", "uuid-1.2", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({agent1a})
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent1b));

            {
                TVector<TDeviceConfig> devices;
                auto error = AllocateDisk(db, state, "disk", {}, {}, 20_GB, devices);
                UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
            }

            {
                TVector<TDeviceConfig> devices;
                UNIT_ASSERT_SUCCESS(AllocateDisk(db, state, "disk", {}, {}, 10_GB, devices));
                UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
                UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", devices[0].GetDeviceUUID());
            }
        });
    }

    Y_UNIT_TEST(ShouldNotAllocateDiskOnAgentWithWarningState)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents = {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1"),
                Device("dev-3", "uuid-1.3", "rack-1"),
                Device("dev-4", "uuid-1.4", "rack-1"),
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1", "rack-2"),
                Device("dev-2", "uuid-2.2", "rack-2"),
                Device("dev-3", "uuid-2.3", "rack-2"),
                Device("dev-4", "uuid-2.4", "rack-2")
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig(agents)
            .WithAgents({agents[0]})
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-1", {}, {}, 40_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(4, devices.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agents[1]));
            auto dd = state.GetDirtyDevices();
            UNIT_ASSERT_VALUES_EQUAL(agents[1].DevicesSize(), dd.size());
            for (const auto& device: dd) {
                state.MarkDeviceAsClean(Now(), db, device.GetDeviceUUID());
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            auto error = state.UpdateAgentState(
                db,
                agents[0].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);

            UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_WARNING, update);
        });

        // start migration
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            for (const auto& [diskId, deviceId]: state.BuildMigrationList()) {
                UNIT_ASSERT_SUCCESS(
                    state.StartDeviceMigration(Now(), db, diskId, deviceId).GetError()
                );
            }
        });

        // finish migration
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            for (const auto& m: diskInfo.Migrations) {
                bool updated = false;
                UNIT_ASSERT_SUCCESS(state.FinishDeviceMigration(
                    db,
                    "disk-1",
                    m.GetSourceDeviceId(),
                    m.GetTargetDevice().GetDeviceUUID(),
                    Now(),
                    &updated));
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto disksToReallocate = state.GetDisksToReallocate();
            UNIT_ASSERT_VALUES_EQUAL(1, disksToReallocate.size());
            for (auto& [diskId, seqNo]: disksToReallocate) {
                state.DeleteDiskToReallocate(db, diskId, seqNo);
            }
        });

        // clean dirty devices
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto dd = state.GetDirtyDevices();
            Sort(dd, TByDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(agents[0].DevicesSize(), dd.size());
            for (size_t i = 0; i != dd.size(); ++i) {
                const auto& uuid = agents[0].GetDevices(i).GetDeviceUUID();

                UNIT_ASSERT_VALUES_EQUAL(uuid, dd[i].GetDeviceUUID());

                state.MarkDeviceAsClean(Now(), db, uuid);
            }
        });

        // should not allocate disk on agent in warning state
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "disk-2", {}, {}, 40_GB, devices);
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });
    }

    Y_UNIT_TEST(ShouldRestoreMigrationList)
    {
        const TVector agents = {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", NProto::DEVICE_STATE_WARNING),
                Device("dev-2", "uuid-1.2"),
            }),
            AgentConfig(2, NProto::AGENT_STATE_WARNING, {
                Device("dev-1", "uuid-2.1"),
                Device("dev-2", "uuid-2.2"),
            }),
            AgentConfig(3, {
                Device("dev-1", "uuid-3.1"),
                Device("dev-2", "uuid-3.2", NProto::DEVICE_STATE_WARNING),
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithDisks({
                Disk("disk-1", {"uuid-1.1", "uuid-3.1"}, NProto::DISK_STATE_WARNING),
                Disk("disk-2", {"uuid-1.2", "uuid-2.2"}, NProto::DISK_STATE_WARNING),
                Disk("disk-3", {"uuid-2.1", "uuid-3.2"}, NProto::DISK_STATE_WARNING),
            })
            .Build();

        auto migrations = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(4, migrations.size());

        UNIT_ASSERT_VALUES_EQUAL("disk-1", migrations[0].DiskId);
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", migrations[0].SourceDeviceId);

        UNIT_ASSERT_VALUES_EQUAL("disk-2", migrations[1].DiskId);
        UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", migrations[1].SourceDeviceId);

        UNIT_ASSERT_VALUES_EQUAL("disk-3", migrations[2].DiskId);
        UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", migrations[2].SourceDeviceId);

        UNIT_ASSERT_VALUES_EQUAL("disk-3", migrations[3].DiskId);
        UNIT_ASSERT_VALUES_EQUAL("uuid-3.2", migrations[3].SourceDeviceId);
    }

    Y_UNIT_TEST(ShouldRestartMigrationInCaseOfBrokenTargets)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1000, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1"),
            }),
            AgentConfig(2000, {
                Device("dev-1", "uuid-2.1", "rack-2"),
                Device("dev-2", "uuid-2.2", "rack-2"),
            }),
            AgentConfig(3000, {
                Device("dev-1", "uuid-3.1", "rack-3"),
                Device("dev-2", "uuid-3.2", "rack-3"),
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig(agents)
            .WithAgents({ agents[0], agents[1] })
            .WithDisks({ Disk("disk-1", {"uuid-1.1", "uuid-1.2"}) })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                db,
                agents[0].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks
            ));

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());

            for (const auto& [diskId, deviceId]: migrations) {
                UNIT_ASSERT_SUCCESS(
                    state.StartDeviceMigration(Now(), db, diskId, deviceId).GetError()
                );
            }
        });

        UNIT_ASSERT(state.IsMigrationListEmpty());

        auto getTargets = [&] {
            TVector<TString> targets;
            executor.WriteTx([&] (TDiskRegistryDatabase db) {
                TDiskRegistryState::TAllocateDiskResult result {};

                auto error = state.AllocateDisk(
                    Now(),
                    db,
                    TDiskRegistryState::TAllocateDiskParams {
                        .DiskId = "disk-1",
                        .BlockSize = DefaultLogicalBlockSize,
                        .BlocksCount = 20_GB / DefaultLogicalBlockSize
                    },
                    &result);

                UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                targets.reserve(result.Migrations.size());
                for (const auto& m: result.Migrations) {
                    targets.push_back(m.GetTargetDevice().GetDeviceUUID());
                }
                Sort(targets);
            });

            return targets;
        };

        const auto targets = getTargets();

        UNIT_ASSERT_VALUES_EQUAL(2, targets.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", targets[0]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", targets[1]);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [r, error] = state.RegisterAgent(db, agents[2], Now());
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());
            CleanDevices(state, db);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [r, error] = state.RegisterAgent(
                db,
                AgentConfig(agents[1].GetNodeId(), {}),
                Now());

            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());

            for (const auto& [diskId, deviceId]: migrations) {
                UNIT_ASSERT_SUCCESS(
                    state.StartDeviceMigration(Now(), db, diskId, deviceId).GetError()
                );
            }
        });

        const auto newTargets = getTargets();

        UNIT_ASSERT_VALUES_EQUAL(2, newTargets.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3.1", newTargets[0]);
        UNIT_ASSERT_VALUES_EQUAL("uuid-3.2", newTargets[1]);

    }

    Y_UNIT_TEST(ShouldAdjustDevices)
    {
        const ui64 referenceDeviceSize = 10_GB;

        const auto agent1a = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1", 512,  referenceDeviceSize + 1_GB),
            Device("dev-2", "uuid-2", "rack-1", 8_KB, referenceDeviceSize),
            Device("dev-3", "uuid-3", "rack-1", 4_KB, referenceDeviceSize - 1_GB),
            Device("dev-4", "uuid-4", "rack-1", 4_KB, referenceDeviceSize - 2_GB),
        });

        const auto agent1b = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1", 512,  referenceDeviceSize + 1_GB),
            Device("dev-2", "uuid-2", "rack-1", 8_KB, referenceDeviceSize),
            Device("dev-3", "uuid-3", "rack-1", 4_KB, referenceDeviceSize - 1_GB),
            Device("dev-4", "uuid-4", "rack-1", 4_KB, referenceDeviceSize - 2_GB),

            Device("dev-5", "uuid-5", "rack-1", 64_KB, referenceDeviceSize + 2_GB),
            Device("dev-6", "uuid-6", "rack-1", 4_KB, referenceDeviceSize - 3_GB),
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto config = CreateDefaultStorageConfigProto();
        config.SetAllocationUnitNonReplicatedSSD(10);

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithStorageConfig(config)
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, {agent1b});
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent1a));
        });

        {
            auto device = state.GetDevice("uuid-1");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                referenceDeviceSize / device.GetBlockSize(),
                device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (referenceDeviceSize + 1_GB) / device.GetBlockSize(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(512, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-2");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                referenceDeviceSize / device.GetBlockSize(),
                device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                device.GetBlocksCount(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(8_KB, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-3");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ERROR, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                device.GetBlocksCount(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (referenceDeviceSize - 1_GB) / device.GetBlockSize(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-4");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ERROR, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                device.GetBlocksCount(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (referenceDeviceSize - 2_GB) / device.GetBlockSize(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, device.GetBlockSize());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent1b));
        });

        {
            auto device = state.GetDevice("uuid-1");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                referenceDeviceSize / device.GetBlockSize(),
                device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (referenceDeviceSize + 1_GB) / device.GetBlockSize(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(512, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-2");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                referenceDeviceSize / device.GetBlockSize(),
                device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                device.GetBlocksCount(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(8_KB, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-3");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ERROR, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                device.GetBlocksCount(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (referenceDeviceSize - 1_GB) / device.GetBlockSize(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-4");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ERROR, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                device.GetBlocksCount(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (referenceDeviceSize - 2_GB) / device.GetBlockSize(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-5");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                referenceDeviceSize / device.GetBlockSize(),
                device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (referenceDeviceSize + 2_GB) / device.GetBlockSize(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(64_KB, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-6");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ERROR, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                device.GetBlocksCount(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (referenceDeviceSize - 3_GB) / device.GetBlockSize(),
                device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, device.GetBlockSize());
        }
    }

    Y_UNIT_TEST(ShouldAdjustDeviceAtMigration)
    {
        const ui64 referenceDeviceSize = 93_GB;
        const ui64 blockCount = 24413696; // ~ 93.13 GB
        const ui64 blockSize = 4_KB;

        const TVector agents {
            AgentConfig(1000, {
                Device("dev-1", "uuid-1.1", "rack-1", blockSize, blockCount * blockSize),
                Device("dev-2", "uuid-1.2", "rack-1", blockSize, blockCount * blockSize),
            }),
            AgentConfig(2000, {
                Device("dev-1", "uuid-2.1", "rack-2", blockSize, 94_GB),
                Device("dev-2", "uuid-2.2", "rack-2", blockSize, 94_GB),
            }),
        };

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto config = CreateDefaultStorageConfigProto();
        config.SetAllocationUnitNonReplicatedSSD(referenceDeviceSize / 1_GB);

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithStorageConfig(config)
            .WithDisks({Disk("disk-1", {"uuid-1.1", "uuid-1.2"})})
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, agents);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agents[0]));
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agents[1]));
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());
            CleanDevices(state, db);
        });

        {
            auto device = state.GetDevice("uuid-1.1");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(blockCount, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(blockCount, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-1.2");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(blockCount, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(blockCount, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-2.1");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                referenceDeviceSize / device.GetBlockSize(),
                device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(94_GB/blockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-2.2");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                referenceDeviceSize / device.GetBlockSize(),
                device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(94_GB/blockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            const auto error = state.UpdateDeviceState(
                db,
                "uuid-1.1",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                "test",
                affectedDisk);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);

            UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_WARNING, update);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            const auto ml = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(1, ml.size());
            const auto& m = ml[0];
            UNIT_ASSERT_VALUES_EQUAL("disk-1", m.DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", m.SourceDeviceId);
            UNIT_ASSERT_SUCCESS(
                state.StartDeviceMigration(Now(), db, m.DiskId, m.SourceDeviceId).GetError()
            );
            UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};

            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "disk-1",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = blockCount*2
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", result.Devices[1].GetDeviceUUID());

            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_OK, result.IOMode);
            UNIT_ASSERT_UNEQUAL(TInstant::Zero(), result.IOModeTs);

            UNIT_ASSERT_VALUES_EQUAL(1, result.Migrations.size());
            const auto& m = result.Migrations[0];
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", m.GetSourceDeviceId());

            THashSet<TString> ids { "uuid-2.1", "uuid-2.2" };

            const auto targetId = m.GetTargetDevice().GetDeviceUUID();

            // adjusted to source device
            {
                auto device = state.GetDevice(targetId);
                UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
                UNIT_ASSERT_VALUES_EQUAL(blockCount, device.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(94_GB/blockSize, device.GetUnadjustedBlockCount());
                UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
            }

            ids.erase(targetId);

            // adjusted to reference size
            {
                auto device = state.GetDevice(*ids.begin());
                UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
                UNIT_ASSERT_VALUES_EQUAL(
                    referenceDeviceSize / device.GetBlockSize(),
                    device.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(94_GB/blockSize, device.GetUnadjustedBlockCount());
                UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
            }
        });
    }

    Y_UNIT_TEST(ShouldCheckDeviceOverridesUponMigrationStart)
    {
        const ui64 referenceDeviceSize = 93_GB;
        const ui64 blockCount = 24413696; // ~ 93.13 GB
        const ui64 blockSize = 4_KB;

        const TVector agents {
            AgentConfig(1000, {
                Device("dev-1", "uuid-1.1", "rack-1", blockSize, blockCount * blockSize),
                Device("dev-2", "uuid-1.2", "rack-1", blockSize, blockCount * blockSize),
            }),
            AgentConfig(2000, {
                Device("dev-1", "uuid-2.1", "rack-2", blockSize, 94_GB),
                Device("dev-2", "uuid-2.2", "rack-2", blockSize, 94_GB),
            }),
        };

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto config = CreateDefaultStorageConfigProto();
        config.SetAllocationUnitNonReplicatedSSD(referenceDeviceSize / 1_GB);

        TVector<NProto::TDeviceOverride> deviceOverrides;
        deviceOverrides.emplace_back();
        deviceOverrides.back().SetDiskId("disk-1");
        deviceOverrides.back().SetDevice("uuid-1.1");
        deviceOverrides.back().SetBlocksCount(referenceDeviceSize / blockSize);

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithStorageConfig(config)
            .WithDisks({Disk("disk-1", {"uuid-1.1", "uuid-1.2"})})
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateConfig(state, db, agents, deviceOverrides);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agents[0]));
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agents[1]));
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());
            CleanDevices(state, db);
        });

        {
            auto device = state.GetDevice("uuid-1.1");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(blockCount, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(blockCount, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-1.2");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(blockCount, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(blockCount, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-2.1");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                referenceDeviceSize / device.GetBlockSize(),
                device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(94_GB / blockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
        }

        {
            auto device = state.GetDevice("uuid-2.2");
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(
                referenceDeviceSize / device.GetBlockSize(),
                device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(94_GB / blockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            const auto error = state.UpdateAgentState(
                db,
                "agent-1000",
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);

            UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_WARNING, update);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            const auto ml = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(2, ml.size());
            const auto& m0 = ml[0];
            UNIT_ASSERT_VALUES_EQUAL("disk-1", m0.DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", m0.SourceDeviceId);
            UNIT_ASSERT_SUCCESS(
                state.StartDeviceMigration(Now(), db, m0.DiskId, m0.SourceDeviceId).GetError()
            );
            const auto& m1 = ml[1];
            UNIT_ASSERT_VALUES_EQUAL("disk-1", m1.DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", m1.SourceDeviceId);
            UNIT_ASSERT_SUCCESS(
                state.StartDeviceMigration(Now(), db, m1.DiskId, m1.SourceDeviceId).GetError()
            );
            UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};

            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "disk-1",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = referenceDeviceSize * 2 / DefaultLogicalBlockSize
                },
                &result));

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", result.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", result.Devices[1].GetDeviceUUID());

            UNIT_ASSERT_EQUAL(NProto::VOLUME_IO_OK, result.IOMode);
            UNIT_ASSERT_UNEQUAL(TInstant::Zero(), result.IOModeTs);

            SortBy(result.Migrations, [] (const auto& x) {
                return x.GetSourceDeviceId();
            });

            UNIT_ASSERT_VALUES_EQUAL(2, result.Migrations.size());
            const auto& m0 = result.Migrations[0];
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", m0.GetSourceDeviceId());
            const auto& m1 = result.Migrations[1];
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", m1.GetSourceDeviceId());

            // adjusted to source device
            {
                auto device = state.GetDevice(m0.GetTargetDevice().GetDeviceUUID());
                UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
                UNIT_ASSERT_VALUES_EQUAL(
                    referenceDeviceSize / blockSize,
                    device.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    referenceDeviceSize / DefaultLogicalBlockSize,
                    m0.GetTargetDevice().GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(94_GB / blockSize, device.GetUnadjustedBlockCount());
                UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
            }

            // adjusted to source device
            {
                auto device = state.GetDevice(m1.GetTargetDevice().GetDeviceUUID());
                UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
                UNIT_ASSERT_VALUES_EQUAL(blockCount, device.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(blockCount, m1.GetTargetDevice().GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(94_GB / blockSize, device.GetUnadjustedBlockCount());
                UNIT_ASSERT_VALUES_EQUAL(blockSize, device.GetBlockSize());
            }
        });
    }

    Y_UNIT_TEST(ShouldProcessPlacementGroups)
    {
        const TVector<TString> expected {"group1", "group2", "group3"};

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithSpreadPlacementGroups(expected)
            .Build();

        TVector<TPlacementGroupInfo> groups;

        for (const auto& [diskId, config]: state.GetPlacementGroups()) {
            groups.push_back(config);
        }

        SortBy(groups, [] (const auto& x) {
            return x.Config.GetGroupId();
        });

        UNIT_ASSERT_VALUES_EQUAL(expected.size(), groups.size());
        for (size_t i = 0; i != expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(expected[i], groups[i].Config.GetGroupId());
        }
    }

    Y_UNIT_TEST(ShouldRemoveDestroyedDiskFromNotifyList)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1")
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1", "rack-2"),
                Device("dev-2", "uuid-2.2", "rack-2")
            }),
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithDisksToReallocate({ "disk-1" })
            .WithDisks({
                Disk("disk-1", {"uuid-1.1", "uuid-2.1"}),
                Disk("disk-2", {"uuid-1.2", "uuid-2.2"})
            })
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetDisksToReallocate().size());
        UNIT_ASSERT_GT(state.GetDisksToReallocate().at("disk-1"), 0);
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskStateUpdates().size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisk;
            UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                db,
                agents[0].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisk));

            UNIT_ASSERT_VALUES_EQUAL(2, affectedDisk.size());
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDiskStateUpdates().size());
        });

        UNIT_ASSERT_VALUES_EQUAL(2, state.GetDisksToReallocate().size());
        UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));
        UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-2"));

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
        });

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetDisksToReallocate().size());
        UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-2"));

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-2"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-2"));
        });

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDisksToReallocate().size());
    }

    Y_UNIT_TEST(ShouldMigrateToDeviceWithBiggerSize)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 11_GB),
            }),
            AgentConfig(2, {
                [] {
                    auto config = Device(
                        "dev-1",
                        "uuid-2.1",
                        "rack-2", DefaultBlockSize, 10_GB);
                    config.SetUnadjustedBlockCount(12_GB / DefaultBlockSize);
                    return config;
                } ()
            }),
        };

        auto config = CreateDefaultStorageConfigProto();
        config.SetAllocationUnitNonReplicatedSSD(10);

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithStorageConfig(config)
            .WithKnownAgents(agents)
            .WithDisksToReallocate({ "disk-1" })
            .WithDisks({ Disk("disk-1", { "uuid-1.1" }) })
            .Build();

        {
            TVector<NProto::TDeviceConfig> devices;
            UNIT_ASSERT_SUCCESS(state.GetDiskDevices("disk-1", devices));
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(11_GB/DefaultBlockSize, devices[0].GetBlocksCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, devices[0].GetState());
        }

        {
            auto device = state.GetDevice("uuid-1.1");
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(11_GB/DefaultBlockSize, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(11_GB/DefaultBlockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, device.GetState());
        }

        {
            auto device = state.GetDevice("uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(10_GB/DefaultBlockSize, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(12_GB/DefaultBlockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, device.GetState());
        }

        // migrate from uuid-1.1 to uuid-2.1

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                db,
                agents[0].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks));

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);

            UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_WARNING, update);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            const auto m = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(1, m.size());
            auto& [diskId, sourceId] = m[0];

            UNIT_ASSERT_VALUES_EQUAL("disk-1", diskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", sourceId);

            auto target = state.StartDeviceMigration(Now(), db, diskId, sourceId);
            UNIT_ASSERT_SUCCESS(target.GetError());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", target.GetResult().GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        });

        {
            auto device = state.GetDevice("uuid-1.1");
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(11_GB/DefaultBlockSize, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(11_GB/DefaultBlockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, device.GetState());
        }

        {
            auto device = state.GetDevice("uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(11_GB/DefaultBlockSize, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(12_GB/DefaultBlockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, device.GetState());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agents[1]));
        });

        {
            auto device = state.GetDevice("uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(11_GB/DefaultBlockSize, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(12_GB/DefaultBlockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, device.GetState());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;

            UNIT_ASSERT_SUCCESS(state.FinishDeviceMigration(
                db,
                "disk-1",
                "uuid-1.1",
                "uuid-2.1",
                Now(),
                &updated));

            UNIT_ASSERT(updated);

            UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ONLINE, update);
            UNIT_ASSERT_VALUES_EQUAL(1, update.SeqNo);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto disksToReallocate = state.GetDisksToReallocate();
            UNIT_ASSERT_VALUES_EQUAL(1, disksToReallocate.size());
            for (auto& [diskId, seqNo]: disksToReallocate) {
                state.DeleteDiskToReallocate(db, diskId, seqNo);
            }

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDirtyDevices().size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", state.GetDirtyDevices()[0].GetDeviceUUID());

            CleanDevices(state, db);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agents[1]));
        });

        {
            auto device = state.GetDevice("uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(11_GB/DefaultBlockSize, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(12_GB/DefaultBlockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, device.GetState());
        }

        // migrate from uuid-2.1 to uuid-1.1

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                db,
                agents[0].GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                Now(),
                "state message",
                affectedDisks));

            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                db,
                agents[1].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks));

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);

            UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_WARNING, update);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            const auto m = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(1, m.size());
            auto& [diskId, sourceId] = m[0];

            UNIT_ASSERT_VALUES_EQUAL("disk-1", diskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", sourceId);

            auto target = state.StartDeviceMigration(Now(), db, diskId, sourceId);
            UNIT_ASSERT_SUCCESS(target.GetError());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", target.GetResult().GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;

            UNIT_ASSERT_SUCCESS(state.FinishDeviceMigration(
                db,
                "disk-1",
                "uuid-2.1",
                "uuid-1.1",
                Now(),
                &updated));

            UNIT_ASSERT(updated);
            UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ONLINE, update);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto disksToReallocate = state.GetDisksToReallocate();
            UNIT_ASSERT_VALUES_EQUAL(1, disksToReallocate.size());
            for (auto& [diskId, seqNo]: disksToReallocate) {
                state.DeleteDiskToReallocate(db, diskId, seqNo);
            }

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDirtyDevices().size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", state.GetDirtyDevices()[0].GetDeviceUUID());

            CleanDevices(state, db);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agents[1]));
        });

        {
            auto device = state.GetDevice("uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(10_GB/DefaultBlockSize, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(12_GB/DefaultBlockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, device.GetState());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agents[1]));
        });

        {
            auto device = state.GetDevice("uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(10_GB/DefaultBlockSize, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(12_GB/DefaultBlockSize, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, device.GetState());
        }
    }

    Y_UNIT_TEST(ShouldNotRegisterAgentIfSeqNumberIsSmallerThanCurrent)
    {
        auto agentId = "host-1.cloud.yandex.net";

        const auto agent1a = AgentConfig(1, agentId, 0, {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB, "#1"),
            Device("dev-3", "uuid-1.3", "rack-1", DefaultBlockSize, 10_GB)
        });

        const auto agent1b = AgentConfig(2, agentId, 1, {
            Device("dev-1", "uuid-1.1", "rack-1", DefaultBlockSize, 10_GB),
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB, "#1"),
            Device("dev-3", "uuid-1.3", "rack-1", DefaultBlockSize, 10_GB)
        });

        const auto agent1c = AgentConfig(3, agentId, 0, {
            Device("dev-2", "uuid-1.2", "rack-1", DefaultBlockSize, 10_GB, "#2"),
            Device("dev-3", "uuid-1.3", "rack-1", DefaultBlockSize, 20_GB)
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1a })
            .Build();

        auto CheckDevices = [&] () {
            const auto dev1 = state.GetDevice("uuid-1.1");
            UNIT_ASSERT_VALUES_EQUAL(10_GB / DefaultBlockSize, dev1.GetBlocksCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev1.GetState());

            const auto dev2 = state.GetDevice("uuid-1.2");
            UNIT_ASSERT_VALUES_EQUAL(10_GB / DefaultBlockSize, dev2.GetBlocksCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev2.GetState());
            UNIT_ASSERT_EQUAL("#1", dev2.GetTransportId());

            const auto dev3 = state.GetDevice("uuid-1.3");
            UNIT_ASSERT_VALUES_EQUAL(10_GB / DefaultBlockSize, dev3.GetBlocksCount());
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, dev3.GetState());
        };

        CheckDevices();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [r, error] = state.RegisterAgent(db, agent1b, Now());
            UNIT_ASSERT_C(!HasError(error), error);

            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, r.DisksToReallocate.size());
        });

        CheckDevices();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [r, error] = state.RegisterAgent(db, agent1c, Now());
            UNIT_ASSERT_VALUES_EQUAL_C(E_INVALID_STATE, error.GetCode(), error);

            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, r.DisksToReallocate.size());
        });

        CheckDevices();
    }

    Y_UNIT_TEST(ShouldSetUserId)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, { Device("dev-1", "uuid-1.1") })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithDisksToReallocate({ "disk-1" })
            .WithDisks({ Disk("disk-1", { "uuid-1.1" }) })
            .Build();

        {
            TDiskInfo info;

            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", info));
            UNIT_ASSERT_VALUES_EQUAL("", info.UserId);
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            UNIT_ASSERT_SUCCESS(state.SetUserId(db, "disk-1", "foo"));
            UNIT_ASSERT_VALUES_EQUAL(
                E_NOT_FOUND,
                state.SetUserId(db, "disk-x", "bar").GetCode());
        });

        {
            TDiskInfo info;

            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", info));
            UNIT_ASSERT_VALUES_EQUAL("foo", info.UserId);
        }
    }

    Y_UNIT_TEST(ShouldUpdateVolumeConfig)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(2, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agentConfig1 })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-1",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "group-2",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                {}));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            UNIT_ASSERT_SUCCESS(AllocateDisk(
                db,
                state,
                "disk-1",
                "group-1",
                {},
                10_GB,
                devices));

            auto [config, seqNo] = state.GetVolumeConfigUpdate("disk-1");
            UNIT_ASSERT_VALUES_EQUAL("", config.GetPlacementGroupId());
            UNIT_ASSERT_VALUES_EQUAL(0, seqNo);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {};
            UNIT_ASSERT_SUCCESS(
                state.AlterPlacementGroupMembership(
                    db,
                    "group-1",
                    0,
                    2,
                    disksToAdd,
                    {"disk-1"}));

            auto [config, seqNo] = state.GetVolumeConfigUpdate("disk-1");
            UNIT_ASSERT_VALUES_EQUAL("", config.GetPlacementGroupId());
            UNIT_ASSERT_VALUES_EQUAL(0, seqNo);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-1"};
            UNIT_ASSERT_SUCCESS(
                state.AlterPlacementGroupMembership(
                    db,
                    "group-2",
                    0,
                    1,
                    disksToAdd,
                    {}));

            auto [config, seqNo] = state.GetVolumeConfigUpdate("disk-1");
            UNIT_ASSERT_VALUES_EQUAL("group-2", config.GetPlacementGroupId());
            UNIT_ASSERT_VALUES_EQUAL(1, seqNo);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.DestroyPlacementGroup(db, "group-2", affectedDisks));

            auto [config, seqNo] = state.GetVolumeConfigUpdate("disk-1");
            UNIT_ASSERT(config.HasPlacementGroupId());
            UNIT_ASSERT_VALUES_EQUAL("", config.GetPlacementGroupId());
            UNIT_ASSERT_VALUES_EQUAL(seqNo, 2);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> disksToAdd = {"disk-1"};
            UNIT_ASSERT_SUCCESS(
                state.AlterPlacementGroupMembership(
                    db,
                    "group-1",
                    0,
                    3,
                    disksToAdd,
                    {}));

            auto [config, seqNo] = state.GetVolumeConfigUpdate("disk-1");
            UNIT_ASSERT_VALUES_EQUAL("group-1", config.GetPlacementGroupId());
            UNIT_ASSERT_VALUES_EQUAL(seqNo, 3);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));

            auto [config, seqNo] = state.GetVolumeConfigUpdate("disk-1");
            UNIT_ASSERT(!config.HasPlacementGroupId());
            UNIT_ASSERT_VALUES_EQUAL(0, seqNo);
        });
    }

    Y_UNIT_TEST(ShouldMuteIOErrors)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1")
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({agent1})
            .WithDisks({
                Disk("disk-1", {"uuid-1", "uuid-2"}),
            })
            .Build();

        auto allocateDisk = [&] (auto& db, auto& diskId, auto totalSize) {
            TDiskRegistryState::TAllocateDiskResult result {};
            state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = diskId,
                    .PlacementGroupId = {},
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = totalSize / DefaultLogicalBlockSize,
                },
                &result);
            return result;
        };

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto result = allocateDisk(db, "disk-1", 20_GB);
            UNIT_ASSERT_EQUAL(result.IOMode, NProto::VOLUME_IO_OK);
            UNIT_ASSERT_EQUAL(result.MuteIOErrors, false);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                Now(),
                "state message",
                affectedDisks);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto result = allocateDisk(db, "disk-1", 20_GB);
            UNIT_ASSERT_EQUAL(result.IOMode, NProto::VOLUME_IO_OK);
            UNIT_ASSERT_EQUAL(result.MuteIOErrors, true);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            state.UpdateAgentState(
                db,
                agent1.GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                Now(),
                "state message",
                affectedDisks);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto result = allocateDisk(db, "disk-1", 20_GB);
            UNIT_ASSERT_EQUAL(result.IOMode, NProto::VOLUME_IO_OK);
            UNIT_ASSERT_EQUAL(result.MuteIOErrors, false);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TString affectedDisk;
            state.UpdateDeviceState(
                db,
                "uuid-1",
                NProto::DEVICE_STATE_ERROR,
                Now(),
                "reason",
                affectedDisk);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto result = allocateDisk(db, "disk-1", 20_GB);
            UNIT_ASSERT_EQUAL(result.IOMode, NProto::VOLUME_IO_ERROR_READ_ONLY);
            UNIT_ASSERT_EQUAL(result.MuteIOErrors, true);
        });
    }

    Y_UNIT_TEST(ShouldAllowMigrationForDiskWithUnavailableState)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-2", "rack-2"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({agent1, agent2})
            .WithDisks({
                Disk("disk-1", {"uuid-1", "uuid-2"}),
            })
            .Build();

        auto updateAgentState = [&] (auto db, const auto& agent, auto desiredState) {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                db,
                agent.GetAgentId(),
                desiredState,
                Now(),
                "state message",
                affectedDisks));
            return affectedDisks;
        };

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto affectedDisks = updateAgentState(db, agent1, NProto::AGENT_STATE_UNAVAILABLE);
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT(state.IsMigrationListEmpty());
        });

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE, diskInfo.State);
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto affectedDisks = updateAgentState(db, agent2, NProto::AGENT_STATE_WARNING);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", migrations[0].SourceDeviceId);
        });

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE, diskInfo.State);
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto affectedDisks = updateAgentState(db, agent1, NProto::AGENT_STATE_WARNING);
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(2, state.BuildMigrationList().size());
        });

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_WARNING, diskInfo.State);
        }
    }

    Y_UNIT_TEST(ShouldDeferReleaseMigrationDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1"),
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1", "rack-2"),
                Device("dev-2", "uuid-2.2", "rack-2"),
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithDisks({
                Disk("disk-1", {"uuid-1.1", "uuid-1.2"}),
            })
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        UNIT_ASSERT(state.IsMigrationListEmpty());

        auto allocateDisk = [&] (auto& db)
            -> TResultOrError<TDiskRegistryState::TAllocateDiskResult>
        {
            TDiskRegistryState::TAllocateDiskResult result {};

            auto error = state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "disk-1",
                    .BlockSize = DefaultLogicalBlockSize,
                    .BlocksCount = 20_GB / DefaultLogicalBlockSize
                },
                &result);
            if (HasError(error)) {
                return error;
            }

            return result;
        };

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [result, error] = allocateDisk(db);
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(0, result.Migrations.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto affectedDisks = UpdateAgentState(
                state,
                db,
                agents[0],
                NProto::AGENT_STATE_WARNING);
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT(!state.IsMigrationListEmpty());
        });

        const auto migrations = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());

        TVector<TString> targets;
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            for (const auto& [diskId, uuid]: migrations) {
                auto [config, error] = state.StartDeviceMigration(Now(), db, diskId, uuid);
                UNIT_ASSERT_SUCCESS(error);
                targets.push_back(config.GetDeviceUUID());
            }
        });
        Sort(targets);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto [result, error] = allocateDisk(db);
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(2, result.Migrations.size());
        });

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.FinishedMigrations.size());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

        // cancel migrations
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto affectedDisks = UpdateAgentState(
                state,
                db,
                agents[0],
                NProto::AGENT_STATE_ONLINE);
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT(state.IsMigrationListEmpty());
        });

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.FinishedMigrations.size());
            SortBy(diskInfo.FinishedMigrations, [] (auto& m) {
                return m.DeviceId;
            });

            for (size_t i = 0; i != targets.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    targets[i],
                    diskInfo.FinishedMigrations[i].DeviceId);
            }
        }

        UNIT_ASSERT(!state.GetDisksToReallocate().empty());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto notifications = state.GetDisksToReallocate();
            for (const auto& [diskId, seqNo]: notifications) {
                state.DeleteDiskToReallocate(db, diskId, seqNo);
            }
        });

        UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.FinishedMigrations.size());
        }
    }

    Y_UNIT_TEST(ShouldAdjustDeviceBlockCountAfterSecureErase)
    {
        const ui64 referenceDeviceSize = 93_GB;
        const ui64 physicalDeviceBlockCount = 24641024;

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", "rack-1", 4_KB, physicalDeviceBlockCount * 4_KB),
                Device("dev-2", "uuid-1.2", "rack-1", 4_KB, physicalDeviceBlockCount * 4_KB),
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithStorageConfig([] {
                auto config = CreateDefaultStorageConfigProto();
                config.SetAllocationUnitNonReplicatedSSD(referenceDeviceSize / 1_GB);
                return config;
            }())
            .WithDisks({
                Disk("disk-1", {"uuid-1.1", "uuid-1.2"}),
            })
            .Build();

        for (TString uuid: { "uuid-1.1", "uuid-1.2" }) {
            auto device = state.GetDevice(uuid);
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(physicalDeviceBlockCount, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(physicalDeviceBlockCount, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, device.GetBlockSize());
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
            CleanDevices(state, db);
        });

        for (TString uuid: { "uuid-1.1", "uuid-1.2" }) {
            auto device = state.GetDevice(uuid);
            UNIT_ASSERT_EQUAL_C(NProto::DEVICE_STATE_ONLINE, device.GetState(), device);
            UNIT_ASSERT_VALUES_EQUAL(referenceDeviceSize / 4_KB, device.GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(physicalDeviceBlockCount, device.GetUnadjustedBlockCount());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, device.GetBlockSize());
        }
    }

    Y_UNIT_TEST(ShouldHandleLostDevices)
    {
        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1") | WithTotalSize(94_GB, 4_KB),
                Device("dev-2", "uuid-1.2") | WithTotalSize(94_GB, 4_KB),
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1") | WithTotalSize(94_GB, 4_KB),
                Device("dev-2", "uuid-2.2") | WithTotalSize(94_GB, 4_KB),
            }),
            AgentConfig(3, {
                Device("dev-1", "uuid-3.1") | WithTotalSize(94_GB, 4_KB),
                Device("dev-2", "uuid-3.2") | WithTotalSize(94_GB, 4_KB),
            })
        };

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            db.InitSchema();
            db.WriteDiskRegistryConfig(MakeConfig(agents));
        });

        std::optional<TDiskRegistryState> state = TDiskRegistryStateBuilder()
            .WithConfig(agents)
            .WithStorageConfig([] {
                auto config = CreateDefaultStorageConfigProto();
                config.SetAllocationUnitNonReplicatedSSD(93);
                return config;
            }())
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            for (size_t i = 0; i != agents.size() - 1; ++i) {
                auto [r, error] =
                    state->RegisterAgent(db, agents[i], TInstant::Now());
                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
                UNIT_ASSERT_VALUES_EQUAL(0, r.DisksToReallocate.size());

                UNIT_ASSERT_VALUES_EQUAL(
                    agents[i].DevicesSize(),
                    state->GetDirtyDevices().size());

                for (auto& d: state->GetDirtyDevices()) {
                    auto diskId = state->MarkDeviceAsClean(Now(), db, d.GetDeviceUUID());
                    UNIT_ASSERT_VALUES_EQUAL("", diskId);
                }
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state->AllocateDisk(
                TInstant::Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "foo",
                    .PlacementGroupId = {},
                    .BlockSize = 4_KB,
                    .BlocksCount = 93_GB / 4_KB * 4,
                },
                &result);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(4, result.Devices.size());
            for (auto& d: result.Devices) {
                UNIT_ASSERT_VALUES_UNEQUAL(0, d.GetNodeId());
                UNIT_ASSERT_VALUES_EQUAL(4_KB, d.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(93_GB / 4_KB, d.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(94_GB / 4_KB, d.GetUnadjustedBlockCount());
            }
        });

        // reject agent

        NProto::TAgentConfig agentToAbuse;

        {
            TVector<NProto::TDeviceConfig> devices;
            auto error = state->GetDiskDevices("foo", devices);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(4, devices.size());
            ui32 nodeIdToAbuse = 0;
            for (auto& d: devices) {
                if (d.GetNodeId() != devices[0].GetNodeId()) {
                    nodeIdToAbuse = d.GetNodeId();
                    break;
                }
            }
            UNIT_ASSERT_VALUES_UNEQUAL(0, nodeIdToAbuse);
            auto* config = FindIfPtr(agents, [=] (auto& agent) {
                return agent.GetNodeId() == nodeIdToAbuse;
            });

            UNIT_ASSERT(config != nullptr);
            agentToAbuse = *config;
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            auto error = state->UpdateAgentState(
                db,
                agentToAbuse.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                TInstant::Now(),
                "test",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("foo", affectedDisks[0]);

            UNIT_ASSERT_VALUES_UNEQUAL(0, state->GetDiskStateUpdates().size());
            const auto& update = state->GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE(
                "foo",
                DISK_STATE_TEMPORARILY_UNAVAILABLE,
                update);
        });

        {
            TVector<NProto::TDeviceConfig> devices;
            auto error = state->GetDiskDevices("foo", devices);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(4, devices.size());

            for (auto& d: devices) {
                UNIT_ASSERT_VALUES_UNEQUAL(0, d.GetNodeId());
                UNIT_ASSERT_VALUES_EQUAL(4_KB, d.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(93_GB / 4_KB, d.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(94_GB / 4_KB, d.GetUnadjustedBlockCount());
            }
        }

        // replace agent with new one

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto agent = agents.back();
            agent.SetNodeId(agentToAbuse.GetNodeId());

            auto [r, error] = state->RegisterAgent(db, agent, TInstant::Now());
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());

            UNIT_ASSERT_VALUES_EQUAL(1, r.DisksToReallocate.size());
            UNIT_ASSERT_VALUES_EQUAL("foo", r.DisksToReallocate[0]);

            UNIT_ASSERT_VALUES_EQUAL(
                agent.DevicesSize(),
                state->GetDirtyDevices().size());

            for (auto& d: state->GetDirtyDevices()) {
                auto diskId = state->MarkDeviceAsClean(Now(), db, d.GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL("", diskId);
            }
        });

        {
            TVector<NProto::TDeviceConfig> devices;
            auto error = state->GetDiskDevices("foo", devices);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(4, devices.size());

            for (auto& d: devices) {
                if (d.GetNodeId() == 0) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        agentToAbuse.GetAgentId(),
                        d.GetAgentId());
                }
                UNIT_ASSERT_VALUES_EQUAL(4_KB, d.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(93_GB / 4_KB, d.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(94_GB / 4_KB, d.GetUnadjustedBlockCount());
            }
            UNIT_ASSERT_VALUES_EQUAL(2, CountIf(devices, [] (auto& d) {
                return d.GetNodeId() == 0;
            }));
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result {};
            auto error = state->AllocateDisk(
                TInstant::Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "foo",
                    .PlacementGroupId = {},
                    .BlockSize = 4_KB,
                    .BlocksCount = 93_GB / 4_KB * 4,
                },
                &result);

            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(4, result.Devices.size());
            for (auto& d: result.Devices) {
                if (d.GetNodeId() == 0) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        agentToAbuse.GetAgentId(),
                        d.GetAgentId());
                }
                UNIT_ASSERT_VALUES_EQUAL(4_KB, d.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(93_GB / 4_KB, d.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(94_GB / 4_KB, d.GetUnadjustedBlockCount());
            }
            UNIT_ASSERT_VALUES_EQUAL(2, CountIf(result.Devices, [] (auto& d) {
                return d.GetNodeId() == 0;
            }));
        });

        // restore from DB
        state.reset();

        auto monitoring = CreateMonitoringServiceStub();
        auto rootGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto serverGroup = rootGroup->GetSubgroup("component", "server");
        InitCriticalEventsCounter(serverGroup);

        auto criticalEvents = serverGroup->FindCounter(
            "AppCriticalEvents/DiskRegistryDeviceNotFound");

        UNIT_ASSERT_VALUES_EQUAL(0, criticalEvents->Val());

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            NProto::TDiskRegistryConfig config;
            UNIT_ASSERT(db.ReadDiskRegistryConfig(config));

            TVector<NProto::TAgentConfig> agents;
            UNIT_ASSERT(db.ReadAgents(agents));
            UNIT_ASSERT_VALUES_EQUAL(3, agents.size());
            Sort(agents, TByAgentId());

            if (agentToAbuse.GetAgentId() == "agent-1") {
                UNIT_ASSERT_VALUES_EQUAL("agent-1", agents[0].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(0, agents[0].GetNodeId());

                UNIT_ASSERT_VALUES_EQUAL("agent-2", agents[1].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(2, agents[1].GetNodeId());
            } else {
                UNIT_ASSERT_VALUES_EQUAL("agent-1", agents[0].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(1, agents[0].GetNodeId());

                UNIT_ASSERT_VALUES_EQUAL("agent-2", agents[1].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(0, agents[1].GetNodeId());
            }

            UNIT_ASSERT_VALUES_EQUAL("agent-3", agents[2].GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(agentToAbuse.GetNodeId(), agents[2].GetNodeId());

            TVector<NProto::TDiskConfig> disks;
            UNIT_ASSERT(db.ReadDisks(disks));
            UNIT_ASSERT_VALUES_EQUAL(1, disks.size());

            TVector<NProto::TPlacementGroupConfig> placementGroups;
            UNIT_ASSERT(db.ReadPlacementGroups(placementGroups));
            UNIT_ASSERT_VALUES_EQUAL(0, placementGroups.size());

            TVector<TBrokenDiskInfo> brokenDisks;
            UNIT_ASSERT(db.ReadBrokenDisks(brokenDisks));
            UNIT_ASSERT_VALUES_EQUAL(0, brokenDisks.size());

            TVector<TString> disksToReallocate;
            UNIT_ASSERT(db.ReadDisksToReallocate(disksToReallocate));
            UNIT_ASSERT_VALUES_EQUAL(1, disksToReallocate.size());

            TVector<TDiskStateUpdate> diskStateUpdates;
            UNIT_ASSERT(db.ReadDiskStateChanges(diskStateUpdates));
            UNIT_ASSERT_VALUES_EQUAL(1, diskStateUpdates.size());

            ui64 diskStateSeqNo = 0;
            UNIT_ASSERT(db.ReadLastDiskStateSeqNo(diskStateSeqNo));
            UNIT_ASSERT_LT(0, diskStateSeqNo);

            TVector<TDirtyDevice> dirtyDevices;
            UNIT_ASSERT(db.ReadDirtyDevices(dirtyDevices));
            UNIT_ASSERT_VALUES_EQUAL(0, dirtyDevices.size());

            TVector<TString> disksToCleanup;
            UNIT_ASSERT(db.ReadDisksToCleanup(disksToCleanup));
            UNIT_ASSERT_VALUES_EQUAL(0, disksToCleanup.size());

            TVector<TString> errorNotifications;
            UNIT_ASSERT(db.ReadErrorNotifications(errorNotifications));
            UNIT_ASSERT_VALUES_EQUAL(1, errorNotifications.size());
            UNIT_ASSERT_VALUES_EQUAL("foo", errorNotifications[0]);

            TVector<NProto::TUserNotification> userNotifications;
            UNIT_ASSERT(db.ReadUserNotifications(userNotifications));
            UNIT_ASSERT_VALUES_EQUAL(1, userNotifications.size());
            UNIT_ASSERT(userNotifications[0].HasDiskError());
            UNIT_ASSERT_VALUES_EQUAL(
                "foo",
                userNotifications[0].GetDiskError().GetDiskId());
            UNIT_ASSERT(userNotifications[0].GetHasLegacyCopy());

            TVector<TString> outdatedVolumeConfigs;
            UNIT_ASSERT(db.ReadOutdatedVolumeConfigs(outdatedVolumeConfigs));
            UNIT_ASSERT_VALUES_EQUAL(0, outdatedVolumeConfigs.size());

            TVector<NProto::TSuspendedDevice> suspendedDevices;
            UNIT_ASSERT(db.ReadSuspendedDevices(suspendedDevices));
            UNIT_ASSERT_VALUES_EQUAL(0, suspendedDevices.size());

            TDeque<TAutomaticallyReplacedDeviceInfo> automaticallyReplacedDevices;
            UNIT_ASSERT(db.ReadAutomaticallyReplacedDevices(
                automaticallyReplacedDevices));
            UNIT_ASSERT_VALUES_EQUAL(0, automaticallyReplacedDevices.size());

            THashMap<TString, NProto::TDiskRegistryAgentParams> diskRegistryAgentListParams;
            UNIT_ASSERT(db.ReadDiskRegistryAgentListParams(
                diskRegistryAgentListParams));
            UNIT_ASSERT_VALUES_EQUAL(0, diskRegistryAgentListParams.size());

            state.emplace(TDiskRegistryState {
                CreateLoggingService("console"),
                CreateStorageConfig([] {
                    auto proto = CreateDefaultStorageConfigProto();
                    proto.SetAllocationUnitNonReplicatedSSD(93);
                    return proto;
                }()),
                rootGroup,
                std::move(config),
                std::move(agents),
                std::move(disks),
                std::move(placementGroups),
                std::move(brokenDisks),
                std::move(disksToReallocate),
                std::move(diskStateUpdates),
                diskStateSeqNo,
                std::move(dirtyDevices),
                std::move(disksToCleanup),
                std::move(errorNotifications),
                std::move(userNotifications),
                std::move(outdatedVolumeConfigs),
                std::move(suspendedDevices),
                std::move(automaticallyReplacedDevices),
                std::move(diskRegistryAgentListParams)
            });
        });

        UNIT_ASSERT_VALUES_EQUAL(0, criticalEvents->Val());

        // return agent

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            agentToAbuse.SetNodeId(100);

            auto [r, error] =
                state->RegisterAgent(db, agentToAbuse, TInstant::Now());

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, r.AffectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, r.DisksToReallocate.size());

            UNIT_ASSERT_VALUES_EQUAL("foo", r.AffectedDisks[0]);

            UNIT_ASSERT_VALUES_UNEQUAL(0, state->GetDiskStateUpdates().size());
            const auto& update = state->GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("foo", DISK_STATE_WARNING, update);

            UNIT_ASSERT_VALUES_EQUAL(0, state->GetDirtyDevices().size());
        });

        {
            TVector<NProto::TDeviceConfig> devices;
            auto error = state->GetDiskDevices("foo", devices);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(4, devices.size());

            for (auto& d: devices) {
                if (d.GetAgentId() == agentToAbuse.GetAgentId()) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        agentToAbuse.GetNodeId(),
                        d.GetNodeId());
                }
                UNIT_ASSERT_VALUES_UNEQUAL(0, d.GetNodeId());
                UNIT_ASSERT_VALUES_EQUAL(4_KB, d.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(93_GB / 4_KB, d.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(94_GB / 4_KB, d.GetUnadjustedBlockCount());
            }
            UNIT_ASSERT_VALUES_EQUAL(0, CountIf(devices, [] (auto& d) {
                return d.GetNodeId() == 0;
            }));
        }

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> errorNotifications;
            UNIT_ASSERT(db.ReadErrorNotifications(errorNotifications));
            UNIT_ASSERT_VALUES_EQUAL(1, errorNotifications.size());
            UNIT_ASSERT_VALUES_EQUAL("foo", errorNotifications[0]);

            TVector<NProto::TUserNotification> userNotifications;
            UNIT_ASSERT(db.ReadUserNotifications(userNotifications));
            UNIT_ASSERT_VALUES_EQUAL(2, userNotifications.size());

            UNIT_ASSERT(userNotifications[0].HasDiskError());
            UNIT_ASSERT_VALUES_EQUAL(
                "foo",
                userNotifications[0].GetDiskError().GetDiskId());
            UNIT_ASSERT(userNotifications[0].GetHasLegacyCopy());

            UNIT_ASSERT(userNotifications[1].HasDiskBackOnline());
            UNIT_ASSERT_VALUES_EQUAL(
                "foo",
                userNotifications[1].GetDiskBackOnline().GetDiskId());
            UNIT_ASSERT(!userNotifications[1].GetHasLegacyCopy());
        });
    }

    Y_UNIT_TEST(ShouldUpdateMediaKind)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1"),
                Device("dev-2", "uuid-1.2"),
                Device("dev-3", "uuid-1.3"),
                Device("dev-4", "uuid-1.4"),
                Device("dev-5", "uuid-1.5"),
                Device("dev-6", "uuid-1.6"),
                Device("dev-7", "uuid-1.7"),
            }),
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithDisks([] {
                TVector disks {
                    Disk("local", {"uuid-1.1"}),

                    Disk("m2",   {}),
                    Disk("m2/0", {"uuid-1.2"}),
                    Disk("m2/1", {"uuid-1.3"}),
                    Disk("m2/2", {"uuid-1.4"}),

                    Disk("m3",   {}),
                    Disk("m3/0", {"uuid-1.3"}),
                    Disk("m3/1", {"uuid-1.4"}),
                    Disk("m3/2", {"uuid-1.5"}),
                    Disk("m3/3", {"uuid-1.6"}),

                    Disk("nrd", {"uuid-1.7"}),
                };

                disks[0].SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);
                disks[1].SetReplicaCount(1);
                disks[5].SetReplicaCount(2);

                return disks;
            }())
            .Build();

        {
            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("local", info));
            UNIT_ASSERT_EQUAL(NProto::STORAGE_MEDIA_SSD_LOCAL, info.MediaKind);
        }

        {
            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("m2", info));
            UNIT_ASSERT_EQUAL(NProto::STORAGE_MEDIA_SSD_MIRROR2, info.MediaKind);
        }

        {
            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("m3", info));
            UNIT_ASSERT_EQUAL(NProto::STORAGE_MEDIA_SSD_MIRROR3, info.MediaKind);
        }

        {
            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("nrd", info));
            UNIT_ASSERT_EQUAL(
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                info.MediaKind);
        }
    }

    Y_UNIT_TEST(ShouldUpdateMTBF)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        NProto::TAgentConfig agent;
        agent.SetNodeId(42);
        agent.SetAgentId("agent-1");
        agent.SetWorkTs(TInstant::Days(10).Seconds());
        *agent.AddDevices() = Device("dev-1", "uuid-1.1", "rack-1");

        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");
        auto agentCounters = diskRegistryGroup
            ->GetSubgroup("agent", agent.GetAgentId());

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(diskRegistryGroup)
            .WithKnownAgents({agent})
            .Build();

        auto changeAgentState = [&] (const auto& newState, const auto& ts) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) {
                TVector<TString> affectedDisks;
                auto error = state.UpdateAgentState(
                    db,
                    agent.GetAgentId(),
                    newState,
                    ts,
                    "test",
                    affectedDisks);
            });
        };

        NProto::TAgentStats stats;
        stats.SetNodeId(agent.GetNodeId());

        auto testCounter = [&] (const auto& ts) {
            state.UpdateAgentCounters(stats);
            UNIT_ASSERT_VALUES_EQUAL(
                agentCounters->GetCounter("MeanTimeBetweenFailures")->Val(),
                ts.Seconds());
        };

        changeAgentState(NProto::AGENT_STATE_UNAVAILABLE, TInstant::Days(12));
        testCounter(TInstant::Days(2));

        state.PublishCounters(TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            diskRegistryGroup->GetCounter("MeanTimeBetweenFailures")->Val(),
            TInstant::Days(2).Seconds());


        changeAgentState(NProto::AGENT_STATE_WARNING, TInstant::Days(13));
        testCounter(TInstant::Days(2));

        changeAgentState(NProto::AGENT_STATE_UNAVAILABLE, TInstant::Days(23));
        testCounter(TInstant::Days(2));

        changeAgentState(NProto::AGENT_STATE_UNAVAILABLE, TInstant::Days(40));
        testCounter(TInstant::Days(2));

        changeAgentState(NProto::AGENT_STATE_ONLINE, TInstant::Days(50));
        testCounter(TInstant::Days(2));

        changeAgentState(NProto::AGENT_STATE_WARNING, TInstant::Days(60));
        testCounter(TInstant::Days(2));

        changeAgentState(NProto::AGENT_STATE_ONLINE, TInstant::Days(70));
        testCounter(TInstant::Days(2));

        changeAgentState(NProto::AGENT_STATE_UNAVAILABLE, TInstant::Days(80));
        testCounter(TInstant::Days(16));
    }

    Y_UNIT_TEST(ShouldSuppressAddingRepeatingUserNotificationDuplicates)
    {
        NDiskRegistry::TNotificationSystem state(
            CreateStorageConfig(CreateDefaultStorageConfigProto()),
            {},
            {},
            {},
            {},
            0,
            {});

        TTestExecutor executor;
        executor.WriteTx([] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TString diskId = "disk";

        executor.WriteTx([&state, &diskId] (TDiskRegistryDatabase db) {
            NProto::TUserNotification error;
            error.MutableDiskError()->SetDiskId(diskId);

            NProto::TUserNotification online;
            online.MutableDiskBackOnline()->SetDiskId(diskId);

            ui64 seqNo = 0;
            auto add = [&state, &db, &seqNo] (auto notif) {
                notif.SetSeqNo(++seqNo);
                state.AddUserNotification(db, std::move(notif));
            };

            state.AllowNotifications(diskId);

            add(error);
            add(error);
            add(error);

            add(online);
            add(online);
            add(online);
            add(online);

            add(error);

            add(online);
            add(online);
        });

        TVector<NProto::TUserNotification> userNotifications;
        state.GetUserNotifications(userNotifications);
        // Account for possible future implementation changes
        SortBy(userNotifications, [] (const auto& notif) {
            return notif.GetSeqNo();
        });

        UNIT_ASSERT_VALUES_EQUAL(4, userNotifications.size());
        UNIT_ASSERT(userNotifications[0].HasDiskError());
        UNIT_ASSERT(userNotifications[1].HasDiskBackOnline());
        UNIT_ASSERT(userNotifications[2].HasDiskError());
        UNIT_ASSERT(userNotifications[3].HasDiskBackOnline());

        executor.ReadTx([&diskId] (TDiskRegistryDatabase db) {
            TVector<TString> errorNotifications;
            UNIT_ASSERT(db.ReadErrorNotifications(errorNotifications));
            UNIT_ASSERT_VALUES_EQUAL(1, errorNotifications.size());
            UNIT_ASSERT_VALUES_EQUAL(diskId, errorNotifications[0]);

            TVector<NProto::TUserNotification> userNotifications;
            UNIT_ASSERT(db.ReadUserNotifications(userNotifications));
            UNIT_ASSERT_VALUES_EQUAL(4, userNotifications.size());

            auto count = CountIf(userNotifications, [] (const auto& notif) {
                return notif.HasDiskError() && notif.GetHasLegacyCopy();
            });
            UNIT_ASSERT_VALUES_EQUAL(2, count);
        });
    }

    Y_UNIT_TEST(ShouldPullInLegacyDiskErrorUserNotifications)
    {
        auto state = TDiskRegistryStateBuilder()
            .WithErrorNotifications({"disk0", "disk1", "disk2"})
            .Build();

        UNIT_ASSERT_VALUES_EQUAL(3, state.GetUserNotifications().Count);

        TVector<NProto::TUserNotification> userNotifications;
        state.GetUserNotifications(userNotifications);

        SortBy(userNotifications, [] (const auto& notif) -> decltype(auto) {
            return notif.GetDiskError().GetDiskId();
        });

        UNIT_ASSERT_VALUES_EQUAL(3, userNotifications.size());
        UNIT_ASSERT_VALUES_EQUAL(
                "disk0",
                userNotifications[0].GetDiskError().GetDiskId());
        UNIT_ASSERT(userNotifications[0].GetHasLegacyCopy());
        UNIT_ASSERT_VALUES_EQUAL(
                "disk1",
                userNotifications[1].GetDiskError().GetDiskId());
        UNIT_ASSERT(userNotifications[1].GetHasLegacyCopy());
        UNIT_ASSERT_VALUES_EQUAL(
                "disk2",
                userNotifications[2].GetDiskError().GetDiskId());
        UNIT_ASSERT(userNotifications[2].GetHasLegacyCopy());
    }

    Y_UNIT_TEST(ReplaceSpecificDevice)
    {
        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1"),
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-2.1", "rack-2"),
            Device("dev-2", "uuid-2.2", "rack-2"),
            Device("dev-3", "uuid-2.3", "rack-2"),
            Device("dev-4", "uuid-2.4", "rack-2"),
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({ agent1, agent2 })
            .WithDisks({ Disk("disk-1", { "uuid-1.1", "uuid-2.1" }) })
            .Build();

        auto moveDeviceToErrorState = [&] (auto deviceId, auto seqNum) {
            executor.WriteTx([&](TDiskRegistryDatabase db) mutable {
                TString affectedDisk;
                const auto error = state.UpdateDeviceState(
                    db,
                    deviceId,
                    NProto::DEVICE_STATE_ERROR,
                    Now(),
                    "test",
                    affectedDisk);

                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);

                UNIT_ASSERT_VALUES_EQUAL(seqNum+1, state.GetDiskStateUpdates().size());
                const auto &update = state.GetDiskStateUpdates().back();

                UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_ERROR, update);
                UNIT_ASSERT_VALUES_EQUAL(seqNum, update.SeqNo);
            });
        };

        auto replaceDevice = [&](
            auto fromDevId,
            auto toDevId,
            auto manual,
            auto seqNum)
        {
            executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
                bool updated = false;
                const auto error = state.ReplaceDevice(
                    db,
                    "disk-1",
                    fromDevId,
                    toDevId,
                    Now(),
                    "",     // message
                    manual,
                    &updated);

                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                UNIT_ASSERT(updated);

                UNIT_ASSERT_VALUES_EQUAL(
                    seqNum + 1,
                    state.GetDiskStateUpdates().size());
                const auto& update = state.GetDiskStateUpdates().back();

                UNIT_ASSERT_VALUES_EQUAL("disk-1", update.State.GetDiskId());
                UNIT_ASSERT_EQUAL(
                    NProto::DISK_STATE_ONLINE,
                    update.State.GetState());
                UNIT_ASSERT_VALUES_EQUAL(seqNum, update.SeqNo);
            });
        };

        auto checkDevices = [&](
            TVector<TString> diskDevices,
            TVector<TString> dirtyDevices,
            TVector<TString> autoReplacedDevices)
        {
            TVector<TDeviceConfig> devices;
            const auto error = state.GetDiskDevices("disk-1", devices);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(diskDevices.size(), devices.size());
            Sort(devices, TByDeviceUUID());
            for (ui32 i=0; i < diskDevices.size(); i++) {
                UNIT_ASSERT_EQUAL(
                    NProto::DEVICE_STATE_ONLINE,
                    devices[i].GetState());
                UNIT_ASSERT_VALUES_EQUAL(
                    diskDevices[i],
                    devices[i].GetDeviceUUID());
            }

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", info));
            // device replacement list should be empty for non-mirrored disks
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, info.DeviceReplacementIds);

            auto stateDirtyDevices = state.GetDirtyDevices();
            UNIT_ASSERT_VALUES_EQUAL(
                dirtyDevices.size(),
                stateDirtyDevices.size());
            Sort(stateDirtyDevices, TByDeviceUUID());
            for (ui32 i = 0; i < dirtyDevices.size(); i++) {
                UNIT_ASSERT_EQUAL(
                    NProto::DEVICE_STATE_ERROR,
                    stateDirtyDevices[i].GetState());
                UNIT_ASSERT_VALUES_EQUAL(
                    dirtyDevices[i],
                    stateDirtyDevices[i].GetDeviceUUID());
            }

            TVector<TAutomaticallyReplacedDeviceInfo> stateAutoReplacedDevices(
                state.GetAutomaticallyReplacedDevices().begin(),
                state.GetAutomaticallyReplacedDevices().end());
            UNIT_ASSERT_VALUES_EQUAL(
                autoReplacedDevices.size(),
                stateAutoReplacedDevices.size());
            SortBy(stateAutoReplacedDevices, [] (auto& x) { return x.DeviceId; });
            for (ui32 i = 0; i < autoReplacedDevices.size(); i++) {
                UNIT_ASSERT_VALUES_EQUAL(
                    autoReplacedDevices[i],
                    stateAutoReplacedDevices[i].DeviceId);
            }
        };

        // replace device with replacement from free list
        moveDeviceToErrorState("uuid-2.1", 0);
        replaceDevice("uuid-2.1", "uuid-2.4", true /* manual */, 1);
        // replaced device is now dirty
        checkDevices({"uuid-1.1", "uuid-2.4"}, {"uuid-2.1"}, {});

        // replace device with replacement from dirty list
        moveDeviceToErrorState("uuid-2.4", 2);
        replaceDevice("uuid-2.4", "uuid-2.1", true /* manual */, 3);
        // make sure uuid-2.1 is not considered dirty anymore
        checkDevices({"uuid-1.1", "uuid-2.1"}, {"uuid-2.4"}, {});

        // replace device automatically
        moveDeviceToErrorState("uuid-2.1", 4);
        replaceDevice("uuid-2.1", "", false /* not manual */, 5);
        // make sure uuid-2.1 is in dirty and automatically replaced devices list
        checkDevices({"uuid-1.1", "uuid-2.2"}, {"uuid-2.1", "uuid-2.4"}, {"uuid-2.1"});

        // replace device with replacement from automatically replaced list
        moveDeviceToErrorState("uuid-1.1", 6);
        replaceDevice("uuid-1.1", "uuid-2.1", true /* manual */, 7);
        // make sure uuid-2.1 is not considered dirty or automatically replaced anymore
        checkDevices({"uuid-2.1", "uuid-2.2"}, {"uuid-1.1", "uuid-2.4"}, {});
    }

    Y_UNIT_TEST(ReplaceSpecificDeviceOnPreviouslyUsedOne)
    {
        const auto agent1 = AgentConfig(1, {
            Device("dev-1", "uuid-1.1", "rack-1"),
            Device("dev-1", "uuid-1.2", "rack-1"),
            Device("dev-1", "uuid-1.3", "rack-1"),
            Device("dev-1", "uuid-1.4", "rack-1"),
        });

        const auto agent2 = AgentConfig(2, {
            Device("dev-1", "uuid-2.1", "rack-2"),
            Device("dev-2", "uuid-2.2", "rack-2"),
            Device("dev-3", "uuid-2.3", "rack-2"),
            Device("dev-4", "uuid-2.4", "rack-2"),
        });

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state =
            TDiskRegistryStateBuilder()
                .WithKnownAgents({agent1, agent2})
                .WithDisks(
                    {Disk("disk-1", {"uuid-1.1", "uuid-1.2"}),
                     Disk("disk-2", {"uuid-2.1", "uuid-2.2"})})
                .Build();

        auto moveDeviceToState =
            [&](TString deviceId, NProto::EDeviceState deviceState)
        {
            executor.WriteTx(
                [&](TDiskRegistryDatabase db) mutable
                {
                    TString affectedDisk;
                    const auto error = state.UpdateDeviceState(
                        db,
                        deviceId,
                        deviceState,
                        Now(),
                        "test",
                        affectedDisk);
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                });
        };

        auto replaceDevice = [&](
            TString diskId,
            auto fromDevId,
            auto toDevId,
            auto manual)
        {
            executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
                bool updated = false;
                const auto error = state.ReplaceDevice(
                    db,
                    diskId,
                    fromDevId,
                    toDevId,
                    Now(),
                    "",     // message
                    manual,
                    &updated);

                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

                const auto& update = state.GetDiskStateUpdates().back();

                UNIT_ASSERT_VALUES_EQUAL(diskId, update.State.GetDiskId());
                UNIT_ASSERT_EQUAL(
                    NProto::DISK_STATE_ONLINE,
                    update.State.GetState());
            });
        };

        auto checkDevices = [&](
            TString diskId,
            TVector<TString> diskDevices,
            TVector<TString> dirtyDevices,
            TVector<TString> autoReplacedDevices)
        {
            TVector<TDeviceConfig> devices;
            const auto error = state.GetDiskDevices(diskId, devices);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(diskDevices.size(), devices.size());
            Sort(devices, TByDeviceUUID());
            for (ui32 i=0; i < diskDevices.size(); i++) {
                UNIT_ASSERT_EQUAL(
                    NProto::DEVICE_STATE_ONLINE,
                    devices[i].GetState());
                UNIT_ASSERT_VALUES_EQUAL(
                    diskDevices[i],
                    devices[i].GetDeviceUUID());
            }

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo(diskId, info));
            // device replacement list should be empty for non-mirrored disks
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, info.DeviceReplacementIds);

            auto stateDirtyDevices = state.GetDirtyDevices();
            UNIT_ASSERT_VALUES_EQUAL(
                dirtyDevices.size(),
                stateDirtyDevices.size());
            Sort(stateDirtyDevices, TByDeviceUUID());
            for (ui32 i = 0; i < dirtyDevices.size(); i++) {
                UNIT_ASSERT_EQUAL(
                    NProto::DEVICE_STATE_ERROR,
                    stateDirtyDevices[i].GetState());
                UNIT_ASSERT_VALUES_EQUAL(
                    dirtyDevices[i],
                    stateDirtyDevices[i].GetDeviceUUID());
            }

            TVector<TAutomaticallyReplacedDeviceInfo> stateAutoReplacedDevices(
                state.GetAutomaticallyReplacedDevices().begin(),
                state.GetAutomaticallyReplacedDevices().end());
            UNIT_ASSERT_VALUES_EQUAL(
                autoReplacedDevices.size(),
                stateAutoReplacedDevices.size());
            SortBy(stateAutoReplacedDevices, [] (auto& x) { return x.DeviceId; });
            for (ui32 i = 0; i < autoReplacedDevices.size(); i++) {
                UNIT_ASSERT_VALUES_EQUAL(
                    autoReplacedDevices[i],
                    stateAutoReplacedDevices[i].DeviceId);
            }
        };

        // Break and replace "uuid-1.1" in disk-1
        moveDeviceToState("uuid-1.1", NProto::DEVICE_STATE_ERROR);
        replaceDevice("disk-1", "uuid-1.1", "uuid-1.3", false /* manual */);
        checkDevices(
            "disk-1",
            {"uuid-1.2", "uuid-1.3"},
            {"uuid-1.1"},
            {"uuid-1.1"});

        // Break and replace "uuid-2.1" in disk-2
        moveDeviceToState("uuid-2.1", NProto::DEVICE_STATE_ERROR);
        replaceDevice("disk-2", "uuid-2.1", "uuid-2.3", true /* manual */);
        checkDevices(
            "disk-2",
            {"uuid-2.2", "uuid-2.3"},
            {"uuid-1.1", "uuid-2.1"},
            {"uuid-1.1"});

        // Break "uuid-2.2" in disk-2
        moveDeviceToState("uuid-2.2", NProto::DEVICE_STATE_ERROR);
        // Enable "uuid-1.1" and try to replace broken "uuid-2.2" to it.
        moveDeviceToState("uuid-1.1", NProto::DEVICE_STATE_ONLINE);
        // Automatic replacement should return E_ARGUMENT.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                bool updated = false;
                const auto error = state.ReplaceDevice(
                    db,
                    "disk-2",
                    "uuid-2.2",
                    "uuid-1.1",
                    Now(),
                    "test",   // message
                    false,    // manual
                    &updated);
                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            });
        // But the manual process works just fine.
        replaceDevice("disk-2", "uuid-2.2", "uuid-1.1", true /* manual */);
        checkDevices(
            "disk-2",
            {"uuid-1.1", "uuid-2.3"},
            {"uuid-2.1", "uuid-2.2"},
            {});
    }

    Y_UNIT_TEST(ShouldAddToPendingCleanupOnlyWhenNecessary)
    {
        const auto agent1 = AgentConfig(
            1,
            {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-1", "uuid-1.2", "rack-1"),
                Device("dev-1", "uuid-1.3", "rack-1"),
                Device("dev-1", "uuid-1.4", "rack-1"),
            });

        const auto agent2 = AgentConfig(
            2,
            {
                Device("dev-1", "uuid-2.1", "rack-2"),
                Device("dev-2", "uuid-2.2", "rack-2"),
                Device("dev-3", "uuid-2.3", "rack-2"),
                Device("dev-4", "uuid-2.4", "rack-2"),
            });

        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        TDiskRegistryState state =
            TDiskRegistryStateBuilder()
                .WithKnownAgents({agent1, agent2})
                .WithDisks(
                    {Disk("disk-1", {"uuid-1.1", "uuid-1.2"}),
                     Disk("disk-2", {"uuid-2.1", "uuid-2.2"})})
                .Build();

        auto moveDeviceToState =
            [&](TString deviceId, NProto::EDeviceState deviceState)
        {
            executor.WriteTx(
                [&](TDiskRegistryDatabase db) mutable
                {
                    TString affectedDisk;
                    const auto error = state.UpdateDeviceState(
                        db,
                        deviceId,
                        deviceState,
                        Now(),
                        "test",
                        affectedDisk);
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                });
        };

        auto replaceDevice =
            [&](TString diskId, auto fromDevId, auto toDevId, auto manual)
        {
            executor.WriteTx(
                [&](TDiskRegistryDatabase db) mutable
                {
                    bool updated = false;
                    const auto error = state.ReplaceDevice(
                        db,
                        diskId,
                        fromDevId,
                        toDevId,
                        Now(),
                        "",   // message
                        manual,
                        &updated);
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                });
        };

        auto checkDevices = [&](TString diskId,
                                TVector<TString> diskDevices,
                                TVector<TString> dirtyDevices,
                                TVector<TString> autoReplacedDevices)
        {
            TVector<TDeviceConfig> devices;
            const auto error = state.GetDiskDevices(diskId, devices);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(diskDevices.size(), devices.size());
            for (ui32 i = 0; i < diskDevices.size(); i++) {
                UNIT_ASSERT_EQUAL(
                    NProto::DEVICE_STATE_ONLINE,
                    devices[i].GetState());
                UNIT_ASSERT_VALUES_EQUAL(
                    diskDevices[i],
                    devices[i].GetDeviceUUID());
            }

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo(diskId, info));
            // device replacement list should be empty for non-mirrored disks
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, info.DeviceReplacementIds);

            auto stateDirtyDevices = state.GetDirtyDevices();
            UNIT_ASSERT_VALUES_EQUAL(
                dirtyDevices.size(),
                stateDirtyDevices.size());
            Sort(stateDirtyDevices, TByDeviceUUID());
            for (ui32 i = 0; i < dirtyDevices.size(); i++) {
                UNIT_ASSERT_VALUES_EQUAL(
                    dirtyDevices[i],
                    stateDirtyDevices[i].GetDeviceUUID());
            }

            TVector<TAutomaticallyReplacedDeviceInfo> stateAutoReplacedDevices(
                state.GetAutomaticallyReplacedDevices().begin(),
                state.GetAutomaticallyReplacedDevices().end());
            UNIT_ASSERT_VALUES_EQUAL(
                autoReplacedDevices.size(),
                stateAutoReplacedDevices.size());
            SortBy(
                stateAutoReplacedDevices,
                [](auto& x) { return x.DeviceId; });
            for (ui32 i = 0; i < autoReplacedDevices.size(); i++) {
                UNIT_ASSERT_VALUES_EQUAL(
                    autoReplacedDevices[i],
                    stateAutoReplacedDevices[i].DeviceId);
            }
        };

        // Automatic replacements shouldn't be be added to pending cleanup.
        replaceDevice("disk-1", "uuid-1.1", "uuid-1.3", false /* manual */);
        checkDevices(
            "disk-1",
            {"uuid-1.3", "uuid-1.2"},
            {"uuid-1.1"},
            {"uuid-1.1"});
        // Device is broken. The pending cleanup is empty.
        UNIT_ASSERT(!state.HasPendingCleanup("disk-1"));

        // Manual replacements also does not add to pending cleanup since the
        // source device state is changing to ERROR.
        replaceDevice("disk-1", "uuid-1.2", "uuid-1.4", true /* manual */);
        UNIT_ASSERT(!state.HasPendingCleanup("disk-1"));
        checkDevices(
            "disk-1",
            {"uuid-1.3", "uuid-1.4"},
            {"uuid-1.1", "uuid-1.2"},
            {"uuid-1.1"});

        moveDeviceToState("uuid-2.1", NProto::DEVICE_STATE_WARNING);

        // Start a migration.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto migrations = state.BuildMigrationList();
                UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());

                UNIT_ASSERT_VALUES_EQUAL("disk-2", migrations[0].DiskId);
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-2.1",
                    migrations[0].SourceDeviceId);

                auto r =
                    state.StartDeviceMigration(Now(), db, "disk-2", "uuid-2.1");
                UNIT_ASSERT_SUCCESS(r.GetError());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-2.3",
                    r.GetResult().GetDeviceUUID());
            });
        UNIT_ASSERT(!state.HasPendingCleanup("disk-2"));

        // Finish the migration.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                bool updated = false;
                const auto error = state.FinishDeviceMigration(
                    db,
                    "disk-2",
                    "uuid-2.1",
                    "uuid-2.3",
                    Now(),
                    &updated);

                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                UNIT_ASSERT(updated);
            });
        UNIT_ASSERT(!state.HasPendingCleanup("disk-2"));

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto disksToReallocate = state.GetDisksToReallocate();
                UNIT_ASSERT_VALUES_EQUAL(2, disksToReallocate.size());
                for (auto& [diskId, seqNo]: disksToReallocate) {
                    state.DeleteDiskToReallocate(db, diskId, seqNo);
                }
            });
        UNIT_ASSERT(state.HasPendingCleanup("disk-2"));
    }

    Y_UNIT_TEST(ShouldPreserveDeviceErrorState)
    {
        const TString errorMessage = "broken device";

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig = AgentConfig(1000, {
            Device("dev-1", "uuid-1", "rack-1")
        });

        auto state = TDiskRegistryStateBuilder()
            .WithConfig({agentConfig})
            .Build();

        // Register new agent with one device.
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            const TInstant now = Now();
            UNIT_ASSERT_SUCCESS(
                state.RegisterAgent(db, agentConfig, now).GetError());

            auto d = state.GetDevice("uuid-1");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ONLINE, d.GetState());
            UNIT_ASSERT_VALUES_EQUAL(now.MicroSeconds(), d.GetStateTs());
            UNIT_ASSERT_VALUES_EQUAL("", d.GetStateMessage());
        });

        // Break the device.
        agentConfig.MutableDevices(0)->SetState(NProto::DEVICE_STATE_ERROR);
        agentConfig.MutableDevices(0)->SetStateMessage(errorMessage);

        const TInstant errorTs = Now();

        // Register the agent with the broken device.
        // Now we expect to see our device in an error state.
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(
                state.RegisterAgent(db, agentConfig, errorTs).GetError());

            auto d = state.GetDevice("uuid-1");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ERROR, d.GetState());
            UNIT_ASSERT_VALUES_EQUAL(errorTs.MicroSeconds(), d.GetStateTs());
            UNIT_ASSERT_VALUES_EQUAL(errorMessage, d.GetStateMessage());
        });

        // Fix the device
        agentConfig.MutableDevices(0)->SetState(NProto::DEVICE_STATE_ONLINE);
        agentConfig.MutableDevices(0)->SetStateMessage("");

        // Register the agent with fixed device.
        // But we expect that the device state remains the same (error).
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(
                state.RegisterAgent(db, agentConfig, Now()).GetError());

            auto d = state.GetDevice("uuid-1");
            UNIT_ASSERT_EQUAL(NProto::DEVICE_STATE_ERROR, d.GetState());
            UNIT_ASSERT_VALUES_EQUAL(errorTs.MicroSeconds(), d.GetStateTs());
            UNIT_ASSERT_VALUES_EQUAL(errorMessage, d.GetStateMessage());
        });
    }

    Y_UNIT_TEST(ShouldCleanMultipleDevicesFromOneDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const auto agent1 = AgentConfig(
            1,
            {Device("dev-1", "uuid-1", "rack-1")});

        const auto agent2 = AgentConfig(
            2,
            {Device("dev-2", "uuid-2", "rack-1")});

        TDiskRegistryState state =
            TDiskRegistryStateBuilder()
                .WithKnownAgents({agent1, agent2})
                .WithDisks({Disk("disk-1", {"uuid-1", "uuid-2"})})
                .WithDirtyDevices(
                    {TDirtyDevice{"uuid-1", "disk-1"},
                     TDirtyDevice{"uuid-2", "disk-1"}})
                .Build();

        UNIT_ASSERT_EQUAL(state.GetDirtyDevices().size(), 2);

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto& cleanDisks = state.MarkDevicesAsClean(
                    Now(),
                    db,
                    TVector<TString>{"uuid-1", "uuid-2"});
                UNIT_ASSERT_EQUAL(cleanDisks.size(), 1);
                UNIT_ASSERT_EQUAL(cleanDisks[0], "disk-1");
            });

        UNIT_ASSERT(state.GetDirtyDevices().empty());
    }

    Y_UNIT_TEST(ShouldCleanMultipleDevicesFromDifferentDisks)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const auto agent1 = AgentConfig(
            1,
            {Device("dev-1", "uuid-1", "rack-1")});

        const auto agent2 = AgentConfig(
            2,
            {Device("dev-2", "uuid-2", "rack-1")});

        TDiskRegistryState state =
            TDiskRegistryStateBuilder()
                .WithKnownAgents({agent1, agent2})
                .WithDisks(
                    {Disk("disk-1", {"uuid-1"}), Disk("disk-1", {"uuid-2"})})
                .WithDirtyDevices(
                    {TDirtyDevice{"uuid-1", "disk-1"},
                     TDirtyDevice{"uuid-2", "disk-2"}})
                .Build();

        UNIT_ASSERT_EQUAL(state.GetDirtyDevices().size(), 2);

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto& cleanDisks = state.MarkDevicesAsClean(
                    Now(),
                    db,
                    TVector<TString>{"uuid-1", "uuid-2"});
                UNIT_ASSERT_EQUAL(cleanDisks.size(), 2);
                UNIT_ASSERT_EQUAL(cleanDisks[0], "disk-1");
                UNIT_ASSERT_EQUAL(cleanDisks[1], "disk-2");
            });

        UNIT_ASSERT(state.GetDirtyDevices().empty());
    }

    Y_UNIT_TEST(ShouldCalculateDecommissionedBytesProperlyDuringMigration)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const auto agents = TVector {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1"),
                Device("dev-3", "uuid-1.3", "rack-1"),
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1", "rack-2"),
                Device("dev-2", "uuid-2.2", "rack-2"),
                Device("dev-3", "uuid-2.3", "rack-2"),
            }),
        };

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(counters)
            .WithKnownAgents(agents)
            .Build();

        TDiskRegistrySelfCounters::TDevicePoolCounters defaultPool;
        defaultPool.Init(counters->GetSubgroup("pool", "default"));

        auto freeBytes = counters->GetCounter("FreeBytes");
        auto totalBytes = counters->GetCounter("TotalBytes");
        auto decommissionedBytes = counters->GetCounter("DecommissionedBytes");
        auto allocatedBytes = counters->GetCounter("AllocatedBytes");
        auto dirtyBytes = counters->GetCounter("DirtyBytes");

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(60_GB, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, allocatedBytes->Val());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            auto error = AllocateDisk(db, state, "vol1", {}, {}, 10_GB, devices);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
        });

        TDiskInfo diskInfo;
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("vol1", diskInfo));
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
        UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, diskInfo.State);

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(50_GB, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyBytes->Val());

        // disable an agent hosting vol1
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UpdateAgentState(
                state,
                db,
                diskInfo.Devices[0].GetAgentId(),
                NProto::AGENT_STATE_WARNING);
        });

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(30_GB, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyBytes->Val());

        // start a migration
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            const auto m = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(1, m.size());

            const auto& [diskId, sourceId] = m[0];

            UNIT_ASSERT_VALUES_EQUAL("vol1", diskId);
            UNIT_ASSERT_VALUES_EQUAL(
                diskInfo.Devices[0].GetDeviceUUID(),
                sourceId);

            auto r = state.StartDeviceMigration(Now(), db, diskId, sourceId);
            UNIT_ASSERT_SUCCESS(r.GetError());
        });

        // update the info
        diskInfo = {};
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("vol1", diskInfo));
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Migrations.size());
        UNIT_ASSERT_EQUAL(NProto::DISK_STATE_WARNING, diskInfo.State);

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyBytes->Val());

        // finish the migration
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;

            const auto error = state.FinishDeviceMigration(
                db,
                "vol1",
                diskInfo.Migrations[0].GetSourceDeviceId(),
                diskInfo.Migrations[0].GetTargetDevice().GetDeviceUUID(),
                Now(),
                &updated);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(updated);
        });

        // update the info
        diskInfo = {};
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("vol1", diskInfo));
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.FinishedMigrations.size());
        UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, diskInfo.State);

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyBytes->Val());

        executor.WriteTx([&](TDiskRegistryDatabase db) mutable {
            const auto& disks = state.GetDisksToReallocate();
            UNIT_ASSERT_VALUES_EQUAL(1, disks.size());

            const auto seqNo = disks.at("vol1");
            UNIT_ASSERT_VALUES_UNEQUAL(0, seqNo);

            state.DeleteDiskToReallocate(db, "vol1", seqNo);
        });

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, dirtyBytes->Val());

        executor.WriteTx([&](TDiskRegistryDatabase db) mutable {
            state.MarkDeviceAsClean(
                Now(),
                db,
                diskInfo.FinishedMigrations[0].DeviceId);
        });

        state.PublishCounters(Now());

        UNIT_ASSERT_VALUES_EQUAL(60_GB, totalBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20_GB, freeBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(30_GB, decommissionedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10_GB, allocatedBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyBytes->Val());
    }

    Y_UNIT_TEST(ShouldCleanUnoccupiedDevicesWithNewSerialNumber)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto rootGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto serverGroup = rootGroup->GetSubgroup("component", "server");
        InitCriticalEventsCounter(serverGroup);

        auto criticalEvents = serverGroup->FindCounter(
            "AppCriticalEvents/"
            "DiskRegistryOccupiedDeviceConfigurationHasChanged");

        UNIT_ASSERT_VALUES_EQUAL(0, criticalEvents->Val());

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig = AgentConfig(1, {
            Device("NVMENBS01", "uuid-1.1"),
            Device("NVMENBS02", "uuid-1.2"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({agentConfig})
            .WithDisks({
                Disk("disk-1", {"uuid-1.2"}),
            })
            .Build();

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [r, error] = state.RegisterAgent(db, agentConfig, Now());

                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
                UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
            });

        agentConfig.MutableDevices(0)->SetSerialNumber("SN-X-1");
        agentConfig.MutableDevices(1)->SetSerialNumber("SN-Y-1");
        agentConfig.MutableDevices(0)->SetPhysicalOffset(1000);
        agentConfig.MutableDevices(1)->SetPhysicalOffset(1000);

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [r, error] = state.RegisterAgent(db, agentConfig, Now());

                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
                UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
            });

        agentConfig.MutableDevices(0)->SetSerialNumber("SN-X-2");
        agentConfig.MutableDevices(1)->SetSerialNumber("SN-Y-2");

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [r, error] = state.RegisterAgent(db, agentConfig, Now());

                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_EQUAL(1, r.AffectedDisks.size());
                UNIT_ASSERT_VALUES_EQUAL("disk-1", r.AffectedDisks[0]);
                const auto& dd = state.GetDirtyDevices();
                UNIT_ASSERT_VALUES_EQUAL(1, dd.size());
                UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", dd[0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    agentConfig.GetDevices(0).GetSerialNumber(),
                    dd[0].GetSerialNumber());
            });

        UNIT_ASSERT_VALUES_EQUAL(1, criticalEvents->Val());

        // Change the PhysicalOffset for uuid-1.2
        agentConfig.MutableDevices(1)->SetPhysicalOffset(42);

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [r, error] = state.RegisterAgent(db, agentConfig, Now());

                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
            });

        UNIT_ASSERT_VALUES_EQUAL(2, criticalEvents->Val());

        // Change the SN for uuid-1.2 once more
        agentConfig.MutableDevices(1)->SetSerialNumber("SN-Y-3");

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [r, error] = state.RegisterAgent(db, agentConfig, Now());

                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());
            });

        // Crit event shouldn't be reported
        UNIT_ASSERT_VALUES_EQUAL(2, criticalEvents->Val());
    }

    Y_UNIT_TEST(ShouldRemoveAlreadyLeakedDevices)
    {
        TTestExecutor executor;

        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const auto agent = AgentConfig(
            1,
            "agent-1",
            {
                Device("NVMENBS01", "uuid-2.1", "rack-2"),
                Device("NVMENBS02", "uuid-2.2", "rack-2"),
                Device("NVMENBS03", "uuid-2.3", "rack-2"),
            });

        const TString leakedDirtyDevice = "uuid-100.1";
        const TString leakedSuspendedDevice = "uuid-100.2";
        const TString leakedAutomaticallyReplacedDevice = "uuid-100.3";

        const TVector<TString> allLeakedDevices = {
            leakedDirtyDevice,
            leakedSuspendedDevice,
            leakedAutomaticallyReplacedDevice};

        // Add leaked devices.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                db.UpdateDirtyDevice(leakedDirtyDevice, "");
                NProto::TSuspendedDevice device;
                device.SetId(leakedSuspendedDevice);
                db.UpdateSuspendedDevice(device);
                db.AddAutomaticallyReplacedDevice(
                    {.DeviceId = leakedAutomaticallyReplacedDevice,
                     .ReplacementTs = Now()});
            });

        // Register agent.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto state = TDiskRegistryStateBuilder::LoadState(db).Build();

                auto orphanDevices = state.FindOrphanDevices();
                UNIT_ASSERT_EQUAL(static_cast<size_t>(3), orphanDevices.size());
                for (const auto& leakedDevice: allLeakedDevices) {
                    UNIT_ASSERT_UNEQUAL(
                        orphanDevices.end(),
                        Find(orphanDevices, leakedDevice));
                }

                state.RemoveOrphanDevices(db, orphanDevices);

                // Check that device cleaned up from tables.
                const auto dirtyDevicesFromState = state.GetDirtyDevices();
                UNIT_ASSERT_EQUAL(
                    dirtyDevicesFromState.end(),
                    FindIf(
                        dirtyDevicesFromState,
                        [&](const TDeviceConfig& val)
                        { return val.deviceuuid() == leakedDirtyDevice; }));

                TVector<TDirtyDevice> dirtyDevicesDb;
                db.ReadDirtyDevices(dirtyDevicesDb);
                UNIT_ASSERT_EQUAL(
                    dirtyDevicesDb.end(),
                    FindIf(
                        dirtyDevicesDb,
                        [&](const TDirtyDevice& val)
                        { return val.Id == leakedDirtyDevice; }));

                const auto suspendedDevicesFromState =
                    state.GetSuspendedDevices();

                auto deviceIdPredicateForSuspendDevices =
                    [&](const NProto::TSuspendedDevice& val)
                {
                    return val.GetId() == leakedSuspendedDevice;
                };

                UNIT_ASSERT_EQUAL(
                    suspendedDevicesFromState.end(),
                    FindIf(
                        suspendedDevicesFromState,
                        deviceIdPredicateForSuspendDevices));

                TVector<NProto::TSuspendedDevice> suspendedDevicesDb;
                db.ReadSuspendedDevices(suspendedDevicesDb);
                UNIT_ASSERT_EQUAL(
                    suspendedDevicesDb.end(),
                    FindIf(
                        suspendedDevicesDb,
                        deviceIdPredicateForSuspendDevices));

                auto deviceIdPredicateForAutomaticallyReplacedDevices =
                    [&](const TAutomaticallyReplacedDeviceInfo& val)
                {
                    return val.DeviceId == leakedAutomaticallyReplacedDevice;
                };

                const auto automaticallyReplacedDevicesFromState =
                    state.GetAutomaticallyReplacedDevices();
                UNIT_ASSERT_EQUAL(
                    automaticallyReplacedDevicesFromState.end(),
                    FindIf(
                        automaticallyReplacedDevicesFromState,
                        deviceIdPredicateForAutomaticallyReplacedDevices));

                TDeque<TAutomaticallyReplacedDeviceInfo>
                    automaticallyReplacedDevicesFromDb;
                db.ReadAutomaticallyReplacedDevices(
                    automaticallyReplacedDevicesFromDb);
                UNIT_ASSERT_EQUAL(
                    automaticallyReplacedDevicesFromDb.end(),
                    FindIf(
                        automaticallyReplacedDevicesFromDb,
                        deviceIdPredicateForAutomaticallyReplacedDevices));
            });
    }

    Y_UNIT_TEST(ShouldTellIfCanAllocateLocalDiskAfterSecureErase)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        constexpr ui64 LocalDeviceSize = 99999997952;   // ~ 93.13 GiB
        constexpr ui64 LocalDeviceDefaultLogicalBlockSize = 512; // 512 B

        auto makeLocalDevice = [](const auto* name, const auto* uuid)
        {
            auto device = Device(name, uuid) |
                   WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL) |
                   WithTotalSize(LocalDeviceSize, LocalDeviceDefaultLogicalBlockSize);
            return device;
        };

        auto agentConfig = AgentConfig(
            1,
            {
                makeLocalDevice("NVMELOCAL01", "uuid-1"),
                makeLocalDevice("NVMELOCAL02", "uuid-2"),
                makeLocalDevice("NVMELOCAL03", "uuid-3"),
            });
        const TVector agents{
            agentConfig,
        };

        TDiskRegistryState state =
            TDiskRegistryStateBuilder()
                .WithConfig(
                    [&]
                    {
                        auto config = MakeConfig(0, agents);

                        auto* pool = config.AddDevicePoolConfigs();
                        pool->SetName("local-ssd");
                        pool->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                        pool->SetAllocationUnit(LocalDeviceSize);

                        return config;
                    }())
                .WithStorageConfig(
                    []
                    {
                        auto config = CreateDefaultStorageConfigProto();
                        config.SetNonReplicatedDontSuspendDevices(true);
                        config.SetAsyncDeallocLocalDisk(true);
                        return config;
                    }())
                .WithAgents(agents)
                .WithDirtyDevices(
                    {TDirtyDevice{"uuid-2", {}},})
                .Build();

        auto allocate = [&](auto db, ui32 deviceCount)
        {
            TDiskRegistryState::TAllocateDiskResult result;

            auto error = state.AllocateDisk(
                TInstant::Zero(),
                db,
                TDiskRegistryState::TAllocateDiskParams{
                    .DiskId = "local0",
                    .BlockSize = LocalDeviceDefaultLogicalBlockSize,
                    .BlocksCount =
                        deviceCount * LocalDeviceSize / LocalDeviceDefaultLogicalBlockSize,
                    .AgentIds = {"agent-1"},
                    .PoolName = "local-ssd",
                    .MediaKind = NProto::STORAGE_MEDIA_SSD_LOCAL,
                },
                &result);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        };

        auto canAllocateLater = [&](ui32 deviceCount)
        {
            return state.CanAllocateLocalDiskAfterSecureErase(
                {"agent-1"},
                "local-ssd",
                deviceCount * LocalDeviceSize);
        };

        // Register agents.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                UNIT_ASSERT_SUCCESS(
                    RegisterAgent(state, db, agentConfig, Now()));
            });

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetConfig().KnownAgentsSize());
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetDirtyDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDevices().size());

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                // Can allocate disk of 3 devices
                UNIT_ASSERT(canAllocateLater(3));
                {
                    // Allocate 1 device
                    allocate(db, 1);
                }
                // Cannot allocate disk of 3 devices
                UNIT_ASSERT(!canAllocateLater(3));
                // Can allocate disk of 2 devices
                UNIT_ASSERT(canAllocateLater(2));
                {
                    // Suspend 1 device
                    auto error = state.SuspendDevice(db, "uuid-3");
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                }
                // Cannot allocate disk of 2 devices
                UNIT_ASSERT(!canAllocateLater(2));
                // Can allocate disk of 1 device
                UNIT_ASSERT(canAllocateLater(1));
                {
                    // Move dirty device to warning
                    TString affectedDisk;
                    const auto error = state.UpdateDeviceState(
                        db,
                        "uuid-2",
                        NProto::DEVICE_STATE_WARNING,
                        Now(),
                        "test",
                        affectedDisk);
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                }
                // Cannot allocate disk of 1 device
                UNIT_ASSERT(!canAllocateLater(1));
            });
        }
}

}   // namespace NCloud::NBlockStore::NStorage
