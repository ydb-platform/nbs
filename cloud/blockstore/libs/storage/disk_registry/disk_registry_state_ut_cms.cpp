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

Y_UNIT_TEST_SUITE(TDiskRegistryStateCMSTest)
{
    Y_UNIT_TEST(ShouldAddNewDevice)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agentConfig = AgentConfig(1, {
            Device("NVMENBS01", "uuid-1.1", "rack-1"),
            Device("NVMENBS02", "uuid-1.2", "rack-1"),
            Device("NVMENBS03", "uuid-1.3", "rack-1"),
            Device("NVMENBS04", "uuid-1.4", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetConfig().KnownAgentsSize());

            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agentConfig, Now()));

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDevices().size());

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());

            const auto& agent = state.GetAgents()[0];

            UNIT_ASSERT_VALUES_EQUAL(agentConfig.GetAgentId(), agent.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(0, agent.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(4, agent.UnknownDevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetConfig().KnownAgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetConfigVersion());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto result = state.UpdateCmsDeviceState(
                db,
                agentConfig.GetAgentId(),
                "NVMENBS01",
                NProto::DEVICE_STATE_ONLINE,
                Now(),
                false,  // shouldResumeDevice
                false); // dryRun

            UNIT_ASSERT_SUCCESS(result.Error);

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetConfig().KnownAgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetConfigVersion());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                state.GetConfig().GetKnownAgents(0).DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1.1",
                state.GetConfig().GetKnownAgents(0).GetDevices(0).GetDeviceUUID());

            ASSERT_VECTORS_EQUAL(TVector<TString>{}, result.AffectedDisks);
            UNIT_ASSERT_VALUES_EQUAL(TDuration {}, result.Timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDirtyDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDevices().size());

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());

            const auto& agent = state.GetAgents()[0];

            UNIT_ASSERT_VALUES_EQUAL(1, agent.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(3, agent.UnknownDevicesSize());
        });
    }

    Y_UNIT_TEST(ShouldRemoveDevice)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agentConfig = AgentConfig(1, {
            Device("NVMENBS01", "uuid-1.1", "rack-1"),
            Device("NVMENBS02", "uuid-1.2", "rack-1"),
            Device("NVMENBS03", "uuid-1.3", "rack-1"),
            Device("NVMENBS04", "uuid-1.4", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig({agentConfig})
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetConfigVersion());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetConfig().KnownAgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                4,
                state.GetConfig().GetKnownAgents(0).DevicesSize());

            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agentConfig, Now()));

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(4, state.GetDirtyDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());

            const auto& agent = state.GetAgents()[0];

            UNIT_ASSERT_VALUES_EQUAL(agentConfig.GetAgentId(), agent.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(0, agent.UnknownDevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(4, agent.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetConfig().KnownAgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetConfigVersion());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto result = state.UpdateCmsDeviceState(
                db,
                agentConfig.GetAgentId(),
                "NVMENBS01",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                false,  // shouldResumeDevice
                false); // dryRun

            UNIT_ASSERT_SUCCESS(result.Error);

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetConfigVersion());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetConfig().KnownAgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                4,
                state.GetConfig().GetKnownAgents(0).DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(4, state.GetDirtyDevices().size());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, result.AffectedDisks);
            UNIT_ASSERT_VALUES_EQUAL(TDuration {}, result.Timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());

            const auto& agent = state.GetAgents()[0];

            UNIT_ASSERT_VALUES_EQUAL(4, agent.DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, agent.UnknownDevicesSize());
        });
    }

    Y_UNIT_TEST(ShouldRemoveBrokenDeviceWithDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agentConfig = AgentConfig(1, {
            Device("NVMENBS01", "uuid-1.1", "rack-1"),
            Device("NVMENBS02", "uuid-1.2", "rack-1"),
            Device("NVMENBS03", "uuid-1.3", "rack-1"),
            Device("NVMENBS04", "uuid-1.4", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig({agentConfig})
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetConfigVersion());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetConfig().KnownAgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                4,
                state.GetConfig().GetKnownAgents(0).DevicesSize());

            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agentConfig, Now()));

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(4, state.GetDirtyDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());

            const auto& agent = state.GetAgents()[0];

            UNIT_ASSERT_VALUES_EQUAL(agentConfig.GetAgentId(), agent.GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(0, agent.UnknownDevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(4, agent.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetConfig().KnownAgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetConfigVersion());

            for (auto& d: agentConfig.GetDevices()) {
                state.MarkDeviceAsClean(Now(), db, d.GetDeviceUUID());
            }
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result;
            UNIT_ASSERT_SUCCESS(state.AllocateDisk(Now(), db, {
                .DiskId = "vol0",
                .BlockSize = DefaultLogicalBlockSize,
                .BlocksCount = agentConfig.DevicesSize() * DefaultDeviceSize
                    / DefaultLogicalBlockSize
            }, &result));

            UNIT_ASSERT_VALUES_EQUAL(4, result.Devices.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto result = state.UpdateCmsDeviceState(
                db,
                agentConfig.GetAgentId(),
                "NVMENBS01",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                false,  // shouldResumeDevice
                false); // dryRun

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                result.Error.GetCode(),
                result.Error);
            ASSERT_VECTORS_EQUAL(TVector<TString>{"vol0"}, result.AffectedDisks);
            UNIT_ASSERT_VALUES_UNEQUAL(TDuration {}, result.Timeout);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TString affectedDisk;
            UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                db,
                "uuid-1.1",
                NProto::DEVICE_STATE_ERROR,
                Now(),
                "test",
                affectedDisk));
            UNIT_ASSERT_VALUES_EQUAL("vol0", affectedDisk);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto result = state.UpdateCmsDeviceState(
                db,
                agentConfig.GetAgentId(),
                "NVMENBS01",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                false,  // shouldResumeDevice
                false); // dryRun

            UNIT_ASSERT_SUCCESS(result.Error);

            ASSERT_VECTORS_EQUAL(TVector<TString>{}, result.AffectedDisks);
            UNIT_ASSERT_VALUES_EQUAL(TDuration {}, result.Timeout);

            TVector<NProto::TDeviceConfig> devices;
            auto error = state.GetDiskDevices("vol0", devices);
            UNIT_ASSERT_SUCCESS(error);

            UNIT_ASSERT_VALUES_EQUAL(4, devices.size());
        });
    }

    Y_UNIT_TEST(ShouldAddUnknownHost)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto lostAgentConfig = AgentConfig(1, "agent-1", {});

        const auto agentConfig = AgentConfig(1, "agent-2", {
            Device("NVMENBS01", "uuid-1.1", "rack-1"),
            Device("NVMENBS02", "uuid-1.2", "rack-1"),
            Device("NVMENBS03", "uuid-1.3", "rack-1"),
            Device("NVMENBS04", "uuid-1.4", "rack-1"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder().Build();

        // prepare agent-1
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetConfig().KnownAgentsSize());

            NProto::TDiskRegistryConfig config;
            config.AddKnownAgents()->SetAgentId(lostAgentConfig.GetAgentId());

            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(
                state.UpdateConfig(db, config, true, affectedDisks));

            UNIT_ASSERT_SUCCESS(
                RegisterAgent(state, db, lostAgentConfig, Now()));

            UNIT_ASSERT_SUCCESS(state.UpdateConfig(
                db,
                NProto::TDiskRegistryConfig{},
                true,
                affectedDisks));

            UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                db,
                lostAgentConfig.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                Now(),
                "lost",
                affectedDisks));

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());
            UNIT_ASSERT_VALUES_EQUAL(
                lostAgentConfig.GetNodeId(),
                state.GetAgents()[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());
        });

        // prepare agent-2
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agentConfig, Now()));

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDevices().size());

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetAgents().size());

            auto agents = state.GetAgents();
            SortBy(
                agents,
                [](const auto& agent) { return agent.GetAgentId(); });

            UNIT_ASSERT_VALUES_EQUAL(2, agents.size());

            UNIT_ASSERT_VALUES_EQUAL(
                lostAgentConfig.GetAgentId(),
                agents[0].GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                0, // agent-1 lost his nodeId
                agents[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(0, agents[0].DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, agents[0].UnknownDevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                agentConfig.GetAgentId(),
                agents[1].GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                agentConfig.GetNodeId(),
                agents[1].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(0, agents[1].DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                agentConfig.DevicesSize(),
                agents[1].UnknownDevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetConfig().KnownAgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetConfigVersion());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            TDuration timeout;
            UNIT_ASSERT_SUCCESS(state.UpdateCmsHostState(
                db,
                agentConfig.GetAgentId(),
                NProto::AGENT_STATE_ONLINE,
                Now(),
                false, // dryRun
                affectedDisks,
                timeout));

            auto knownAgents = state.GetConfig().GetKnownAgents();

            UNIT_ASSERT_VALUES_EQUAL(1, knownAgents.size());
            UNIT_ASSERT_VALUES_EQUAL(3, state.GetConfigVersion());
            UNIT_ASSERT_VALUES_EQUAL(
                agentConfig.DevicesSize(),
                knownAgents.Get(0).DevicesSize());

            SortBy(
                *knownAgents.Mutable(0)->MutableDevices(),
                [](const auto& d) {
                    return d.GetDeviceUUID();
                });
            for (size_t i = 0; i != agentConfig.DevicesSize(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    agentConfig.GetDevices(i).GetDeviceUUID(),
                    knownAgents.Get(0).GetDevices(i).GetDeviceUUID());
            }

            ASSERT_VECTORS_EQUAL(TVector<TString>{}, affectedDisks);
            UNIT_ASSERT_VALUES_EQUAL(TDuration {}, timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(
                agentConfig.DevicesSize(),
                state.GetDirtyDevices().size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDevices().size());

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetAgents().size());

            auto agents = state.GetAgents();
            SortBy(
                agents,
                [](const auto& agent) { return agent.GetAgentId(); });

            UNIT_ASSERT_VALUES_EQUAL(2, agents.size());

            UNIT_ASSERT_VALUES_EQUAL(
                lostAgentConfig.GetAgentId(),
                agents[0].GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(0, agents[0].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(0, agents[0].DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, agents[0].UnknownDevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                agentConfig.GetAgentId(),
                agents[1].GetAgentId());
            UNIT_ASSERT_VALUES_EQUAL(
                agentConfig.GetNodeId(),
                agents[1].GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(
                agentConfig.DevicesSize(),
                agents[1].DevicesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, agents[1].UnknownDevicesSize());
        });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
