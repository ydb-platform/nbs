#include "disk_registry_state.h"

#include "disk_registry_database.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
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

        auto statePtr = TDiskRegistryStateBuilder().Build();
        TDiskRegistryState& state = *statePtr;

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

        auto statePtr =
            TDiskRegistryStateBuilder().WithConfig({agentConfig}).Build();
        TDiskRegistryState& state = *statePtr;

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

        auto statePtr =
            TDiskRegistryStateBuilder().WithConfig({agentConfig}).Build();
        TDiskRegistryState& state = *statePtr;

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

        const auto lostAgentConfig = AgentConfig(1, "agent-1", {
            Device("NVMENBS01", "uuid-2.1", "rack-1"),
        });

        const auto agentConfig = AgentConfig(1, "agent-2", {
            Device("NVMENBS01", "uuid-1.1", "rack-1"),
            Device("NVMENBS02", "uuid-1.2", "rack-1"),
            Device("NVMENBS03", "uuid-1.3", "rack-1"),
            Device("NVMENBS04", "uuid-1.4", "rack-1"),
        });

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents({lostAgentConfig})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        // prepare agent-1
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;

            UNIT_ASSERT_SUCCESS(
                RegisterAgent(state, db, lostAgentConfig, Now()));

            for (const auto& device: lostAgentConfig.GetDevices()) {
                state.MarkDeviceAsClean(Now(), db, device.GetDeviceUUID());
            }

            TDiskRegistryState::TAllocateDiskResult result;
            UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                Now(),
                db,
                {
                    .DiskId = "vol0",
                    .BlockSize = DefaultBlockSize,
                    .BlocksCount = DefaultDeviceSize / DefaultBlockSize,
                    .AgentIds = {lostAgentConfig.GetAgentId()},
                },
                &result));

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
            UNIT_ASSERT_VALUES_EQUAL(1, agents[0].DevicesSize());
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

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetConfig().KnownAgentsSize());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetConfigVersion());
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

            UNIT_ASSERT_VALUES_EQUAL(2, knownAgents.size());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetConfigVersion());
            UNIT_ASSERT_VALUES_EQUAL(
                agentConfig.DevicesSize(),
                knownAgents.Get(1).DevicesSize());

            SortBy(
                *knownAgents.Mutable(1)->MutableDevices(),
                [](const auto& d) {
                    return d.GetDeviceUUID();
                });
            for (size_t i = 0; i != agentConfig.DevicesSize(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    agentConfig.GetDevices(i).GetDeviceUUID(),
                    knownAgents.Get(1).GetDevices(i).GetDeviceUUID());
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
            UNIT_ASSERT_VALUES_EQUAL(
                lostAgentConfig.DevicesSize(),
                agents[0].DevicesSize());
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

    Y_UNIT_TEST(ShouldCleanupDRConfig)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents{
            AgentConfig(1, "agent-1", {}),
            AgentConfig(
                2,
                "agent-2",
                {
                    Device("NVMENBS01", "uuid-2.1", "rack-2"),
                    Device("NVMENBS02", "uuid-2.2", "rack-2"),
                    Device("NVMENBS03", "uuid-2.3", "rack-2"),
                    Device("NVMENBS04", "uuid-2.4", "rack-2"),
                }),
            AgentConfig(
                3,
                "agent-3",
                {
                    Device(
                        "NVMENBS01",
                        "uuid-3.1",
                        "rack-3",
                        DefaultBlockSize,
                        DefaultDeviceSize,
                        {},
                        NProto::DEVICE_STATE_ERROR),
                })};

        auto statePtr = TDiskRegistryStateBuilder().WithConfig(agents).Build();
        TDiskRegistryState& state = *statePtr;

        // Register agents.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                for (const auto& agentConfig: agents) {
                    UNIT_ASSERT_SUCCESS(
                        RegisterAgent(state, db, agentConfig, Now()));
                    for (const auto& device: agentConfig.GetDevices()) {
                        state.MarkDeviceAsClean(
                            Now(),
                            db,
                            device.GetDeviceUUID());
                    }
                }
            });

        UNIT_ASSERT_VALUES_EQUAL(3, state.GetConfig().KnownAgentsSize());
        UNIT_ASSERT_VALUES_EQUAL(3, state.GetAgents().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetBrokenDevices().size());

        // Allocate a disk.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TDiskRegistryState::TAllocateDiskResult result;
                auto error = state.AllocateDisk(
                    TInstant::Zero(),
                    db,
                    TDiskRegistryState::TAllocateDiskParams{
                        .DiskId = "nrd0",
                        .BlockSize = 4_KB,
                        .BlocksCount =
                            2 * DefaultDeviceSize / DefaultLogicalBlockSize,
                    },
                    &result);
                UNIT_ASSERT_VALUES_EQUAL_C(error.GetCode(), S_OK, error);
                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                for (const auto& device: result.Devices) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        agents[1].GetAgentId(),
                        device.GetAgentId());
                }
            });

        // Remove the agent.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                TDuration timeout;
                UNIT_ASSERT_SUCCESS(state.UpdateCmsHostState(
                    db,
                    agents[0].GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    Now(),
                    false,   // dryRun
                    affectedDisks,
                    timeout));

                UNIT_ASSERT_VALUES_EQUAL(
                    3,
                    state.GetConfig().KnownAgentsSize());
                UNIT_ASSERT_VALUES_EQUAL(3, state.GetAgents().size());

                UNIT_ASSERT_SUCCESS(state.PurgeHost(
                    db,
                    agents[0].GetAgentId(),
                    Now(),
                    false,   // dryRun
                    affectedDisks));

                UNIT_ASSERT_VALUES_EQUAL(
                    2,
                    state.GetConfig().KnownAgentsSize());
                UNIT_ASSERT_VALUES_EQUAL(3, state.GetAgents().size());

                UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                    db,
                    agents[0].GetAgentId(),
                    NProto::AGENT_STATE_UNAVAILABLE,
                    Now(),
                    "lost",
                    affectedDisks));

                UNIT_ASSERT_VALUES_EQUAL(
                    2,
                    state.GetConfig().KnownAgentsSize());
                UNIT_ASSERT_VALUES_EQUAL(2, state.GetAgents().size());
            });

        // Remove the second agent.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                TDuration timeout;

                // Failed because we still have dependent disks.
                auto error = state.UpdateCmsHostState(
                    db,
                    agents[1].GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    Now(),
                    false,   // dryRun
                    affectedDisks,
                    timeout);
                UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());

                UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "nrd0"));
                UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "nrd0"));
                UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());

                UNIT_ASSERT_SUCCESS(state.UpdateCmsHostState(
                    db,
                    agents[1].GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    Now(),
                    false,   // dryRun
                    affectedDisks,
                    timeout));

                UNIT_ASSERT_VALUES_EQUAL(
                    2,
                    state.GetConfig().KnownAgentsSize());
                UNIT_ASSERT_VALUES_EQUAL(2, state.GetAgents().size());

                UNIT_ASSERT_SUCCESS(state.PurgeHost(
                    db,
                    agents[1].GetAgentId(),
                    Now(),
                    false,   // dryRun
                    affectedDisks));

                UNIT_ASSERT_VALUES_EQUAL(
                    1,
                    state.GetConfig().KnownAgentsSize());
                UNIT_ASSERT_VALUES_EQUAL(2, state.GetAgents().size());

                // Devices are now gone.
                const auto* secondAgent =
                    state.FindAgent(agents[1].GetAgentId());
                UNIT_ASSERT(secondAgent);
                UNIT_ASSERT(secondAgent->GetDevices().empty());

                // Make sure that known devices were transfered to unknown.
                UNIT_ASSERT(!secondAgent->GetUnknownDevices().empty());
                TVector<TString> unknownDevices(
                    secondAgent->GetUnknownDevices().size());
                Transform(
                    secondAgent->GetUnknownDevices().begin(),
                    secondAgent->GetUnknownDevices().end(),
                    unknownDevices.begin(),
                    [](const auto& device) { return device.GetDeviceUUID(); });
                TVector<TString> previouslyKnownDevices(
                    agents[1].GetDevices().size());
                Transform(
                    agents[1].GetDevices().begin(),
                    agents[1].GetDevices().end(),
                    previouslyKnownDevices.begin(),
                    [](const auto& device) { return device.GetDeviceUUID(); });
                Sort(unknownDevices);
                Sort(previouslyKnownDevices);
                UNIT_ASSERT_VALUES_EQUAL(
                    previouslyKnownDevices,
                    unknownDevices);

                UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                    db,
                    agents[1].GetAgentId(),
                    NProto::AGENT_STATE_UNAVAILABLE,
                    Now(),
                    "lost",
                    affectedDisks));

                UNIT_ASSERT_VALUES_EQUAL(
                    1,
                    state.GetConfig().KnownAgentsSize());
                UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());
            });

        // Remove the third agent.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                TDuration timeout;
                UNIT_ASSERT_SUCCESS(state.UpdateCmsHostState(
                    db,
                    agents[2].GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    Now(),
                    false,   // dryRun
                    affectedDisks,
                    timeout));

                UNIT_ASSERT_VALUES_EQUAL(
                    1,
                    state.GetConfig().KnownAgentsSize());
                UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());

                UNIT_ASSERT_SUCCESS(state.PurgeHost(
                    db,
                    agents[2].GetAgentId(),
                    Now(),
                    false,   // dryRun
                    affectedDisks));

                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    state.GetConfig().KnownAgentsSize());
                UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());

                UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                    db,
                    agents[2].GetAgentId(),
                    NProto::AGENT_STATE_UNAVAILABLE,
                    Now(),
                    "lost",
                    affectedDisks));

                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    state.GetConfig().KnownAgentsSize());
                UNIT_ASSERT_VALUES_EQUAL(0, state.GetAgents().size());
            });
    }

    Y_UNIT_TEST(ShouldRemoveDevicesAfterAgentDelete)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const auto agent = AgentConfig(
            2,
            "agent-2",
            {
                Device("NVMENBS01", "uuid-2.1", "rack-2"),
                Device("NVMENBS02", "uuid-2.2", "rack-2"),
                Device("NVMENBS03", "uuid-2.3", "rack-2"),
            });

        // Init state.
        {
            auto statePtr =
                TDiskRegistryStateBuilder().WithConfig({agent}).Build();
            TDiskRegistryState& state = *statePtr;

            // Register agent.
            executor.WriteTx(
                [&](TDiskRegistryDatabase db)
                {
                    UNIT_ASSERT_SUCCESS(RegisterAgent(state, db, agent, Now()));
                    for (const auto& device: agent.GetDevices()) {
                        state.MarkDeviceAsClean(
                            Now(),
                            db,
                            device.GetDeviceUUID());
                    }
                });

            // Mark devices.
            executor.WriteTx(
                [&](TDiskRegistryDatabase db)
                {
                    state.MarkDeviceAsDirty(
                        db,
                        agent.devices()[0].deviceuuid());
                    state.SuspendDevice(db, agent.devices()[1].deviceuuid());
                    db.AddAutomaticallyReplacedDevice(
                        TAutomaticallyReplacedDeviceInfo{
                            agent.devices()[2].deviceuuid(),
                            Now()});
                });
        }

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                // Load state from db.
                auto statePtr = TDiskRegistryStateBuilder::LoadState(db)
                                    .WithConfig({agent})
                                    .Build();
                TDiskRegistryState& state = *statePtr;

                // Remove agent.
                TVector<TString> affectedDisks;
                TDuration timeout;

                UNIT_ASSERT_SUCCESS(state.UpdateCmsHostState(
                    db,
                    agent.GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    Now(),
                    false,   // dryRun
                    affectedDisks,
                    timeout));

                UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());

                UNIT_ASSERT_SUCCESS(state.PurgeHost(
                    db,
                    agent.GetAgentId(),
                    Now(),
                    false,   // dryRun
                    affectedDisks));

                UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                    db,
                    agent.GetAgentId(),
                    NProto::AGENT_STATE_UNAVAILABLE,
                    Now(),
                    "lost",
                    affectedDisks));

                UNIT_ASSERT_VALUES_EQUAL(0, state.GetAgents().size());

                UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
                TVector<TDirtyDevice> dirtyDevices;
                db.ReadDirtyDevices(dirtyDevices);
                UNIT_ASSERT_VALUES_EQUAL(0, dirtyDevices.size());

                UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
                TVector<NProto::TSuspendedDevice> suspendedDevices;
                db.ReadSuspendedDevices(suspendedDevices);
                UNIT_ASSERT_VALUES_EQUAL(0, suspendedDevices.size());

                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    state.GetAutomaticallyReplacedDevices().size());
                TDeque<TAutomaticallyReplacedDeviceInfo>
                    automaticalyReplacedDevices;
                db.ReadAutomaticallyReplacedDevices(
                    automaticalyReplacedDevices);
                UNIT_ASSERT_VALUES_EQUAL(0, automaticalyReplacedDevices.size());
            });
    }

    Y_UNIT_TEST(ShouldReturnAffectedDisksFromPurgeHost)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents{AgentConfig(
            2,
            "agent-2",
            {
                Device("NVMENBS01", "uuid-1.1"),
                Device("NVMENBS02", "uuid-1.2"),
                Device("NVMENBS03", "uuid-1.3"),
                Device("NVMENBS04", "uuid-1.4"),
                Device("NVMELOCAL01", "uuid-1.5") |
                    WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                Device("NVMELOCAL02", "uuid-1.6") |
                    WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
            })};

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithConfig(
                    MakeConfig(0, agents) | WithPoolConfig(
                                                "local-ssd",
                                                NProto::DEVICE_POOL_KIND_LOCAL,
                                                DefaultDeviceSize))
                .Build();
        TDiskRegistryState& state = *statePtr;

        // Register agents.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                for (const auto& agentConfig: agents) {
                    UNIT_ASSERT_SUCCESS(
                        RegisterAgent(state, db, agentConfig, Now()));
                    for (const auto& device: agentConfig.GetDevices()) {
                        if (!state.IsSuspendedDevice(device.GetDeviceUUID())) {
                            state.MarkDeviceAsClean(
                                Now(),
                                db,
                                device.GetDeviceUUID());
                        }
                    }
                }
            });

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetConfig().KnownAgentsSize());
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetAgents().size());
        UNIT_ASSERT_VALUES_EQUAL(2, state.GetSuspendedDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDevices().size());

        // Allocate disks.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TDiskRegistryState::TAllocateDiskResult result1;
                UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                    TInstant::Zero(),
                    db,
                    TDiskRegistryState::TAllocateDiskParams{
                        .DiskId = "nrd0",
                        .BlockSize = DefaultLogicalBlockSize,
                        .BlocksCount =
                            DefaultDeviceSize / DefaultLogicalBlockSize,
                    },
                    &result1));

                state.ResumeDevices(
                    TInstant::Zero(),
                    db,
                    {"uuid-1.5", "uuid-1.6"});
                state.MarkDeviceAsClean(Now(), db, "uuid-1.5");
                state.MarkDeviceAsClean(Now(), db, "uuid-1.6");
                TDiskRegistryState::TAllocateDiskResult result2;
                UNIT_ASSERT_SUCCESS(state.AllocateDisk(
                    Now(),
                    db,
                    TDiskRegistryState::TAllocateDiskParams{
                        .DiskId = "local0",
                        .BlockSize = DefaultLogicalBlockSize,
                        .BlocksCount =
                            DefaultDeviceSize / DefaultLogicalBlockSize,
                        .MediaKind = NProto::STORAGE_MEDIA_SSD_LOCAL},
                    &result2));
                UNIT_ASSERT_VALUES_EQUAL(1u, result2.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    "NVMELOCAL01",
                    result2.Devices[0].GetDeviceName());
            });

        // Remove the agent.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                UNIT_ASSERT_SUCCESS(state.PurgeHost(
                    db,
                    agents[0].GetAgentId(),
                    Now(),
                    false,   // dryRun
                    affectedDisks));

                Sort(affectedDisks);
                const TVector<TString> expectedAffectedDisks = {
                    "local0",
                    "nrd0"};
                ASSERT_VECTORS_EQUAL(expectedAffectedDisks, affectedDisks);
            });
    }

    Y_UNIT_TEST(ShouldKeepAgentsWithBrokenDisksInConfig)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents{
            AgentConfig(
                1,
                "agent-1",
                {
                    Device("NVMENBS01", "uuid-1.1", "rack-1"),
                    Device("NVMENBS02", "uuid-1.2", "rack-1"),
                    Device("NVMENBS03", "uuid-1.3", "rack-1"),
                    Device("NVMENBS04", "uuid-1.4", "rack-1"),
                }),
            AgentConfig(
                2,
                "agent-2",
                {
                    Device("NVMENBS01", "uuid-2.1", "rack-2"),
                    Device("NVMENBS02", "uuid-2.2", "rack-2"),
                    Device("NVMENBS03", "uuid-2.3", "rack-2"),
                    Device("NVMENBS04", "uuid-2.4", "rack-2"),
                })};

        auto statePtr = TDiskRegistryStateBuilder().WithConfig(agents).Build();
        TDiskRegistryState& state = *statePtr;

        // Register agents.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                for (const auto& agentConfig: agents) {
                    UNIT_ASSERT_SUCCESS(
                        RegisterAgent(state, db, agentConfig, Now()));
                    for (const auto& device: agentConfig.GetDevices()) {
                        state.MarkDeviceAsClean(
                            Now(),
                            db,
                            device.GetDeviceUUID());
                    }
                }
            });

        UNIT_ASSERT_VALUES_EQUAL(2, state.GetConfig().KnownAgentsSize());
        UNIT_ASSERT_VALUES_EQUAL(2, state.GetAgents().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetSuspendedDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetBrokenDevices().size());

        // Allocate a disk.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TDiskRegistryState::TAllocateDiskResult result;
                auto error = state.AllocateDisk(
                    TInstant::Zero(),
                    db,
                    TDiskRegistryState::TAllocateDiskParams{
                        .DiskId = "nrd0",
                        .BlockSize = 4_KB,
                        .BlocksCount =
                            4 * DefaultDeviceSize / DefaultLogicalBlockSize,
                        .AgentIds = {agents[0].GetAgentId()}},
                    &result);
                UNIT_ASSERT_VALUES_EQUAL_C(error.GetCode(), S_OK, error);
                UNIT_ASSERT_VALUES_EQUAL(4, result.Devices.size());
                for (const auto& device: result.Devices) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        agents[0].GetAgentId(),
                        device.GetAgentId());
                }
            });

        // Break the disk.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TString affectedDisk;
                auto error = state.UpdateDeviceState(
                    db,
                    agents[0].GetDevices()[0].GetDeviceUUID(),
                    NProto::DEVICE_STATE_ERROR,
                    Now(),
                    "IO errors",
                    affectedDisk);
                UNIT_ASSERT_VALUES_EQUAL("nrd0", affectedDisk);
            });

        // Try ro remove the agent. It won't be deleted since it has a broken
        // dependent disk.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                TDuration timeout;

                // Failed because we still have dependent disks.
                auto error = state.UpdateCmsHostState(
                    db,
                    agents[0].GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    Now(),
                    false,   // dryRun
                    affectedDisks,
                    timeout);
                UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            });

        // Migrate the disk.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto migrations = state.BuildMigrationList();
                UNIT_ASSERT_VALUES_EQUAL(3, migrations.size());
                for (const auto& migration: migrations) {
                    UNIT_ASSERT_VALUES_UNEQUAL(
                        agents[0].GetDevices()[0].GetDeviceUUID(),
                        migration.SourceDeviceId);
                    UNIT_ASSERT_VALUES_EQUAL("nrd0", migration.DiskId);
                    auto [device, error] = state.StartDeviceMigration(
                        Now(),
                        db,
                        migration.DiskId,
                        migration.SourceDeviceId);
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                    bool diskStateUpdated = false;
                    error = state.FinishDeviceMigration(
                        db,
                        migration.DiskId,
                        migration.SourceDeviceId,
                        device.GetDeviceUUID(),
                        Now(),
                        &diskStateUpdated);
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                }

                auto notification = state.GetDisksToReallocate().find("nrd0");
                UNIT_ASSERT(notification != state.GetDisksToReallocate().end());
                state.DeleteDiskToReallocate(
                    Now(),
                    db,
                    TDiskNotificationResult{
                        TDiskNotification{"nrd0", notification->second},
                        {},
                    });
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                TDuration timeout;

                UNIT_ASSERT_SUCCESS(state.UpdateCmsHostState(
                    db,
                    agents[0].GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    Now(),
                    false,   // dryRun
                    affectedDisks,
                    timeout));

                UNIT_ASSERT_VALUES_EQUAL(
                    2,
                    state.GetConfig().KnownAgentsSize());
                UNIT_ASSERT_VALUES_EQUAL(2, state.GetAgents().size());

                UNIT_ASSERT_SUCCESS(state.UpdateAgentState(
                    db,
                    agents[0].GetAgentId(),
                    NProto::AGENT_STATE_UNAVAILABLE,
                    Now(),
                    "lost",
                    affectedDisks));

                UNIT_ASSERT_VALUES_EQUAL(
                    2,
                    state.GetConfig().KnownAgentsSize());
                UNIT_ASSERT_VALUES_EQUAL(2, state.GetAgents().size());

                const auto* agentConfig =
                    state.FindAgent(agents[0].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(
                    agents[0].GetDevices().size(),
                    agentConfig->GetDevices().size());
                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    agentConfig->GetUnknownDevices().size());
            });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
