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

Y_UNIT_TEST_SUITE(TDiskRegistryStateQueryAgentsInfoTest)
{
    Y_UNIT_TEST(QueryAgentsInfo)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents{
            AgentConfig(
                1,
                {Device("dev-1", "uuid-1.1"),
                 Device("dev-2", "uuid-1.2") |
                     WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                 Device("dev-3", "uuid-1.3"),
                 Device("dev-4", "uuid-1.4")}),
            AgentConfig(
                2,
                {Device("dev-1", "uuid-2.1") |
                     WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL),
                 Device("dev-2", "uuid-2.2"),
                 Device("dev-3", "uuid-2.3"),
                 Device("dev-4", "uuid-2.4")})};

        auto state =
            TDiskRegistryStateBuilder()
                .WithConfig(
                    [&]
                    {
                        auto config = MakeConfig(0, agents);

                        auto* local = config.AddDevicePoolConfigs();
                        local->SetName("local-ssd");
                        local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                        local->SetAllocationUnit(DefaultDeviceSize);

                        return config;
                    }())
                .WithAgents(agents)
                .Build();

        const TInstant agentStateTs = TInstant::FromValue(100042);
        const TString agentStateMessage = "message";

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                auto error = state->UpdateAgentState(
                    db,
                    agents[1].GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    agentStateTs,
                    agentStateMessage,
                    affectedDisks);
                UNIT_ASSERT_SUCCESS(error);
            });

        auto agentsInfo = state->QueryAgentsInfo({});
        UNIT_ASSERT_VALUES_EQUAL(2, agentsInfo.size());

        {
            const auto& agentInfo = agentsInfo[0];
            const auto& agent = agents[0];

            const auto& device = agent.GetDevices(0);
            const auto& deviceInfo = agentInfo.GetDevices(0);

            UNIT_ASSERT_VALUES_EQUAL(agent.GetAgentId(), agentInfo.GetAgentId());
            UNIT_ASSERT_EQUAL(
                NProto::AGENT_STATE_ONLINE,
                agentInfo.GetState());
            UNIT_ASSERT_VALUES_EQUAL("", agentInfo.GetStateMessage());
            UNIT_ASSERT_VALUES_EQUAL(0, agentInfo.GetStateTs());
            UNIT_ASSERT_VALUES_EQUAL(agent.DevicesSize(), agentInfo.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                device.GetDeviceName(),
                deviceInfo.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(
                device.GetSerialNumber(),
                deviceInfo.GetDeviceSerialNumber());

            const auto deviceSpaceInBytes =
                device.GetBlockSize() * device.GetBlocksCount();
            UNIT_ASSERT_VALUES_EQUAL(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceTotalSpaceInBytes());
            UNIT_ASSERT_VALUES_EQUAL(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceFreeSpaceInBytes());
        }

        {
            const auto& agentInfo = agentsInfo[1];
            const auto& agent = agents[1];

            const auto& device = agent.GetDevices(0);
            const auto& deviceInfo = agentInfo.GetDevices(0);

            UNIT_ASSERT_VALUES_EQUAL(agent.GetAgentId(), agentInfo.GetAgentId());
            UNIT_ASSERT_EQUAL(
                NProto::AGENT_STATE_WARNING,
                agentInfo.GetState());
            UNIT_ASSERT_VALUES_EQUAL(
                agentStateMessage,
                agentInfo.GetStateMessage());
            UNIT_ASSERT_VALUES_EQUAL(
                agentStateTs,
                TInstant::MicroSeconds(agentInfo.GetStateTs()));
            UNIT_ASSERT_VALUES_EQUAL(agent.DevicesSize(), agentInfo.DevicesSize());

            UNIT_ASSERT_VALUES_EQUAL(
                device.GetDeviceName(),
                deviceInfo.GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(
                device.GetSerialNumber(),
                deviceInfo.GetDeviceSerialNumber());

            const auto deviceSpaceInBytes =
                device.GetBlockSize() * device.GetBlocksCount();
            UNIT_ASSERT_VALUES_EQUAL(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceTotalSpaceInBytes());
            UNIT_ASSERT_VALUES_EQUAL_C(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceDecommissionedSpaceInBytes(), deviceInfo);
        }
    }

    Y_UNIT_TEST(QueryAgentsInfoWithDirtyDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents{AgentConfig(
            1,
            {Device("dev-1", "uuid-1.1"), Device("dev-2", "uuid-1.2")})};

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithConfig(
                    [&]
                    {
                        auto config = MakeConfig(0, agents);

                        auto* local = config.AddDevicePoolConfigs();
                        local->SetName("local-ssd");
                        local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                        local->SetAllocationUnit(DefaultDeviceSize);

                        return config;
                    }())
                .WithAgents(agents)
                .WithDirtyDevices({TDirtyDevice{"uuid-1.1", {}}})
                .Build();
        TDiskRegistryState& state = *statePtr;

        auto agentsInfo = state.QueryAgentsInfo({});
        UNIT_ASSERT_VALUES_EQUAL(1, agentsInfo.size());
        const auto& agentInfo = agentsInfo[0];
        const auto& agent = agents[0];
        const auto& device = agent.GetDevices(0);
        UNIT_ASSERT_VALUES_EQUAL(agent.DevicesSize(), agentInfo.DevicesSize());
        const auto& deviceInfo = agentInfo.GetDevices(0);
        const auto deviceSpaceInBytes =
            device.GetBlockSize() * device.GetBlocksCount();
        UNIT_ASSERT_VALUES_EQUAL(
            deviceSpaceInBytes,
            deviceInfo.GetDeviceTotalSpaceInBytes());
        UNIT_ASSERT_VALUES_EQUAL(
            deviceSpaceInBytes,
            deviceInfo.GetDeviceDirtySpaceInBytes());
    }

    Y_UNIT_TEST(QueryAgentsInfoWithSuspendedDevice)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents{AgentConfig(
            1,
            {Device("dev-1", "uuid-1.1"), Device("dev-2", "uuid-1.2")})};

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithConfig(
                    [&]
                    {
                        auto config = MakeConfig(0, agents);

                        auto* local = config.AddDevicePoolConfigs();
                        local->SetName("local-ssd");
                        local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                        local->SetAllocationUnit(DefaultDeviceSize);

                        return config;
                    }())
                .WithAgents(agents)
                .WithSuspendedDevices({"uuid-1.1"})
                .Build();
        TDiskRegistryState& state = *statePtr;

        auto agentsInfo = state.QueryAgentsInfo({});
        UNIT_ASSERT_VALUES_EQUAL(1, agentsInfo.size());
        const auto& agentInfo = agentsInfo[0];
        const auto& agent = agents[0];
        const auto& device = agent.GetDevices(0);
        UNIT_ASSERT_VALUES_EQUAL(agent.DevicesSize(), agentInfo.DevicesSize());
        const auto& deviceInfo = agentInfo.GetDevices(0);
        const auto deviceSpaceInBytes =
            device.GetBlockSize() * device.GetBlocksCount();
        UNIT_ASSERT_VALUES_EQUAL(
            deviceSpaceInBytes,
            deviceInfo.GetDeviceTotalSpaceInBytes());
        UNIT_ASSERT_VALUES_EQUAL(
            deviceSpaceInBytes,
            deviceInfo.GetDeviceSuspendedSpaceInBytes());
    }

    Y_UNIT_TEST(QueryAgentsInfoWithAllocatedDevice)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents{AgentConfig(
            1,
            {Device("dev-1", "uuid-1.1"), Device("dev-2", "uuid-1.2")})};

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithConfig(
                    [&]
                    {
                        auto config = MakeConfig(0, agents);

                        auto* local = config.AddDevicePoolConfigs();
                        local->SetName("local-ssd");
                        local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                        local->SetAllocationUnit(DefaultDeviceSize);

                        return config;
                    }())
                .WithAgents(agents)
                .Build();
        TDiskRegistryState& state = *statePtr;

        auto allocate = [&](auto db,
                            TString agentId,
                            TString diskId,
                            ui32 blockSize,
                            ui32 blockCount)
        {
            TDiskRegistryState::TAllocateDiskResult result;

            auto error = state.AllocateDisk(
                TInstant::Zero(),
                db,
                TDiskRegistryState::TAllocateDiskParams{
                    .DiskId = std::move(diskId),
                    .BlockSize = blockSize,
                    .BlocksCount = blockCount,
                    .AgentIds = {agentId}},
                &result);

            return std::make_pair(std::move(result), error);
        };

        const auto& agent = agents[0];
        const auto& device = agent.GetDevices(0);
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [result, error] = allocate(
                    db,
                    agent.GetAgentId(),
                    device.GetDeviceName(),
                    device.GetBlockSize(),
                    device.GetBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            });

        auto agentsInfo = state.QueryAgentsInfo({});
        UNIT_ASSERT_VALUES_EQUAL(1, agentsInfo.size());
        const auto& agentInfo = agentsInfo[0];

        UNIT_ASSERT_VALUES_EQUAL(agent.DevicesSize(), agentInfo.DevicesSize());
        const auto& deviceInfo = agentInfo.GetDevices(0);
        const auto deviceSpaceInBytes =
            device.GetBlockSize() * device.GetBlocksCount();
        UNIT_ASSERT_VALUES_EQUAL(
            deviceSpaceInBytes,
            deviceInfo.GetDeviceTotalSpaceInBytes());
        UNIT_ASSERT_VALUES_EQUAL(
            deviceSpaceInBytes,
            deviceInfo.GetDeviceAllocatedSpaceInBytes());
    }

    Y_UNIT_TEST(QueryAgentsInfoWithBrokenSpace)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents{
            AgentConfig(
                1,
                NProto::EAgentState::AGENT_STATE_UNAVAILABLE,
                {Device("dev-1", "uuid-1.1")}),
            AgentConfig(
                2,
                {Device(
                    "dev-2",
                    "uuid-2.2",
                    NProto::EDeviceState::DEVICE_STATE_ERROR)})};

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithConfig(
                    [&]
                    {
                        auto config = MakeConfig(0, agents);
                        auto* local = config.AddDevicePoolConfigs();
                        local->SetName("local-ssd");
                        local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                        local->SetAllocationUnit(DefaultDeviceSize);

                        return config;
                    }())
                .WithAgents(agents)
                .Build();
        TDiskRegistryState& state = *statePtr;

        const auto agentsInfo = state.QueryAgentsInfo({});
        UNIT_ASSERT_VALUES_EQUAL(2, agentsInfo.size());

        {
            const auto& agentInfo = agentsInfo[0];
            const auto& agent = agents[0];
            const auto& device = agent.GetDevices(0);
            UNIT_ASSERT_VALUES_EQUAL(
                agent.DevicesSize(),
                agentInfo.DevicesSize());
            const auto& deviceInfo = agentInfo.GetDevices(0);
            const auto deviceSpaceInBytes =
                device.GetBlockSize() * device.GetBlocksCount();
            UNIT_ASSERT_VALUES_EQUAL(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceTotalSpaceInBytes());
            UNIT_ASSERT_VALUES_EQUAL(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceBrokenSpaceInBytes());
        }

        {
            const auto& agentInfo = agentsInfo[1];
            const auto& agent = agents[1];
            const auto& device = agent.GetDevices(0);
            UNIT_ASSERT_VALUES_EQUAL(
                agent.DevicesSize(),
                agentInfo.DevicesSize());
            const auto& deviceInfo = agentInfo.GetDevices(0);
            const auto deviceSpaceInBytes =
                device.GetBlockSize() * device.GetBlocksCount();
            UNIT_ASSERT_VALUES_EQUAL(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceTotalSpaceInBytes());
            UNIT_ASSERT_VALUES_EQUAL(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceBrokenSpaceInBytes());
        }
    }

    Y_UNIT_TEST(QueryAgentsInfoWithDecommisionedSpace)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents{
            AgentConfig(
                1,
                NProto::EAgentState::AGENT_STATE_WARNING,
                {Device("dev-1", "uuid-1.1")}),
            AgentConfig(
                2,
                {Device(
                    "dev-2",
                    "uuid-2.2",
                    NProto::EDeviceState::DEVICE_STATE_WARNING)})};

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithConfig(
                    [&]
                    {
                        auto config = MakeConfig(0, agents);
                        auto* local = config.AddDevicePoolConfigs();
                        local->SetName("local-ssd");
                        local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
                        local->SetAllocationUnit(DefaultDeviceSize);

                        return config;
                    }())
                .WithAgents(agents)
                .Build();
        TDiskRegistryState& state = *statePtr;

        const auto agentsInfo = state.QueryAgentsInfo({});
        UNIT_ASSERT_VALUES_EQUAL(2, agentsInfo.size());

        {
            const auto& agentInfo = agentsInfo[0];
            const auto& agent = agents[0];
            const auto& device = agent.GetDevices(0);
            UNIT_ASSERT_VALUES_EQUAL(
                agent.DevicesSize(),
                agentInfo.DevicesSize());
            const auto& deviceInfo = agentInfo.GetDevices(0);
            const auto deviceSpaceInBytes =
                device.GetBlockSize() * device.GetBlocksCount();
            UNIT_ASSERT_VALUES_EQUAL(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceTotalSpaceInBytes());
            UNIT_ASSERT_VALUES_EQUAL(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceDecommissionedSpaceInBytes());
        }

        {
            const auto& agentInfo = agentsInfo[1];
            const auto& agent = agents[1];
            const auto& device = agent.GetDevices(0);
            UNIT_ASSERT_VALUES_EQUAL(
                agent.DevicesSize(),
                agentInfo.DevicesSize());
            const auto& deviceInfo = agentInfo.GetDevices(0);
            const auto deviceSpaceInBytes =
                device.GetBlockSize() * device.GetBlocksCount();
            UNIT_ASSERT_VALUES_EQUAL(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceTotalSpaceInBytes());
            UNIT_ASSERT_VALUES_EQUAL(
                deviceSpaceInBytes,
                deviceInfo.GetDeviceDecommissionedSpaceInBytes());
        }
    }

    Y_UNIT_TEST(ShouldFilterQueryAgentsStates)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto agentConfig1 = AgentConfig(
            1,
            {
                Device("dev-1", "uuid-1", "rack-1"),
                Device("dev-2", "uuid-2", "rack-1"),
                Device("dev-3", "uuid-3", "rack-1"),
            });

        auto agentConfig2 = AgentConfig(
            2,
            {
                Device("dev-4", "uuid-4", "rack-2"),
                Device("dev-5", "uuid-5", "rack-2"),
                Device("dev-6", "uuid-6", "rack-2"),
            });

        auto agentConfig3 = AgentConfig(
            3,
            {
                Device("dev-7", "uuid-7", "rack-3"),
                Device("dev-8", "uuid-8", "rack-3"),
                Device("dev-9", "uuid-9", "rack-3"),
            });

        auto agentConfig4 = AgentConfig(
            4,
            {
                Device("dev-10", "uuid-10", "rack-4"),
                Device("dev-11", "uuid-11", "rack-4"),
                Device("dev-12", "uuid-12", "rack-4"),
            });

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithKnownAgents(
                    {agentConfig1, agentConfig2, agentConfig3, agentConfig4})
                .Build();
        TDiskRegistryState& state = *statePtr;
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                state.UpdateAgentState(
                    db,
                    agentConfig1.GetAgentId(),
                    NProto::EAgentState::AGENT_STATE_WARNING,
                    TInstant::Now(),
                    "test",
                    affectedDisks);
                state.UpdateAgentState(
                    db,
                    agentConfig2.GetAgentId(),
                    NProto::EAgentState::AGENT_STATE_UNAVAILABLE,
                    TInstant::Now(),
                    "test",
                    affectedDisks);
            });

        auto queryAgentsInfo =
            [&](const NProto::TQueryAgentsInfoRequest::TAgentFilter& filter)
        {
            ui32 online = 0, warning = 0, unavailable = 0;
            auto agentsInfo = state.QueryAgentsInfo(filter);

            for (auto& agentInfo: agentsInfo) {
                switch (agentInfo.GetState()) {
                    case NProto::EAgentState::AGENT_STATE_ONLINE:
                        online += 1;
                        break;
                    case NProto::EAgentState::AGENT_STATE_WARNING:
                        warning += 1;
                        break;
                    case NProto::EAgentState::AGENT_STATE_UNAVAILABLE:
                        unavailable += 1;
                        break;
                    default:
                        break;
                }
            }
            return std::make_tuple(online, warning, unavailable);
        };

        ui32 online = 0;
        ui32 warning = 0;
        ui32 unavailable = 0;

        NProto::TQueryAgentsInfoRequest::TAgentFilter filter;
        filter.MutableStates()->Add(NProto::EAgentState::AGENT_STATE_ONLINE);

        std::tie(online, warning, unavailable) = queryAgentsInfo(filter);
        UNIT_ASSERT_VALUES_EQUAL(online, 2);
        UNIT_ASSERT_VALUES_EQUAL(warning, 0);
        UNIT_ASSERT_VALUES_EQUAL(unavailable, 0);

        filter.MutableStates()->Clear();
        filter.MutableStates()->Add(NProto::EAgentState::AGENT_STATE_WARNING);
        filter.MutableStates()->Add(
            NProto::EAgentState::AGENT_STATE_UNAVAILABLE);

        std::tie(online, warning, unavailable) = queryAgentsInfo(filter);
        UNIT_ASSERT_VALUES_EQUAL(online, 0);
        UNIT_ASSERT_VALUES_EQUAL(warning, 1);
        UNIT_ASSERT_VALUES_EQUAL(unavailable, 1);

        std::tie(online, warning, unavailable) = queryAgentsInfo({});
        UNIT_ASSERT_VALUES_EQUAL(online, 2);
        UNIT_ASSERT_VALUES_EQUAL(warning, 1);
        UNIT_ASSERT_VALUES_EQUAL(unavailable, 1);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
