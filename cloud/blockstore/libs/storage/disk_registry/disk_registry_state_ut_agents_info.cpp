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

        auto agentsInfo = state.QueryAgentsInfo();
        UNIT_ASSERT_VALUES_EQUAL(2, agentsInfo.size());
        const auto& agentInfo = agentsInfo[0];
        const auto& agent = agents[0];
        const auto& device = agent.GetDevices(0);
        UNIT_ASSERT_VALUES_EQUAL(agent.GetAgentId(), agentInfo.GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(agent.DevicesSize(), agentInfo.DevicesSize());

        const auto& deviceInfo = agentInfo.GetDevices(0);
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

        auto agentsInfo = state.QueryAgentsInfo();
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

        auto agentsInfo = state.QueryAgentsInfo();
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

        auto agentsInfo = state.QueryAgentsInfo();
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

        const auto agentsInfo = state.QueryAgentsInfo();
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

        const auto agentsInfo = state.QueryAgentsInfo();
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
}

}   // namespace NCloud::NBlockStore::NStorage
