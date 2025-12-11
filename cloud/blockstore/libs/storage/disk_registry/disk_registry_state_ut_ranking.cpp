#include "disk_registry_state.h"

#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NDiskRegistryStateTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    static inline TString TestCloudId = "test-cloud";
    static constexpr ui32 RacksCount = 3;
    static constexpr ui32 AgentsPerRack = 2;
    static constexpr ui32 DevicesPerAgent = 15;
    static constexpr ui32 DeviceBlockSize = 4_KB;
    static constexpr ui64 DeviceBlocksCount = 93_GB / DeviceBlockSize;

    TTestExecutor Executor;
    TVector<NProto::TAgentConfig> AgentConfigs;
    std::unique_ptr<TDiskRegistryState> State;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        Executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        AgentConfigs = CreateAgentConfigs();

        State = TDiskRegistryStateBuilder()
                    .With(CreateStorageConfig())
                    .WithAgents(AgentConfigs)
                    .WithConfig(AgentConfigs)
                    .Build();
    }

    auto AllocateDisk(TDiskRegistryState::TAllocateDiskParams params)
        -> TResultOrError<TDiskRegistryState::TAllocateDiskResult>
    {
        TDiskRegistryState::TAllocateDiskResult result;
        NProto::TError error;
        Executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            { error = State->AllocateDisk(Now(), db, params, &result); });

        if (HasError(error)) {
            return error;
        }

        return result;
    }

    TDiskRegistryState::TAllocateDiskResult AllocateDevices(
        const TString& diskId,
        ui32 devicesCount,
        const TString& agentId = {})
    {
        TVector<TString> agentIds;
        if (agentId) {
            agentIds = {agentId};
        }
        auto [r, error] = AllocateDisk({
            .DiskId = diskId,
            .CloudId = TestCloudId,
            .BlockSize = DeviceBlockSize,
            .BlocksCount = DeviceBlocksCount * devicesCount,
            .AgentIds = std::move(agentIds),
        });
        UNIT_ASSERT_SUCCESS(error);
        UNIT_ASSERT_VALUES_EQUAL(devicesCount, r.Devices.size());
        if (agentId) {
            for (const auto& d: r.Devices) {
                UNIT_ASSERT_VALUES_EQUAL_C(agentId, d.GetAgentId(), diskId);
            }
        }

        return r;
    }

    static TStorageConfigPtr CreateStorageConfig()
    {
        NProto::TStorageServiceConfig config =
            CreateDefaultStorageConfigProto();

        config.SetAllocationUnitNonReplicatedSSD(
            DeviceBlocksCount * DeviceBlockSize / 1_GB);
        config.SetNonreplAllocationPolicy(
            NProto::NONREPL_ALLOC_POLICY_USER_ANTI_AFFINITY);

        return std::make_shared<TStorageConfig>(
            std::move(config),
            NFeatures::TFeaturesConfigPtr());
    }

    static TVector<NProto::TAgentConfig> CreateAgentConfigs()
    {
        TVector<NProto::TAgentConfig> configs;
        configs.reserve(RacksCount * AgentsPerRack);

        for (ui32 i = 0; i != RacksCount; ++i) {
            const TString rack = TStringBuilder() << "rack" << (i + 1);

            for (ui32 j = 0; j != AgentsPerRack; ++j) {
                NProto::TAgentConfig& config = configs.emplace_back();

                config.SetAgentId(
                    TStringBuilder() << "agent-" << (i + 1) << "." << (j + 1));
                config.SetNodeId(1 + j + i * AgentsPerRack);

                for (ui32 k = 0; k != DevicesPerAgent; ++k) {
                    NProto::TDeviceConfig* device = config.AddDevices();
                    device->SetRack(rack);
                    device->SetNodeId(config.GetNodeId());
                    device->SetAgentId(config.GetAgentId());
                    device->SetDeviceName(
                        TStringBuilder()
                        << "/dev/disk/by-partlabel/NBSNVME0" << (k + 1));
                    device->SetDeviceUUID(
                        TStringBuilder()
                        << "uuid-" << config.GetNodeId() << "." << (k + 1));
                    device->SetBlockSize(DeviceBlockSize);
                    device->SetBlocksCount(DeviceBlocksCount);
                }
            }
        }
        return configs;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateRankingTest)
{
    Y_UNIT_TEST_F(ShouldDownrankNodesOccupiedWithSameUser, TFixture)
    {
        const auto& agent1 = AgentConfigs[0].GetAgentId();
        const auto& agent2 = AgentConfigs[1].GetAgentId();
        const auto& agent3 = AgentConfigs[2].GetAgentId();
        const auto& agent4 = AgentConfigs[3].GetAgentId();
        const auto& agent5 = AgentConfigs[4].GetAgentId();
        const auto& agent6 = AgentConfigs[5].GetAgentId();

        AllocateDevices("vol0", 12, {agent2});
        AllocateDevices("vol1", 6, {agent3});
        AllocateDevices("vol2", 12, {agent4});
        AllocateDevices("vol3", 9, {agent5});
        AllocateDevices("vol4", 12, {agent6});

        {
            // rack1 *agent1[0]: 15, agent2[1]: 3  18
            // rack2  agent3[1]:  9, agent4[1]: 3  12
            // rack3  agent5[1]:  6, agent6[1]: 3   9

            auto r = AllocateDevices("vol5", 1);
            UNIT_ASSERT_VALUES_EQUAL(1, r.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("rack1", r.Devices[0].GetRack());
            UNIT_ASSERT_VALUES_EQUAL(agent1, r.Devices[0].GetAgentId());
        }

        {
            // rack1 *agent1[1]: 14, agent2[1]: 3  17
            // rack2  agent3[1]:  9, agent4[1]: 3  12
            // rack3  agent5[1]:  6, agent6[1]: 3   9

            auto r = AllocateDevices("vol6", 1);
            UNIT_ASSERT_VALUES_EQUAL(1, r.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("rack1", r.Devices[0].GetRack());
            UNIT_ASSERT_VALUES_EQUAL(agent1, r.Devices[0].GetAgentId());
        }

        {
            // rack1 agent1[2]: 13, *agent2[1]: 3  16
            // rack2 agent3[1]:  9,  agent4[1]: 3  12
            // rack3 agent5[1]:  6,  agent6[1]: 3   9

            auto r = AllocateDevices("vol7", 1);
            UNIT_ASSERT_VALUES_EQUAL(1, r.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("rack1", r.Devices[0].GetRack());
            UNIT_ASSERT_VALUES_EQUAL(agent2, r.Devices[0].GetAgentId());
        }

        {
            // rack1  agent1[2]: 13, agent2[2]: 2  15
            // rack2 *agent3[1]:  9, agent4[1]: 3  12
            // rack3  agent5[1]:  6, agent6[1]: 3   9

            auto r = AllocateDevices("vol8", 2);
            UNIT_ASSERT_VALUES_EQUAL(2, r.Devices.size());
            for (const auto& d: r.Devices) {
                UNIT_ASSERT_VALUES_EQUAL("rack2", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent3, d.GetAgentId());
            }
        }

        // Allocate vol9 for another cloudId
        {
            // rack1 *agent1[2]: 13, agent2[2]: 2  15
            // rack2  agent3[2]:  7, agent4[1]: 3  10
            // rack3  agent5[1]:  6, agent6[1]: 3   9

            auto [r, error] = AllocateDisk({
                .DiskId = "vol9",
                .CloudId = "another-" + TestCloudId,
                .BlockSize = DeviceBlockSize,
                .BlocksCount = DeviceBlocksCount,
            });
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, r.Devices.size());
            for (const auto& d: r.Devices) {
                UNIT_ASSERT_VALUES_EQUAL("rack1", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent1, d.GetAgentId());
            }
        }

        {
            // rack1 agent1[2]: 12,  agent2[2]: 2  14
            // rack2 agent3[2]:  7, *agent4[1]: 3  10
            // rack3 agent5[1]:  6,  agent6[1]: 3   9

            auto r = AllocateDevices("vol10", 3);
            UNIT_ASSERT_VALUES_EQUAL(3, r.Devices.size());
            for (const auto& d: r.Devices) {
                UNIT_ASSERT_VALUES_EQUAL("rack2", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent4, d.GetAgentId());
            }
        }

        {
            // rack1  agent1[2]: 12,  agent2[2]: 2  14
            // rack2  agent3[2]:  7                  7
            // rack3 *agent5[1]:  6,  agent6[1]: 3   9

            auto r = AllocateDevices("vol11", 6);
            UNIT_ASSERT_VALUES_EQUAL(6, r.Devices.size());
            for (const auto& d: r.Devices) {
                UNIT_ASSERT_VALUES_EQUAL("rack3", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent5, d.GetAgentId());
            }
        }

        {
            // rack1 *agent1[2]: 12,  agent2[2]: 2  14
            // rack2  agent3[2]:  7                  7
            // rack3                 *agent6[1]: 3   3

            auto r = AllocateDevices("vol12", 6);
            UNIT_ASSERT_VALUES_EQUAL(6, r.Devices.size());
            auto it = r.Devices.begin();
            auto mid = std::next(it, 3);
            auto end = r.Devices.end();

            for (const auto& d: MakeIteratorRange(it, mid)) {
                UNIT_ASSERT_VALUES_EQUAL("rack3", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent6, d.GetAgentId());
            }

            for (const auto& d: MakeIteratorRange(mid, end)) {
                UNIT_ASSERT_VALUES_EQUAL("rack1", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent1, d.GetAgentId());
            }
        }

        {
            // rack1 agent1[3]:  9, *agent2[2]: 2  11
            // rack2 agent3[2]:  7                  7

            auto r = AllocateDevices("vol13", 2);
            UNIT_ASSERT_VALUES_EQUAL(2, r.Devices.size());
            for (const auto& d: r.Devices) {
                UNIT_ASSERT_VALUES_EQUAL("rack1", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent2, d.GetAgentId());
            }
        }

        {
            // rack1 *agent1[3]: 9
            // rack2 *agent3[2]: 7

            auto r = AllocateDevices("vol14", 16);
            UNIT_ASSERT_VALUES_EQUAL(16, r.Devices.size());
            auto it = r.Devices.begin();
            auto mid = std::next(it, 7);
            auto end = r.Devices.end();

            for (const auto& d: MakeIteratorRange(it, mid)) {
                UNIT_ASSERT_VALUES_EQUAL("rack2", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent3, d.GetAgentId());
            }

            for (const auto& d: MakeIteratorRange(mid, end)) {
                UNIT_ASSERT_VALUES_EQUAL("rack1", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent1, d.GetAgentId());
            }
        }

        {
            auto [_, error] = AllocateDisk({
                .DiskId = "vol15",
                .CloudId = TestCloudId,
                .BlockSize = DeviceBlockSize,
                .BlocksCount = DeviceBlocksCount,
            });
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                error.GetCode());
        }
    }

    Y_UNIT_TEST_F(ShouldPreferResizeDiskToTheSameNode, TFixture)
    {
        const auto& agent1 = AgentConfigs[0].GetAgentId();
        const auto& agent2 = AgentConfigs[1].GetAgentId();

        AllocateDevices("vol0", 10, {agent1});
        AllocateDevices("vol1", 1, {agent1});
        AllocateDevices("vol2", 1, {agent2});

        // rack1 agent1[vol0 vol1]:  4 agent2[vol2]: 14  18
        // rack2 agent3[]:          15 agent4[]:     15  30
        // rack3 agent5[]:          15 agent6[]:     15  30

        // resize vol0 from 10 to 14
        {
            auto r = AllocateDevices("vol0", 14);
            UNIT_ASSERT_VALUES_EQUAL(14, r.Devices.size());
            for (const auto& d: r.Devices) {
                UNIT_ASSERT_VALUES_EQUAL("rack1", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent1, d.GetAgentId());
            }
        }

        // resize vol0 from 14 to 15
        {
            auto r = AllocateDevices("vol0", 15);
            UNIT_ASSERT_VALUES_EQUAL(15, r.Devices.size());
            auto it = r.Devices.begin();
            auto mid = std::next(it, 14);
            auto end = r.Devices.end();
            for (const auto& d: MakeIteratorRange(it, mid)) {
                UNIT_ASSERT_VALUES_EQUAL("rack1", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent1, d.GetAgentId());
            }
            // rack1 occupied by vol1 and vol2
            for (const auto& d: MakeIteratorRange(mid, end)) {
                UNIT_ASSERT_VALUES_UNEQUAL("rack1", d.GetRack());
                UNIT_ASSERT_VALUES_UNEQUAL(agent1, d.GetAgentId());
                UNIT_ASSERT_VALUES_UNEQUAL(agent2, d.GetAgentId());
            }
        }
    }

    Y_UNIT_TEST_F(ShouldPreferMigrateDiskToTheSameNode, TFixture)
    {
        const auto& agent1 = AgentConfigs[0].GetAgentId();
        const auto& agent2 = AgentConfigs[1].GetAgentId();

        AllocateDevices("vol0", 1, {agent1});
        AllocateDevices("vol1", 1, {agent2});
        auto vol2 = AllocateDevices("vol2", 10, {agent2});

        // rack1 agent1[vol0]: 14 agent2[vol1, vol2]:  4  18
        // rack2 agent3[]:     15 agent4[]:           15  30
        // rack3 agent5[]:     15 agent6[]:           15  30

        Executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [d, error] = State->StartDeviceMigration(
                    Now(),
                    db,
                    "vol2",
                    vol2.Devices[0].GetDeviceUUID());
                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_EQUAL("rack1", d.GetRack());
                UNIT_ASSERT_VALUES_EQUAL(agent2, d.GetAgentId());
            });
    }

    Y_UNIT_TEST_F(ShouldAvoidOccupiedNodesDuringDiskMigration, TFixture)
    {
        const auto& agent1 = AgentConfigs[0].GetAgentId();
        const auto& agent2 = AgentConfigs[1].GetAgentId();

        AllocateDevices("vol0", 1, {agent1});
        auto vol1 = AllocateDevices("vol1", 2, {agent2});

        // rack1 agent1[vol0]: 14 agent2[vol1]:  13  17
        // rack2 agent3[]:     15 agent4[]:      15  30
        // rack3 agent5[]:     15 agent6[]:      15  30

        Executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                UNIT_ASSERT_SUCCESS(State->UpdateAgentState(
                    db,
                    agent2,
                    NProto::AGENT_STATE_WARNING,
                    Now(),
                    "test",
                    affectedDisks));
                UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
                UNIT_ASSERT_VALUES_EQUAL("vol1", affectedDisks[0]);

                const auto migrations = State->BuildMigrationList();
                UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());
                for (const auto& m: migrations) {
                    auto [d, error] = State->StartDeviceMigration(
                        Now(),
                        db,
                        m.DiskId,
                        m.SourceDeviceId);
                    UNIT_ASSERT_SUCCESS(error);
                    UNIT_ASSERT_VALUES_UNEQUAL("rack1", d.GetRack());
                    UNIT_ASSERT_VALUES_UNEQUAL(agent1, d.GetAgentId());
                    UNIT_ASSERT_VALUES_UNEQUAL(agent2, d.GetAgentId());
                }
            });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
