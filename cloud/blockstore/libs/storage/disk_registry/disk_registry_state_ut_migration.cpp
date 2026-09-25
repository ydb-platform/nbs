#include "disk_registry_state.h"

#include "disk_registry_database.h"
#include "disk_registry_schema.h"

#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NDiskRegistryStateTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTableUpdateCounter final
    : public NKikimr::NTable::ITableObserver
{
public:
    size_t UpdateCount = 0;

    void OnUpdate(
        NKikimr::NTable::ERowOp,
        TArrayRef<const NKikimr::TRawTypeValue>,
        TArrayRef<const NKikimr::NTable::TUpdateOp>,
        NKikimr::TRowVersion) override
    {
        ++UpdateCount;
    }

    void OnUpdateTx(
        NKikimr::NTable::ERowOp,
        TArrayRef<const NKikimr::TRawTypeValue>,
        TArrayRef<const NKikimr::NTable::TUpdateOp>,
        ui64) override
    {}
};

////////////////////////////////////////////////////////////////////////////////

auto ChangeAgentState(
    TDiskRegistryState& state,
    TDiskRegistryDatabase db,
    const NProto::TAgentConfig& config,
    NProto::EAgentState newState)
{
    TVector<TString> affectedDisks;

    auto error = state.UpdateAgentState(
        db,
        config.GetAgentId(),
        newState,
        TInstant::Now(),
        "test",
        affectedDisks);
    UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

    return affectedDisks;
};

TResultOrError<TDiskRegistryState::TAllocateDiskResult> AllocateDisk(
    TDiskRegistryDatabase& db,
    TDiskRegistryState& state,
    TString diskId)
{
    TDiskRegistryState::TAllocateDiskResult result{};

    auto error = state.AllocateDisk(
        Now(),
        db,
        TDiskRegistryState::TAllocateDiskParams{
            .DiskId = std::move(diskId),
            .BlockSize = DefaultLogicalBlockSize,
            .BlocksCount = 20_GB / DefaultLogicalBlockSize},
        &result);
    if (HasError(error)) {
        return error;
    }

    return result;
}

TVector<NProto::TAgentConfig> CreateSeveralAgents()
{
    return {
        AgentConfig(
            1,
            {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1"),
            }),
        AgentConfig(
            2,
            {
                Device("dev-1", "uuid-2.1", "rack-2"),
                Device("dev-2", "uuid-2.2", "rack-2"),
            })};
}

std::unique_ptr<TDiskRegistryState> CreateTestState(
    const TVector<NProto::TAgentConfig>& agents)
{
    return TDiskRegistryStateBuilder()
        .WithKnownAgents(agents)
        .WithDisks({
            Disk("disk-1", {"uuid-1.1", "uuid-1.2"}),
        })
        .Build();
}

enum class EMigrationDeviceAction
{
    FailAgent,
    FailDevice,
    RegisterBrokenDevice,
    StartMigration,
};

enum class EMigrationDeviceKind
{
    ActiveTarget,
    CanceledTarget,
    CompletedSource,
};

void DoTestMirroredMigrationDevice(
    EMigrationDeviceKind deviceKind,
    EMigrationDeviceAction action,
    bool reloadState = false)
{
    TTestExecutor executor;
    executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

    const TVector agents{
        AgentConfig(1, {Device("dev-1", "uuid-1", "rack-1")}),
        AgentConfig(2, {Device("dev-1", "uuid-2", "rack-2")}),
        AgentConfig(3, {Device("dev-1", "uuid-3", "rack-3")}),
        AgentConfig(4, {Device("dev-1", "uuid-4", "rack-4")}),
    };

    auto statePtr =
        TDiskRegistryStateBuilder().WithKnownAgents(agents).Build();
    TDiskRegistryState& state = *statePtr;

    TVector<TDeviceConfig> devices;
    TVector<TVector<TDeviceConfig>> replicas;
    TVector<NProto::TDeviceMigration> migrations;
    TVector<TString> deviceReplacementIds;
    executor.WriteTx(
        [&](TDiskRegistryDatabase db)
        {
            UNIT_ASSERT_SUCCESS(AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds));
        });

    UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
    UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
    UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
    UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
    UNIT_ASSERT_VALUES_EQUAL(0, deviceReplacementIds.size());

    const TString sourceId = devices[0].GetDeviceUUID();
    const TString replicaId = "disk-1/0";

    executor.WriteTx(
        [&](TDiskRegistryDatabase db) mutable
        {
            TString affectedDisk;
            UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                db,
                sourceId,
                NProto::DEVICE_STATE_WARNING,
                Now(),
                "test",
                affectedDisk));
            UNIT_ASSERT_VALUES_EQUAL(replicaId, affectedDisk);
        });

    const auto pendingMigrations = state.BuildMigrationList();
    UNIT_ASSERT_VALUES_EQUAL(1, pendingMigrations.size());
    UNIT_ASSERT_VALUES_EQUAL(replicaId, pendingMigrations[0].DiskId);
    UNIT_ASSERT_VALUES_EQUAL(
        sourceId,
        pendingMigrations[0].SourceDeviceId);

    NProto::TDeviceConfig target;
    executor.WriteTx(
        [&](TDiskRegistryDatabase db) mutable
        {
            auto [device, error] = StartDeviceMigration(
                state,
                Now(),
                db,
                replicaId,
                sourceId);
            UNIT_ASSERT_SUCCESS(error);
            target = std::move(device);
        });

    const bool isActive = deviceKind == EMigrationDeviceKind::ActiveTarget;
    const bool isCanceled = deviceKind == EMigrationDeviceKind::CanceledTarget;
    const bool isCompleted =
        deviceKind == EMigrationDeviceKind::CompletedSource;
    const bool expectedIsCanceled = isActive || isCanceled || reloadState;
    UNIT_ASSERT(
        !isActive || action != EMigrationDeviceAction::StartMigration);
    const TString finishedDeviceId =
        isCompleted ? sourceId : target.GetDeviceUUID();
    const TString diskDeviceId =
        isCompleted ? target.GetDeviceUUID() : sourceId;
    const TString failedAgentId =
        isCompleted ? devices[0].GetAgentId() : target.GetAgentId();

    if (!isActive) {
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                if (isCanceled) {
                    // Cancel the migration by recovering its source. The target
                    // stays allocated to the replica until the volume
                    // acknowledges reallocation.
                    TString affectedDisk;
                    UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                        db,
                        sourceId,
                        NProto::DEVICE_STATE_ONLINE,
                        Now(),
                        "test",
                        affectedDisk));
                    UNIT_ASSERT_VALUES_EQUAL(replicaId, affectedDisk);
                } else {
                    // The source stays allocated to the replica until the volume
                    // acknowledges reallocation.
                    UNIT_ASSERT_SUCCESS(FinishDeviceMigration(
                        state,
                        db,
                        replicaId,
                        sourceId,
                        target.GetDeviceUUID()));
                }
            });
    }

    {
        TDiskInfo diskInfo;
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo(replicaId, diskInfo));
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL(
            diskDeviceId,
            diskInfo.Devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(isActive ? 1 : 0, diskInfo.Migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(
            isActive ? 0 : 1,
            diskInfo.FinishedMigrations.size());
        if (!isActive) {
            UNIT_ASSERT_VALUES_EQUAL(
                finishedDeviceId,
                diskInfo.FinishedMigrations[0].DeviceId);
            UNIT_ASSERT_VALUES_EQUAL(
                isCanceled,
                diskInfo.FinishedMigrations[0].IsCanceled);
        }
    }

    std::unique_ptr<TDiskRegistryState> reloadedStatePtr;
    if (reloadState) {
        UNIT_ASSERT(!isActive);
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                reloadedStatePtr = TDiskRegistryStateBuilder::LoadState(db)
                                       .WithKnownAgents(agents)
                                       .Build();
            });

        TDiskInfo diskInfo;
        UNIT_ASSERT_SUCCESS(
            reloadedStatePtr->GetDiskInfo(replicaId, diskInfo));
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.FinishedMigrations.size());
        UNIT_ASSERT_VALUES_EQUAL(
            finishedDeviceId,
            diskInfo.FinishedMigrations[0].DeviceId);
        UNIT_ASSERT(diskInfo.FinishedMigrations[0].IsCanceled);
    }

    TDiskRegistryState& testedState =
        reloadState ? *reloadedStatePtr : state;

    NMonitoring::TDynamicCountersPtr counters =
        new NMonitoring::TDynamicCounters();
    InitCriticalEventsCounter(counters);
    auto critCounter = counters->GetCounter(
        "AppCriticalEvents/MirroredDiskDeviceReplacementForbidden",
        true);
    UNIT_ASSERT_VALUES_EQUAL(0, critCounter->Val());

    executor.WriteTx(
        [&](TDiskRegistryDatabase db) mutable
        {
            switch (action) {
                case EMigrationDeviceAction::FailAgent: {
                    TVector<TString> affectedDisks;
                    UNIT_ASSERT_SUCCESS(testedState.UpdateAgentState(
                        db,
                        failedAgentId,
                        NProto::AGENT_STATE_UNAVAILABLE,
                        Now(),
                        "test",
                        affectedDisks));
                    break;
                }
                case EMigrationDeviceAction::FailDevice: {
                    TString affectedDisk;
                    UNIT_ASSERT_SUCCESS(testedState.UpdateDeviceState(
                        db,
                        finishedDeviceId,
                        NProto::DEVICE_STATE_ERROR,
                        Now(),
                        "test",
                        affectedDisk));
                    break;
                }
                case EMigrationDeviceAction::RegisterBrokenDevice: {
                    // Simulate a Disk Agent restart: the retained migration
                    // device is reported as ERROR during re-registration.
                    const auto it = FindIf(
                        agents,
                        [&](const auto& agent)
                        { return agent.GetAgentId() == failedAgentId; });
                    UNIT_ASSERT(it != agents.end());

                    NProto::TAgentConfig agent = *it;
                    UNIT_ASSERT_VALUES_EQUAL(1, agent.DevicesSize());
                    auto* device = agent.MutableDevices(0);
                    UNIT_ASSERT_VALUES_EQUAL(
                        finishedDeviceId,
                        device->GetDeviceUUID());
                    device->SetState(NProto::DEVICE_STATE_ERROR);
                    device->SetStateTs(Now().MicroSeconds());
                    device->SetStateMessage("test");

                    UNIT_ASSERT_SUCCESS(
                        testedState.RegisterAgent(db, agent, Now()).GetError());
                    break;
                }
                case EMigrationDeviceAction::StartMigration: {
                    auto [device, error] = StartDeviceMigration(
                        testedState,
                        Now(),
                        db,
                        replicaId,
                        finishedDeviceId);
                    UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                    UNIT_ASSERT(device.GetDeviceUUID().empty());
                    break;
                }
            }
        });

    UNIT_ASSERT_VALUES_EQUAL(0, critCounter->Val());
    UNIT_ASSERT_VALUES_EQUAL(
        0,
        testedState.GetAutomaticallyReplacedDevices().size());

    TDiskInfo diskInfo;
    UNIT_ASSERT_SUCCESS(testedState.GetDiskInfo(replicaId, diskInfo));
    UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
    UNIT_ASSERT_VALUES_EQUAL(
        diskDeviceId,
        diskInfo.Devices[0].GetDeviceUUID());
    UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
    UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.FinishedMigrations.size());
    UNIT_ASSERT_VALUES_EQUAL(
        finishedDeviceId,
        diskInfo.FinishedMigrations[0].DeviceId);
    UNIT_ASSERT_VALUES_EQUAL(
        expectedIsCanceled,
        diskInfo.FinishedMigrations[0].IsCanceled);

    if (isActive) {
        const auto restartedMigrations = testedState.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(1, restartedMigrations.size());
        UNIT_ASSERT_VALUES_EQUAL(
            sourceId,
            restartedMigrations[0].SourceDeviceId);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
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
        Executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        AgentConfigs = CreateAgentConfigs();

        State = TDiskRegistryStateBuilder()
            .With(CreateStorageConfig())
            .WithAgents(AgentConfigs)
            .WithConfig(AgentConfigs)
            .Build();
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

}   //namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateMigrationTest)
{
    Y_UNIT_TEST(ShouldRespectPlacementGroups)
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
            AgentConfig(2, { Device("dev-1", "uuid-2.1", "rack-2") }),
            AgentConfig(3, { Device("dev-1", "uuid-3.1", "rack-1") }),
            AgentConfig(4, {
                Device("dev-1", "uuid-4.1", "rack-3"),
                Device("dev-2", "uuid-4.2", "rack-3"),
                Device("dev-3", "uuid-4.3", "rack-3")
            })
        };

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithKnownAgents(agents)
                .WithDisks({
                    Disk("foo", {"uuid-1.1", "uuid-1.2"}),   // rack-1
                    Disk("bar", {"uuid-2.1"})                // rack-2
                })
                .WithDirtyDevices(
                    {TDirtyDevice{"uuid-4.1", {}},
                     TDirtyDevice{"uuid-4.2", {}},
                     TDirtyDevice{"uuid-4.3", {}}})
                .Build();
        TDiskRegistryState& state = *statePtr;

        UNIT_ASSERT(state.IsMigrationListEmpty());

        // create & initialize `pg`
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db, "pg", NProto::PLACEMENT_STRATEGY_SPREAD, {}
            ));

            TVector<TString> disksToAdd {"foo", "bar"};
            UNIT_ASSERT_SUCCESS(state.AlterPlacementGroupMembership(
                db, "pg", 0, 1, disksToAdd, {}
            ));
        });

        {
            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("foo", info));
            UNIT_ASSERT_VALUES_EQUAL(2, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("rack-1", info.Devices[0].GetRack());
            UNIT_ASSERT_VALUES_EQUAL("rack-1", info.Devices[1].GetRack());
            UNIT_ASSERT_VALUES_EQUAL("pg", info.PlacementGroupId);
        }

        {
            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("bar", info));
            UNIT_ASSERT_VALUES_EQUAL(1, info.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("rack-2", info.Devices[0].GetRack());
            UNIT_ASSERT_VALUES_EQUAL("pg", info.PlacementGroupId);
        }

        // enable migrations of disk-1 & disk-2
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            const TString diskIds[] { "foo", "bar" };
            for (const int i: {0, 1}) {
                auto affectedDisks = ChangeAgentState(
                    state,
                    db,
                    agents[i],
                    NProto::AGENT_STATE_WARNING);

                UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
                UNIT_ASSERT_VALUES_EQUAL(diskIds[i], affectedDisks[0]);

                UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
                const auto& update = state.GetDiskStateUpdates().back();

                UNIT_ASSERT_DISK_STATE(diskIds[i], DISK_STATE_WARNING, update);
            }
        });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(3, migrations.size());

            SortBy(migrations, [] (auto& m) {
                return std::tie(m.DiskId, m.SourceDeviceId);
            });

            UNIT_ASSERT_VALUES_EQUAL("bar", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", migrations[0].SourceDeviceId);

            UNIT_ASSERT_VALUES_EQUAL("foo", migrations[1].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", migrations[1].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("foo", migrations[2].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", migrations[2].SourceDeviceId);
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] = StartDeviceMigration(state, Now(), db, "bar", "uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        // start migration for foo:uuid-1.1 -> uuid-3.1
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] = StartDeviceMigration(state, Now(), db, "foo", "uuid-1.1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL("uuid-3.1", device.GetDeviceUUID());
        });

        // cleanup dirty device
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto dirtyDevices = state.GetDirtyDevices();
            UNIT_ASSERT_VALUES_EQUAL(3, dirtyDevices.size());

            SortBy(dirtyDevices, [] (const auto& d) {
                return d.GetDeviceUUID();
            });

            for (int i = 0; i != 3; ++i) {
                const auto& d = dirtyDevices[i];
                UNIT_ASSERT_VALUES_EQUAL(Sprintf("uuid-4.%d", i + 1), d.GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL("rack-3", d.GetRack());
                state.MarkDeviceAsClean(Now(), db, d.GetDeviceUUID());
            }
        });

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

        // start migration for foo:uuid-1.2 -> uuid-4.X
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] = StartDeviceMigration(state, Now(), db, "foo", "uuid-1.2");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("rack-3", device.GetRack());
            UNIT_ASSERT(device.GetDeviceUUID().StartsWith("uuid-4."));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] = StartDeviceMigration(state, Now(), db, "bar", "uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        // finish migration for foo:uuid-1.1 -> uuid-3.1
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto error = FinishDeviceMigration(
                state,
                db,
                "foo",
                "uuid-1.1",
                "uuid-3.1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] = StartDeviceMigration(state, Now(), db, "bar", "uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        // cancel migration for foo:uuid-1.2 -> uuid-4.X
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto affectedDisks = ChangeAgentState(
                state,
                db,
                agents[0],
                NProto::AGENT_STATE_ONLINE);

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("foo", affectedDisks[0]);

            UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("foo", DISK_STATE_ONLINE, update);
        });

        // start migration for bar:uuid-2.1 -> uuid-4.X
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] = StartDeviceMigration(state, Now(), db, "bar", "uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("rack-3", device.GetRack());
            UNIT_ASSERT(device.GetDeviceUUID().StartsWith("uuid-4."));
        });
    }

    Y_UNIT_TEST(ShouldNotDuplicateMigrations)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto updateCounter = MakeIntrusive<TTableUpdateCounter>();
        executor.DB.SetTableObserver(
            TDiskRegistrySchema::Disks::TableId,
            updateCounter);

        const TVector agents {
            AgentConfig(1, { Device("dev-1", "uuid-1.1", "rack-1") }),
            AgentConfig(2, { Device("dev-1", "uuid-2.1", "rack-1") }),
            AgentConfig(3, { Device("dev-1", "uuid-3.1", "rack-1") }),
        };

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents(agents)
                            .WithDisks({Disk("foo", {"uuid-1.1"})})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto affectedDisks = ChangeAgentState(
                state,
                db,
                agents[0],
                NProto::AGENT_STATE_WARNING);

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("foo", affectedDisks[0]);

            UNIT_ASSERT_VALUES_UNEQUAL(0, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("foo", DISK_STATE_WARNING, update)
        });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());

            UNIT_ASSERT_VALUES_EQUAL("foo", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", migrations[0].SourceDeviceId);
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] =
                StartDeviceMigration(state, Now(), db, "foo", "uuid-1.1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", device.GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            updateCounter->UpdateCount = 0;

            const auto stateUpdateCount = state.GetDiskStateUpdates().size();
            auto error = FinishDeviceMigration(
                state,
                db,
                "foo",
                "uuid-1.1",
                "uuid-2.1");

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                stateUpdateCount + 1,
                state.GetDiskStateUpdates().size());
            UNIT_ASSERT_VALUES_EQUAL(1, updateCounter->UpdateCount);
        });

        UNIT_ASSERT(state.IsMigrationListEmpty());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto affectedDisks = ChangeAgentState(
                state,
                db,
                agents[0],
                NProto::AGENT_STATE_UNAVAILABLE);

            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto configCounter = counters->GetCounter(
            "AppCriticalEvents/DiskRegistryWrongMigratedDeviceOwnership",
            true);
        UNIT_ASSERT_VALUES_EQUAL(0, configCounter->Val());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto affectedDisks = ChangeAgentState(
                state,
                db,
                agents[0],
                NProto::AGENT_STATE_WARNING);

            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        UNIT_ASSERT(state.IsMigrationListEmpty());
        UNIT_ASSERT_VALUES_EQUAL(0, configCounter->Val());
    }

    Y_UNIT_TEST(ShouldEraseMigrationsForDeletedDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1"),
                Device("dev-3", "uuid-1.3", "rack-1"),
                Device("dev-4", "uuid-1.4", "rack-1"),
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1"),
                Device("dev-3", "uuid-1.3", "rack-1"),
                Device("dev-4", "uuid-1.4", "rack-1"),
            }),
        };

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents(agents)
                            .WithDisks(
                                {Disk("foo", {"uuid-1.1"}),
                                 Disk("bar", {"uuid-1.2", "uuid-1.3"})})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;

            auto error = state.UpdateAgentState(
                db,
                agents[0].GetAgentId(),
                NProto::AGENT_STATE_WARNING,
                TInstant::Now(),
                "test",
                affectedDisks);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL(2, affectedDisks.size());
        });

        UNIT_ASSERT_VALUES_EQUAL(3, state.BuildMigrationList().size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "foo"));
            auto error = state.DeallocateDisk(db, "foo");
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        });

        UNIT_ASSERT_VALUES_EQUAL(2, state.BuildMigrationList().size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "bar"));
            auto error = state.DeallocateDisk(db, "bar");
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        });

        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
    }

    void DoTestShouldMigrateMirroredDiskReplicas(
        ui32 agentNo,
        const TString& replicaTableRepr)
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

        TVector<NProto::TAgentConfig> agents{
            agentConfig1,
            agentConfig2,
            agentConfig3,
            agentConfig4,
        };

        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        auto statePtr = TDiskRegistryStateBuilder()
                            .With(diskRegistryGroup)
                            .WithKnownAgents(agents)
                            .Build();
        TDiskRegistryState& state = *statePtr;

        auto minusCounter =
            diskRegistryGroup->GetCounter("Mirror3DisksMinus1");
        state.PublishCounters(Now());
        UNIT_ASSERT_VALUES_EQUAL(minusCounter->Val(), 0);

        UNIT_ASSERT(state.IsMigrationListEmpty());

        TVector<TString> expectedDevices{
            "uuid-1",
            "uuid-2",
            "uuid-4",
            "uuid-5",
            "uuid-7",
            "uuid-8",
        };

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                20_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[0],
                devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[1],
                devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[2],
                replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[3],
                replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[4],
                replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[5],
                replicas[1][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        state.PublishCounters(Now());
        UNIT_ASSERT_VALUES_EQUAL(minusCounter->Val(), 0);

        const auto affectedReplica = "disk-1/" + ToString(agentNo);

        // enable migrations
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto affectedDisks = ChangeAgentState(
                state,
                db,
                agents[agentNo],
                NProto::AGENT_STATE_WARNING);

            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(affectedReplica, affectedDisks[0]);

            // We should change master disk state to warning.
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
            UNIT_ASSERT_VALUES_EQUAL(
                "disk-1",
                state.GetDiskStateUpdates()[0].State.GetDiskId());
            UNIT_ASSERT(
                NProto::DISK_STATE_WARNING ==
                state.GetDiskStateUpdates()[0].State.GetState());
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::EDiskState_Name(NProto::DISK_STATE_WARNING),
                NProto::EDiskState_Name(state.GetDiskState(affectedReplica)));
        });

        state.PublishCounters(Now());
        UNIT_ASSERT_VALUES_EQUAL(minusCounter->Val(), 0);

        const auto source1 = agents[agentNo].GetDevices(0).GetDeviceUUID();
        const auto source2 = agents[agentNo].GetDevices(1).GetDeviceUUID();
        const auto target1 = agents[3].GetDevices(0).GetDeviceUUID();
        const auto target2 = agents[3].GetDevices(1).GetDeviceUUID();

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());

            SortBy(migrations, [] (auto& m) {
                return std::tie(m.DiskId, m.SourceDeviceId);
            });

            UNIT_ASSERT_VALUES_EQUAL(affectedReplica, migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL(source1, migrations[0].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL(affectedReplica, migrations[1].DiskId);
            UNIT_ASSERT_VALUES_EQUAL(source2, migrations[1].SourceDeviceId);
        }

        auto diskUpdates = MakeIntrusive<TTableUpdateCounter>();
        auto groupUpdates = MakeIntrusive<TTableUpdateCounter>();
        auto notificationUpdates = MakeIntrusive<TTableUpdateCounter>();
        executor.DB.SetTableObserver(
            TDiskRegistrySchema::Disks::TableId, diskUpdates);
        executor.DB.SetTableObserver(
            TDiskRegistrySchema::PlacementGroups::TableId, groupUpdates);
        executor.DB.SetTableObserver(
            TDiskRegistrySchema::DisksToNotify::TableId, notificationUpdates);

        // Start both migrations in one batch and persist the replica once.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto results = state.StartDeviceMigrations(
                    Now(),
                    db,
                    {{affectedReplica, source1}, {affectedReplica, source2}});
                UNIT_ASSERT_VALUES_EQUAL(2, results.size());
                for (size_t i = 0; i < results.size(); ++i) {
                    const auto& [diskId, sourceId, target] = results[i];
                    UNIT_ASSERT_VALUES_EQUAL(affectedReplica, diskId);
                    UNIT_ASSERT_VALUES_EQUAL(
                        i == 0 ? source1 : source2,
                        sourceId);
                    UNIT_ASSERT_SUCCESS(target.GetError());
                    UNIT_ASSERT_VALUES_EQUAL(
                        i == 0 ? target1 : target2,
                        target.GetResult().GetDeviceUUID());
                }
                UNIT_ASSERT_VALUES_EQUAL(1, diskUpdates->UpdateCount);
                UNIT_ASSERT_VALUES_EQUAL(1, groupUpdates->UpdateCount);
                UNIT_ASSERT_VALUES_EQUAL(1, notificationUpdates->UpdateCount);
            });
        executor.ReadTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> notifications;
                UNIT_ASSERT(db.ReadDisksToReallocate(notifications));
                UNIT_ASSERT_VALUES_EQUAL(
                    (TVector<TString>{"disk-1"}),
                    notifications);
            });

        state.PublishCounters(Now());
        UNIT_ASSERT_VALUES_EQUAL(minusCounter->Val(), 0);

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetDisksToReallocate().size());
        auto notification = state.GetDisksToReallocate().find("disk-1");
        UNIT_ASSERT(notification != state.GetDisksToReallocate().end());
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<TDeviceConfig> devices;
                TVector<TVector<TDeviceConfig>> replicas;
                TVector<NProto::TDeviceMigration> migrations;
                TVector<TString> deviceReplacementIds;
                auto error = AllocateMirroredDisk(
                    db,
                    state,
                    "disk-1",
                    20_GB,
                    2,
                    devices,
                    replicas,
                    migrations,
                    deviceReplacementIds);
                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    expectedDevices[0],
                    devices[0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    expectedDevices[1],
                    devices[1].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
                UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
                UNIT_ASSERT_VALUES_EQUAL(
                    expectedDevices[2],
                    replicas[0][0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    expectedDevices[3],
                    replicas[0][1].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
                UNIT_ASSERT_VALUES_EQUAL(
                    expectedDevices[4],
                    replicas[1][0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    expectedDevices[5],
                    replicas[1][1].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    source1,
                    migrations[0].GetSourceDeviceId());
                UNIT_ASSERT_VALUES_EQUAL(
                    target1,
                    migrations[0].GetTargetDevice().GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    source2,
                    migrations[1].GetSourceDeviceId());
                UNIT_ASSERT_VALUES_EQUAL(
                    target2,
                    migrations[1].GetTargetDevice().GetDeviceUUID());
                ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);

                state.DeleteDiskToReallocate(
                    Now(),
                    db,
                    TDiskNotificationResult{
                        TDiskNotification{"disk-1", notification->second},
                        {},
                    });
            });

        auto checkDiskInfo = [&] (const TDiskInfo& diskInfo) {
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[0],
                diskInfo.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[1],
                diskInfo.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[2],
                diskInfo.Replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[3],
                diskInfo.Replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[4],
                diskInfo.Replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[5],
                diskInfo.Replicas[1][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                source1,
                diskInfo.Migrations[0].GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                target1,
                diskInfo.Migrations[0].GetTargetDevice().GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                source2,
                diskInfo.Migrations[1].GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                target2,
                diskInfo.Migrations[1].GetTargetDevice().GetDeviceUUID());
        };

        {
            TDiskInfo diskInfo;
            auto error = state.StartAcquireDisk("disk-1", diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            checkDiskInfo(diskInfo);
        }

        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            checkDiskInfo(diskInfo);
        }

        // finish migrations
        auto replicaId =
            state.FindReplicaByMigration("disk-1", source1, target2);
        UNIT_ASSERT_VALUES_EQUAL("", replicaId);
        replicaId = state.FindReplicaByMigration("disk-1", source2, target1);
        UNIT_ASSERT_VALUES_EQUAL("", replicaId);
        replicaId = state.FindReplicaByMigration("disk-1", source1, target1);
        UNIT_ASSERT_VALUES_EQUAL(affectedReplica, replicaId);

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto error = FinishDeviceMigration(
                state,
                db,
                affectedReplica,
                source1,
                target1);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        state.PublishCounters(Now());
        UNIT_ASSERT_VALUES_EQUAL(minusCounter->Val(), 0);

        replicaId = state.FindReplicaByMigration("disk-1", source1, target1);
        UNIT_ASSERT_VALUES_EQUAL("", replicaId);

        expectedDevices[agentNo * 2] = target1;

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetDisksToReallocate().size());
        notification = state.GetDisksToReallocate().find("disk-1");
        UNIT_ASSERT(notification != state.GetDisksToReallocate().end());

        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo(affectedReplica, diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.FinishedMigrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                source1,
                diskInfo.FinishedMigrations[0].DeviceId);
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                20_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[0],
                devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[1],
                devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[2],
                replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[3],
                replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[4],
                replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[5],
                replicas[1][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                source2,
                migrations[0].GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                target2,
                migrations[0].GetTargetDevice().GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);

            state.DeleteDiskToReallocate(
                Now(),
                db,
                TDiskNotificationResult{
                    TDiskNotification{"disk-1", notification->second},
                    {},
                });
        });

        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo(affectedReplica, diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.FinishedMigrations.size());
        }

        replicaId = state.FindReplicaByMigration("disk-1", source1, target2);
        UNIT_ASSERT_VALUES_EQUAL("", replicaId);
        replicaId = state.FindReplicaByMigration("disk-1", source2, target1);
        UNIT_ASSERT_VALUES_EQUAL("", replicaId);
        replicaId = state.FindReplicaByMigration("disk-1", source2, target2);
        UNIT_ASSERT_VALUES_EQUAL(affectedReplica, replicaId);

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto error = FinishDeviceMigration(
                state,
                db,
                affectedReplica,
                source2,
                target2);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDiskStateUpdates().size());
            UNIT_ASSERT_VALUES_EQUAL(
                "disk-1",
                state.GetDiskStateUpdates()[1].State.GetDiskId());
            UNIT_ASSERT(
                NProto::DISK_STATE_ONLINE ==
                state.GetDiskStateUpdates()[1].State.GetState());

            UNIT_ASSERT_EQUAL(
                NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
                NProto::EDiskState_Name(state.GetDiskState(affectedReplica)));
        });

        state.PublishCounters(Now());
        UNIT_ASSERT_VALUES_EQUAL(minusCounter->Val(), 0);

        replicaId = state.FindReplicaByMigration("disk-1", source1, target1);
        UNIT_ASSERT_VALUES_EQUAL("", replicaId);

        expectedDevices[agentNo * 2 + 1] = target2;

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetDisksToReallocate().size());
        notification = state.GetDisksToReallocate().find("disk-1");
        UNIT_ASSERT(notification != state.GetDisksToReallocate().end());

        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo(affectedReplica, diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.FinishedMigrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                source2,
                diskInfo.FinishedMigrations[0].DeviceId);
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                20_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[0],
                devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[1],
                devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[2],
                replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[3],
                replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[4],
                replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                expectedDevices[5],
                replicas[1][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);

            state.DeleteDiskToReallocate(
                Now(),
                db,
                TDiskNotificationResult{
                    TDiskNotification{"disk-1", notification->second},
                    {},
                });
        });

        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo(affectedReplica, diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.FinishedMigrations.size());
        }

        state.PublishCounters(Now());
        UNIT_ASSERT_VALUES_EQUAL(minusCounter->Val(), 0);

        const auto rt = GetReplicaTableRepr(state, "disk-1");
        UNIT_ASSERT_VALUES_EQUAL(replicaTableRepr, rt);
    }

    Y_UNIT_TEST(ShouldMigrateMirroredDiskReplicas0)
    {
        DoTestShouldMigrateMirroredDiskReplicas(
            0,
            "|uuid-10|uuid-4|uuid-7|"
            "|uuid-11|uuid-5|uuid-8|");
    }

    Y_UNIT_TEST(ShouldMigrateMirroredDiskReplicas1)
    {
        DoTestShouldMigrateMirroredDiskReplicas(
            1,
            "|uuid-1|uuid-10|uuid-7|"
            "|uuid-2|uuid-11|uuid-8|");
    }

    Y_UNIT_TEST(ShouldMigrateMirroredDiskReplicas2)
    {
        DoTestShouldMigrateMirroredDiskReplicas(
            2,
            "|uuid-1|uuid-4|uuid-10|"
            "|uuid-2|uuid-5|uuid-11|");
    }

    Y_UNIT_TEST(ShouldntMigrateLocalDisks)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent = AgentConfig(1, {
            Device("dev-1", "uuid-1.1"),
            Device("dev-2", "uuid-1.2")
        });

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents({agent})
                            .WithDisks(
                                {Disk("foo", {"uuid-1.1"}),
                                 []
                                 {
                                     auto config = Disk("bar", {"uuid-1.2"});
                                     config.SetStorageMediaKind(
                                         NProto::STORAGE_MEDIA_SSD_LOCAL);
                                     return config;
                                 }()})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        UNIT_ASSERT(state.IsMigrationListEmpty());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto affectedDisks = ChangeAgentState(
                state,
                db,
                agent,
                NProto::AGENT_STATE_WARNING);

            UNIT_ASSERT_VALUES_EQUAL(2, affectedDisks.size());
        });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());

            UNIT_ASSERT_VALUES_EQUAL("foo", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", migrations[0].SourceDeviceId);
        }
    }

    void DoTestShouldNotMigrateMoreThanNDevicesAtTheSameTime(
        NProto::TStorageServiceConfig config)
    {
        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1", "rack-1"),
                Device("dev-2", "uuid-1.2", "rack-1"),
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1", "rack-2"),
            }),
            AgentConfig(3, {
                Device("dev-1", "uuid-3.1", "rack-3"),
                Device("dev-2", "uuid-3.2", "rack-3"),
                Device("dev-3", "uuid-3.3", "rack-3"),
            })
        };

        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents(agents)
                            .WithDisks({
                                Disk("disk-1", {"uuid-1.1", "uuid-1.2"}),
                                Disk("disk-2", {"uuid-2.1"}),
                            })
                            .WithStorageConfig(std::move(config))
                            .Build();
        TDiskRegistryState& state = *statePtr;

        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;

            const auto error = state.UpdateAgentState(
                db,
                "agent-1",
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks
            );
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisks[0]);

            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();
            UNIT_ASSERT_DISK_STATE("disk-1", DISK_STATE_WARNING, update);
        });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", migrations[0].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-1", migrations[1].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", migrations[1].SourceDeviceId);
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;

            const auto error = state.UpdateAgentState(
                db,
                "agent-2",
                NProto::AGENT_STATE_WARNING,
                Now(),
                "state message",
                affectedDisks
            );
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", affectedDisks[0]);

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDiskStateUpdates().size());
            const auto& update = state.GetDiskStateUpdates().back();

            UNIT_ASSERT_DISK_STATE("disk-2", DISK_STATE_WARNING, update);
        });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", migrations[0].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-1", migrations[1].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", migrations[1].SourceDeviceId);
        }

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            for (const auto& [diskId, deviceId]: state.BuildMigrationList()) {
                UNIT_ASSERT_SUCCESS(
                    StartDeviceMigration(state, Now(), db, diskId, deviceId).GetError()
                );
            }
        });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
        }

        // finish migration
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            for (const auto& m: diskInfo.Migrations) {
                UNIT_ASSERT_SUCCESS(FinishDeviceMigration(
                    state,
                    db,
                    "disk-1",
                    m.GetSourceDeviceId(),
                    m.GetTargetDevice().GetDeviceUUID()));
            }
        });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-2", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", migrations[0].SourceDeviceId);
        }
    }

    Y_UNIT_TEST(ShouldNotMigrateMoreThanNDevicesAtTheSameTime)
    {
        auto config = CreateDefaultStorageConfigProto();
        config.SetMaxNonReplicatedDeviceMigrationsInProgress(2);
        config.SetMaxNonReplicatedDeviceMigrationPercentageInProgress(1); // min limit
        DoTestShouldNotMigrateMoreThanNDevicesAtTheSameTime(std::move(config));
    }

    Y_UNIT_TEST(ShouldNotMigrateMoreThanAPercentageOfDevicesAtTheSameTime)
    {
        auto config = CreateDefaultStorageConfigProto();
        config.SetMaxNonReplicatedDeviceMigrationsInProgress(1); // min limit
        config.SetMaxNonReplicatedDeviceMigrationPercentageInProgress(34);
        DoTestShouldNotMigrateMoreThanNDevicesAtTheSameTime(std::move(config));
    }

    Y_UNIT_TEST(ShouldNotStartAlreadyFinishedMigrationAgent)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents = CreateSeveralAgents();

        auto statePtr = CreateTestState(agents);
        TDiskRegistryState& state = *statePtr;

        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        UNIT_ASSERT(state.IsMigrationListEmpty());

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto critCounter = counters->GetCounter(
            "AppCriticalEvents/DiskRegistryWrongMigratedDeviceOwnership",
            true);
        UNIT_ASSERT_VALUES_EQUAL(0, critCounter->Val());

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [result, error] = AllocateDisk(db, state, "disk-1");
                UNIT_ASSERT_SUCCESS(error);

                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(0, result.Migrations.size());
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<TString> affectedDisks;
                TDuration timeout;
                auto error = state.UpdateCmsHostState(
                    db,
                    agents[0].agentid(),
                    NProto::AGENT_STATE_WARNING,
                    /*customMessage=*/TString(),
                    Now(),
                    false,   // dryRun
                    affectedDisks,
                    timeout);

                UNIT_ASSERT_VALUES_EQUAL(error.code(), E_TRY_AGAIN);
                UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
                UNIT_ASSERT(!state.IsMigrationListEmpty());
            });

        const auto migrations = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());

        TVector<TString> targets;
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                for (const auto& [diskId, uuid]: migrations) {
                    auto [config, error] =
                        StartDeviceMigration(state, Now(), db, diskId, uuid);
                    UNIT_ASSERT_SUCCESS(error);
                    targets.push_back(config.GetDeviceUUID());
                }
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [result, error] = AllocateDisk(db, state, "disk-1");
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

        // finish migrations
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<NProto::TDeviceMigrationIds> expectedMigrationIds;
                for (size_t i = 0; i < migrations.size(); ++i) {
                    auto& migration = expectedMigrationIds.emplace_back();
                    migration.SetSourceDeviceId(migrations[i].SourceDeviceId);
                    migration.SetTargetDeviceId(targets[i]);
                }

                TVector<NProto::TDeviceMigrationIds> migrationIds;
                UNIT_ASSERT_SUCCESS(state.FinishDeviceMigrations(
                    db,
                    "disk-1",
                    expectedMigrationIds,
                    Now(),
                    [&](const auto& ids, const auto& error) {
                        migrationIds.push_back(ids);
                        UNIT_ASSERT_SUCCESS(error);
                    }));

                UNIT_ASSERT_VALUES_EQUAL(migrationIds.size(), expectedMigrationIds.size());
                for (size_t i = 0; i != expectedMigrationIds.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        expectedMigrationIds[i].GetSourceDeviceId(),
                        migrationIds[i].GetSourceDeviceId());

                    UNIT_ASSERT_VALUES_EQUAL(
                        expectedMigrationIds[i].GetTargetDeviceId(),
                        migrationIds[i].GetTargetDeviceId());
                }
            });

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.FinishedMigrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                diskInfo.FinishedMigrations[0].SeqNo,
                diskInfo.FinishedMigrations[1].SeqNo);
        }

        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<TString> affectedDisks;
                TDuration timeout;
                auto error = state.UpdateCmsHostState(
                    db,
                    agents[0].agentid(),
                    NProto::AGENT_STATE_WARNING,
                    /*customMessage=*/TString(),
                    Now(),
                    false,   // dryRun
                    affectedDisks,
                    timeout);

                UNIT_ASSERT_VALUES_EQUAL(error.code(), E_TRY_AGAIN);
                UNIT_ASSERT(state.IsMigrationListEmpty());
            });

        auto migrationsAfterSecondRequest = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(0, migrationsAfterSecondRequest.size());
        critCounter = counters->GetCounter(
            "AppCriticalEvents/DiskRegistryWrongMigratedDeviceOwnership",
            true);
        UNIT_ASSERT_VALUES_EQUAL(0, critCounter->Val());
    }

    Y_UNIT_TEST(ShouldStartCanceledMigrationAgent)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents = CreateSeveralAgents();

        auto statePtr = CreateTestState(agents);
        TDiskRegistryState& state = *statePtr;

        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        UNIT_ASSERT(state.IsMigrationListEmpty());

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [result, error] = AllocateDisk(db, state, "disk-1");
                UNIT_ASSERT_SUCCESS(error);

                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(0, result.Migrations.size());
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
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
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                for (const auto& [diskId, uuid]: migrations) {
                    auto [config, error] =
                        StartDeviceMigration(state, Now(), db, diskId, uuid);
                    UNIT_ASSERT_SUCCESS(error);
                    targets.push_back(config.GetDeviceUUID());
                }
            });
        Sort(targets);

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [result, error] = AllocateDisk(db, state, "disk-1");
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
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<TString> affectedDisks;
                TDuration timeout;
                auto error = state.UpdateCmsHostState(
                    db,
                    agents[0].agentid(),
                    NProto::AGENT_STATE_ONLINE,
                    /*customMessage=*/TString(),
                    Now(),
                    false,   // dryRun
                    affectedDisks,
                    timeout);
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
        }

        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<TString> affectedDisks;
                TDuration timeout;
                auto error = state.UpdateCmsHostState(
                    db,
                    agents[0].agentid(),
                    NProto::AGENT_STATE_WARNING,
                    /*customMessage=*/TString(),
                    Now(),
                    false,   // dryRun
                    affectedDisks,
                    timeout);

                UNIT_ASSERT_VALUES_EQUAL(error.code(), E_TRY_AGAIN);
                UNIT_ASSERT(!state.IsMigrationListEmpty());
            });

        auto migrationsAfterSecondRequest = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(2, migrationsAfterSecondRequest.size());
    }

    Y_UNIT_TEST(ShouldNotTreatCanceledMigrationTargetAsSource)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents = CreateSeveralAgents();

        auto statePtr = CreateTestState(agents);
        TDiskRegistryState& state = *statePtr;

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto critCounter = counters->GetCounter(
            "AppCriticalEvents/DiskRegistryWrongMigratedDeviceOwnership",
            true);
        UNIT_ASSERT_VALUES_EQUAL(0, critCounter->Val());

        // Put uuid-1.1 into the migration queue.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TString affectedDisk;
                UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                    db,
                    "uuid-1.1",
                    NProto::DEVICE_STATE_WARNING,
                    Now(),
                    "test",
                    affectedDisk));
                UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);
            });

        const auto migrations = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", migrations[0].DiskId);
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", migrations[0].SourceDeviceId);

        // Start migration to a target device on agent-2.
        TString targetId;
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                auto [target, error] = StartDeviceMigration(
                    state,
                    Now(),
                    db,
                    migrations[0].DiskId,
                    migrations[0].SourceDeviceId);

                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_EQUAL("agent-2", target.GetAgentId());
                targetId = target.GetDeviceUUID();
            });

        // Make the target agent unavailable.
        // This cancels the active migration and requeues its source device.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                ChangeAgentState(
                    state,
                    db,
                    agents[1],
                    NProto::AGENT_STATE_UNAVAILABLE);
            });

        // The active migration is gone, but its canceled target remains in
        // FinishedMigrations until the volume acknowledges disk reallocation.
        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.FinishedMigrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                targetId,
                diskInfo.FinishedMigrations[0].DeviceId);
            UNIT_ASSERT(diskInfo.FinishedMigrations[0].IsCanceled);
        }

        // Move the target agent from UNAVAILABLE to WARNING.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                ChangeAgentState(
                    state,
                    db,
                    agents[1],
                    NProto::AGENT_STATE_WARNING);
            });

        UNIT_ASSERT_VALUES_EQUAL(0, critCounter->Val());
        // The canceled target must not be treated as a new migration source.
        // Only the original source device remains in the migration queue.
        {
            const auto pendingMigrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(1, pendingMigrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1.1",
                pendingMigrations[0].SourceDeviceId);
        }
    }

    Y_UNIT_TEST(ShouldNotTreatCanceledMigrationTargetAsSourceOnDeviceStateChange)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents = CreateSeveralAgents();

        auto statePtr = CreateTestState(agents);
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TString affectedDisk;
                UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                    db,
                    "uuid-1.1",
                    NProto::DEVICE_STATE_WARNING,
                    Now(),
                    "test",
                    affectedDisk));
                UNIT_ASSERT_VALUES_EQUAL("disk-1", affectedDisk);
            });

        const auto migrations = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", migrations[0].SourceDeviceId);

        TString targetId;
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                auto [target, error] = StartDeviceMigration(
                    state,
                    Now(),
                    db,
                    migrations[0].DiskId,
                    migrations[0].SourceDeviceId);
                UNIT_ASSERT_SUCCESS(error);
                targetId = target.GetDeviceUUID();
            });

        // Breaking the active target cancels the migration and requeues its
        // original source.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TString affectedDisk;
                UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                    db,
                    targetId,
                    NProto::DEVICE_STATE_ERROR,
                    Now(),
                    "test",
                    affectedDisk));
            });

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.FinishedMigrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                targetId,
                diskInfo.FinishedMigrations[0].DeviceId);
            UNIT_ASSERT(diskInfo.FinishedMigrations[0].IsCanceled);
        }

        // The canceled target must not be added to the migration queue when
        // its state changes to WARNING.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TString affectedDisk;
                UNIT_ASSERT_SUCCESS(state.UpdateDeviceState(
                    db,
                    targetId,
                    NProto::DEVICE_STATE_WARNING,
                    Now(),
                    "test",
                    affectedDisk));
            });

        const auto pendingMigrations = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(1, pendingMigrations.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1.1",
            pendingMigrations[0].SourceDeviceId);
    }

    Y_UNIT_TEST(ShouldRestartActiveMigrationOnTargetAgentFailure)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::ActiveTarget,
            EMigrationDeviceAction::FailAgent);
    }

    Y_UNIT_TEST(ShouldRestartActiveMigrationOnTargetDeviceFailure)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::ActiveTarget,
            EMigrationDeviceAction::FailDevice);
    }

    Y_UNIT_TEST(ShouldRestartActiveMigrationOnAgentRegistrationWithBrokenTarget)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::ActiveTarget,
            EMigrationDeviceAction::RegisterBrokenDevice);
    }

    Y_UNIT_TEST(ShouldNotReplaceCanceledMigrationTargetOnAgentFailure)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::CanceledTarget,
            EMigrationDeviceAction::FailAgent);
    }

    Y_UNIT_TEST(ShouldNotReplaceCanceledMigrationTargetOnDeviceFailure)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::CanceledTarget,
            EMigrationDeviceAction::FailDevice);
    }

    Y_UNIT_TEST(
        ShouldNotReplaceCanceledMigrationTargetOnAgentRegistrationWithBrokenDevice)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::CanceledTarget,
            EMigrationDeviceAction::RegisterBrokenDevice);
    }

    Y_UNIT_TEST(
        ShouldNotReplaceCanceledMigrationTargetOnRegistrationAfterStateReload)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::CanceledTarget,
            EMigrationDeviceAction::RegisterBrokenDevice,
            true);
    }

    Y_UNIT_TEST(ShouldNotStartMigrationFromCanceledTarget)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::CanceledTarget,
            EMigrationDeviceAction::StartMigration);
    }

    Y_UNIT_TEST(ShouldNotReplaceCompletedMigrationSourceOnAgentFailure)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::CompletedSource,
            EMigrationDeviceAction::FailAgent);
    }

    Y_UNIT_TEST(ShouldNotReplaceCompletedMigrationSourceOnDeviceFailure)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::CompletedSource,
            EMigrationDeviceAction::FailDevice);
    }

    Y_UNIT_TEST(
        ShouldNotReplaceCompletedMigrationSourceOnAgentRegistrationWithBrokenDevice)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::CompletedSource,
            EMigrationDeviceAction::RegisterBrokenDevice);
    }

    Y_UNIT_TEST(
        ShouldNotReplaceCompletedMigrationSourceOnRegistrationAfterStateReload)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::CompletedSource,
            EMigrationDeviceAction::RegisterBrokenDevice,
            true);
    }

    Y_UNIT_TEST(ShouldNotStartMigrationFromCompletedSource)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::CompletedSource,
            EMigrationDeviceAction::StartMigration);
    }

    Y_UNIT_TEST(ShouldNotStartMigrationFromCompletedSourceAfterStateReload)
    {
        DoTestMirroredMigrationDevice(
            EMigrationDeviceKind::CompletedSource,
            EMigrationDeviceAction::StartMigration,
            true);
    }

    Y_UNIT_TEST(ShouldNotStartAlreadyFinishedMigrationDevice)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents = CreateSeveralAgents();

        auto statePtr = CreateTestState(agents);
        TDiskRegistryState& state = *statePtr;

        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        UNIT_ASSERT(state.IsMigrationListEmpty());

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [result, error] = AllocateDisk(db, state, "disk-1");
                UNIT_ASSERT_SUCCESS(error);

                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(0, result.Migrations.size());
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                auto result = state.UpdateCmsDeviceState(
                    db,
                    agents[0].agentid(),
                    agents[0].GetDevices()[0].GetDeviceName(),
                    NProto::DEVICE_STATE_WARNING,
                    /*customMessage=*/TString(),
                    Now(),
                    false,    // shouldResumeDevice
                    false);   // dryRun

                UNIT_ASSERT_VALUES_EQUAL(result.Error.code(), E_TRY_AGAIN);
                UNIT_ASSERT_VALUES_EQUAL(1, result.AffectedDisks.size());
                UNIT_ASSERT(!state.IsMigrationListEmpty());
            });

        const auto migrations = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());
        const auto& migration = migrations[0];

        TString target;
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                auto [config, error] = StartDeviceMigration(
                    state,
                    Now(),
                    db,
                    migration.DiskId,
                    migration.SourceDeviceId);
                UNIT_ASSERT_SUCCESS(error);
                target = config.GetDeviceUUID();
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [result, error] = AllocateDisk(db, state, "disk-1");
                UNIT_ASSERT_SUCCESS(error);

                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(1, result.Migrations.size());
            });

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.FinishedMigrations.size());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

        // finish migration
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                const auto& diskId = migration.DiskId;
                const auto& uuid = migration.SourceDeviceId;

                auto error = FinishDeviceMigration(
                    state,
                    db,
                    diskId,
                    uuid,
                    target);

                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            });

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.FinishedMigrations.size());
        }

        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                auto result = state.UpdateCmsDeviceState(
                    db,
                    agents[0].agentid(),
                    agents[0].GetDevices()[0].GetDeviceName(),
                    NProto::DEVICE_STATE_WARNING,
                    /*customMessage=*/TString(),
                    Now(),
                    false,    // shouldResumeDevice
                    false);   // dryRun

                UNIT_ASSERT_VALUES_EQUAL(result.Error.code(), E_TRY_AGAIN);
                UNIT_ASSERT(state.IsMigrationListEmpty());
            });

        auto migrationsAfterSecondRequest = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(0, migrationsAfterSecondRequest.size());
    }

    Y_UNIT_TEST(ShouldStartCanceledMigrationDevice)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents = CreateSeveralAgents();

        auto statePtr = CreateTestState(agents);
        TDiskRegistryState& state = *statePtr;

        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
        UNIT_ASSERT(state.IsMigrationListEmpty());

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [result, error] = AllocateDisk(db, state, "disk-1");
                UNIT_ASSERT_SUCCESS(error);

                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(0, result.Migrations.size());
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                auto result = state.UpdateCmsDeviceState(
                    db,
                    agents[0].agentid(),
                    agents[0].GetDevices()[0].GetDeviceName(),
                    NProto::DEVICE_STATE_WARNING,
                    /*customMessage=*/TString(),
                    Now(),
                    false,    // shouldResumeDevice
                    false);   // dryRun

                UNIT_ASSERT_VALUES_EQUAL(result.Error.code(), E_TRY_AGAIN);
                UNIT_ASSERT_VALUES_EQUAL(1, result.AffectedDisks.size());
                UNIT_ASSERT(!state.IsMigrationListEmpty());
            });

        const auto migrations = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());
        const auto& migration = migrations[0];

        TString target;
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                auto [config, error] = StartDeviceMigration(
                    state,
                    Now(),
                    db,
                    migration.DiskId,
                    migration.SourceDeviceId);
                UNIT_ASSERT_SUCCESS(error);
                target = config.GetDeviceUUID();
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [result, error] = AllocateDisk(db, state, "disk-1");
                UNIT_ASSERT_SUCCESS(error);

                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(1, result.Migrations.size());
            });

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.FinishedMigrations.size());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

        // cancel migration
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                auto result = state.UpdateCmsDeviceState(
                    db,
                    agents[0].agentid(),
                    agents[0].GetDevices()[0].GetDeviceName(),
                    NProto::DEVICE_STATE_ONLINE,
                    /*customMessage=*/TString(),
                    Now(),
                    false,    // shouldResumeDevice
                    false);   // dryRun
                UNIT_ASSERT(state.IsMigrationListEmpty());
            });

        {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.FinishedMigrations.size());
        }

        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                auto result = state.UpdateCmsDeviceState(
                    db,
                    agents[0].agentid(),
                    agents[0].GetDevices()[0].GetDeviceName(),
                    NProto::DEVICE_STATE_WARNING,
                    /*customMessage=*/TString(),
                    Now(),
                    false,    // shouldResumeDevice
                    false);   // dryRun

                UNIT_ASSERT_VALUES_EQUAL(result.Error.code(), E_TRY_AGAIN);
                UNIT_ASSERT(!state.IsMigrationListEmpty());
            });

        auto migrationsAfterSecondRequest = state.BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(1, migrationsAfterSecondRequest.size());
    }

    Y_UNIT_TEST(ShouldValidateForcedMigrationTarget)
    {
        TTestExecutor executor;
        executor.WriteTx([](TDiskRegistryDatabase db) { db.InitSchema(); });
        auto state = TDiskRegistryStateBuilder()
                         .WithKnownAgents({
                             AgentConfig(1, {Device("dev-1", "source", "rack-1")}),
                             AgentConfig(2, {Device("dev-2", "target", "rack-2")})})
                         .WithDisks({Disk("disk", {"source"})})
                         .Build();

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                // Invalid explicit targets must leave the disk and the free
                // target device unchanged.
                for (const TString targetId: {"", "missing", "source"}) {
                    const auto result = state->StartForceMigration(
                        Now(), db, "disk", "source", targetId);
                    UNIT_ASSERT_VALUES_EQUAL(
                        targetId == "source" ? E_BS_DISK_ALLOCATION_FAILED
                                             : E_NOT_FOUND,
                        result.GetError().GetCode());
                    UNIT_ASSERT(state->FindDisk("target").empty());
                    TDiskInfo disk;
                    UNIT_ASSERT_SUCCESS(state->GetDiskInfo("disk", disk));
                    UNIT_ASSERT(disk.Migrations.empty());
                }

                const auto result = state->StartForceMigration(
                    Now(), db, "disk", "source", "target");
                UNIT_ASSERT_SUCCESS(result.GetError());
                UNIT_ASSERT_VALUES_EQUAL(
                    "target",
                    result.GetResult().GetDeviceUUID());
            });
        executor.ReadTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<NProto::TDiskConfig> disks;
                UNIT_ASSERT(db.ReadDisks(disks));
                UNIT_ASSERT_VALUES_EQUAL(1, disks.size());
                UNIT_ASSERT_VALUES_EQUAL(1, disks[0].MigrationsSize());
                UNIT_ASSERT_VALUES_EQUAL(
                    "target",
                    disks[0].GetMigrations(0).GetTargetDevice().GetDeviceUUID());
            });
    }

    Y_UNIT_TEST(ShouldPersistForcedMigrationOnce)
    {
        for (const bool withGroup: {false, true}) {
            TTestExecutor executor;
            executor.WriteTx([](TDiskRegistryDatabase db) { db.InitSchema(); });
            auto group = SpreadPlacementGroup("pg", {"disk"});
            group.SetConfigVersion(7);
            group.MutableDisks(0)->AddDeviceRacks("rack-1");
            TVector<NProto::TPlacementGroupConfig> groups;
            if (withGroup) {
                groups.push_back(group);
            }
            auto state =
                TDiskRegistryStateBuilder()
                    .WithKnownAgents({
                        AgentConfig(1, {Device("dev-1", "source", "rack-1")}),
                        AgentConfig(2, {Device("dev-2", "target", "rack-2")})})
                    .WithDisks({Disk("disk", {"source"})})
                    .WithPlacementGroups(groups)
                    .Build();
            auto diskUpdates = MakeIntrusive<TTableUpdateCounter>();
            auto groupUpdates = MakeIntrusive<TTableUpdateCounter>();
            auto notificationUpdates = MakeIntrusive<TTableUpdateCounter>();
            executor.DB.SetTableObserver(TDiskRegistrySchema::Disks::TableId,
                                         diskUpdates);
            executor.DB.SetTableObserver(
                TDiskRegistrySchema::PlacementGroups::TableId, groupUpdates);
            executor.DB.SetTableObserver(
                TDiskRegistrySchema::DisksToNotify::TableId,
                notificationUpdates);

            executor.WriteTx(
                [&](TDiskRegistryDatabase db)
                {
                    const auto result = state->StartForceMigration(
                        Now(), db, "disk", "source", "target");
                    UNIT_ASSERT_SUCCESS(result.GetError());
                    UNIT_ASSERT_VALUES_EQUAL(
                        "target",
                        result.GetResult().GetDeviceUUID());
                    UNIT_ASSERT_VALUES_EQUAL("disk", state->FindDisk("target"));
                });
            UNIT_ASSERT_VALUES_EQUAL(1, diskUpdates->UpdateCount);
            UNIT_ASSERT_VALUES_EQUAL(withGroup ? 1 : 0,
                                     groupUpdates->UpdateCount);
            UNIT_ASSERT_VALUES_EQUAL(1, notificationUpdates->UpdateCount);

            if (!withGroup) {
                continue;
            }

            executor.ReadTx(
                [&](TDiskRegistryDatabase db)
                {
                    TVector<NProto::TPlacementGroupConfig> groups;
                    UNIT_ASSERT(db.ReadPlacementGroups(groups));
                    UNIT_ASSERT_VALUES_EQUAL(1, groups.size());
                    UNIT_ASSERT_VALUES_EQUAL(8, groups[0].GetConfigVersion());
                    const auto& racks = groups[0].GetDisks(0).GetDeviceRacks();
                    UNIT_ASSERT(FindPtr(racks, "rack-1"));
                    UNIT_ASSERT(FindPtr(racks, "rack-2"));
                });
        }
    }

    Y_UNIT_TEST(ShouldPersistStartedMigrationsOncePerDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents{
            AgentConfig(1, {Device("dev-1", "source-1", "rack-1"),
                            Device("dev-2", "source-2", "rack-1"),
                            Device("dev-3", "source-3", "rack-1")}),
            AgentConfig(2, {Device("dev-1", "target-1", "rack-2"),
                            Device("dev-2", "target-2", "rack-2")})};
        auto state =
            TDiskRegistryStateBuilder()
                .WithKnownAgents(agents)
                .WithDisks({Disk("foo", {"source-1", "source-2", "source-3"})})
                .Build();
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) {
                ChangeAgentState(*state, db, agents[0],
                                 NProto::AGENT_STATE_WARNING);
            });
        TDiskInfo initialDisk;
        UNIT_ASSERT_SUCCESS(state->GetDiskInfo("foo", initialDisk));

        auto diskUpdates = MakeIntrusive<TTableUpdateCounter>();
        auto notificationUpdates = MakeIntrusive<TTableUpdateCounter>();
        executor.DB.SetTableObserver(TDiskRegistrySchema::Disks::TableId,
                                     diskUpdates);
        executor.DB.SetTableObserver(
            TDiskRegistrySchema::DisksToNotify::TableId, notificationUpdates);

        TVector<TString> targets;
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto results = state->StartDeviceMigrations(
                    Now(),
                    db,
                    state->BuildMigrationList());
                UNIT_ASSERT_VALUES_EQUAL(3, results.size());
                for (size_t i = 0; i < results.size(); ++i) {
                    const auto& [diskId, sourceId, target] = results[i];
                    UNIT_ASSERT_VALUES_EQUAL("foo", diskId);
                    UNIT_ASSERT_VALUES_EQUAL(
                        TStringBuilder() << "source-" << i + 1,
                        sourceId);
                    if (i < 2) {
                        UNIT_ASSERT_SUCCESS(target.GetError());
                        targets.push_back(target.GetResult().GetDeviceUUID());
                        UNIT_ASSERT_VALUES_EQUAL(
                            "foo",
                            state->FindDisk(targets.back()));
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(
                            E_BS_DISK_ALLOCATION_FAILED,
                            target.GetError().GetCode());
                    }
                }
                UNIT_ASSERT_VALUES_UNEQUAL(targets[0], targets[1]);
                UNIT_ASSERT_VALUES_EQUAL(1, diskUpdates->UpdateCount);
                UNIT_ASSERT_VALUES_EQUAL(1, notificationUpdates->UpdateCount);
            });

        const auto pending = state->BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(1, pending.size());
        UNIT_ASSERT_VALUES_EQUAL("foo", pending[0].DiskId);
        UNIT_ASSERT_VALUES_EQUAL("source-3", pending[0].SourceDeviceId);

        TVector<NProto::TDiskConfig> disks;
        executor.ReadTx(
            [&](TDiskRegistryDatabase db)
            {
                UNIT_ASSERT(db.ReadDisks(disks));
                UNIT_ASSERT_VALUES_EQUAL(1, disks.size());
                UNIT_ASSERT_VALUES_EQUAL(2, disks[0].MigrationsSize());
                UNIT_ASSERT_VALUES_EQUAL(initialDisk.History.size() + 2,
                                         disks[0].HistorySize());
                TVector<TString> notifications;
                UNIT_ASSERT(db.ReadDisksToReallocate(notifications));
                UNIT_ASSERT_VALUES_EQUAL(1, notifications.size());
                UNIT_ASSERT_VALUES_EQUAL("foo", notifications[0]);
            });
        auto reloaded = TDiskRegistryStateBuilder()
                            .WithKnownAgents(agents)
                            .WithDisks(disks)
                            .Build();
        TDiskInfo disk;
        UNIT_ASSERT_SUCCESS(reloaded->GetDiskInfo("foo", disk));
        UNIT_ASSERT_VALUES_EQUAL(2, disk.Migrations.size());
        SortBy(disk.Migrations, [](const auto& migration) {
            return migration.GetSourceDeviceId();
        });
        for (size_t i = 0; i < disk.Migrations.size(); ++i) {
            const auto& migration = disk.Migrations[i];
            UNIT_ASSERT_VALUES_EQUAL(
                TStringBuilder() << "source-" << i + 1,
                migration.GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                targets[i], migration.GetTargetDevice().GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("foo", reloaded->FindDisk(targets[i]));
        }
    }

    Y_UNIT_TEST(ShouldPersistStartedMigrationPlacementGroupsOnce)
    {
        // Without the second target rack, the second disk must fail even
        // though the first target rack still has two free devices.
        for (const bool secondTargetRack: {false, true}) {
            TTestExecutor executor;
            executor.WriteTx([](TDiskRegistryDatabase db) { db.InitSchema(); });
            TVector agents{AgentConfig(1, NProto::AGENT_STATE_WARNING,
                                       {Device("dev-a1", "a1", "rack-1"),
                                        Device("dev-a2", "a2", "rack-1")}),
                           AgentConfig(2, NProto::AGENT_STATE_WARNING,
                                       {Device("dev-b1", "b1", "rack-2"),
                                        Device("dev-b2", "b2", "rack-2")}),
                           AgentConfig(3, {Device("dev-t1", "t1", "rack-3"),
                                           Device("dev-t2", "t2", "rack-3"),
                                           Device("dev-t3", "t3", "rack-3"),
                                           Device("dev-t4", "t4", "rack-3")})};
            if (secondTargetRack) {
                agents.push_back(
                    AgentConfig(4, {Device("dev-t5", "t5", "rack-4"),
                                    Device("dev-t6", "t6", "rack-4")}));
            }
            auto group = SpreadPlacementGroup("pg", {"disk-a", "disk-b"});
            group.SetConfigVersion(7);
            group.MutableDisks(0)->AddDeviceRacks("rack-1");
            group.MutableDisks(1)->AddDeviceRacks("rack-2");
            auto state = TDiskRegistryStateBuilder()
                             .WithKnownAgents(agents)
                             .WithDisks({Disk("disk-a", {"a1", "a2"},
                                              NProto::DISK_STATE_WARNING),
                                         Disk("disk-b", {"b1", "b2"},
                                              NProto::DISK_STATE_WARNING)})
                             .WithPlacementGroups({group})
                             .Build();
            auto diskUpdates = MakeIntrusive<TTableUpdateCounter>();
            auto groupUpdates = MakeIntrusive<TTableUpdateCounter>();
            auto notificationUpdates = MakeIntrusive<TTableUpdateCounter>();
            executor.DB.SetTableObserver(TDiskRegistrySchema::Disks::TableId,
                                         diskUpdates);
            executor.DB.SetTableObserver(
                TDiskRegistrySchema::PlacementGroups::TableId, groupUpdates);
            executor.DB.SetTableObserver(
                TDiskRegistrySchema::DisksToNotify::TableId,
                notificationUpdates);

            TVector<TString> targetRacks;
            executor.WriteTx(
                [&](TDiskRegistryDatabase db)
                {
                    const auto results = state->StartDeviceMigrations(
                        Now(),
                        db,
                        state->BuildMigrationList());
                    UNIT_ASSERT_VALUES_EQUAL(4, results.size());
                    for (size_t i = 0; i < results.size(); ++i) {
                        const auto& [diskId, sourceId, target] = results[i];
                        UNIT_ASSERT_VALUES_EQUAL(
                            i < 2 ? "disk-a" : "disk-b",
                            diskId);
                        if (i < 2 || secondTargetRack) {
                            UNIT_ASSERT_SUCCESS(target.GetError());
                            targetRacks.push_back(target.GetResult().GetRack());
                        } else {
                            UNIT_ASSERT_VALUES_EQUAL(
                                E_BS_DISK_ALLOCATION_FAILED,
                                target.GetError().GetCode());
                        }
                    }
                    UNIT_ASSERT_VALUES_EQUAL(1, groupUpdates->UpdateCount);
                    UNIT_ASSERT_VALUES_EQUAL(secondTargetRack ? 2 : 1,
                                             diskUpdates->UpdateCount);
                    UNIT_ASSERT_VALUES_EQUAL(secondTargetRack ? 2 : 1,
                                             notificationUpdates->UpdateCount);
                });
            UNIT_ASSERT_VALUES_EQUAL(targetRacks[0], targetRacks[1]);
            if (secondTargetRack) {
                UNIT_ASSERT_VALUES_EQUAL(targetRacks[2], targetRacks[3]);
                UNIT_ASSERT_VALUES_UNEQUAL(targetRacks[0], targetRacks[2]);
            } else {
                const auto remaining = state->BuildMigrationList();
                UNIT_ASSERT_VALUES_EQUAL(2, remaining.size());
                for (const auto& [diskId, sourceId]: remaining) {
                    UNIT_ASSERT_VALUES_EQUAL("disk-b", diskId);
                }
                UNIT_ASSERT_VALUES_EQUAL("b1", remaining[0].SourceDeviceId);
                UNIT_ASSERT_VALUES_EQUAL("b2", remaining[1].SourceDeviceId);
            }
            executor.ReadTx(
                [&](TDiskRegistryDatabase db)
                {
                    TVector<NProto::TPlacementGroupConfig> groups;
                    UNIT_ASSERT(db.ReadPlacementGroups(groups));
                    UNIT_ASSERT_VALUES_EQUAL(1, groups.size());
                    UNIT_ASSERT_VALUES_EQUAL(8, groups[0].GetConfigVersion());
                    UNIT_ASSERT_VALUES_EQUAL(
                        state->FindPlacementGroup("pg")->SerializeAsString(),
                        groups[0].SerializeAsString());
                    TVector<NProto::TDiskConfig> disks;
                    UNIT_ASSERT(db.ReadDisks(disks));
                    UNIT_ASSERT_VALUES_EQUAL(secondTargetRack ? 2 : 1,
                                             disks.size());
                    for (const auto& disk: disks) {
                        UNIT_ASSERT_VALUES_EQUAL(2, disk.MigrationsSize());
                    }
                });
        }
    }

    Y_UNIT_TEST(ShouldNotPersistEmptyOrFailedMigrationBatch)
    {
        TTestExecutor executor;
        executor.WriteTx([](TDiskRegistryDatabase db) { db.InitSchema(); });
        const auto agent =
            AgentConfig(1, {Device("dev-1", "source", "rack-1")});
        auto state =
            TDiskRegistryStateBuilder()
                .WithKnownAgents({agent})
                .WithDisks({Disk("disk", {"source"})})
                .WithPlacementGroups({SpreadPlacementGroup("pg", {"disk"})})
                .Build();
        auto diskUpdates = MakeIntrusive<TTableUpdateCounter>();
        auto groupUpdates = MakeIntrusive<TTableUpdateCounter>();
        auto notificationUpdates = MakeIntrusive<TTableUpdateCounter>();
        executor.DB.SetTableObserver(TDiskRegistrySchema::Disks::TableId,
                                     diskUpdates);
        executor.DB.SetTableObserver(
            TDiskRegistrySchema::PlacementGroups::TableId, groupUpdates);
        executor.DB.SetTableObserver(
            TDiskRegistrySchema::DisksToNotify::TableId, notificationUpdates);
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto results = state->StartDeviceMigrations(
                    Now(),
                    db,
                    state->BuildMigrationList());
                UNIT_ASSERT(results.empty());
            });
        UNIT_ASSERT_VALUES_EQUAL(0, diskUpdates->UpdateCount);
        UNIT_ASSERT_VALUES_EQUAL(0, groupUpdates->UpdateCount);
        UNIT_ASSERT_VALUES_EQUAL(0, notificationUpdates->UpdateCount);
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) {
                ChangeAgentState(*state, db, agent,
                                 NProto::AGENT_STATE_WARNING);
            });
        diskUpdates->UpdateCount = 0;
        groupUpdates->UpdateCount = 0;
        notificationUpdates->UpdateCount = 0;
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto results = state->StartDeviceMigrations(
                    Now(),
                    db,
                    state->BuildMigrationList());
                UNIT_ASSERT_VALUES_EQUAL(1, results.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    E_BS_DISK_ALLOCATION_FAILED,
                    results[0].Target.GetError().GetCode());
            });
        UNIT_ASSERT_VALUES_EQUAL(0, diskUpdates->UpdateCount);
        UNIT_ASSERT_VALUES_EQUAL(0, groupUpdates->UpdateCount);
        UNIT_ASSERT_VALUES_EQUAL(0, notificationUpdates->UpdateCount);
        const auto pending = state->BuildMigrationList();
        UNIT_ASSERT_VALUES_EQUAL(1, pending.size());
        UNIT_ASSERT_VALUES_EQUAL("source", pending[0].SourceDeviceId);
    }

    Y_UNIT_TEST(ShouldLimitSizeOfDeviceMigrationBatch)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const size_t agentWithDiskCount = 2;
        const size_t agentCount = 2 * agentWithDiskCount;
        const size_t devicesPerAgent = 128;
        const size_t devicesPerDisk = 32;
        const size_t disksPerAgent = devicesPerAgent / devicesPerDisk;
        const ui32 migrationsBatchSize = 8;
        const ui32 maxMigrationsInProgress =
            devicesPerAgent * agentWithDiskCount - devicesPerAgent / 2;

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            agentWithDiskCount * devicesPerAgent % migrationsBatchSize);

        //  initialize agents

        TVector<NProto::TAgentConfig> agents;
        agents.reserve(agentCount);

        for (ui32 i = 0; i != agentCount; ++i) {
            auto& agent = agents.emplace_back(AgentConfig(i  + 1, {}));

            auto& devices = *agent.MutableDevices();
            for (ui32 j = 0; j != devicesPerAgent; ++j) {
                auto device = Device(
                    Sprintf("/dev/disk/by-partlabel/NBSNVME%02d", j % 32),
                    Sprintf("uuid-%d-%d", i, j));
                device.SetSerialNumber("SERIAL_NUMBER_0123456789");
                devices.Add(std::move(device));
            }
        }

        // initialize disks

        TVector<NProto::TDiskConfig> disks;
        disks.reserve(agentWithDiskCount * disksPerAgent);
        for (ui32 i = 0; i != agentWithDiskCount; ++i) {
            const auto& agent = agents[i];
            for (ui32 j = 0; j != disksPerAgent; ++j) {
                auto& disk = disks.emplace_back(Disk(Sprintf("disk-%d-%d", i, j), {}));
                for (ui32 k = 0; k != devicesPerDisk; ++k) {
                    const ui32 index = j * devicesPerDisk + k;
                    disk.AddDeviceUUIDs(
                        agent.GetDevices(index).GetDeviceUUID());
                }
            }
        }

        // initialize the state

        auto config = CreateDefaultStorageConfigProto();
        config.SetMaxNonReplicatedDeviceMigrationBatchSize(migrationsBatchSize);
        config.SetMaxNonReplicatedDeviceMigrationsInProgress(
            maxMigrationsInProgress);

        auto statePtr = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithDisks(disks)
            .WithStorageConfig(config)
            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                for (ui32 i = 0; i != agentWithDiskCount; ++i) {
                    const auto& agent = agents[i];
                    TVector affectedDisks = ChangeAgentState(
                        state,
                        db,
                        agent,
                        NProto::AGENT_STATE_WARNING);
                    Sort(affectedDisks);

                    UNIT_ASSERT_VALUES_EQUAL(
                        disksPerAgent,
                        affectedDisks.size());
                    for (ui32 j = 0; j != disksPerAgent; ++j) {
                        const ui32 index = i * disksPerAgent + j;
                        UNIT_ASSERT_VALUES_EQUAL(
                            disks[index].GetDiskId(),
                            affectedDisks[j]);
                    }
                }
            });

        // start migrations

        ui32 migrationsInProgress = 0;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                for (;;) {
                    TVector list = state.BuildMigrationList();
                    if (list.empty()) {
                        break;
                    }

                    UNIT_ASSERT_LE(list.size(), migrationsBatchSize);
                    migrationsInProgress += list.size();
                    for (const auto& [diskId, deviceId]: list) {
                        const auto result = StartDeviceMigration(
                            state,
                            TInstant::FromValue(100500),
                            db,
                            diskId,
                            deviceId);
                        UNIT_ASSERT_C(
                            !HasError(result),
                            FormatError(result.GetError()));
                    }
                }
            });

        UNIT_ASSERT_VALUES_EQUAL(maxMigrationsInProgress, migrationsInProgress);
        UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());
    }

    Y_UNIT_TEST_F(ShouldPrefereSameRackDuringDiskMigration, TFixture)
    {
        Executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                UNIT_ASSERT_SUCCESS(State->UpdateAgentState(
                    db,
                    AgentConfigs[0].GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    Now(),
                    "test",
                    affectedDisks));
            });

        TVector<NProto::TDeviceConfig> devices;
        Executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TDiskRegistryState::TAllocateDiskResult r;
                UNIT_ASSERT_SUCCESS(State->AllocateDisk(
                    Now(),
                    db,
                    {
                        .DiskId = "vol0",
                        .BlockSize = DeviceBlockSize,
                        .BlocksCount = DeviceBlocksCount * DevicesPerAgent,
                    },
                    &r));
                UNIT_ASSERT_VALUES_EQUAL(DevicesPerAgent, r.Devices.size());
                const auto& agentId = r.Devices[0].GetAgentId();
                for (const auto& d: r.Devices) {
                    UNIT_ASSERT_VALUES_EQUAL(agentId, d.GetAgentId());
                }
                devices = std::move(r.Devices);
            });

        Executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                UNIT_ASSERT_SUCCESS(State->UpdateAgentState(
                    db,
                    devices[0].GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    Now(),
                    "test",
                    affectedDisks));
                UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            });

        TString targetAgentId;
        Executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto migrations = State->BuildMigrationList();
                UNIT_ASSERT_VALUES_EQUAL(DevicesPerAgent, migrations.size());

                auto [d, error] = StartDeviceMigration(
                    *State,
                    Now(),
                    db,
                    migrations[0].DiskId,
                    migrations[0].SourceDeviceId);
                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_UNEQUAL(
                    AgentConfigs[0].GetAgentId(),
                    d.GetAgentId());
                targetAgentId = d.GetAgentId();
            });

        Executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                UNIT_ASSERT_SUCCESS(State->UpdateAgentState(
                    db,
                    AgentConfigs[0].GetAgentId(),
                    NProto::AGENT_STATE_ONLINE,
                    Now(),
                    "test",
                    affectedDisks));
            });

        Executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto migrations = State->BuildMigrationList();
                UNIT_ASSERT_VALUES_EQUAL(
                    DevicesPerAgent - 1,
                    migrations.size());

                for (const auto& m: migrations) {
                    auto [d, error] = StartDeviceMigration(
                        *State,
                        Now(),
                        db,
                        m.DiskId,
                        m.SourceDeviceId);
                    UNIT_ASSERT_SUCCESS(error);
                    UNIT_ASSERT_VALUES_EQUAL(targetAgentId, d.GetAgentId());
                }
            });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
