#include "disk_registry_state.h"

#include "disk_registry_database.h"

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

        TDiskRegistryState state =
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
            auto [device, error] = state.StartDeviceMigration(Now(), db, "bar", "uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        // start migration for foo:uuid-1.1 -> uuid-3.1
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] = state.StartDeviceMigration(Now(), db, "foo", "uuid-1.1");
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
            auto [device, error] = state.StartDeviceMigration(Now(), db, "foo", "uuid-1.2");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("rack-3", device.GetRack());
            UNIT_ASSERT(device.GetDeviceUUID().StartsWith("uuid-4."));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] = state.StartDeviceMigration(Now(), db, "bar", "uuid-2.1");
            UNIT_ASSERT_VALUES_EQUAL(E_BS_DISK_ALLOCATION_FAILED, error.GetCode());
        });

        // finish migration for foo:uuid-1.1 -> uuid-3.1
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;
            auto error = state.FinishDeviceMigration(
                db,
                "foo",
                "uuid-1.1",
                "uuid-3.1",
                TInstant::Now(),
                &updated);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(!updated);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] = state.StartDeviceMigration(Now(), db, "bar", "uuid-2.1");
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
            auto [device, error] = state.StartDeviceMigration(Now(), db, "bar", "uuid-2.1");
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

        const TVector agents {
            AgentConfig(1, { Device("dev-1", "uuid-1.1", "rack-1") }),
            AgentConfig(2, { Device("dev-1", "uuid-2.1", "rack-1") }),
            AgentConfig(3, { Device("dev-1", "uuid-3.1", "rack-1") }),
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithDisks({ Disk("foo", { "uuid-1.1" }) })
            .Build();

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
                state.StartDeviceMigration(Now(), db, "foo", "uuid-1.1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", device.GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            bool updated = false;
            auto error = state.FinishDeviceMigration(
                db,
                "foo",
                "uuid-1.1",
                "uuid-2.1",
                TInstant::Now(),
                &updated);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(updated);
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

        {
            for (const auto& m: state.BuildMigrationList()) {
                Cerr << "migration: " << m.DiskId << " " << m.SourceDeviceId << Endl;
            }
        }

        UNIT_ASSERT(state.IsMigrationListEmpty());
        // Now bug is fixed, but, if it reproduce in future, we must report
        // event.
        UNIT_ASSERT_VALUES_EQUAL(1, configCounter->Val());
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

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithDisks({
                Disk("foo", { "uuid-1.1" }),
                Disk("bar", { "uuid-1.2", "uuid-1.3" })
            })
            .Build();

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

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(diskRegistryGroup)
            .WithKnownAgents(agents)
            .Build();

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

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskStateUpdates().size());
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

        // start migrations
        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] = state.StartDeviceMigration(
                Now(),
                db,
                affectedReplica,
                source1);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(target1, device.GetDeviceUUID());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto [device, error] = state.StartDeviceMigration(
                Now(),
                db,
                affectedReplica,
                source2);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(target2, device.GetDeviceUUID());
        });

        state.PublishCounters(Now());
        UNIT_ASSERT_VALUES_EQUAL(minusCounter->Val(), 0);

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetDisksToReallocate().size());
        auto notification = state.GetDisksToReallocate().find("disk-1");
        UNIT_ASSERT(notification != state.GetDisksToReallocate().end());
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

            state.DeleteDiskToReallocate(db, "disk-1", notification->second);
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
            bool updated = false;
            auto error = state.FinishDeviceMigration(
                db,
                affectedReplica,
                source1,
                target1,
                TInstant::Now(),
                &updated);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(!updated);
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

            state.DeleteDiskToReallocate(db, "disk-1", notification->second);
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
            bool updated = false;
            auto error = state.FinishDeviceMigration(
                db,
                affectedReplica,
                source2,
                target2,
                TInstant::Now(),
                &updated);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT(updated);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskStateUpdates().size());
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

            state.DeleteDiskToReallocate(db, "disk-1", notification->second);
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

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({agent})
            .WithDisks({
                Disk("foo", {"uuid-1.1"}),
                [] {
                    auto config = Disk("bar", {"uuid-1.2"});
                    config.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);
                    return config;
                }()
            })
            .Build();

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

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .WithDisks({
                Disk("disk-1", {"uuid-1.1", "uuid-1.2"}),
                Disk("disk-2", {"uuid-2.1"}),
            })
            .WithStorageConfig(std::move(config))
            .Build();

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
                    state.StartDeviceMigration(Now(), db, diskId, deviceId).GetError()
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
}

}   // namespace NCloud::NBlockStore::NStorage
