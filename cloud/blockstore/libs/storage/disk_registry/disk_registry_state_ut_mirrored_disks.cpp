#include "disk_registry_state.h"

#include "disk_registry_database.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <google/protobuf/util/message_differencer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NDiskRegistryStateTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateMirroredDisksTest)
{
    Y_UNIT_TEST(ShouldAllocateDisks)
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
                agentConfig4,
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", replicas[1][0].GetDeviceName());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        auto* group = state.FindPlacementGroup("disk-1/g");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("disk-1/g", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(3, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1/0", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-1/1", group->GetDisks(1).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-1/2", group->GetDisks(2).GetDiskId());

        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", diskInfo.Devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL(
                "dev-4",
                diskInfo.Replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL(
                "dev-7",
                diskInfo.Replicas[1][0].GetDeviceName());
        }

        {
            TDiskInfo diskInfo;
            auto error = state.StartAcquireDisk("disk-1", diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", diskInfo.Devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL(
                "dev-4",
                diskInfo.Replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL(
                "dev-7",
                diskInfo.Replicas[1][0].GetDeviceName());
        }

        {
            TDiskInfo diskInfo;
            auto error = state.StartAcquireDisk("disk-1", diskInfo);
            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());
        }

        state.FinishAcquireDisk("disk-1");

        auto diskIds = state.GetDiskIds();
        UNIT_ASSERT_VALUES_EQUAL(4, diskIds.size());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", diskIds[0]);
        UNIT_ASSERT_VALUES_EQUAL("disk-1/0", diskIds[1]);
        UNIT_ASSERT_VALUES_EQUAL("disk-1/1", diskIds[2]);
        UNIT_ASSERT_VALUES_EQUAL("disk-1/2", diskIds[3]);

        diskIds = state.GetMasterDiskIds();
        UNIT_ASSERT_VALUES_EQUAL(1, diskIds.size());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", diskIds[0]);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
        });

        group = state.FindPlacementGroup("disk-1/g");
        UNIT_ASSERT(!group);

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskIds().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMasterDiskIds().size());
    }

    Y_UNIT_TEST(ShouldCleanupAfterAllocationFailure)
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

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                30_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_DISK_ALLOCATION_FAILED,
                error.GetCode(),
                error.GetMessage());
        });

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskIds().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMasterDiskIds().size());

        auto* group = state.FindPlacementGroup("disk-1/g");
        UNIT_ASSERT(!group);

        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_FOUND,
                error.GetCode(),
                error.GetMessage());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, state.GetBrokenDisks().size());
        UNIT_ASSERT_VALUES_EQUAL("disk-1", state.GetBrokenDisks()[0].DiskId);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto dd = state.GetDirtyDevices();
            UNIT_ASSERT_VALUES_EQUAL(6, dd.size());
            for (const auto& device: dd) {
                state.MarkDeviceAsClean(Now(), db, device.GetDeviceUUID());
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-2",
                30_GB,
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-3", devices[2].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(3, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-5", replicas[0][1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-6", replicas[0][2].GetDeviceName());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        group = state.FindPlacementGroup("disk-2/g");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL("disk-2/g", group->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL(2, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-2/0", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-2/1", group->GetDisks(1).GetDiskId());

        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-2", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(3, diskInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", diskInfo.Devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", diskInfo.Devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-3", diskInfo.Devices[2].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(3, diskInfo.Replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL(
                "dev-4",
                diskInfo.Replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(
                "dev-5",
                diskInfo.Replicas[0][1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(
                "dev-6",
                diskInfo.Replicas[0][2].GetDeviceName());
        }

        auto diskIds = state.GetDiskIds();
        UNIT_ASSERT_VALUES_EQUAL(3, diskIds.size());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", diskIds[0]);
        UNIT_ASSERT_VALUES_EQUAL("disk-2/0", diskIds[1]);
        UNIT_ASSERT_VALUES_EQUAL("disk-2/1", diskIds[2]);

        diskIds = state.GetMasterDiskIds();
        UNIT_ASSERT_VALUES_EQUAL(1, diskIds.size());
        UNIT_ASSERT_VALUES_EQUAL("disk-2", diskIds[0]);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-2"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-2"));
        });

        group = state.FindPlacementGroup("disk-2/g");
        UNIT_ASSERT(!group);

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskIds().size());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetMasterDiskIds().size());
    }

    Y_UNIT_TEST(ShouldResizeDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
            Device("dev-3", "uuid-3", "rack-1"),
            Device("dev-4", "uuid-4", "rack-1"),
            Device("dev-5", "uuid-5", "rack-1"),
            Device("dev-6", "uuid-6", "rack-1"),
        });

        auto agentConfig2 = AgentConfig(2, {
            Device("dev-7", "uuid-7", "rack-2"),
            Device("dev-8", "uuid-8", "rack-2"),
            Device("dev-9", "uuid-9", "rack-2"),
        });

        auto agentConfig3 = AgentConfig(3, {
            Device("dev-10", "uuid-10", "rack-1"),
            Device("dev-11", "uuid-11", "rack-1"),
            Device("dev-12", "uuid-12", "rack-1"),
        });

        auto agentConfig4 = AgentConfig(4, {
            Device("dev-13", "uuid-13", "rack-3"),
            Device("dev-14", "uuid-14", "rack-3"),
            Device("dev-15", "uuid-15", "rack-3"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3,
                agentConfig4,
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                30_GB,
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-3", devices[2].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(3, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-8", replicas[0][1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-9", replicas[0][2].GetDeviceName());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                60_GB,
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(6, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-3", devices[2].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-10", devices[3].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-11", devices[4].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-12", devices[5].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(6, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-8", replicas[0][1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-9", replicas[0][2].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-13", replicas[0][3].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-14", replicas[0][4].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-15", replicas[0][5].GetDeviceName());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
        });
    }

    Y_UNIT_TEST(ShouldAllocateReplicas)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                AgentConfig(1, {
                    Device("dev-1", "uuid-1", "rack-1"),
                    Device("dev-2", "uuid-2", "rack-1"),
                    Device("dev-3", "uuid-3", "rack-1"),
                }),
                AgentConfig(2, {
                    Device("dev-4", "uuid-4", "rack-2"),
                    Device("dev-5", "uuid-5", "rack-2"),
                    Device("dev-6", "uuid-6", "rack-2"),
                }),
                AgentConfig(3, {
                    Device("dev-7", "uuid-7", "rack-3"),
                    Device("dev-8", "uuid-8", "rack-3"),
                    Device("dev-9", "uuid-9", "rack-3"),
                }),
                AgentConfig(4, {
                    Device("dev-10", "uuid-10", "rack-4"),
                    Device("dev-11", "uuid-11", "rack-4"),
                    Device("dev-12", "uuid-12", "rack-4"),
                }),
                AgentConfig(5, {
                    Device("dev-13", "uuid-13", "rack-5"),
                    Device("dev-14", "uuid-14", "rack-5"),
                    Device("dev-15", "uuid-15", "rack-5"),
                }),
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> d;
            TVector<TVector<TDeviceConfig>> addedReplicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                20_GB,
                1,
                d,
                addedReplicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL_C(2, addedReplicas[0].size(),
                "should allocate disk with 2 devices");

            const auto* group = state.FindPlacementGroup("disk-1/g");
            UNIT_ASSERT(group);
            UNIT_ASSERT_VALUES_EQUAL(2, group->DisksSize());

            error = state.AllocateDiskReplicas(Now(), db, "disk-1", 0);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should not let to allocate zero replicas");

            error = state.AllocateDiskReplicas(Now(), db, "undefined", 1);
            UNIT_ASSERT_EQUAL_C(E_NOT_FOUND, error.GetCode(),
                "should not let to allocate replicas for undefined disk");

            TVector<TDeviceConfig> nd;
            error = AllocateDisk(db, state, "not-mirrored-disk", {}, {}, 10_GB, nd);
            UNIT_ASSERT_SUCCESS(error);
            error = state.AllocateDiskReplicas(Now(), db, "not-mirrored-disk",
                1);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should not let to allocate replicas for a not mirrored disk");

            UNIT_ASSERT_VALUES_EQUAL("disk-1/0",
                group->GetDisks(0).GetDiskId());
            error = state.AllocateDiskReplicas(Now(), db, "disk-1/0", 1);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should not let to allocate replicas for a not master disk");

            error = state.AllocateDiskReplicas(Now(), db, "disk-1", 10);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should not let to allocate a lot of replicas which is above "
                "the limit");

            error = state.AllocateDiskReplicas(Now(), db, "disk-1", 1);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(),
                "should allocate one more replica");
            UNIT_ASSERT_VALUES_EQUAL_C(3, group->DisksSize(),
                "placement group size should be increased");
            TDiskInfo diskInfo;
            error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL_C(2, diskInfo.Replicas.size(),
                "disk's replicaCount should be increased");

            const auto prototypeReplicaDevices = diskInfo.Replicas[0];
            const auto addedReplicaDevices = diskInfo.Replicas[1];
            UNIT_ASSERT_VALUES_EQUAL_C(2, prototypeReplicaDevices.size(),
                "should have 2 devices");
            UNIT_ASSERT_VALUES_EQUAL_C(prototypeReplicaDevices.size(),
                addedReplicaDevices.size(),
                "should allocate same amount of devices as prototype");
            for (size_t i = 0; i < prototypeReplicaDevices.size(); i++) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    prototypeReplicaDevices[i].GetBlockSize(),
                    addedReplicaDevices[i].GetBlockSize(),
                    "added replica device should have same block size as the "
                    "prototype replica");
                UNIT_ASSERT_VALUES_EQUAL_C(
                    prototypeReplicaDevices[i].GetBlocksCount(),
                    addedReplicaDevices[i].GetBlocksCount(),
                    "added replica device should have same blocks count as the "
                    "prototype replica");
            }
        });
    }

    Y_UNIT_TEST(ShouldDeallocateReplicas)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                AgentConfig(1, {
                    Device("dev-1", "uuid-1", "rack-1"),
                    Device("dev-2", "uuid-2", "rack-1"),
                    Device("dev-3", "uuid-3", "rack-1"),
                }),
                AgentConfig(2, {
                    Device("dev-4", "uuid-4", "rack-2"),
                    Device("dev-5", "uuid-5", "rack-2"),
                    Device("dev-6", "uuid-6", "rack-2"),
                }),
                AgentConfig(3, {
                    Device("dev-7", "uuid-7", "rack-3"),
                    Device("dev-8", "uuid-8", "rack-3"),
                    Device("dev-9", "uuid-9", "rack-3"),
                }),
                AgentConfig(4, {
                    Device("dev-10", "uuid-10", "rack-4"),
                    Device("dev-11", "uuid-11", "rack-4"),
                    Device("dev-12", "uuid-12", "rack-4"),
                }),
                AgentConfig(5, {
                    Device("dev-13", "uuid-13", "rack-5"),
                    Device("dev-14", "uuid-14", "rack-5"),
                    Device("dev-15", "uuid-15", "rack-5"),
                }),
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> d;
            TVector<TVector<TDeviceConfig>> m;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> r;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                2,
                d,
                m,
                migrations,
                r);
            UNIT_ASSERT_SUCCESS(error);

            const auto* group = state.FindPlacementGroup("disk-1/g");
            UNIT_ASSERT(group);
            UNIT_ASSERT_VALUES_EQUAL(3, group->DisksSize());

            error = state.DeallocateDiskReplicas(db, "disk-1", 0);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should not let to deallocate zero replicas");

            error = state.DeallocateDiskReplicas(db, "undefined", 1);
            UNIT_ASSERT_EQUAL_C(E_NOT_FOUND, error.GetCode(),
                "should not let to deallocate replicas for undefined disk");

            TVector<TDeviceConfig> nd;
            error = AllocateDisk(db, state, "not-mirrored-disk", {}, {}, 10_GB, nd);
            UNIT_ASSERT_SUCCESS(error);
            error = state.DeallocateDiskReplicas(db, "not-mirrored-disk", 1);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(), "should not let "
                "to deallocate replicas for a not mirrored disk");

            UNIT_ASSERT_VALUES_EQUAL("disk-1/0",
                group->GetDisks(0).GetDiskId());
            error = state.DeallocateDiskReplicas(db, "disk-1/0", 1);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should not let to deallocate replicas for a not master disk");

            error = state.DeallocateDiskReplicas(db, "disk-1", 10);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(), "should not let "
                "to deallocate a not sufficient amount of replicas");

            error = state.DeallocateDiskReplicas(db, "disk-1", 2);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should not let to turn a mirrored disk to a simple one");

            error = state.DeallocateDiskReplicas(db, "disk-1", 1);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(),
                "should deallocate one replica");
            UNIT_ASSERT_VALUES_EQUAL_C(2, group->DisksSize(),
                "placement group size should be decreased");
            TDiskInfo diskInfo;
            error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL_C(1, diskInfo.Replicas.size(),
                "disk replicaCount should be decreased");
        });
    }

    Y_UNIT_TEST(ShouldUpdateReplicaCount)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                AgentConfig(1, {
                    Device("dev-1", "uuid-1", "rack-1"),
                    Device("dev-2", "uuid-2", "rack-1"),
                    Device("dev-3", "uuid-3", "rack-1"),
                }),
                AgentConfig(2, {
                    Device("dev-4", "uuid-4", "rack-2"),
                    Device("dev-5", "uuid-5", "rack-2"),
                    Device("dev-6", "uuid-6", "rack-2"),
                }),
                AgentConfig(3, {
                    Device("dev-7", "uuid-7", "rack-3"),
                    Device("dev-8", "uuid-8", "rack-3"),
                    Device("dev-9", "uuid-9", "rack-3"),
                }),
                AgentConfig(4, {
                    Device("dev-10", "uuid-10", "rack-4"),
                    Device("dev-11", "uuid-11", "rack-4"),
                    Device("dev-12", "uuid-12", "rack-4"),
                }),
                AgentConfig(5, {
                    Device("dev-13", "uuid-13", "rack-5"),
                    Device("dev-14", "uuid-14", "rack-5"),
                    Device("dev-15", "uuid-15", "rack-5"),
                }),
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskInfo diskInfo;

            TVector<TDeviceConfig> d;
            TVector<TVector<TDeviceConfig>> m;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> r;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                1,
                d,
                m,
                migrations,
                r);
            UNIT_ASSERT_SUCCESS(error);

            error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas.size());

            const auto* group = state.FindPlacementGroup("disk-1/g");
            UNIT_ASSERT(group);
            UNIT_ASSERT_VALUES_EQUAL(2, group->DisksSize());

            error = state.UpdateDiskReplicaCount(db, "undefined", 1);
            UNIT_ASSERT_EQUAL_C(E_NOT_FOUND, error.GetCode(),
                "should not let to deallocate replicas for undefined disk");

            TVector<TDeviceConfig> nd;
            error = AllocateDisk(db, state, "not-mirrored-disk", {}, {}, 10_GB, nd);
            UNIT_ASSERT_SUCCESS(error);
            error = state.UpdateDiskReplicaCount(db, "not-mirrored-disk", 2);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(), "should not let "
                "to deallocate replicas for a not mirrored disk");

            UNIT_ASSERT_VALUES_EQUAL("disk-1/0",
                group->GetDisks(0).GetDiskId());
            error = state.UpdateDiskReplicaCount(db, "disk-1/0", 2);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should not let to deallocate replicas for a not master disk");

            error = state.UpdateDiskReplicaCount(db, "disk-1", 1);
            UNIT_ASSERT_EQUAL_C(S_FALSE, error.GetCode(), "should not update "
                "the amount of replicas to the same one");

            error = state.UpdateDiskReplicaCount(db, "disk-1", 10);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(), "should not let "
                "to allocate a lot of replicas which is above the limit");

            error = state.UpdateDiskReplicaCount(db, "disk-1", 0);
            UNIT_ASSERT_EQUAL_C(E_ARGUMENT, error.GetCode(),
                "should not let to turn a mirrored disk to a simple one");

            error = state.UpdateDiskReplicaCount(db, "disk-1", 2);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(),
                "should allocate one replica");
            UNIT_ASSERT_VALUES_EQUAL_C(3, group->DisksSize(),
                "placement group size should be increased");
            error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL_C(2, diskInfo.Replicas.size(),
                "disk replicaCount should be updated");

            error = state.UpdateDiskReplicaCount(db, "disk-1", 1);
            UNIT_ASSERT_EQUAL_C(S_OK, error.GetCode(),
                "should deallocate one replica");
            UNIT_ASSERT_VALUES_EQUAL_C(2, group->DisksSize(),
                "placement group size should be decreased");
            error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL_C(1, diskInfo.Replicas.size(),
                "disk replicaCount should be updated");
        });
    }

    Y_UNIT_TEST(ShouldReplaceBrokenDevicesUponAgentFailure)
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
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", replicas[1][0].GetDeviceName());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        TDiskInfo diskInfo;
        auto error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("dev-1", diskInfo.Devices[0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[0].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-4",
            diskInfo.Replicas[0][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[1].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-7",
            diskInfo.Replicas[1][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(diskInfo.State));

        TDiskInfo replicaInfo;
        error = state.GetDiskInfo("disk-1/0", replicaInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(replicaInfo.State));

        replicaInfo = {};
        error = state.GetDiskInfo("disk-1/1", replicaInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(replicaInfo.State));

        replicaInfo = {};
        error = state.GetDiskInfo("disk-1/2", replicaInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(replicaInfo.State));

        const auto changeStateTs = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateAgentState(
                db,
                agentConfig1.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                changeStateTs,
                "unreachable",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        TVector<TString> disksToReallocate;
        TVector<TString> diskIdsWithUpdatedStates;
        TVector<NProto::TUserNotification> userNotifications;
        auto fetchDisksToNotify = [&] () {
            for (const auto& x: state.GetDisksToReallocate()) {
                disksToReallocate.push_back(x.first);
            }

            for (const auto& x: state.GetDiskStateUpdates()) {
                diskIdsWithUpdatedStates.push_back(x.State.GetDiskId());
            }

            state.GetUserNotifications(userNotifications);
        };

        auto deleteDisksToNotify = [&] () {
            executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
                // copying needed to avoid use-after-free upon deletion
                const auto disks = state.GetDisksToReallocate();
                for (const auto& x: disks) {
                    state.DeleteDiskToReallocate(db, x.first, x.second);
                }
            });
            disksToReallocate.clear();
        };

        fetchDisksToNotify();
        ASSERT_VECTORS_EQUAL(TVector<TString>{"disk-1"}, disksToReallocate);
        ASSERT_VECTORS_EQUAL(
            TVector<NProto::TUserNotification>{},
            userNotifications);
        ASSERT_VECTORS_EQUAL(TVector<TString>{}, diskIdsWithUpdatedStates);
        deleteDisksToNotify();

        diskInfo = {};
        error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("dev-10", diskInfo.Devices[0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[0].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-4",
            diskInfo.Replicas[0][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[1].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-7",
            diskInfo.Replicas[1][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(diskInfo.State));
        ASSERT_VECTORS_EQUAL(
            TVector<TString>{"uuid-10"},
            diskInfo.DeviceReplacementIds);

        auto dirtyDevices = state.GetDirtyDevices();
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyDevices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", dirtyDevices[0].GetDeviceUUID());

        const auto& device = state.GetDevice("uuid-1");
        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ONLINE,
            device.GetState());

        auto replaced = state.GetAutomaticallyReplacedDevices();
        UNIT_ASSERT_VALUES_EQUAL(1, replaced.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", replaced[0].DeviceId);
        UNIT_ASSERT_VALUES_EQUAL(changeStateTs, replaced[0].ReplacementTs);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeleteAutomaticallyReplacedDevices(db, changeStateTs);
            replaced = state.GetAutomaticallyReplacedDevices();
            UNIT_ASSERT_VALUES_EQUAL(0, replaced.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-10", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", replicas[1][0].GetDeviceName());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"uuid-10"},
                deviceReplacementIds);
        });

        fetchDisksToNotify();
        ASSERT_VECTORS_EQUAL(TVector<TString>{}, disksToReallocate);
        ASSERT_VECTORS_EQUAL(
            TVector<NProto::TUserNotification>{},
            userNotifications);
        ASSERT_VECTORS_EQUAL(TVector<TString>{}, diskIdsWithUpdatedStates);
        deleteDisksToNotify();

        auto now = TInstant::Seconds(1);

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto error = state.MarkReplacementDevice(
                now,
                db,
                "disk-1",
                "uuid-10",
                false);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            error = state.MarkReplacementDevice(
                now,
                db,
                "nonexistent-disk",
                "",
                false);
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());

            error = state.MarkReplacementDevice(
                now,
                db,
                "disk-1",
                "nonexistent-device",
                false);
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());

            error = state.MarkReplacementDevice(
                now,
                db,
                "disk-1",
                "uuid-1",
                false);
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());

            error = state.MarkReplacementDevice(
                now,
                db,
                "disk-1",
                "uuid-10",
                false);
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, error.GetCode());
        });

        fetchDisksToNotify();
        ASSERT_VECTORS_EQUAL(TVector<TString>{"disk-1"}, disksToReallocate);
        ASSERT_VECTORS_EQUAL(
            TVector<NProto::TUserNotification>{},
            userNotifications);
        ASSERT_VECTORS_EQUAL(TVector<TString>{}, diskIdsWithUpdatedStates);
        deleteDisksToNotify();

        diskInfo = {};
        error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("dev-10", diskInfo.Devices[0].GetDeviceName());
        ASSERT_VECTORS_EQUAL(
            TVector<TString>{},
            diskInfo.DeviceReplacementIds);

        // 1 autoreplacement + 1 unmark replacement
        UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.History.size());

        replicaInfo = {};
        error = state.GetDiskInfo("disk-1/0", replicaInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, replicaInfo.History.size());

        replicaInfo = {};
        error = state.GetDiskInfo("disk-1/1", replicaInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, replicaInfo.History.size());

        replicaInfo = {};
        error = state.GetDiskInfo("disk-1/2", replicaInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, replicaInfo.History.size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-10", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", replicas[1][0].GetDeviceName());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeallocateDisk(db, "disk-1");
        });
    }

    Y_UNIT_TEST(ShouldReplaceBrokenDevicesUponDeviceFailure)
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

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
            })
            .Build();

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
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-5", replicas[0][1].GetDeviceName());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            // mirrored disk replicas should not delay host/device maintenance
            // FIXME NBS-4169

            auto result = state.UpdateCmsDeviceState(
                db,
                agentConfig1.GetAgentId(),
                "dev-3",
                NProto::DEVICE_STATE_WARNING,
                Now(),
                false,  // shouldResumeDevice
                false); // dryRun

            UNIT_ASSERT_VALUES_EQUAL(S_OK, result.Error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), result.Timeout);
            ASSERT_VECTORS_EQUAL(TVector<TString>(), result.AffectedDisks);
        });

        TDiskInfo diskInfo;
        auto error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("dev-1", diskInfo.Devices[0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL("dev-2", diskInfo.Devices[1].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas[0].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-4",
            diskInfo.Replicas[0][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-5",
            diskInfo.Replicas[0][1].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(diskInfo.State));

        TDiskInfo replicaInfo;
        error = state.GetDiskInfo("disk-1/0", replicaInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(replicaInfo.State));

        replicaInfo = {};
        error = state.GetDiskInfo("disk-1/1", replicaInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(replicaInfo.State));

        const auto changeStateTs = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            TDuration timeout;
            auto error = state.UpdateDeviceState(
                db,
                "uuid-4",
                NProto::DEVICE_STATE_ERROR,
                changeStateTs,
                "io error",
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            UNIT_ASSERT_VALUES_EQUAL("", affectedDisk);
        });

        TVector<TString> disksToReallocate;
        TVector<TString> diskIdsWithUpdatedStates;
        TVector<NProto::TUserNotification> userNotifications;
        auto fetchDisksToNotify = [&] () {
            for (const auto& x: state.GetDisksToReallocate()) {
                disksToReallocate.push_back(x.first);
            }

            for (const auto& x: state.GetDiskStateUpdates()) {
                diskIdsWithUpdatedStates.push_back(x.State.GetDiskId());
            }

            state.GetUserNotifications(userNotifications);
        };

        auto deleteDisksToNotify = [&] () {
            executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
                // copying needed to avoid use-after-free upon deletion
                const auto disks = state.GetDisksToReallocate();
                for (const auto& x: disks) {
                    state.DeleteDiskToReallocate(db, x.first, x.second);
                }
            });
            disksToReallocate.clear();
        };

        fetchDisksToNotify();
        ASSERT_VECTORS_EQUAL(TVector<TString>{"disk-1"}, disksToReallocate);
        ASSERT_VECTORS_EQUAL(
            TVector<NProto::TUserNotification>{},
            userNotifications);
        ASSERT_VECTORS_EQUAL(TVector<TString>{}, diskIdsWithUpdatedStates);
        deleteDisksToNotify();

        auto dirtyDevices = state.GetDirtyDevices();
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyDevices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-4", dirtyDevices[0].GetDeviceUUID());

        const auto& device = state.GetDevice("uuid-4");
        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ERROR,
            device.GetState());

        auto replaced = state.GetAutomaticallyReplacedDevices();
        UNIT_ASSERT_VALUES_EQUAL(1, replaced.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-4", replaced[0].DeviceId);
        UNIT_ASSERT_VALUES_EQUAL(changeStateTs, replaced[0].ReplacementTs);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeleteAutomaticallyReplacedDevices(db, changeStateTs);
            replaced = state.GetAutomaticallyReplacedDevices();
            UNIT_ASSERT_VALUES_EQUAL(0, replaced.size());
        });

        diskInfo = {};
        error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("dev-1", diskInfo.Devices[0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL("dev-2", diskInfo.Devices[1].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas[0].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-6",
            diskInfo.Replicas[0][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-5",
            diskInfo.Replicas[0][1].GetDeviceName());
        ASSERT_VECTORS_EQUAL(
            TVector<TString>{"uuid-6"},
            diskInfo.DeviceReplacementIds);

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
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-6", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-5", replicas[0][1].GetDeviceName());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"uuid-6"},
                deviceReplacementIds);
        });

        fetchDisksToNotify();
        ASSERT_VECTORS_EQUAL(TVector<TString>{}, disksToReallocate);
        ASSERT_VECTORS_EQUAL(
            TVector<NProto::TUserNotification>{},
            userNotifications);
        ASSERT_VECTORS_EQUAL(TVector<TString>{}, diskIdsWithUpdatedStates);
        deleteDisksToNotify();

        auto now = TInstant::Seconds(1);

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            auto error =
                state.MarkReplacementDevice(now, db, "disk-1", "uuid-6", false);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        fetchDisksToNotify();
        ASSERT_VECTORS_EQUAL(TVector<TString>{"disk-1"}, disksToReallocate);
        ASSERT_VECTORS_EQUAL(
            TVector<NProto::TUserNotification>{},
            userNotifications);
        ASSERT_VECTORS_EQUAL(TVector<TString>{}, diskIdsWithUpdatedStates);
        deleteDisksToNotify();

        diskInfo = {};
        error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas[0].size());
        UNIT_ASSERT_VALUES_EQUAL("dev-6", diskInfo.Replicas[0][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL("dev-5", diskInfo.Replicas[0][1].GetDeviceName());
        ASSERT_VECTORS_EQUAL(
            TVector<TString>{},
            diskInfo.DeviceReplacementIds);

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
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-2", devices[1].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-6", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL("dev-5", replicas[0][1].GetDeviceName());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeallocateDisk(db, "disk-1");
        });
    }

    Y_UNIT_TEST(ShouldRemoveReplacementDeviceIdsUponReplacementDeviceReplacement)
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

        auto agentConfig5 = AgentConfig(5, {
            Device("dev-13", "uuid-13", "rack-5"),
            Device("dev-14", "uuid-14", "rack-5"),
            Device("dev-15", "uuid-15", "rack-5"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3,
                agentConfig4,
                agentConfig5,
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", replicas[1][0].GetDeviceName());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        const auto changeStateTs = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateAgentState(
                db,
                agentConfig1.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                changeStateTs,
                "unreachable",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        TDiskInfo diskInfo;
        auto error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("dev-10", diskInfo.Devices[0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[0].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-4",
            diskInfo.Replicas[0][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[1].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-7",
            diskInfo.Replicas[1][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(diskInfo.State));
        ASSERT_VECTORS_EQUAL(
            TVector<TString>{"uuid-10"},
            diskInfo.DeviceReplacementIds);

        auto dirtyDevices = state.GetDirtyDevices();
        UNIT_ASSERT_VALUES_EQUAL(1, dirtyDevices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", dirtyDevices[0].GetDeviceUUID());

        auto replaced = state.GetAutomaticallyReplacedDevices();
        UNIT_ASSERT_VALUES_EQUAL(1, replaced.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", replaced[0].DeviceId);
        UNIT_ASSERT_VALUES_EQUAL(changeStateTs, replaced[0].ReplacementTs);

        const auto changeStateTs2 = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateAgentState(
                db,
                agentConfig4.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                changeStateTs2,
                "unreachable",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        diskInfo = {};
        error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("dev-13", diskInfo.Devices[0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[0].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-4",
            diskInfo.Replicas[0][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[1].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-7",
            diskInfo.Replicas[1][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(diskInfo.State));
        ASSERT_VECTORS_EQUAL(
            TVector<TString>({"uuid-13"}),
            diskInfo.DeviceReplacementIds);

        dirtyDevices = state.GetDirtyDevices();
        SortBy(dirtyDevices, []  (const auto& d) {
            return d.GetDeviceUUID();
        });
        UNIT_ASSERT_VALUES_EQUAL(2, dirtyDevices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", dirtyDevices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-10", dirtyDevices[1].GetDeviceUUID());

        replaced = state.GetAutomaticallyReplacedDevices();
        UNIT_ASSERT_VALUES_EQUAL(2, replaced.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", replaced[0].DeviceId);
        UNIT_ASSERT_VALUES_EQUAL("uuid-10", replaced[1].DeviceId);
        UNIT_ASSERT_VALUES_EQUAL(changeStateTs, replaced[0].ReplacementTs);
        UNIT_ASSERT_VALUES_EQUAL(changeStateTs2, replaced[1].ReplacementTs);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeallocateDisk(db, "disk-1");
        });
    }

    Y_UNIT_TEST(ShouldTakeReplicaAvailabilityIntoAccount)
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
                agentConfig3,
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", replicas[0][0].GetDeviceName());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateAgentState(
                db,
                agentConfig1.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                Now(),
                "unreachable",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateAgentState(
                db,
                agentConfig2.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                Now(),
                "unreachable",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1/1", affectedDisks[0]);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskStateUpdates().size());
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::EDiskState_Name(NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE),
                NProto::EDiskState_Name(state.GetDiskState(affectedDisks[0]))
            );
        });

        // agent2 unavailability should not have caused device replacement

        TDiskInfo diskInfo;
        auto error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("dev-7", diskInfo.Devices[0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[0].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-4",
            diskInfo.Replicas[0][0].GetDeviceName());

        TDiskInfo replicaInfo;
        error = state.GetDiskInfo("disk-1/0", replicaInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(replicaInfo.State));

        replicaInfo = {};
        error = state.GetDiskInfo("disk-1/1", replicaInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE),
            NProto::EDiskState_Name(replicaInfo.State));

        TVector<TString> disksToReallocate;
        for (const auto& x: state.GetDisksToReallocate()) {
            disksToReallocate.push_back(x.first);
        }
        ASSERT_VECTORS_EQUAL(TVector<TString>{"disk-1"}, disksToReallocate);

        TVector<TString> diskIdsWithUpdatedStates;
        for (const auto& x: state.GetDiskStateUpdates()) {
            diskIdsWithUpdatedStates.push_back(x.State.GetDiskId());
        }
        ASSERT_VECTORS_EQUAL(TVector<TString>{}, diskIdsWithUpdatedStates);

        TVector<NProto::TUserNotification> userNotifications;
        state.GetUserNotifications(userNotifications);
        ASSERT_VECTORS_EQUAL(
            TVector<NProto::TUserNotification>{},
            userNotifications);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", replicas[0][0].GetDeviceName());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"uuid-7"},
                deviceReplacementIds);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeallocateDisk(db, "disk-1");
        });
    }

    Y_UNIT_TEST(ShouldBuildReplicaTableUponStateConstruction)
    {
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

        TVector<NProto::TDiskConfig> disks = {
            Disk("disk-1/0", {"uuid-1", "uuid-2", "uuid-3"}, NProto::DISK_STATE_ONLINE),
            Disk("disk-1/1", {"uuid-4", "uuid-5", "uuid-6"}, NProto::DISK_STATE_ONLINE),
            Disk("disk-1/2", {"uuid-7", "uuid-8", "uuid-9"}, NProto::DISK_STATE_ONLINE),
        };

        for (auto& disk: disks) {
            disk.SetMasterDiskId("disk-1");
        }

        disks.push_back(Disk("disk-1", {}, NProto::DISK_STATE_ONLINE));
        disks.back().SetReplicaCount(2);
        *disks.back().AddDeviceReplacementUUIDs() = "uuid-1";
        *disks.back().AddDeviceReplacementUUIDs() = "uuid-6";

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3,
            })
            .WithDisks(disks)
            .Build();

        const auto rt = GetReplicaTableRepr(state, "disk-1");

        UNIT_ASSERT_VALUES_EQUAL(
            "|uuid-1*|uuid-4|uuid-7|"
            "|uuid-2|uuid-5|uuid-8|"
            "|uuid-3|uuid-6*|uuid-9|",
            rt);
    }

    Y_UNIT_TEST(ShouldReturnAffectedDiskListOnAgentFailure)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
            Device("dev-2", "uuid-2", "rack-1"),
        });

        auto agentConfig2 = AgentConfig(2, {
            Device("dev-1", "uuid-3", "rack-2"),
            Device("dev-2", "uuid-4", "rack-2"),
        });

        auto agentConfig3 = AgentConfig(3, {
            Device("dev-1", "uuid-5", "rack-3"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3
            })
            .Build();

        // Create a mirror-2 disk
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
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][1].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        // All replicas must be online

        {
            TDiskInfo replicaInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/0", replicaInfo));
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
                NProto::EDiskState_Name(replicaInfo.State));
        }

        {
            TDiskInfo replicaInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/1", replicaInfo));
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
                NProto::EDiskState_Name(replicaInfo.State));
        }

        // Breaking the first agent

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateAgentState(
                db,
                agentConfig1.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                Now(),
                "unreachable",
                affectedDisks);

            // Now first replica (disk-1/0) should be broken

            TDiskInfo replicaInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/0", replicaInfo));
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::EDiskState_Name(NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE),
                NProto::EDiskState_Name(replicaInfo.State));

            // And second one (disk-1/1) should be OK

            replicaInfo = {};
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/1", replicaInfo));
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
                NProto::EDiskState_Name(replicaInfo.State));

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);

            // The state of 'disk-1/0' has been changed so 'disk-1/0' should be
            // in 'affectedDisks'
            ASSERT_VECTORS_EQUAL(TVector<TString>{"disk-1/0"}, affectedDisks);

            auto replaced = state.GetAutomaticallyReplacedDevices();
            UNIT_ASSERT_VALUES_EQUAL(1, replaced.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", replaced[0].DeviceId);

            UNIT_ASSERT_SUCCESS(state.MarkReplacementDevice(
                Now(),
                db,
                "disk-1",
                "uuid-5",
                false));
        });
    }

    Y_UNIT_TEST(ShouldProperlyCleanupAutomaticallyReplacedDevices)
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

        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(diskRegistryGroup)
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3,
                agentConfig4,
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                30_GB,
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", devices[2].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(3, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-6", replicas[0][2].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        const auto changeStateTs1 = Now();
        const auto changeStateTs2 = changeStateTs1 + TDuration::Hours(1);

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateAgentState(
                db,
                agentConfig1.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                changeStateTs1,
                "unreachable",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());

            error = state.MarkReplacementDevice(
                changeStateTs1,
                db,
                "disk-1",
                "uuid-7",
                false);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            error = state.MarkReplacementDevice(
                changeStateTs1,
                db,
                "disk-1",
                "uuid-8",
                false);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            error = state.MarkReplacementDevice(
                changeStateTs1,
                db,
                "disk-1",
                "uuid-9",
                false);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

            error = state.UpdateAgentState(
                db,
                agentConfig2.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                changeStateTs2,
                "unreachable",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());

            error = state.MarkReplacementDevice(
                changeStateTs2,
                db,
                "disk-1",
                "uuid-10",
                false);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            error = state.MarkReplacementDevice(
                changeStateTs2,
                db,
                "disk-1",
                "uuid-11",
                false);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            error = state.MarkReplacementDevice(
                changeStateTs2,
                db,
                "disk-1",
                "uuid-12",
                false);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        });

        TDiskInfo diskInfo;
        auto error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(3, diskInfo.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-7", diskInfo.Devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-8", diskInfo.Devices[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-9", diskInfo.Devices[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(3, diskInfo.Replicas[0].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-10",
            diskInfo.Replicas[0][0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-11",
            diskInfo.Replicas[0][1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-12",
            diskInfo.Replicas[0][2].GetDeviceUUID());
        ASSERT_VECTORS_EQUAL(TVector<TString>(), diskInfo.DeviceReplacementIds);

        diskInfo = {};
        error = state.GetDiskInfo("disk-1/0", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        // 3 x replacement
        UNIT_ASSERT_VALUES_EQUAL(3, diskInfo.History.size());

        diskInfo = {};
        error = state.GetDiskInfo("disk-1/1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        // 3 x replacement
        UNIT_ASSERT_VALUES_EQUAL(3, diskInfo.History.size());

        auto dirtyDevices = state.GetDirtyDevices();
        UNIT_ASSERT_VALUES_EQUAL(6, dirtyDevices.size());
        SortBy(dirtyDevices, [] (const auto& d) {
            return d.GetDeviceUUID();
        });
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", dirtyDevices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-2", dirtyDevices[1].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3", dirtyDevices[2].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-4", dirtyDevices[3].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-5", dirtyDevices[4].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-6", dirtyDevices[5].GetDeviceUUID());

        auto device = state.GetDevice("uuid-1");
        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ONLINE,
            device.GetState());
        device = state.GetDevice("uuid-2");
        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ONLINE,
            device.GetState());
        device = state.GetDevice("uuid-3");
        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ONLINE,
            device.GetState());
        device = state.GetDevice("uuid-4");
        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ONLINE,
            device.GetState());
        device = state.GetDevice("uuid-5");
        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ONLINE,
            device.GetState());
        device = state.GetDevice("uuid-6");
        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ONLINE,
            device.GetState());

        auto replaced = state.GetAutomaticallyReplacedDevices();
        UNIT_ASSERT_VALUES_EQUAL(6, replaced.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", replaced[0].DeviceId);
        UNIT_ASSERT_VALUES_EQUAL("uuid-2", replaced[1].DeviceId);
        UNIT_ASSERT_VALUES_EQUAL("uuid-3", replaced[2].DeviceId);
        UNIT_ASSERT_VALUES_EQUAL("uuid-4", replaced[3].DeviceId);
        UNIT_ASSERT_VALUES_EQUAL("uuid-5", replaced[4].DeviceId);
        UNIT_ASSERT_VALUES_EQUAL("uuid-6", replaced[5].DeviceId);
        UNIT_ASSERT_VALUES_EQUAL(changeStateTs1, replaced[0].ReplacementTs);
        UNIT_ASSERT_VALUES_EQUAL(changeStateTs1, replaced[1].ReplacementTs);
        UNIT_ASSERT_VALUES_EQUAL(changeStateTs1, replaced[2].ReplacementTs);
        UNIT_ASSERT_VALUES_EQUAL(changeStateTs2, replaced[3].ReplacementTs);
        UNIT_ASSERT_VALUES_EQUAL(changeStateTs2, replaced[4].ReplacementTs);
        UNIT_ASSERT_VALUES_EQUAL(changeStateTs2, replaced[5].ReplacementTs);

        auto deviceCounter =
            diskRegistryGroup->GetCounter("AutomaticallyReplacedDevices");

        state.PublishCounters(Now());
        UNIT_ASSERT_VALUES_EQUAL(deviceCounter->Val(), 6);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeleteAutomaticallyReplacedDevices(db, changeStateTs1);
            replaced = state.GetAutomaticallyReplacedDevices();
            UNIT_ASSERT_VALUES_EQUAL(3, replaced.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replaced[0].DeviceId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replaced[1].DeviceId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-6", replaced[2].DeviceId);
            UNIT_ASSERT_VALUES_EQUAL(changeStateTs2, replaced[0].ReplacementTs);
            UNIT_ASSERT_VALUES_EQUAL(changeStateTs2, replaced[1].ReplacementTs);
            UNIT_ASSERT_VALUES_EQUAL(changeStateTs2, replaced[2].ReplacementTs);

            state.PublishCounters(Now());
            UNIT_ASSERT_VALUES_EQUAL(deviceCounter->Val(), 3);

            state.DeleteAutomaticallyReplacedDevices(db, changeStateTs2);
            replaced = state.GetAutomaticallyReplacedDevices();
            UNIT_ASSERT_VALUES_EQUAL(0, replaced.size());

            state.PublishCounters(Now());
            UNIT_ASSERT_VALUES_EQUAL(deviceCounter->Val(), 0);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeallocateDisk(db, "disk-1");
        });
    }

    void DoTestShouldWaitForReplicaMigrationUponCmsRemoveRequest(
        bool isAgent)
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
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        const auto changeStateTs = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            NProto::TError error;
            TDuration timeout;

            if (isAgent) {
                TVector<TString> affectedDisks;
                error = state.UpdateCmsHostState(
                    db,
                    agentConfig1.GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    changeStateTs,
                    false,  // dryRun
                    affectedDisks,
                    timeout);

                ASSERT_VECTORS_EQUAL(TVector{"disk-1/0"}, affectedDisks);
            } else {
                auto result = state.UpdateCmsDeviceState(
                    db,
                    agentConfig1.GetAgentId(),
                    "dev-1",
                    NProto::DEVICE_STATE_WARNING,
                    changeStateTs,
                    false,  // shouldResumeDevice
                    false); // dryRun

                error = result.Error;
                timeout = result.Timeout;

                ASSERT_VECTORS_EQUAL(TVector{"disk-1/0"}, result.AffectedDisks);
            }

            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Days(1), timeout);

            const auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL("disk-1/0", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", migrations[0].SourceDeviceId);
            auto r = state.StartDeviceMigration(
                changeStateTs,
                db,
                "disk-1/0",
                "uuid-1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, r.GetError().GetCode());
        });

        const auto targetUuid = isAgent ? "uuid-10" : "uuid-2";

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                migrations[0].GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                targetUuid,
                migrations[0].GetTargetDevice().GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(TVector<TString>(), deviceReplacementIds);

            bool diskStateUpdated = false;
            state.FinishDeviceMigration(
                db,
                "disk-1/0",
                "uuid-1",
                targetUuid,
                Now(),
                &diskStateUpdated);

            UNIT_ASSERT(diskStateUpdated);

            error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(targetUuid, devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, migrations.size());
            ASSERT_VECTORS_EQUAL(TVector<TString>(), deviceReplacementIds);

            state.DeleteDiskToReallocate(db, "disk-1", 4);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            NProto::TError error;
            TDuration timeout;

            if (isAgent) {
                TVector<TString> affectedDisks;
                error = state.UpdateCmsHostState(
                    db,
                    agentConfig1.GetAgentId(),
                    NProto::AGENT_STATE_WARNING,
                    changeStateTs,
                    false,  // dryRun
                    affectedDisks,
                    timeout);

                ASSERT_VECTORS_EQUAL(TVector<TString>(), affectedDisks);
            } else {
                auto result = state.UpdateCmsDeviceState(
                    db,
                    agentConfig1.GetAgentId(),
                    "dev-1",
                    NProto::DEVICE_STATE_WARNING,
                    changeStateTs,
                    false,  // shouldResumeDevice
                    false); // dryRun

                error = result.Error;
                timeout = result.Timeout;

                ASSERT_VECTORS_EQUAL(TVector<TString>(), result.AffectedDisks);
            }

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
        });

        TDiskInfo diskInfo;
        state.GetDiskInfo("disk-1/0", diskInfo);
        // state change + migration start + migration finish + state change
        UNIT_ASSERT_VALUES_EQUAL(4, diskInfo.History.size());

        diskInfo = {};
        state.GetDiskInfo("disk-1/1", diskInfo);
        // state change + migration start + migration finish + state change
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.History.size());

        diskInfo = {};
        state.GetDiskInfo("disk-1/2", diskInfo);
        // state change + migration start + migration finish + state change
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.History.size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeallocateDisk(db, "disk-1");
        });
    }

    Y_UNIT_TEST(ShouldWaitForReplicaMigrationUponCmsRemoveHostRequest)
    {
        DoTestShouldWaitForReplicaMigrationUponCmsRemoveRequest(true);
    }

    Y_UNIT_TEST(ShouldWaitForReplicaMigrationUponCmsRemoveDeviceRequest)
    {
        DoTestShouldWaitForReplicaMigrationUponCmsRemoveRequest(false);
    }

    Y_UNIT_TEST(ShouldReportDeviceReplacementFailureUponAgentFailure)
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

        // intentionally setting agent count to 3 to get E_DISK_ALLOCATION_FAILED
        // upon ReplaceDevice call
        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3,
            })
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("dev-1", devices[0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-4", replicas[0][0].GetDeviceName());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("dev-7", replicas[1][0].GetDeviceName());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        auto monitoring = CreateMonitoringServiceStub();
        auto rootGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto serverGroup = rootGroup->GetSubgroup("component", "server");
        InitCriticalEventsCounter(serverGroup);

        auto criticalEvents = serverGroup->FindCounter(
            "AppCriticalEvents/MirroredDiskDeviceReplacementFailure");

        UNIT_ASSERT_VALUES_EQUAL(0, criticalEvents->Val());

        const auto changeStateTs = Now();

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/0", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::EDiskState_Name(
                    NProto::DISK_STATE_ONLINE),
                NProto::EDiskState_Name(diskInfo.State));

            TVector<TString> affectedDisks;
            TDuration timeout;
            auto error = state.UpdateAgentState(
                db,
                agentConfig1.GetAgentId(),
                NProto::AGENT_STATE_UNAVAILABLE,
                changeStateTs,
                "unreachable",
                affectedDisks);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            ASSERT_VECTORS_EQUAL(TVector<TString>{"disk-1/0"}, affectedDisks);

            diskInfo = {};
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/0", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::EDiskState_Name(
                    NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE),
                NProto::EDiskState_Name(diskInfo.State));
        });

        UNIT_ASSERT_VALUES_EQUAL(1, criticalEvents->Val());

        TVector<TString> disksToReallocate;
        TVector<TString> diskIdsWithUpdatedStates;
        TVector<NProto::TUserNotification> userNotifications;
        auto fetchDisksToNotify = [&] () {
            for (const auto& x: state.GetDisksToReallocate()) {
                disksToReallocate.push_back(x.first);
            }

            for (const auto& x: state.GetDiskStateUpdates()) {
                diskIdsWithUpdatedStates.push_back(x.State.GetDiskId());
            }

            state.GetUserNotifications(userNotifications);
        };

        auto deleteDisksToNotify = [&] () {
            executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
                // copying needed to avoid use-after-free upon deletion
                const auto disks = state.GetDisksToReallocate();
                for (const auto& x: disks) {
                    state.DeleteDiskToReallocate(db, x.first, x.second);
                }
            });
            disksToReallocate.clear();
        };

        fetchDisksToNotify();
        ASSERT_VECTORS_EQUAL(TVector<TString>{"disk-1"}, disksToReallocate);
        ASSERT_VECTORS_EQUAL(
            TVector<NProto::TUserNotification>{},
            userNotifications);
        ASSERT_VECTORS_EQUAL(TVector<TString>{}, diskIdsWithUpdatedStates);
        deleteDisksToNotify();

        TDiskInfo diskInfo;
        auto error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL("dev-1", diskInfo.Devices[0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
        UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[0].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-4",
            diskInfo.Replicas[0][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[1].size());
        UNIT_ASSERT_VALUES_EQUAL(
            "dev-7",
            diskInfo.Replicas[1][0].GetDeviceName());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(diskInfo.State));
        ASSERT_VECTORS_EQUAL(TVector<TString>{}, diskInfo.DeviceReplacementIds);

        auto dirtyDevices = state.GetDirtyDevices();
        UNIT_ASSERT_VALUES_EQUAL(0, dirtyDevices.size());

        auto replaced = state.GetAutomaticallyReplacedDevices();
        UNIT_ASSERT_VALUES_EQUAL(0, replaced.size());

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeallocateDisk(db, "disk-1");
        });
    }

    Y_UNIT_TEST(ShouldReportDeviceReplacementFailureUponDeviceFailure)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto agentConfig1 = AgentConfig(1, {
            Device("dev-1", "uuid-1", "rack-1"),
        });

        auto agentConfig2 = AgentConfig(2, {
            Device("dev-1", "uuid-2", "rack-2"),
        });

        auto agentConfig3 = AgentConfig(3, {
            Device("dev-1", "uuid-3", "rack-3"),
        });

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
                agentConfig3,
            })
            .Build();

        // Creating a mirror-3 (one device per replica)

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                2,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", replicas[1][0].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        auto monitoring = CreateMonitoringServiceStub();
        auto rootGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto serverGroup = rootGroup->GetSubgroup("component", "server");
        InitCriticalEventsCounter(serverGroup);

        auto criticalEvents = serverGroup->FindCounter(
            "AppCriticalEvents/MirroredDiskDeviceReplacementFailure");

        UNIT_ASSERT_VALUES_EQUAL(0, criticalEvents->Val());

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {

            // A first replica should be OK

            TDiskInfo diskInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/0", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::EDiskState_Name(
                    NProto::DISK_STATE_ONLINE),
                NProto::EDiskState_Name(diskInfo.State));

            // Breakin a device

            TString affectedDisk;
            TDuration timeout;
            auto error = state.UpdateDeviceState(
                db,
                "uuid-1",
                NProto::DEVICE_STATE_ERROR,
                Now(),
                "test",
                affectedDisk);

            // Now the first replca is not OK, its state has been changed to
            // ERROR so we should see disk-1/0 in affectedDisk

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            UNIT_ASSERT_VALUES_EQUAL("disk-1/0", affectedDisk);

            // Ensuring that disk-1/0 is broken

            diskInfo = {};
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/0", diskInfo));
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::EDiskState_Name(
                    NProto::DISK_STATE_ERROR),
                NProto::EDiskState_Name(diskInfo.State));
        });

        UNIT_ASSERT_VALUES_EQUAL(1, criticalEvents->Val());
    }

    Y_UNIT_TEST(ShouldStopAutomaticReplacementIfReplacementRateIsTooHigh)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        /*
         *  Configuring many agents to be able to break many agents and cause
         *  many device replacements.
         */

        TVector<NProto::TAgentConfig> agents;
        for (ui32 i = 1; i <= 20; ++i) {
            agents.push_back(AgentConfig(i, {Device(
                Sprintf("dev-%u", i),
                Sprintf("uuid-%u", i),
                Sprintf("rack-%u", i)
            )}));
        }

        NProto::TStorageServiceConfig proto;
        proto.SetMaxAutomaticDeviceReplacementsPerHour(10);
        proto.SetAllocationUnitNonReplicatedSSD(10);
        auto storageConfig = CreateStorageConfig(proto);

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(storageConfig)
            .WithKnownAgents(agents)
            .Build();

        /*
         *  Creating a small mirror-2 disk that will use 2 agents.
         */

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            auto error = AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                10_GB,
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas[0].size());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        auto monitoring = CreateMonitoringServiceStub();
        auto rootGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto serverGroup = rootGroup->GetSubgroup("component", "server");
        InitCriticalEventsCounter(serverGroup);

        auto criticalEvents = serverGroup->FindCounter(
            "AppCriticalEvents/MirroredDiskDeviceReplacementRateLimitExceeded");

        UNIT_ASSERT_VALUES_EQUAL(0, criticalEvents->Val());

        /*
         *  10 agent failures should cause 10 replacements which should work
         *  just fine.
         */

        auto changeStateTs = Now();

        TVector<TString> expectedReplacedDeviceIds;

        auto breakAgent = [&] (const TString& agentId) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
                TVector<TString> affectedDisks;
                TDuration timeout;
                auto error = state.UpdateAgentState(
                    db,
                    agentId,
                    NProto::AGENT_STATE_UNAVAILABLE,
                    changeStateTs,
                    "unreachable",
                    affectedDisks);

                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            });
        };

        auto fixAgent = [&] (const TString& agentId) {
            executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
                TVector<TString> affectedDisks;
                TDuration timeout;
                auto error = state.UpdateAgentState(
                    db,
                    agentId,
                    NProto::AGENT_STATE_ONLINE,
                    changeStateTs,
                    "fixed",
                    affectedDisks);

                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
                UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), timeout);
            });
        };

        for (ui32 i = 2; i <= 11; ++i) {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[0].size());
            const auto deviceId = diskInfo.Replicas[0][0].GetDeviceUUID();
            const auto agentId = diskInfo.Replicas[0][0].GetAgentId();

            changeStateTs += TDuration::Minutes(5);

            breakAgent(agentId);

            expectedReplacedDeviceIds.push_back(deviceId);
            TVector<TString> replacedDeviceIds;
            for (const auto& d: state.GetAutomaticallyReplacedDevices()) {
                replacedDeviceIds.push_back(d.DeviceId);
            }
            ASSERT_VECTORS_EQUAL(expectedReplacedDeviceIds, replacedDeviceIds);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, criticalEvents->Val());

        /*
         *  11th agent failure within the same hour should cause a critical
         *  event. Replacement should not happen.
         */

        TDiskInfo diskInfo;
        const auto error = state.GetDiskInfo("disk-1", diskInfo);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas.size());
        UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.Replicas[0].size());
        const auto agentId = diskInfo.Replicas[0][0].GetAgentId();
        const auto deviceId = diskInfo.Replicas[0][0].GetDeviceUUID();

        breakAgent(agentId);
        UNIT_ASSERT_VALUES_EQUAL(1, criticalEvents->Val());

        TVector<TString> replacedDeviceIds;
        for (const auto& d: state.GetAutomaticallyReplacedDevices()) {
            replacedDeviceIds.push_back(d.DeviceId);
        }
        ASSERT_VECTORS_EQUAL(expectedReplacedDeviceIds, replacedDeviceIds);

        /*
         *  After a while automatic replacements should be enabled again.
         */
        changeStateTs += TDuration::Minutes(15);

        fixAgent(agentId);
        breakAgent(agentId);
        UNIT_ASSERT_VALUES_EQUAL(1, criticalEvents->Val());

        expectedReplacedDeviceIds.push_back(deviceId);
        replacedDeviceIds.clear();
        for (const auto& d: state.GetAutomaticallyReplacedDevices()) {
            replacedDeviceIds.push_back(d.DeviceId);
        }
        ASSERT_VECTORS_EQUAL(expectedReplacedDeviceIds, replacedDeviceIds);
    }

    Y_UNIT_TEST(ShouldReportRateLimitExceededCritEventUponDeviceFailure)
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
            Device("dev-1", "uuid-4", "rack-2"),
            Device("dev-2", "uuid-5", "rack-2"),
            Device("dev-3", "uuid-6", "rack-2"),
        });

        NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetAllocationUnitNonReplicatedSSD(10);
        storageConfig.SetMaxAutomaticDeviceReplacementsPerHour(1);

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .With(CreateStorageConfig(storageConfig))
            .WithKnownAgents({
                agentConfig1,
                agentConfig2,
            })
            .Build();

        auto monitoring = CreateMonitoringServiceStub();
        auto rootGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto serverGroup = rootGroup->GetSubgroup("component", "server");
        InitCriticalEventsCounter(serverGroup);

        auto criticalEvents = serverGroup->FindCounter(
            "AppCriticalEvents/MirroredDiskDeviceReplacementRateLimitExceeded");

        UNIT_ASSERT_VALUES_EQUAL(0, criticalEvents->Val());

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
                1,
                devices,
                replicas,
                migrations,
                deviceReplacementIds);
            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            auto error = state.UpdateDeviceState(
                db,
                "uuid-1",
                NProto::DEVICE_STATE_ERROR,
                Now(),
                "test",  // reason
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL("", affectedDisk);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            TString affectedDisk;
            auto error = state.UpdateDeviceState(
                db,
                "uuid-2",
                NProto::DEVICE_STATE_ERROR,
                Now(),
                "test",  // reason
                affectedDisk);

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL("disk-1/0", affectedDisk);
        });

        TDiskInfo info;
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/0", info));

        UNIT_ASSERT_VALUES_UNEQUAL("uuid-1", info.Devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-2", info.Devices[1].GetDeviceUUID());

        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ONLINE,
            info.Devices[0].GetState());

        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ERROR,
            info.Devices[1].GetState());

        UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ERROR, info.State);

        info = {};
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/1", info));
        UNIT_ASSERT_VALUES_EQUAL("uuid-4", info.Devices[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL("uuid-5", info.Devices[1].GetDeviceUUID());

        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ONLINE,
            info.Devices[0].GetState());

        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ONLINE,
            info.Devices[1].GetState());

        UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, info.State);

        UNIT_ASSERT_VALUES_EQUAL(1, criticalEvents->Val());
    }

    Y_UNIT_TEST(ShouldReplaceDeviceOnAgentRegistration)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        TVector agents {
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
            AgentConfig(3, {
                Device("dev-1", "uuid-3.1", "rack-3"),
                Device("dev-2", "uuid-3.2", "rack-3"),
                Device("dev-3", "uuid-3.3", "rack-3"),
            })
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithKnownAgents(agents)
            .Build();

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            UNIT_ASSERT_SUCCESS(AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                20_GB,
                1,  // replicaCount
                devices,
                replicas,
                migrations,
                deviceReplacementIds));
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, deviceReplacementIds.size());
        });

        TDiskInfo diskInfo;
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(diskInfo.State));

        TDiskInfo replicaInfo;
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/0", replicaInfo));
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(replicaInfo.State));

        replicaInfo = {};
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/1", replicaInfo));
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(replicaInfo.State));

        const auto changeStateTs = Now();

        // break uuid-1.1 & uuid-1.2
        agents[0].MutableDevices(0)->SetBlocksCount(1);
        agents[0].MutableDevices(1)->SetBlocksCount(1);

        executor.WriteTx([&] (TDiskRegistryDatabase db) mutable {
            UNIT_ASSERT_SUCCESS(
                state.RegisterAgent(db, agents[0], changeStateTs).GetError());
        });

        diskInfo = {};
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", diskInfo));
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(diskInfo.State));
        ASSERT_VECTORS_EQUAL(
            TVector<TString>({"uuid-1.3", "uuid-3.1"}),
            diskInfo.DeviceReplacementIds);

        replicaInfo = {};
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/0", replicaInfo));
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(replicaInfo.State));

        replicaInfo = {};
        UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/1", replicaInfo));
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::EDiskState_Name(NProto::DISK_STATE_ONLINE),
            NProto::EDiskState_Name(replicaInfo.State));

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TDeviceConfig> devices;
            TVector<TVector<TDeviceConfig>> replicas;
            TVector<NProto::TDeviceMigration> migrations;
            TVector<TString> deviceReplacementIds;
            UNIT_ASSERT_SUCCESS(AllocateMirroredDisk(
                db,
                state,
                "disk-1",
                20_GB,
                1,  // replicaCount
                devices,
                replicas,
                migrations,
                deviceReplacementIds));
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            // uuid-1.1 was automatically replaced by uuid-1.3
            UNIT_ASSERT_VALUES_EQUAL("uuid-1.3", devices[0].GetDeviceUUID());
            // uuid-1.2 was automatically replaced by uuid-3.1
            UNIT_ASSERT_VALUES_EQUAL("uuid-3.1", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.1", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2.2", replicas[0][1].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>({"uuid-1.3", "uuid-3.1"}),
                deviceReplacementIds);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            state.DeallocateDisk(db, "disk-1");
        });
    }
}

bool operator==(const TDiskStateUpdate& l, const TDiskStateUpdate& r)
{
    return l.State.SerializeAsString() == r.State.SerializeAsString()
        && l.SeqNo == r.SeqNo;
}

}   // namespace NCloud::NBlockStore::NStorage

template <>
inline void Out<NCloud::NBlockStore::NStorage::TDiskStateUpdate>(
    IOutputStream& out,
    const NCloud::NBlockStore::NStorage::TDiskStateUpdate& update)
{
    out << update.State.DebugString() << "\t" << update.SeqNo;
}

////////////////////////////////////////////////////////////////////////////////

namespace NCloud::NBlockStore::NProto {

static bool operator==(
    const NCloud::NBlockStore::NProto::TUserNotification& l,
    const NCloud::NBlockStore::NProto::TUserNotification& r)
{
    return  google::protobuf::util::MessageDifferencer::Equals(l, r);
}

}   // NCloud::NBlockStore::NProto
