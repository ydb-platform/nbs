#include "disk_registry_state.h"

#include "disk_registry_database.h"

#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NDiskRegistryStateTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TVector<NProto::TAgentConfig> Agents {
    AgentConfig(1, {
        Device("dev-1", "uuid-1.1", "rack-1"),
        Device("dev-2", "uuid-1.2", "rack-1"),
    }),
    AgentConfig(2, {
        Device("dev-1", "uuid-2.1", "rack-2"),
        Device("dev-2", "uuid-2.2", "rack-2"),
    }),
    AgentConfig(3, {
        Device("dev-1", "uuid-3.1", "rack-3"),
        Device("dev-2", "uuid-3.2", "rack-3"),
    }),
};

auto Params(
    TString diskId,
    ui64 ownerVolumeTabletId,
    ui32 replicaCount = 0,
    ui64 totalSize = DefaultDeviceSize)
{
    return TDiskRegistryState::TAllocateDiskParams{
        .DiskId = std::move(diskId),
        .CloudId = "cloud-1",
        .FolderId = "folder-1",
        .BlockSize = DefaultLogicalBlockSize,
        .BlocksCount = totalSize / DefaultLogicalBlockSize,
        .ReplicaCount = replicaCount,
        .MediaKind = replicaCount
            ? NProto::STORAGE_MEDIA_SSD_MIRROR2
            : NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
        .OwnerVolumeTabletId = ownerVolumeTabletId,
    };
}

NProto::TError AllocateDisk(
    TDiskRegistryDatabase& db,
    TDiskRegistryState& state,
    const TDiskRegistryState::TAllocateDiskParams& params)
{
    TDiskRegistryState::TAllocateDiskResult result;
    return state.AllocateDisk(TInstant::Seconds(100), db, params, &result);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateOwnerVolumeTest)
{
    Y_UNIT_TEST(ShouldPersistOwnerVolumeTabletId)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto statePtr = TDiskRegistryStateBuilder()
            .WithKnownAgents(Agents)
            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(AllocateDisk(db, state, Params("nbs", 0)));
            UNIT_ASSERT_SUCCESS(AllocateDisk(db, state, Params("fs", 42)));
            UNIT_ASSERT_SUCCESS(AllocateDisk(db, state, Params("fs-m2", 43, 1)));
        });

        auto check = [] (const TDiskRegistryState& state) {
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetOwnerVolumeTabletId("nbs"));
            UNIT_ASSERT_VALUES_EQUAL(42, state.GetOwnerVolumeTabletId("fs"));
            UNIT_ASSERT_VALUES_EQUAL(43, state.GetOwnerVolumeTabletId("fs-m2"));
            UNIT_ASSERT_VALUES_EQUAL(43, state.GetOwnerVolumeTabletId("fs-m2/0"));
            UNIT_ASSERT_VALUES_EQUAL(43, state.GetOwnerVolumeTabletId("fs-m2/1"));
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetOwnerVolumeTabletId("unknown"));

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("fs", info));
            UNIT_ASSERT_VALUES_EQUAL(42, info.OwnerVolumeTabletId);
        };

        check(state);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto reloaded = TDiskRegistryStateBuilder::LoadState(db)
                .WithKnownAgents(Agents)
                .Build();
            check(*reloaded);
        });

        const auto backup = state.BackupState();
        for (const auto& disk: backup.GetDisks()) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                state.GetOwnerVolumeTabletId(disk.GetDiskId()),
                disk.GetOwnerVolumeTabletId(),
                disk.GetDiskId());
        }
    }

    Y_UNIT_TEST(ShouldRejectOwnerChangeOnReallocation)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto statePtr = TDiskRegistryStateBuilder()
            .WithKnownAgents(Agents)
            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(AllocateDisk(db, state, Params("nbs", 0)));
            UNIT_ASSERT_SUCCESS(AllocateDisk(db, state, Params("fs", 42)));

            for (auto [diskId, owner]: {
                std::pair{"nbs", 42},
                std::pair{"fs", 0},
                std::pair{"fs", 43}})
            {
                auto error = AllocateDisk(db, state, Params(diskId, owner));
                UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
            }

            // The rejected requests must not touch the existing disks.
            UNIT_ASSERT_VALUES_EQUAL(
                S_ALREADY,
                AllocateDisk(db, state, Params("nbs", 0)).GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                S_ALREADY,
                AllocateDisk(db, state, Params("fs", 42)).GetCode());
            UNIT_ASSERT_VALUES_EQUAL(42, state.GetOwnerVolumeTabletId("fs"));
            UNIT_ASSERT(state.GetBrokenDisks().empty());
        });
    }

    Y_UNIT_TEST(ShouldNotScheduleVolumeDestructionForExternalDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto statePtr = TDiskRegistryStateBuilder()
            .WithKnownAgents(Agents)
            .Build();
        TDiskRegistryState& state = *statePtr;

        const ui64 tooBig = 10 * DefaultDeviceSize;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                AllocateDisk(db, state, Params("fs", 42, 0, tooBig)).GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                AllocateDisk(db, state, Params("fs-m2", 43, 1, tooBig)).GetCode());

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskCount());
            UNIT_ASSERT(state.GetBrokenDisks().empty());

            // Native volumes are still scheduled for destruction.
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                AllocateDisk(db, state, Params("nbs", 0, 0, tooBig)).GetCode());
            UNIT_ASSERT(state.GetBrokenDisks().contains("nbs"));
        });
    }

    Y_UNIT_TEST(ShouldCheckOwnerOnCleanupAndDeallocation)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto statePtr = TDiskRegistryStateBuilder()
            .WithKnownAgents(Agents)
            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(AllocateDisk(db, state, Params("fs", 42)));

            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                state.MarkDiskForCleanup(db, "fs", 0).GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                state.MarkDiskForCleanup(db, "fs", 43).GetCode());
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "fs", 42));

            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                state.DeallocateDisk(db, "fs", 0).GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                state.DeallocateDisk(db, "fs", 43).GetCode());
            UNIT_ASSERT_VALUES_EQUAL(42, state.GetOwnerVolumeTabletId("fs"));

            UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "fs", 42));
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDiskCount());

            // A missing disk is not an ownership violation.
            UNIT_ASSERT_VALUES_EQUAL(
                S_ALREADY,
                state.DeallocateDisk(db, "fs", 0).GetCode());
        });
    }

    Y_UNIT_TEST(ShouldRejectCheckpointAndBlockSizeChangeForExternalDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto statePtr = TDiskRegistryStateBuilder()
            .WithKnownAgents(Agents)
            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(AllocateDisk(db, state, Params("fs", 42)));

            TDiskRegistryState::TAllocateCheckpointResult result;
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                state.AllocateCheckpoint(Now(), db, "fs", "cp", &result)
                    .GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetDiskCount());

            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                state.UpdateDiskBlockSize(Now(), db, "fs", 8_KB, true)
                    .GetCode());

            TDiskInfo info;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("fs", info));
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                info.LogicalBlockSize);
        });
    }

    Y_UNIT_TEST(ShouldNotScheduleVolumeConfigUpdateForExternalDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        auto statePtr = TDiskRegistryStateBuilder()
            .WithKnownAgents(Agents)
            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.CreatePlacementGroup(
                db,
                "pg",
                NProto::PLACEMENT_STRATEGY_SPREAD,
                0));

            auto nbs = Params("nbs", 0);
            nbs.PlacementGroupId = "pg";
            UNIT_ASSERT_SUCCESS(AllocateDisk(db, state, nbs));

            auto fs = Params("fs", 42);
            fs.PlacementGroupId = "pg";
            UNIT_ASSERT_SUCCESS(AllocateDisk(db, state, fs));

            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(
                state.DestroyPlacementGroup(db, "pg", affectedDisks));
            UNIT_ASSERT_VALUES_EQUAL(2, affectedDisks.size());

            // Only the native volume has a config in SchemeShard to update.
            const auto outdated = state.GetOutdatedVolumeConfigs();
            UNIT_ASSERT_VALUES_EQUAL(1, outdated.size());
            UNIT_ASSERT_VALUES_EQUAL("nbs", outdated[0]);
        });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
