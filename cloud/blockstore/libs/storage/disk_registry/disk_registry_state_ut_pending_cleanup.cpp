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

Y_UNIT_TEST_SUITE(TDiskRegistryStatePendingCleanupTest)
{
    Y_UNIT_TEST(ShouldDetachErrorDevicesReportedOnRegistration)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
            db.UpdateDirtyDevice("uuid-1", "vol0");
        });

        auto agent = AgentConfig(
            1,
            {Device("dev-1", "uuid-1")});

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents({agent})
                            .WithAgents({agent})
                            .WithDirtyDevices(
                                {TDirtyDevice{"uuid-1", "vol0"}})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        UNIT_ASSERT(state.HasPendingCleanup("vol0"));

        agent.MutableDevices(0)->SetState(NProto::DEVICE_STATE_ERROR);

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto result = state.RegisterAgent(db, agent, Now());
            UNIT_ASSERT_SUCCESS(result.GetError());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"vol0"},
                result.GetResult().AffectedDisks);
        });

        UNIT_ASSERT(!state.HasPendingCleanup("vol0"));

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> dirtyDevices;
            UNIT_ASSERT(db.ReadDirtyDevices(dirtyDevices));
            UNIT_ASSERT_VALUES_EQUAL(1, dirtyDevices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", dirtyDevices[0].Id);
            UNIT_ASSERT_VALUES_EQUAL("", dirtyDevices[0].DiskId);
        });
    }

    Y_UNIT_TEST(ShouldNotWaitForErrorDevicesOnDeallocation)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const auto agent = AgentConfig(
            1,
            {Device("dev-1", "uuid-nr", NProto::DEVICE_STATE_ERROR),
             Device("dev-2", "uuid-local", NProto::DEVICE_STATE_ERROR)});

        auto nonreplicatedDisk = Disk("vol-nr", {"uuid-nr"});
        nonreplicatedDisk.SetStorageMediaKind(
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        auto localDisk = Disk("vol-local", {"uuid-local"});
        localDisk.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithAgents({agent})
                .WithDisks({nonreplicatedDisk, localDisk})
                .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            for (const auto& diskId: {"vol-nr", "vol-local"}) {
                UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, diskId));
                UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, diskId));
            }
        });

        UNIT_ASSERT(!state.HasPendingCleanup("vol-nr"));
        UNIT_ASSERT(!state.HasPendingCleanup("vol-local"));

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> dirtyDevices;
            UNIT_ASSERT(db.ReadDirtyDevices(dirtyDevices));
            UNIT_ASSERT_VALUES_EQUAL(2, dirtyDevices.size());
            for (const auto& device: dirtyDevices) {
                UNIT_ASSERT_VALUES_EQUAL("", device.DiskId);
            }
        });
    }

    Y_UNIT_TEST(ShouldConsumePendingDeviceWhenCreatingDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
            db.UpdateDirtyDevice("uuid-1", "old-disk");
        });

        const auto agent = AgentConfig(
            1,
            {Device("dev-1", "uuid-1")});

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithAgents({agent})
                            .WithDirtyDevices(
                                {TDirtyDevice{"uuid-1", "old-disk"}})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        UNIT_ASSERT(state.HasPendingCleanup("old-disk"));

        TVector<TString> affectedDisks;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result;
            const auto error = state.CreateDiskFromDevices(
                Now(),
                db,
                true,   // force
                "new-disk",
                4_KB,
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                {state.GetDevice("uuid-1")},
                &result,
                affectedDisks);

            UNIT_ASSERT_SUCCESS(error);
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"old-disk"},
                affectedDisks);
        });

        UNIT_ASSERT(!state.HasPendingCleanup("old-disk"));
        UNIT_ASSERT_VALUES_EQUAL("new-disk", state.FindDisk("uuid-1"));

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> dirtyDevices;
            UNIT_ASSERT(db.ReadDirtyDevices(dirtyDevices));
            UNIT_ASSERT(dirtyDevices.empty());
        });
    }

    Y_UNIT_TEST(ShouldConsumePendingTargetWhenChangingDiskDevice)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
            db.UpdateDirtyDevice("uuid-target", "old-disk");
        });

        const auto agent = AgentConfig(
            1,
            {Device("dev-1", "uuid-source"),
             Device("dev-2", "uuid-target")});

        auto statePtr =
            TDiskRegistryStateBuilder()
                .WithAgents({agent})
                .WithDisks({Disk("current-disk", {"uuid-source"})})
                .WithDirtyDevices(
                    {TDirtyDevice{"uuid-target", "old-disk"}})
                .Build();
        TDiskRegistryState& state = *statePtr;

        UNIT_ASSERT(state.HasPendingCleanup("old-disk"));

        TString affectedDisk;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            const auto error = state.ChangeDiskDevice(
                Now(),
                db,
                "current-disk",
                "uuid-source",
                "uuid-target",
                affectedDisk);

            UNIT_ASSERT_SUCCESS(error);
            UNIT_ASSERT_VALUES_EQUAL("old-disk", affectedDisk);
        });

        UNIT_ASSERT(!state.HasPendingCleanup("old-disk"));
        UNIT_ASSERT_VALUES_EQUAL(
            "current-disk",
            state.FindDisk("uuid-target"));
        UNIT_ASSERT_EQUAL(
            NProto::DEVICE_STATE_ERROR,
            state.GetDevice("uuid-target").GetState());

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> dirtyDevices;
            UNIT_ASSERT(db.ReadDirtyDevices(dirtyDevices));
            UNIT_ASSERT_VALUES_EQUAL(1, dirtyDevices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-source", dirtyDevices[0].Id);
            UNIT_ASSERT_VALUES_EQUAL("", dirtyDevices[0].DiskId);
        });
    }

    Y_UNIT_TEST(ShouldDetachPendingDeviceWhenUpdatingConfig)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
            db.UpdateDirtyDevice("uuid-1", "old-disk");
        });

        const auto agent = AgentConfig(
            1,
            {Device("dev-1", "uuid-1")});

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents({agent})
                            .WithAgents({agent})
                            .WithDirtyDevices(
                                {TDirtyDevice{"uuid-1", "old-disk"}})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        UNIT_ASSERT(state.HasPendingCleanup("old-disk"));

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto config = state.GetConfig();
            config.MutableKnownAgents()->Clear();

            TVector<TString> affectedDisks;
            const auto error = state.UpdateConfig(
                db,
                std::move(config),
                false,   // ignoreVersion
                affectedDisks);

            UNIT_ASSERT_SUCCESS(error);
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"old-disk"},
                affectedDisks);
        });

        UNIT_ASSERT(!state.HasPendingCleanup("old-disk"));

        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            TVector<TDirtyDevice> dirtyDevices;
            UNIT_ASSERT(db.ReadDirtyDevices(dirtyDevices));
            UNIT_ASSERT(dirtyDevices.empty());
        });
    }

    Y_UNIT_TEST(ShouldWaitForDevicesCleanup)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1"),
                Device("dev-2", "uuid-1.2"),
                Device("dev-3", "uuid-1.3"),
                Device("dev-4", "uuid-1.4")
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1"),
                Device("dev-2", "uuid-2.2"),
                Device("dev-3", "uuid-2.3"),
                Device("dev-4", "uuid-2.4")
            })
        };

        auto statePtr = TDiskRegistryStateBuilder().WithAgents(agents).Build();
        TDiskRegistryState& state = *statePtr;

        TVector<NProto::TDeviceConfig> devices;

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TDiskRegistryState::TAllocateDiskResult result;
            auto error = state.AllocateDisk(
                TInstant::Zero(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = "vol0",
                    .BlockSize = 4_KB,
                    .BlocksCount = 4 * DefaultDeviceSize / DefaultLogicalBlockSize,
                    .AgentIds = { agents[0].GetAgentId() }
                },
                &result);
            UNIT_ASSERT_VALUES_EQUAL_C(error.GetCode(), S_OK, error);
            UNIT_ASSERT_VALUES_EQUAL(4, result.Devices.size());
            Sort(result.Devices, TByDeviceUUID());
            for (size_t i = 0; i != result.Devices.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    agents[0].GetDevices(i).GetDeviceUUID(),
                    result.Devices[i].GetDeviceUUID()
                );
            }
            devices = std::move(result.Devices);
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto result = state.UpdateCmsDeviceState(
                db,
                devices[0].GetAgentId(),
                devices[0].GetDeviceName(),
                NProto::DEVICE_STATE_WARNING,
                /*customMessage=*/TString(),
                {},     // now
                false,  // shouldResumeDevice
                false); // dryRun

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                result.Error.GetCode(),
                result.Error);
            ASSERT_VECTORS_EQUAL(TVector{"vol0"}, result.AffectedDisks);
        });

        TString target;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            auto&& [config, error] = state.StartDeviceMigration(
                Now(),
                db,
                "vol0",
                devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL_C(error.GetCode(), S_OK, error);
            UNIT_ASSERT_VALUES_EQUAL(agents[1].GetAgentId(), config.GetAgentId());

            target = config.GetDeviceUUID();
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "vol0"));
            auto error = state.DeallocateDisk(db, "vol0");
            UNIT_ASSERT_VALUES_EQUAL_C(error.GetCode(), S_OK, error);
        });

        TVector<TDirtyDevice> dirtyDevices;
        executor.ReadTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT(db.ReadDirtyDevices(dirtyDevices));
            UNIT_ASSERT_VALUES_EQUAL(5, dirtyDevices.size());
            SortBy(dirtyDevices, [] (auto& x) { return x.Id; });

            for (size_t i = 0; i != 4; ++i) {
                UNIT_ASSERT_VALUES_EQUAL("vol0", dirtyDevices[i].DiskId);
                UNIT_ASSERT_VALUES_EQUAL(
                    agents[0].GetDevices(i).GetDeviceUUID(),
                    dirtyDevices[i].Id);
            }

            UNIT_ASSERT_VALUES_EQUAL("vol0", dirtyDevices[4].DiskId);
            UNIT_ASSERT(
                FindIfPtr(
                    agents[1].GetDevices().begin(),
                    agents[1].GetDevices().end(),
                    [&] (const auto& x) {
                        return x.GetDeviceUUID() == dirtyDevices[4].Id;
                    }));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            for (size_t i = 0; i != dirtyDevices.size() - 1; ++i) {
                auto diskId = state.MarkDeviceAsClean(Now(), db, dirtyDevices[i].Id);
                UNIT_ASSERT_VALUES_EQUAL("", diskId);
            }

            auto diskId = state.MarkDeviceAsClean(Now(), db, dirtyDevices.back().Id);
            UNIT_ASSERT_VALUES_EQUAL("vol0", diskId);
        });
    }

    Y_UNIT_TEST(ShouldEraseDiskCreatedFromSuspendedDevice)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        const TVector agents{AgentConfig(
            1,
            {Device("dev-1", "uuid-1.1"),
             Device("dev-2", "uuid-1.2"),
             Device("dev-3", "uuid-1.3"),
             Device("dev-4", "uuid-1.4")})};

        auto statePtr = TDiskRegistryStateBuilder()
                            .WithAgents(agents)
                            .WithSuspendedDevices({"uuid-1.1"})
                            .WithDirtyDevices({TDirtyDevice{"uuid-1.1", ""}})
                            .Build();
        TDiskRegistryState& state = *statePtr;

        // Create a disk.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TDiskRegistryState::TAllocateDiskResult result;
                TVector<TString> affectedDisks;
                NProto::TDeviceConfig device = state.GetDevice("uuid-1.1");
                auto error = state.CreateDiskFromDevices(
                    TInstant::Zero(),
                    db,
                    /*force=*/true,
                    "vol0",
                    4_KB,
                    NProto::STORAGE_MEDIA_SSD_LOCAL,
                    {device},
                    &result,
                    affectedDisks);

                UNIT_ASSERT_VALUES_EQUAL_C(error.GetCode(), S_OK, error);
                UNIT_ASSERT_VALUES_EQUAL(1, result.Devices.size());
                UNIT_ASSERT_EQUAL(
                    device.GetDeviceUUID(),
                    result.Devices[0].GetDeviceUUID());
            });

        // Create pending deallocation with the disk.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "vol0"));
                auto error = state.DeallocateDisk(db, "vol0");
                UNIT_ASSERT_VALUES_EQUAL_C(error.GetCode(), S_OK, error);
            });

        // Marking the device as clean removes it from PendingCleanup.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TDirtyDevice> dirtyDevices;
                UNIT_ASSERT(db.ReadDirtyDevices(dirtyDevices));
                UNIT_ASSERT_VALUES_EQUAL(1, dirtyDevices.size());
                UNIT_ASSERT_VALUES_EQUAL("uuid-1.1", dirtyDevices[0].Id);

                auto diskId =
                    state.MarkDeviceAsClean(Now(), db, dirtyDevices.back().Id);
                UNIT_ASSERT_VALUES_EQUAL("vol0", diskId);
            });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
