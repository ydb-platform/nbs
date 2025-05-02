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
                NProto::TDeviceConfig device = state.GetDevice("uuid-1.1");
                auto error = state.CreateDiskFromDevices(
                    TInstant::Zero(),
                    db,
                    /*force=*/true,
                    "vol0",
                    4_KB,
                    NProto::STORAGE_MEDIA_SSD_LOCAL,
                    {device},
                    &result);

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
