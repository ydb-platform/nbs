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

Y_UNIT_TEST_SUITE(TDiskRegistryStateSuspendTest)
{
    Y_UNIT_TEST(ShouldSuspendDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            db.InitSchema();
        });

        const TVector agents {
            AgentConfig(1, {
                Device("dev-1", "uuid-1.1") | WithTotalSize(10_GB),
                Device("dev-2", "uuid-1.2") | WithTotalSize(10_GB),
                Device("dev-3", "uuid-1.3") | WithTotalSize(10_GB),
                Device("dev-4", "uuid-1.4") | WithTotalSize(10_GB)
            }),
            AgentConfig(2, {
                Device("dev-1", "uuid-2.1") | WithTotalSize(10_GB),
                Device("dev-2", "uuid-2.2") | WithTotalSize(10_GB),
                Device("dev-3", "uuid-2.3")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL)
                    | WithTotalSize(8_GB),
                Device("dev-4", "uuid-2.4")
                    | WithPool("local-ssd", NProto::DEVICE_POOL_KIND_LOCAL)
                    | WithTotalSize(8_GB)
            })
        };

        auto makeConfig = [] (int version, auto agents) {
            auto config = MakeConfig(version, agents);

            auto* local = config.AddDevicePoolConfigs();
            local->SetName("local-ssd");
            local->SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
            local->SetAllocationUnit(8_GB);

            return config;
        };

        TDiskRegistryState state = TDiskRegistryStateBuilder()
            .WithConfig(makeConfig(0, TVector { agents[0] }))
            .WithAgents({ agents[0] })
            .Build();

        auto allocNRD = [&] (auto db, auto* diskId, auto deviceCount) {
            TDiskRegistryState::TAllocateDiskResult result;
            auto error = state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = diskId,
                    .BlockSize = 4_KB,
                    .BlocksCount = deviceCount * 10_GB / 4_KB
                },
                &result);
            return error.GetCode();
        };

        auto allocLocalSSD = [&] (auto db, auto* diskId, auto deviceCount) {
            TDiskRegistryState::TAllocateDiskResult result;
            auto error = state.AllocateDisk(
                Now(),
                db,
                TDiskRegistryState::TAllocateDiskParams {
                    .DiskId = diskId,
                    .BlockSize = 4_KB,
                    .BlocksCount = deviceCount * 8_GB / 4_KB,
                    .MediaKind = NProto::STORAGE_MEDIA_SSD_LOCAL
                },
                &result);
            return error.GetCode();
        };

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_VALUES_EQUAL(S_OK, allocNRD(db, "nrd0", 3));
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            for (auto* uuid: {"uuid-2.1", "uuid-2.2", "uuid-2.3", "uuid-2.4"}) {
                UNIT_ASSERT_SUCCESS(state.SuspendDevice(db, uuid));
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            UNIT_ASSERT_SUCCESS(state.UpdateConfig(
                db,
                makeConfig(0, agents),
                false,  // ignoreVersion
                affectedDisks));
            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            TVector<TString> affectedDisks;
            TVector<TString> notifiedDisks;

            UNIT_ASSERT_SUCCESS(
                state.RegisterAgent(
                    db,
                    agents[1],
                    Now(),
                    &affectedDisks,
                    &notifiedDisks));

            UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, notifiedDisks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());

            for (const auto& d: agents[0].GetDevices()) {
                UNIT_ASSERT(!state.IsSuspendedDevice(d.GetDeviceUUID()));
            }

            for (const auto& d: agents[1].GetDevices()) {
                UNIT_ASSERT(state.IsSuspendedDevice(d.GetDeviceUUID()));
            }
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                allocNRD(db, "nrd1", 3)
            );
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.ResumeDevice(Now(), db, "uuid-2.1"));
            UNIT_ASSERT_SUCCESS(state.ResumeDevice(Now(),db, "uuid-2.2"));

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());

            UNIT_ASSERT(!state.IsSuspendedDevice("uuid-2.1"));
            UNIT_ASSERT(!state.IsSuspendedDevice("uuid-2.2"));
            UNIT_ASSERT(state.IsSuspendedDevice("uuid-2.3"));
            UNIT_ASSERT(state.IsSuspendedDevice("uuid-2.4"));

            state.MarkDeviceAsClean(Now(), db, "uuid-2.1");
            state.MarkDeviceAsClean(Now(), db, "uuid-2.2");

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                allocNRD(db, "nrd1", 3)
            );
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_DISK_ALLOCATION_FAILED,
                allocLocalSSD(db, "local", 2)
            );
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_SUCCESS(state.ResumeDevice(Now(), db, "uuid-2.3"));
            UNIT_ASSERT_SUCCESS(state.ResumeDevice(Now(), db, "uuid-2.4"));

            UNIT_ASSERT_VALUES_EQUAL(2, state.GetDirtyDevices().size());

            for (const auto& d: agents[1].GetDevices()) {
                UNIT_ASSERT(!state.IsSuspendedDevice(d.GetDeviceUUID()));
            }

            state.MarkDeviceAsClean(Now(), db, "uuid-2.3");
            state.MarkDeviceAsClean(Now(), db, "uuid-2.4");

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetDirtyDevices().size());
        });

        executor.WriteTx([&] (TDiskRegistryDatabase db) {
            UNIT_ASSERT_VALUES_EQUAL(S_OK, allocLocalSSD(db, "local", 2));
        });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
