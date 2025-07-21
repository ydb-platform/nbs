#include "disk_registry_state.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NDiskRegistryStateTest;

namespace {

std::unique_ptr<TDiskRegistryState> CreateDiskRegistryState()
{
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
    auto agentConfig5 = AgentConfig(
        5,
        {
            Device("dev-13", "uuid-13", "rack-5"),
            Device("dev-14", "uuid-14", "rack-5"),
            Device("dev-15", "uuid-15", "rack-5"),
        });

    return TDiskRegistryStateBuilder()
        .WithKnownAgents({
            agentConfig1,
            agentConfig2,
            agentConfig3,
            agentConfig4,
            agentConfig5,
        })
        .Build();
}

void CreateMirror3Disk(TTestExecutor& executor, TDiskRegistryState& state)
{
    executor.WriteTx(
        [&](TDiskRegistryDatabase db)
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
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", replicas[1][1].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
        });
}

void DeleteDisksToNotify(TTestExecutor& executor, TDiskRegistryState& state)
{
    executor.WriteTx(
        [&](TDiskRegistryDatabase db) mutable
        {
            // copying needed to avoid use-after-free upon deletion
            const auto disks = state.GetDisksToReallocate();
            for (const auto& x: disks) {
                state.DeleteDiskToReallocate(db, x.first, x.second);
            }
        });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryStateLaggingAgentsTest)
{
    Y_UNIT_TEST(ShouldAddOutdatedLaggingDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });
        auto statePtr = CreateDiskRegistryState();
        TDiskRegistryState& state = *statePtr;
        CreateMirror3Disk(executor, state);

        // Request with wrong disk id should return an error.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<NProto::TLaggingDevice> laggingDevices;
                NProto::TLaggingDevice laggingDevice;
                laggingDevice.SetDeviceUUID("uuid-4");
                laggingDevice.SetRowIndex(0);
                laggingDevices.push_back(laggingDevice);
                auto error = state.AddOutdatedLaggingDevices(
                    Now(),
                    db,
                    "wrong-disk",
                    std::move(laggingDevices));

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            });

        // Request with device that doesn't belong to the disk should add it to
        // the lagging list.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<NProto::TLaggingDevice> laggingDevices;
                NProto::TLaggingDevice laggingDevice;
                laggingDevice.SetDeviceUUID("wrong-device");
                laggingDevice.SetRowIndex(0);
                laggingDevices.push_back(laggingDevice);
                auto error = state.AddOutdatedLaggingDevices(
                    Now(),
                    db,
                    "disk-1",
                    std::move(laggingDevices));

                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            });
        {
            UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));

            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            const auto& devices = diskInfo.Devices;
            const auto& replicas = diskInfo.Replicas;
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", replicas[1][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.DeviceReplacementIds.size());
            UNIT_ASSERT_VALUES_EQUAL(1, diskInfo.OutdatedLaggingDevices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "wrong-device",
                diskInfo.OutdatedLaggingDevices[0].Device.GetDeviceUUID());
        }
        // After the reallocation all lagging devices are removed.
        DeleteDisksToNotify(executor, state);
        UNIT_ASSERT(state.GetDisksToReallocate().empty());
        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.OutdatedLaggingDevices.size());
        }

        // Add devices from the zeroth replica to lagging list. They should
        // become fresh.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<NProto::TLaggingDevice> laggingDevices;
                NProto::TLaggingDevice laggingDevice;
                laggingDevice.SetDeviceUUID("uuid-4");
                laggingDevice.SetRowIndex(0);
                laggingDevices.push_back(laggingDevice);
                laggingDevice.SetDeviceUUID("uuid-5");
                laggingDevice.SetRowIndex(1);
                laggingDevices.push_back(laggingDevice);
                auto error = state.AddOutdatedLaggingDevices(
                    Now(),
                    db,
                    "disk-1",
                    std::move(laggingDevices));

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    error.GetMessage());
            });

        {
            UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));

            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            const auto& devices = diskInfo.Devices;
            const auto& replicas = diskInfo.Replicas;
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", replicas[1][1].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(
                (TVector<TString>{"uuid-4", "uuid-5"}),
                diskInfo.DeviceReplacementIds);
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.OutdatedLaggingDevices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                diskInfo.OutdatedLaggingDevices[0].Device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                diskInfo.OutdatedLaggingDevices[1].Device.GetDeviceUUID());
        }

        // After the reallocation all lagging devices are removed.
        DeleteDisksToNotify(executor, state);
        UNIT_ASSERT(state.GetDisksToReallocate().empty());
        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.OutdatedLaggingDevices.size());
        }

        // Adding the fresh devices doesn't do anything to the disk itself. We
        // just reallocate it and the volume should reset the migration
        // progress.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<NProto::TLaggingDevice> laggingDevices;
                NProto::TLaggingDevice laggingDevice;
                laggingDevice.SetDeviceUUID("uuid-4");
                laggingDevice.SetRowIndex(0);
                laggingDevices.push_back(laggingDevice);
                laggingDevice.SetDeviceUUID("uuid-5");
                laggingDevice.SetRowIndex(1);
                laggingDevices.push_back(laggingDevice);
                auto error = state.AddOutdatedLaggingDevices(
                    Now(),
                    db,
                    "disk-1",
                    std::move(laggingDevices));

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    error.GetMessage());
            });
        {
            UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));

            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            const auto& replicas = diskInfo.Replicas;
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(
                (TVector<TString>{"uuid-4", "uuid-5"}),
                diskInfo.DeviceReplacementIds);
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.OutdatedLaggingDevices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                diskInfo.OutdatedLaggingDevices[0].Device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-5",
                diskInfo.OutdatedLaggingDevices[1].Device.GetDeviceUUID());
        }

        // After the reallocation all lagging devices are removed.
        DeleteDisksToNotify(executor, state);
        UNIT_ASSERT(state.GetDisksToReallocate().empty());
        {
            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.OutdatedLaggingDevices.size());
        }
    }

    Y_UNIT_TEST(ShouldAddOutdatedLaggingDevices_MigrationSource)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });
        auto statePtr = CreateDiskRegistryState();
        TDiskRegistryState& state = *statePtr;
        CreateMirror3Disk(executor, state);

        // Migrate devices from the first agent.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<TString> affectedDisks;

                auto error = state.UpdateAgentState(
                    db,
                    state.GetAgentId(1),
                    NProto::AGENT_STATE_WARNING,
                    TInstant::Now(),
                    "test",
                    affectedDisks);
                UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
                UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", migrations[0].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-1/0", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", migrations[1].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-1/0", migrations[1].DiskId);
        }
        // Actual start of the migration.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                UNIT_ASSERT_SUCCESS(
                    state.StartDeviceMigration(Now(), db, "disk-1/0", "uuid-1")
                        .GetError());
                UNIT_ASSERT_SUCCESS(
                    state.StartDeviceMigration(Now(), db, "disk-1/0", "uuid-2")
                        .GetError());
            });

        // Check disk config.
        {
            UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));

            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            const auto& devices = diskInfo.Devices;
            const auto& replicas = diskInfo.Replicas;
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", replicas[1][1].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{},
                diskInfo.DeviceReplacementIds);
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                diskInfo.Migrations[0].GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                diskInfo.Migrations[1].GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-10",
                diskInfo.Migrations[0].GetTargetDevice().GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-11",
                diskInfo.Migrations[1].GetTargetDevice().GetDeviceUUID());
        }

        DeleteDisksToNotify(executor, state);

        // Add lagging devices that are sources of the migration.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<NProto::TLaggingDevice> laggingDevices;
                NProto::TLaggingDevice laggingDevice;
                laggingDevice.SetDeviceUUID("uuid-1");
                laggingDevice.SetRowIndex(0);
                laggingDevices.push_back(laggingDevice);
                laggingDevice.SetDeviceUUID("uuid-2");
                laggingDevice.SetRowIndex(1);
                laggingDevices.push_back(laggingDevice);
                auto error = state.AddOutdatedLaggingDevices(
                    Now(),
                    db,
                    "disk-1",
                    std::move(laggingDevices));

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    error.GetMessage());
            });

        // Check disk config.
        {
            UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));
            UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());

            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            const auto& devices = diskInfo.Devices;
            const auto& replicas = diskInfo.Replicas;
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-10", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-11", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", replicas[1][1].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(
                (TVector<TString>{"uuid-10", "uuid-11"}),
                diskInfo.DeviceReplacementIds);
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.OutdatedLaggingDevices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                diskInfo.OutdatedLaggingDevices[0].Device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                diskInfo.OutdatedLaggingDevices[1].Device.GetDeviceUUID());

            TDiskInfo replicaInfo;
            error = state.GetDiskInfo("disk-1/0", replicaInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, replicaInfo.FinishedMigrations.size());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            "Lagging source migration was aborted; diskId=disk-1/0",
            state.FindDevice("uuid-1")->GetStateMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            "Lagging source migration was aborted; diskId=disk-1/0",
            state.FindDevice("uuid-2")->GetStateMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            state.GetAutomaticallyReplacedDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            CountIf(
                state.GetAutomaticallyReplacedDevices(),
                [&](const auto& deviceInfo)
                {
                    return deviceInfo.DeviceId == "uuid-1" ||
                           deviceInfo.DeviceId == "uuid-2";
                }));
    }

    Y_UNIT_TEST(ShouldAddOutdatedLaggingDevices_MigrationSourceButHasNotStarted)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });
        auto statePtr = CreateDiskRegistryState();
        TDiskRegistryState& state = *statePtr;
        CreateMirror3Disk(executor, state);

        // Migrate devices from the first agent.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<TString> affectedDisks;

                auto error = state.UpdateAgentState(
                    db,
                    state.GetAgentId(1),
                    NProto::AGENT_STATE_WARNING,
                    TInstant::Now(),
                    "test",
                    affectedDisks);
                UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
                UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", migrations[0].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-1/0", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", migrations[1].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-1/0", migrations[1].DiskId);
        }

        // Check disk config.
        {
            UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));

            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            const auto& devices = diskInfo.Devices;
            const auto& replicas = diskInfo.Replicas;
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", replicas[1][1].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{},
                diskInfo.DeviceReplacementIds);
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
        }

        // Add lagging devices that are sources of the migration.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<NProto::TLaggingDevice> laggingDevices;
                NProto::TLaggingDevice laggingDevice;
                laggingDevice.SetDeviceUUID("uuid-1");
                laggingDevice.SetRowIndex(0);
                laggingDevices.push_back(laggingDevice);
                laggingDevice.SetDeviceUUID("uuid-2");
                laggingDevice.SetRowIndex(1);
                laggingDevices.push_back(laggingDevice);
                auto error = state.AddOutdatedLaggingDevices(
                    Now(),
                    db,
                    "disk-1",
                    std::move(laggingDevices));

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    error.GetMessage());
            });

        // Check disk config.
        {
            UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));
            UNIT_ASSERT_VALUES_EQUAL(0, state.BuildMigrationList().size());

            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            const auto& devices = diskInfo.Devices;
            const auto& replicas = diskInfo.Replicas;
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-10", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-11", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", replicas[1][1].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(
                (TVector<TString>{"uuid-10", "uuid-11"}),
                diskInfo.DeviceReplacementIds);
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.OutdatedLaggingDevices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                diskInfo.OutdatedLaggingDevices[0].Device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                diskInfo.OutdatedLaggingDevices[1].Device.GetDeviceUUID());

            TDiskInfo replicaInfo;
            error = state.GetDiskInfo("disk-1/0", replicaInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, replicaInfo.FinishedMigrations.size());
        }
    }

    Y_UNIT_TEST(ShouldAddOutdatedLaggingDevices_MigrationTarget)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });
        auto statePtr = CreateDiskRegistryState();
        TDiskRegistryState& state = *statePtr;
        CreateMirror3Disk(executor, state);

        // Migrate devices from the second replica.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<TString> affectedDisks;

                auto error = state.UpdateAgentState(
                    db,
                    state.GetAgentId(3),
                    NProto::AGENT_STATE_WARNING,
                    TInstant::Now(),
                    "test",
                    affectedDisks);
                UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
                UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", migrations[0].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-1/2", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", migrations[1].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-1/2", migrations[1].DiskId);
        }
        // Actual start of the migration.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                UNIT_ASSERT_SUCCESS(
                    state.StartDeviceMigration(Now(), db, "disk-1/2", "uuid-7")
                        .GetError());
                UNIT_ASSERT_SUCCESS(
                    state.StartDeviceMigration(Now(), db, "disk-1/2", "uuid-8")
                        .GetError());
            });

        // Check disk config.
        {
            UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));

            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            const auto& devices = diskInfo.Devices;
            const auto& replicas = diskInfo.Replicas;
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", replicas[1][1].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{},
                diskInfo.DeviceReplacementIds);
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-7",
                diskInfo.Migrations[0].GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-8",
                diskInfo.Migrations[1].GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-10",
                diskInfo.Migrations[0].GetTargetDevice().GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-11",
                diskInfo.Migrations[1].GetTargetDevice().GetDeviceUUID());
        }

        DeleteDisksToNotify(executor, state);

        // Add lagging devices that are targets of the migration.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<NProto::TLaggingDevice> laggingDevices;
                NProto::TLaggingDevice laggingDevice;
                laggingDevice.SetDeviceUUID("uuid-10");
                laggingDevice.SetRowIndex(0);
                laggingDevices.push_back(laggingDevice);
                laggingDevice.SetDeviceUUID("uuid-11");
                laggingDevice.SetRowIndex(1);
                laggingDevices.push_back(laggingDevice);
                auto error = state.AddOutdatedLaggingDevices(
                    Now(),
                    db,
                    "disk-1",
                    std::move(laggingDevices));

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    error.GetMessage());
            });

        // Check disk config.
        {
            UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));
            auto migrationList = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(2, migrationList.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", migrationList[0].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", migrationList[1].SourceDeviceId);

            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            const auto& devices = diskInfo.Devices;
            const auto& replicas = diskInfo.Replicas;
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-5", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-7", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-8", replicas[1][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.DeviceReplacementIds.size());
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.OutdatedLaggingDevices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-10",
                diskInfo.OutdatedLaggingDevices[0].Device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-11",
                diskInfo.OutdatedLaggingDevices[1].Device.GetDeviceUUID());

            TDiskInfo replicaInfo;
            error = state.GetDiskInfo("disk-1/2", replicaInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(2, replicaInfo.FinishedMigrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-10",
                replicaInfo.FinishedMigrations[0].DeviceId);
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-11",
                replicaInfo.FinishedMigrations[1].DeviceId);
        }
        DeleteDisksToNotify(executor, state);

        // Restart the migration.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                auto result =
                    state.StartDeviceMigration(Now(), db, "disk-1/2", "uuid-7");
                UNIT_ASSERT_SUCCESS(result.GetError());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-12",
                    result.GetResult().GetDeviceUUID());

                result =
                    state.StartDeviceMigration(Now(), db, "disk-1/2", "uuid-8");
                UNIT_ASSERT_SUCCESS(result.GetError());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-13",
                    result.GetResult().GetDeviceUUID());
            });
    }

    Y_UNIT_TEST(ShouldAddOutdatedLaggingDevices_ComplicatedDisk)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        TVector<NProto::TAgentConfig> agents;
        for (int i = 0; i < 20; i++) {
            agents.push_back(AgentConfig(
                i + 1,
                {
                    Device(
                        Sprintf("dev-%u", i + 1),
                        Sprintf("uuid-%u", i + 1),
                        Sprintf("rack-%u", i + 1)),
                }));
        }
        auto statePtr = TDiskRegistryStateBuilder()
                            .WithKnownAgents(std::move(agents))
                            .Build();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
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
                UNIT_ASSERT_SUCCESS(error);
                UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
                UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL("agent-1", devices[0].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL("uuid-10", devices[1].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL("agent-10", devices[1].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL("uuid-11", devices[2].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL("agent-11", devices[2].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
                UNIT_ASSERT_VALUES_EQUAL(3, replicas[0].size());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-12",
                    replicas[0][0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "agent-12",
                    replicas[0][0].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-13",
                    replicas[0][1].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "agent-13",
                    replicas[0][1].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-14",
                    replicas[0][2].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "agent-14",
                    replicas[0][2].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(3, replicas[1].size());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-15",
                    replicas[1][0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "agent-15",
                    replicas[1][0].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-16",
                    replicas[1][1].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "agent-16",
                    replicas[1][1].GetAgentId());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-17",
                    replicas[1][2].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "agent-17",
                    replicas[1][2].GetAgentId());
                ASSERT_VECTORS_EQUAL(TVector<TString>{}, deviceReplacementIds);
            });

        // Replace device to create one fresh device.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                bool diskStateUpdated = false;
                auto error = state.ReplaceDevice(
                    db,
                    "disk-1/2",
                    "uuid-17",
                    "uuid-18",
                    Now(),
                    "replace device",
                    true,
                    &diskStateUpdated);
                UNIT_ASSERT_SUCCESS(error);
            });

        // Migrate devices.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<TString> affectedDisks;

                auto error = state.UpdateAgentState(
                    db,
                    state.GetAgentId(1),
                    NProto::AGENT_STATE_WARNING,
                    TInstant::Now(),
                    "test",
                    affectedDisks);
                UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
                UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
                affectedDisks.clear();

                error = state.UpdateAgentState(
                    db,
                    state.GetAgentId(13),
                    NProto::AGENT_STATE_WARNING,
                    TInstant::Now(),
                    "test",
                    affectedDisks);
                UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
                UNIT_ASSERT_VALUES_EQUAL(1, affectedDisks.size());
            });

        {
            auto migrations = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(2, migrations.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", migrations[0].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-1/0", migrations[0].DiskId);
            UNIT_ASSERT_VALUES_EQUAL("uuid-13", migrations[1].SourceDeviceId);
            UNIT_ASSERT_VALUES_EQUAL("disk-1/1", migrations[1].DiskId);
        }

        // Actual start of the migration.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                UNIT_ASSERT_SUCCESS(
                    state.StartDeviceMigration(Now(), db, "disk-1/0", "uuid-1")
                        .GetError());
                UNIT_ASSERT_SUCCESS(
                    state.StartDeviceMigration(Now(), db, "disk-1/1", "uuid-13")
                        .GetError());
            });

        // Check disk config.
        {
            UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));

            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            ASSERT_VECTORS_EQUAL(
                TVector<TString>{"uuid-18"},
                diskInfo.DeviceReplacementIds);
            UNIT_ASSERT_VALUES_EQUAL(2, diskInfo.Migrations.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                diskInfo.Migrations[0].GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-13",
                diskInfo.Migrations[1].GetSourceDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                diskInfo.Migrations[0].GetTargetDevice().GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                diskInfo.Migrations[1].GetTargetDevice().GetDeviceUUID());
        }

        /*
                Current disk state:
                ┌───────────────────────────────────────────────────────┐
                │ uuid-1──►uuid-2  │ uuid-12          │ uuid-15         │
                │──────────────────┼──────────────────┼─────────────────│
                │ uuid-10          │ uuid-13──►uuid-3 │ uuid-16         │
                │──────────────────┼──────────────────┼─────────────────│
                │ uuid-11          │ uuid-14          │ uuid-18(Fresh)  │
                └───────────────────────────────────────────────────────┘
        */

        DeleteDisksToNotify(executor, state);
        // Add lagging devices that are both target and source of migrations.
        // This is technically allowed.
        executor.WriteTx(
            [&](TDiskRegistryDatabase db) mutable
            {
                TVector<NProto::TLaggingDevice> laggingDevices;
                NProto::TLaggingDevice laggingDevice;
                laggingDevice.SetDeviceUUID("uuid-18");
                laggingDevice.SetRowIndex(3);
                laggingDevices.push_back(laggingDevice);
                laggingDevice.SetDeviceUUID("uuid-1");
                laggingDevice.SetRowIndex(0);
                laggingDevices.push_back(laggingDevice);
                laggingDevice.SetDeviceUUID("uuid-2");
                laggingDevice.SetRowIndex(0);
                laggingDevices.push_back(laggingDevice);
                laggingDevice.SetDeviceUUID("uuid-3");
                laggingDevice.SetRowIndex(1);
                laggingDevices.push_back(laggingDevice);
                laggingDevice.SetDeviceUUID("uuid-13");
                laggingDevice.SetRowIndex(1);
                laggingDevices.push_back(laggingDevice);
                auto error = state.AddOutdatedLaggingDevices(
                    Now(),
                    db,
                    "disk-1",
                    std::move(laggingDevices));

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    error.GetMessage());
            });

        // Check disk config.
        {
            UNIT_ASSERT(state.GetDisksToReallocate().contains("disk-1"));
            auto migrationList = state.BuildMigrationList();
            UNIT_ASSERT_VALUES_EQUAL(0, migrationList.size());

            TDiskInfo diskInfo;
            auto error = state.GetDiskInfo("disk-1", diskInfo);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            const auto& devices = diskInfo.Devices;
            const auto& replicas = diskInfo.Replicas;
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-10", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-11", devices[2].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(3, replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-12", replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-4", replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-14", replicas[0][2].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(3, replicas[1].size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-15", replicas[1][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-16", replicas[1][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-18", replicas[1][2].GetDeviceUUID());
            ASSERT_VECTORS_EQUAL(
                (TVector<TString>{"uuid-18", "uuid-2", "uuid-4"}),
                diskInfo.DeviceReplacementIds);
            UNIT_ASSERT_VALUES_EQUAL(0, diskInfo.Migrations.size());

            TDiskInfo replica1Info;
            error = state.GetDiskInfo("disk-1/0", replica1Info);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, replica1Info.FinishedMigrations.size());
            UNIT_ASSERT_VALUES_EQUAL(0, replica1Info.Migrations.size());

            TDiskInfo replica2Info;
            error = state.GetDiskInfo("disk-1/0", replica2Info);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, replica2Info.FinishedMigrations.size());
            UNIT_ASSERT_VALUES_EQUAL(0, replica2Info.Migrations.size());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
