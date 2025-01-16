#include "disk_registry_state.h"

#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_actor.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <util/stream/file.h>
#include <util/system/env.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

///////////////////////////////////////////////////////////////////////////////

const TString BackupFileEnv = "DISK_REGISTRY_BACKUP_PATH";

TString BackupFilePath()
{
    auto result = GetEnv(BackupFileEnv);
    if (!result) {
        ythrow yexception()
            << "Environment variable " << BackupFileEnv.Quote() << " not set";
    }
    return result;
}

TString ReadFile(const TString& filePath)
{
    TFile file(filePath, EOpenModeFlag::RdOnly);
    return TFileInput(file).ReadAll();
}

TDiskRegistryState Load(bool disableFullGroupsCalc)
{
    auto backupPath = BackupFilePath();
    TString backupData = ReadFile(backupPath);

    NProto::TBackupDiskRegistryStateResponse backup;

    auto status =
        google::protobuf::util::JsonStringToMessage(backupData, &backup);
    if (!status.ok()) {
        ythrow yexception()
            << "Error loading state from: " << backupPath.Quote()
            << " error: " << status.ToString();
    }
    auto monitoring = CreateMonitoringServiceStub();
    auto diskRegistryGroup = monitoring->GetCounters()
                                 ->GetSubgroup("counters", "blockstore")
                                 ->GetSubgroup("component", "disk_registry");

    auto snapshot = MakeNewLoadState(std::move(*backup.MutableBackup()));

    auto storeageConfigProto =
        NDiskRegistryStateTest::CreateDefaultStorageConfigProto();
    storeageConfigProto.SetDisableFullPlacementGroupCountCalculation(
        disableFullGroupsCalc);
    storeageConfigProto.SetAllocationUnitNonReplicatedSSD(93);

    return TDiskRegistryState(
        CreateLoggingService("console"),
        NDiskRegistryStateTest::CreateStorageConfig(
            std::move(storeageConfigProto)),
        diskRegistryGroup,
        std::move(snapshot.Config),
        std::move(snapshot.Agents),
        std::move(snapshot.Disks),
        std::move(snapshot.PlacementGroups),
        std::move(snapshot.BrokenDisks),
        std::move(snapshot.DisksToReallocate),
        std::move(snapshot.DiskStateChanges),
        snapshot.LastDiskStateSeqNo,
        std::move(snapshot.DirtyDevices),
        std::move(snapshot.DisksToCleanup),
        std::move(snapshot.ErrorNotifications),
        std::move(snapshot.UserNotifications),
        std::move(snapshot.OutdatedVolumeConfigs),
        std::move(snapshot.SuspendedDevices),
        std::move(snapshot.AutomaticallyReplacedDevices),
        std::move(snapshot.DiskRegistryAgentListParams));
}

}   // namespace

static void PublishCounters_All(benchmark::State& benchmarkState)
{
    auto state = Load(false);
    for (const auto _: benchmarkState) {
        state.PublishCounters(TInstant::Now());
    }
}

static void PublishCounters_DisableFullGroups(benchmark::State& benchmarkState)
{
    auto state = Load(true);
    for (const auto _: benchmarkState) {
        state.PublishCounters(TInstant::Now());
    }
}

void DoCreateDeleteDisk(
    benchmark::State& benchmarkState,
    const TDiskRegistryState::TAllocateDiskParams& diskParams)
{
    TTestExecutor executor;
    executor.WriteTx([&](TDiskRegistryDatabase db) mutable
                     { db.InitSchema(); });

    auto state = Load(false);
    for (const auto _: benchmarkState) {
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TDiskRegistryState::TAllocateDiskResult result{};
                auto error = state.AllocateDisk(
                    TInstant::Now(),
                    db,
                    diskParams,
                    &result);
                UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                UNIT_ASSERT_SUCCESS(
                    state.MarkDiskForCleanup(db, diskParams.DiskId));
                UNIT_ASSERT_SUCCESS(
                    state.DeallocateDisk(db, diskParams.DiskId));
                for (const auto& device: state.GetDirtyDevices()) {
                    state.MarkDeviceAsClean(Now(), db, device.GetDeviceUUID());
                }
            });
    }
}

#define CREATE_AND_DELETE_NRD_DISK(deviceCount)                           \
    void CreateDeleteDisk_##deviceCount(benchmark::State& benchmarkState) \
    {                                                                     \
        TDiskRegistryState::TAllocateDiskParams params{                   \
            .DiskId = "disk-1",                                           \
            .PlacementGroupId = {},                                       \
            .BlockSize = 4_KB,                                            \
            .BlocksCount = 93_GB * (deviceCount) / 4_KB,                  \
        };                                                                \
        DoCreateDeleteDisk(benchmarkState, params);                       \
    }                                                                     \
    BENCHMARK(CreateDeleteDisk_##deviceCount);

#define CREATE_AND_DELETE_MIRROR_DISK(deviceCount)       \
    void CreateDeleteMirrorDisk_##deviceCount(           \
        benchmark::State& benchmarkState)                \
    {                                                    \
        TDiskRegistryState::TAllocateDiskParams params{  \
            .DiskId = "disk-1",                          \
            .PlacementGroupId = {},                      \
            .BlockSize = 4_KB,                           \
            .BlocksCount = 93_GB * (deviceCount) / 4_KB, \
            .ReplicaCount = 2};                          \
        DoCreateDeleteDisk(benchmarkState, params);      \
    }                                                    \
    BENCHMARK(CreateDeleteMirrorDisk_##deviceCount);

BENCHMARK(PublishCounters_All);
BENCHMARK(PublishCounters_DisableFullGroups);
CREATE_AND_DELETE_NRD_DISK(1);
CREATE_AND_DELETE_NRD_DISK(10);
CREATE_AND_DELETE_NRD_DISK(100);
CREATE_AND_DELETE_MIRROR_DISK(1);
CREATE_AND_DELETE_MIRROR_DISK(10);
CREATE_AND_DELETE_MIRROR_DISK(100);

///////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
