#include "disk_registry_state.h"

#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_actor.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <util/stream/file.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

///////////////////////////////////////////////////////////////////////////////

TString FilePath(TStringBuf fileName)
{
    auto arcRoot = ArcadiaSourceRoot();
    return JoinFsPaths(
        arcRoot ? arcRoot : "/work/nbs.3",
        "cloud/blockstore/libs/storage/disk_registry/benchmark",
        fileName);
}

TString ReadFile(const TString& fileName)
{
    TFile file(fileName, EOpenModeFlag::RdOnly);
    return TFileInput(file).ReadAll();
}

TDiskRegistryState Load(const TString& fileName, bool disableFullGroupsCalc)
{
    auto filePath = FilePath(fileName);
    TString backupData = ReadFile(filePath);

    NProto::TBackupDiskRegistryStateResponse backup;

    auto status =
        google::protobuf::util::JsonStringToMessage(backupData, &backup);
    if (!status.ok()) {
        ythrow yexception() << "Error loading state: " << filePath
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
    auto state = Load("vla.json", false);
    for (const auto _: benchmarkState) {
        state.PublishCounters(TInstant::Now());
    }
}

static void PublishCounters_DisableFullGroups(benchmark::State& benchmarkState)
{
    auto state = Load("vla.json", true);
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

    auto state = Load("vla.json", false);
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
                UNIT_ASSERT_SUCCESS(state.MarkDiskForCleanup(db, "disk-1"));
                UNIT_ASSERT_SUCCESS(state.DeallocateDisk(db, "disk-1"));
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
