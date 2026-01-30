#include "disk_registry_state.h"

#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_actor.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/public/api/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/random.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/system/env.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {
namespace {

///////////////////////////////////////////////////////////////////////////////

const TString BackupFileEnv = "DISK_REGISTRY_BACKUP_PATH";

enum ERegisterAgent
{
    ChangeNodeId,
    KeepNodeId,
};

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

    auto snapshot = MakeNewLoadState(std::move(*backup.MutableMemoryBackup()));

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

NProto::TDeviceConfig MakeDevice(
    const TString& agentId,
    const TString& uuid,
    const TString& name,
    const TString& rack,
    ui64 offset)
{
    constexpr ui64 DefaultDeviceBlockCount = 93_GB / DefaultBlockSize;
    NProto::TDeviceConfig config;

    config.SetAgentId(agentId);
    config.SetDeviceName(name);
    config.SetDeviceUUID(uuid);
    config.SetBlockSize(DefaultBlockSize);
    config.SetRack(rack);
    config.SetSerialNumber(Sprintf("serial-%s", uuid.c_str()));
    config.SetPhysicalOffset(offset);
    config.SetBlocksCount(DefaultDeviceBlockCount);

    return config;
}

TVector<NProto::TAgentConfig> MakeNewAgents(ui32 agentCount, ui32 nodeIdStart)
{
    TVector<NProto::TAgentConfig> agents{agentCount};
    TString rack = NUnitTest::RandomString(5);
    constexpr ui32 DeviceCount = 64;
    for (ui32 i = 0; i < agentCount; ++i) {
        if (i % 10 == 0) {
            rack = NUnitTest::RandomString(5);
        }
        const TString agentId = Sprintf("new-agent-%u", i);
        ui64 offset = 0;
        for (ui32 j = 0; j < DeviceCount; ++j) {
            const TString uuid = Sprintf("%s-uuid-%u", agentId.c_str(), j);
            const TString name = Sprintf("%s-name-%u", agentId.c_str(), j);
            auto device = MakeDevice(agentId, uuid, name, rack, offset);
            offset += device.GetBlocksCount() * device.GetBlockSize();
            *agents[i].AddDevices() = std::move(device);
        }
        agents[i].SetAgentId(agentId);
        agents[i].SetNodeId(nodeIdStart++);
        agents[i].SetState(NProto::AGENT_STATE_ONLINE);
        agents[i].SetDedicatedDiskAgent(true);
    }
    return agents;
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

static void DoRegisterAgent(benchmark::State& benchmarkState, bool changeNodeId)
{
    auto state = Load(true);
    auto agents = state.GetAgents();

    TTestExecutor executor;
    executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

    TVector<size_t> agentsToRegister;
    for (size_t i = 0; i < agents.size(); ++i) {
        auto& agent = agents[i];
        if (agent.GetNodeId() != 0) {
            agentsToRegister.push_back(i);
            if (changeNodeId) {
                agent.SetNodeId(agentsToRegister.size());
            }
        }
    }

    size_t i = 0;
    for (const auto _: benchmarkState) {
        size_t agentIndex = agentsToRegister[(i++) % agentsToRegister.size()];
        auto& agent = agents[agentIndex];
        if (changeNodeId) {
            agent.SetNodeId(agent.GetNodeId() + agentsToRegister.size());
        }
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [r, error] =
                    state.RegisterAgent(db, agent, TInstant::Now());

                if (HasError(error)) {
                    Cout << error.GetMessage() << Endl;
                }
            });
    }
}

static void AddHost(benchmark::State& benchmarkState)
{
    auto state = Load(true);
    auto agents = state.GetAgents();

    TTestExecutor executor;
    executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

    for (auto& agent: agents) {
        if (agent.GetNodeId() == 0) {
            continue;
        }
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [r, error] =
                    state.RegisterAgent(db, agent, TInstant::Now());
                if (HasError(error)) {
                    Cout << error.GetMessage() << Endl;
                    return;
                }
            });
    }

    auto newAgents = MakeNewAgents(500, agents.size() + 1);
    for (auto& agent: newAgents) {
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [r, error] =
                    state.RegisterAgent(db, agent, TInstant::Now());
                if (HasError(error)) {
                    Cout << error.GetMessage() << Endl;
                    return;
                }
            });
    }

    size_t i = 0;
    for (const auto _: benchmarkState) {
        const TString& agentId =
            newAgents[(i++) % newAgents.size()].GetAgentId();
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                TDuration timeout;
                auto error = state.UpdateCmsHostState(
                    db,
                    agentId,
                    NProto::AGENT_STATE_ONLINE,
                    TInstant::Now(),
                    /*dryRun=*/false,
                    affectedDisks,
                    timeout);

                if (HasError(error)) {
                    Cout << error.GetMessage()
                         << "; affectedDisks: " << JoinSeq(", ", affectedDisks)
                         << "; timeout: " << timeout.Seconds() << Endl;
                }
            });
    }
}

static void AddSameHostMultipleTimes(benchmark::State& benchmarkState)
{
    auto state = Load(true);
    auto agents = state.GetAgents();

    TTestExecutor executor;
    executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

    for (auto& agent: agents) {
        if (agent.GetNodeId() == 0) {
            continue;
        }
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [r, error] =
                    state.RegisterAgent(db, agent, TInstant::Now());
                if (HasError(error)) {
                    Cout << error.GetMessage() << Endl;
                    return;
                }
            });
    }

    auto newAgents = MakeNewAgents(500, agents.size() + 1);
    for (auto& agent: newAgents) {
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [r, error] =
                    state.RegisterAgent(db, agent, TInstant::Now());
                if (HasError(error)) {
                    Cout << error.GetMessage() << Endl;
                    return;
                }
            });
    }

    for (const auto _: benchmarkState) {
        const TString& agentId = newAgents[0].GetAgentId();
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                TDuration timeout;
                auto error = state.UpdateCmsHostState(
                    db,
                    agentId,
                    NProto::AGENT_STATE_ONLINE,
                    TInstant::Now(),
                    /*dryRun=*/false,
                    affectedDisks,
                    timeout);

                if (HasError(error)) {
                    Cout << error.GetMessage()
                         << "; affectedDisks: " << JoinSeq(", ", affectedDisks)
                         << "; timeout: " << timeout.Seconds() << Endl;
                }
            });
    }
}

static void PurgeHost(benchmark::State& benchmarkState)
{
    auto state = Load(true);
    auto agents = state.GetAgents();

    TTestExecutor executor;
    executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

    TVector<TString> removableAgents;
    for (auto& agent: agents) {
        if (agent.GetNodeId() == 0) {
            continue;
        }
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto [r, error] =
                    state.RegisterAgent(db, agent, TInstant::Now());
                if (HasError(error)) {
                    Cout << error.GetMessage() << Endl;
                    return;
                }

                if (agent.DevicesSize() == 0) {
                    return;
                }
                for (const auto& device: agent.GetDevices()) {
                    if (state.FindDisk(device.GetDeviceUUID())) {
                        return;
                    }
                }
                removableAgents.push_back(agent.GetAgentId());
            });
    }

    Y_ABORT_UNLESS(removableAgents.size() > 0);
    size_t i = 0;
    for (const auto _: benchmarkState) {
        const TString& agentId =
            removableAgents[(i++) % removableAgents.size()];
        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TVector<TString> affectedDisks;
                auto error = state.PurgeHost(
                    db,
                    agentId,
                    TInstant::Now(),
                    /*dryRun=*/false,
                    affectedDisks);

                if (HasError(error)) {
                    Cout << error.GetMessage()
                         << "; affectedDisks: " << JoinSeq(", ", affectedDisks)
                         << Endl;
                }
            });
    }
}

void DoCreateDeleteDisk(
    benchmark::State& benchmarkState,
    const TDiskRegistryState::TAllocateDiskParams& diskParams)
{
    TTestExecutor executor;
    executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

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

#define REGISTER_AGENT(changeNodeId)                                   \
    void RegisterAgent##changeNodeId(benchmark::State& benchmarkState) \
    {                                                                  \
        DoRegisterAgent(                                               \
            benchmarkState,                                            \
            (changeNodeId) == ERegisterAgent::ChangeNodeId);           \
    }                                                                  \
    BENCHMARK(RegisterAgent##changeNodeId);

BENCHMARK(PublishCounters_All);
BENCHMARK(PublishCounters_DisableFullGroups);
BENCHMARK(AddHost);
BENCHMARK(AddSameHostMultipleTimes);
BENCHMARK(PurgeHost);
CREATE_AND_DELETE_NRD_DISK(1);
CREATE_AND_DELETE_NRD_DISK(10);
CREATE_AND_DELETE_NRD_DISK(100);
CREATE_AND_DELETE_MIRROR_DISK(1);
CREATE_AND_DELETE_MIRROR_DISK(10);
CREATE_AND_DELETE_MIRROR_DISK(100);
REGISTER_AGENT(KeepNodeId);
REGISTER_AGENT(ChangeNodeId);

///////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
