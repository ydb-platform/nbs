#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_actor.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_state.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <contrib/libs/protobuf/src/google/protobuf/stubs/port.h>
#include <contrib/libs/protobuf/src/google/protobuf/util/json_util.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/monlib/dynamic_counters/encode.h>

#include <util/random/fast.h>
#include <util/stream/file.h>

#include <google/protobuf/util/json_util.h>

using namespace NCloud::NBlockStore;
using namespace NCloud::NBlockStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 NrdBlockSize = 4096;
constexpr ui32 LocalBlockSize = 512;
constexpr size_t NrdDiskInPlacementGroups = 800;

enum class EDevicePool
{
    Nrd,
    V1,
    V3,
    Chunk29,
    Chunk58,
};

enum class EPlacementGroupType
{
    Spread,
    Partition,
};

enum class EVolumeType
{
    V1,
    V3,
    Nrd,
    Mirror3,
};

template <typename TTag>
struct TEntityInfo
{
    size_t Count = 0;
    size_t Min = 0;
    size_t Max = 0;
    TTag Tag{};
};

constexpr TEntityInfo<bool> Racks[]{
    {.Count = 250},
};

constexpr TEntityInfo<EDevicePool> Hosts[]{
    {.Count = 550, .Min = 15, .Max = 16, .Tag = EDevicePool::Nrd},
    {.Count = 3000, .Min = 31, .Max = 32, .Tag = EDevicePool::Nrd},
    {.Count = 600, .Min = 64, .Max = 64, .Tag = EDevicePool::Nrd},
    {.Count = 300, .Min = 15, .Max = 15, .Tag = EDevicePool::V1},
    {.Count = 500, .Min = 8, .Max = 8, .Tag = EDevicePool::V3},
    {.Count = 200, .Min = 4, .Max = 6, .Tag = EDevicePool::Chunk29},
    {.Count = 25, .Min = 4, .Max = 4, .Tag = EDevicePool::Chunk58},
};

constexpr TEntityInfo<EPlacementGroupType> PlacementGroups[]{
    {.Count = 2000, .Min = 0, .Max = 0, .Tag = EPlacementGroupType::Spread},
    {.Count = 500, .Min = 3, .Max = 5, .Tag = EPlacementGroupType::Partition},
};

constexpr TEntityInfo<EVolumeType> Disks[]{
    {.Count = 250, .Min = 1, .Max = 1, .Tag = EVolumeType::V1},
    {.Count = 2500, .Min = 1, .Max = 1, .Tag = EVolumeType::V3},
    {.Count = 3000, .Min = 1, .Max = 1, .Tag = EVolumeType::Nrd},
    {.Count = 2000, .Min = 2, .Max = 3, .Tag = EVolumeType::Nrd},
    {.Count = 1500, .Min = 4, .Max = 11, .Tag = EVolumeType::Nrd},
    {.Count = 400, .Min = 12, .Max = 22, .Tag = EVolumeType::Nrd},
    {.Count = 400, .Min = 23, .Max = 150, .Tag = EVolumeType::Nrd},
    {.Count = 25, .Min = 151, .Max = 800, .Tag = EVolumeType::Nrd},
    {.Count = 750, .Min = 1, .Max = 1, .Tag = EVolumeType::Mirror3},
    {.Count = 350, .Min = 2, .Max = 2, .Tag = EVolumeType::Mirror3},
    {.Count = 150, .Min = 3, .Max = 3, .Tag = EVolumeType::Mirror3},
    {.Count = 300, .Min = 4, .Max = 14, .Tag = EVolumeType::Mirror3},
    {.Count = 150, .Min = 15, .Max = 40, .Tag = EVolumeType::Mirror3},
    {.Count = 100, .Min = 41, .Max = 50, .Tag = EVolumeType::Mirror3},
    {.Count = 20, .Min = 51, .Max = 100, .Tag = EVolumeType::Mirror3},
    {.Count = 5, .Min = 101, .Max = 150, .Tag = EVolumeType::Mirror3},
};

struct TOptions
{
    TString BackupPath = "backup.json";
    ui64 Seed = 42;

    void Parse(int argc, char** argv)
    {
        NLastGetopt::TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("backup-path")
            .RequiredArgument()
            .StoreResult(&BackupPath);

        opts.AddLongOption("seed")
            .OptionalArgument("NUM")
            .StoreResult(&Seed);

        NLastGetopt::TOptsParseResultException res(&opts, argc, argv);
    }
};

template <typename TTag>
void Generate(
    const std::span<const TEntityInfo<TTag>>& entities,
    std::function<void(size_t i, size_t val, TTag tag)> generator,
    TFastRng64& rand)
{
    size_t i = 0;
    for (const auto& entityInfo: entities) {
        for (size_t cnt = 0; cnt < entityInfo.Count; ++cnt) {
            auto val =
                (rand.GenRand() % (entityInfo.Max - entityInfo.Min + 2)) +
                entityInfo.Min;
            generator(i++, val, entityInfo.Tag);
        }
    }
}

std::unique_ptr<TDiskRegistryState> GenerateAll(ui64 seed)
{
    TMap<TString, ui64> AllocationUnit;
    AllocationUnit[""] = 93_GB;
    AllocationUnit["standard-v1"] = 98924756992;
    AllocationUnit["standard-v3"] = 394062200832;
    AllocationUnit["chunk-2.90TiB"] = 3198924357632;
    AllocationUnit["chunk-5.82TiB"] = 6398924554240;

    TFastRng64 rand(seed);

    TVector<TString> racks;
    auto makeRack = [&](size_t i, size_t val, bool tag)
    {
        Y_UNUSED(val);
        Y_UNUSED(tag);

        racks.push_back(TStringBuilder() << "Rack_" << i);
    };
    Generate(
        {std::begin(Racks), std::end(Racks)},
        std::function<void(size_t i, size_t val, bool tag)>(makeRack),
        rand);

    TVector<NProto::TAgentConfig> agents;
    auto makeHost = [&](size_t i, size_t deviceCount, EDevicePool tag)
    {
        auto rack = racks[rand.GenRand() % racks.size()];

        NProto::TAgentConfig agent;
        agent.SetNodeId(i + 1);
        agent.SetAgentId(TStringBuilder() << "Agent_" << i);
        for (size_t j = 0; j < deviceCount; ++j) {
            auto* device = agent.MutableDevices()->Add();
            device->SetAgentId(agent.GetAgentId());
            device->SetDeviceName(
                TStringBuilder() << "device_" << i << "_" << j);
            device->SetDeviceUUID(TStringBuilder() << "uuid_" << i << "_" << j);
            device->SetRack(rack);
            switch (tag) {
                case EDevicePool::Nrd: {
                    device->SetBlockSize(NrdBlockSize);
                    device->SetBlocksCount(
                        AllocationUnit[""] / DefaultBlockSize);
                } break;
                case EDevicePool::V1: {
                    device->SetBlockSize(LocalBlockSize);
                    device->SetBlocksCount(
                        AllocationUnit["standard-v1"] / LocalBlockSize);
                    device->SetPoolName("standard-v1");
                    device->SetPoolKind(
                        NProto::EDevicePoolKind::DEVICE_POOL_KIND_LOCAL);
                } break;
                case EDevicePool::V3: {
                    device->SetBlockSize(LocalBlockSize);
                    device->SetBlocksCount(
                        AllocationUnit["standard-v3"] / LocalBlockSize);
                    device->SetPoolName("standard-v3");
                    device->SetPoolKind(
                        NProto::EDevicePoolKind::DEVICE_POOL_KIND_LOCAL);
                } break;
                case EDevicePool::Chunk29: {
                    device->SetBlockSize(LocalBlockSize);
                    device->SetBlocksCount(
                        AllocationUnit["chunk-2.90TiB"] / LocalBlockSize);
                    device->SetPoolName("chunk-2.90TiB");
                    device->SetPoolKind(
                        NProto::EDevicePoolKind::DEVICE_POOL_KIND_LOCAL);
                } break;
                case EDevicePool::Chunk58: {
                    device->SetBlockSize(LocalBlockSize);
                    device->SetBlocksCount(
                        AllocationUnit["chunk-5.82TiB"] / LocalBlockSize);
                    device->SetPoolName("chunk-5.82TiB");
                    device->SetPoolKind(
                        NProto::EDevicePoolKind::DEVICE_POOL_KIND_LOCAL);
                } break;
            }
        }
        agents.push_back(std::move(agent));
    };
    Generate(
        {std::begin(Hosts), std::end(Hosts)},
        std::function<void(size_t i, size_t val, EDevicePool tag)>(makeHost),
        rand);

    TVector<NProto::TPlacementGroupConfig> placementGroups;
    auto makePlacementGroup =
        [&](size_t i, size_t groupCount, EPlacementGroupType tag)
    {
        NProto::TPlacementGroupConfig config;
        switch (tag) {
            case EPlacementGroupType::Spread: {
                config.SetGroupId(TStringBuilder() << "Spread_" << i);
                config.SetPlacementStrategy(
                    NProto::EPlacementStrategy::PLACEMENT_STRATEGY_SPREAD);
            } break;
            case EPlacementGroupType::Partition: {
                config.SetGroupId(TStringBuilder() << "Partition_" << i);
                config.SetPlacementStrategy(
                    NProto::EPlacementStrategy::PLACEMENT_STRATEGY_PARTITION);
                config.SetPlacementPartitionCount(groupCount);
            } break;
        }
        placementGroups.push_back(std::move(config));
    };
    Generate(
        {std::begin(PlacementGroups), std::end(PlacementGroups)},
        std::function<void(size_t, size_t, EPlacementGroupType)>(
            makePlacementGroup),
        rand);

    auto monitoring = NCloud::CreateMonitoringServiceStub();
    auto diskRegistryGroup = monitoring->GetCounters()
                                 ->GetSubgroup("counters", "blockstore")
                                 ->GetSubgroup("component", "disk_registry");
    auto statePtr =
        NDiskRegistryStateTest::TDiskRegistryStateBuilder()
            .AddDevicePoolConfig("", 93_GB, NProto::DEVICE_POOL_KIND_DEFAULT)
            .With(diskRegistryGroup)
            .WithAgents(std::move(agents))
            .WithPlacementGroups(placementGroups)
            .AddDevicePoolConfig(
                "standard-v1",
                AllocationUnit["standard-v1"],
                NProto::DEVICE_POOL_KIND_LOCAL)
            .AddDevicePoolConfig(
                "standard-v3",
                AllocationUnit["standard-v3"],
                NProto::DEVICE_POOL_KIND_LOCAL)
            .AddDevicePoolConfig(
                "chunk-2.90TiB",
                AllocationUnit["chunk-2.90TiB"],
                NProto::DEVICE_POOL_KIND_LOCAL)
            .AddDevicePoolConfig(
                "chunk-5.82TiB",
                AllocationUnit["chunk-5.82TiB"],
                NProto::DEVICE_POOL_KIND_LOCAL)
            .Build();
    TDiskRegistryState& state = *statePtr;

    TTestExecutor executor;
    executor.WriteTx([&](TDiskRegistryDatabase db) mutable
                     { db.InitSchema(); });

    // Count probability for placement group
    const auto totalNrdCount = Accumulate(
        Disks,
        size_t{},
        [](size_t acc, const TEntityInfo<EVolumeType>& diskInfo)
        {
            return acc +
                   (diskInfo.Tag == EVolumeType::Nrd ? diskInfo.Count : 0);
        });
    const auto placementGroupProbability =
        static_cast<double>(NrdDiskInPlacementGroups) / totalNrdCount;

    auto makeDisk = [&](size_t i, size_t deviceCount, EVolumeType tag)
    {
        TString placementGroupId;
        ui32 placementPartitionIndex = 0;
        const bool isInPlacementGroup =
            rand.GenRand() <
            placementGroupProbability *
                std::numeric_limits<decltype(rand.GenRand())>::max();
        if (tag == EVolumeType::Nrd && isInPlacementGroup) {
            auto indx = rand.GenRand() % placementGroups.size();
            placementGroupId = placementGroups[indx].GetGroupId();
            if (auto partCount =
                    placementGroups[indx].GetPlacementPartitionCount())
            {
                placementPartitionIndex = rand.GenRand() % partCount;
            }
        }

        auto mediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED;
        TString poolName;
        ui32 blockSize = DefaultBlockSize;
        switch (tag) {
            case EVolumeType::V1:
                poolName = "standard-v1";
                mediaKind = NProto::STORAGE_MEDIA_SSD_LOCAL;
                blockSize = LocalBlockSize;
                break;
            case EVolumeType::V3:
                poolName = "standard-v3";
                mediaKind = NProto::STORAGE_MEDIA_SSD_LOCAL;
                blockSize = LocalBlockSize;
                break;
            case EVolumeType::Mirror3:
                mediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR3;
                break;
            case EVolumeType::Nrd:
                break;
        }

        TDiskRegistryState::TAllocateDiskParams diskParams{
            .DiskId = TStringBuilder() << "disk_" << i,
            .PlacementGroupId = placementGroupId,
            .PlacementPartitionIndex = placementPartitionIndex,
            .BlockSize = blockSize,
            .BlocksCount = AllocationUnit[poolName] * deviceCount / blockSize,
            .ReplicaCount =
                static_cast<ui32>(tag == EVolumeType::Mirror3 ? 2 : 0),
            .PoolName = poolName,
            .MediaKind = mediaKind,
        };

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TDiskRegistryState::TAllocateDiskResult result{};
                auto error = state.AllocateDisk(
                    TInstant::Now(),
                    db,
                    diskParams,
                    &result);
            });
    };
    Generate(
        {std::begin(Disks), std::end(Disks)},
        std::function<void(size_t i, size_t val, EVolumeType tag)>(makeDisk),
        rand);

    {   // Output stat.
        state.PublishCounters(TInstant::Now());
        for (size_t i = 0; i < state.GetAgents().size(); ++i) {
            diskRegistryGroup->RemoveSubgroup(
                "agent",
                TStringBuilder() << "Agent_" << i);
        }

        Cout << "Finish generate state:\n";
        diskRegistryGroup->OutputPlainText(Cout, "\t");
    }
    return statePtr;
}

}   // namespace

int main(int argc, char** argv)
{
    TOptions opts;
    opts.Parse(argc, argv);

    auto state = GenerateAll(opts.Seed);
    NProto::TDiskRegistryStateBackup backupState = state->BackupState();
    NProto::TBackupDiskRegistryStateResponse backup;
    backup.MutableBackup()->Swap(&backupState);

    TProtoStringType str;
    google::protobuf::util::MessageToJsonString(backup, &str);
    TFileOutput(opts.BackupPath).Write(str.c_str());
    return 0;
}
