#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_actor.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_state.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <contrib/libs/protobuf/src/google/protobuf/stubs/port.h>
#include <contrib/libs/protobuf/src/google/protobuf/util/json_util.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/random/fast.h>
#include <util/stream/file.h>

#include <google/protobuf/util/json_util.h>

using namespace NCloud::NBlockStore;
using namespace NCloud::NBlockStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4096;

enum class EDevicePool
{
    Local,
    Nrd,
};

enum class EVolumeType
{
    Local,
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
    {.Count = 1000, .Min = 15, .Max = 16, .Tag = EDevicePool::Nrd},
    {.Count = 1000, .Min = 31, .Max = 32, .Tag = EDevicePool::Nrd},
    {.Count = 1000, .Min = 64, .Max = 64, .Tag = EDevicePool::Nrd},
    {.Count = 1000, .Min = 80, .Max = 80, .Tag = EDevicePool::Nrd},
    {.Count = 1000, .Min = 8, .Max = 8, .Tag = EDevicePool::Local},
};

constexpr TEntityInfo<EVolumeType> Disks[]{
    {.Count = 1000, .Min = 1, .Max = 3, .Tag = EVolumeType::Nrd},
    {.Count = 1000, .Min = 1, .Max = 3, .Tag = EVolumeType::Mirror3},
};

struct TOptions
{
    TString BackupPath;

    void Parse(int argc, char** argv)
    {
        NLastGetopt::TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("backup-path")
            .RequiredArgument()
            .StoreResult(&BackupPath);

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

auto GenerateAll()
{
    TFastRng64 Rand(GetCycleCount());

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
        Rand);

    TVector<NProto::TAgentConfig> agents;
    auto makeHost = [&](size_t i, size_t deviceCount, EDevicePool tag)
    {
        Y_UNUSED(tag);
        auto rack = racks[Rand.GenRand() % racks.size()];

        NProto::TAgentConfig agent;
        agent.SetNodeId(i);
        agent.SetAgentId(TStringBuilder() << "Agent_" << i);
        for (size_t j = 0; j < deviceCount; ++j) {
            auto* device = agent.MutableDevices()->Add();
            device->SetAgentId(agent.GetAgentId());
            device->SetDeviceName(
                TStringBuilder() << "device_" << i << "_" << j);
            device->SetDeviceUUID(TStringBuilder() << "uuid_" << i << "_" << j);
            device->SetBlockSize(DefaultBlockSize);
            device->SetBlocksCount(24379392);
            device->SetRack(rack);
        }
        agents.push_back(std::move(agent));
    };
    Generate(
        {std::begin(Hosts), std::end(Hosts)},
        std::function<void(size_t i, size_t val, EDevicePool tag)>(makeHost),
        Rand);

    auto monitoring = NCloud::CreateMonitoringServiceStub();
    auto diskRegistryGroup = monitoring->GetCounters()
                                 ->GetSubgroup("counters", "blockstore")
                                 ->GetSubgroup("component", "disk_registry");
    auto state =
        NDiskRegistryStateTest::TDiskRegistryStateBuilder()
            .AddDevicePoolConfig("", 93_GB, NProto::DEVICE_POOL_KIND_DEFAULT)
            .With(diskRegistryGroup)
            .WithAgents(std::move(agents))
            .Build();

    TTestExecutor executor;
    executor.WriteTx([&](TDiskRegistryDatabase db) mutable
                     { db.InitSchema(); });

    auto makeDisk = [&](size_t i, size_t deviceCount, EVolumeType tag)
    {
        TDiskRegistryState::TAllocateDiskParams diskParams{
            .DiskId = TStringBuilder() << "disk_" << i,
            .PlacementGroupId = {},
            .BlockSize = 4_KB,
            .BlocksCount = 93_GB * (deviceCount) / 4_KB,
            .ReplicaCount =
                static_cast<ui32>(tag == EVolumeType::Mirror3 ? 2 : 0),
            .MediaKind =
                (tag == EVolumeType::Mirror3
                     ? NProto::STORAGE_MEDIA_SSD_MIRROR3
                     : NProto::STORAGE_MEDIA_SSD_NONREPLICATED)};

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
        Rand);

    return state;
}

}   // namespace

int main(int argc, char** argv)
{
    TOptions opts;
    opts.Parse(argc, argv);

    auto state = GenerateAll();
    NProto::TDiskRegistryStateBackup backupState = state.BackupState();
    NProto::TBackupDiskRegistryStateResponse backup;
    backup.MutableBackup()->Swap(&backupState);

    TProtoStringType str;
    google::protobuf::util::MessageToJsonString(backup, &str);
    TFileOutput(opts.BackupPath).Write(str.c_str());
    return 0;
}
