#include "test_state.h"

namespace NCloud::NBlockStore::NStorage::NDiskRegistryStateTest {

using NProto::TDeviceConfig;

////////////////////////////////////////////////////////////////////////////////

TDeviceConfig Device(
    TString name,
    TString uuid,
    TString rack,
    ui32 blockSize,
    ui64 totalSize,
    TString transportId,
    NProto::EDeviceState state,
    NProto::TRdmaEndpoint rdmaEndpoint)
{
    TDeviceConfig device;

    device.SetDeviceName(std::move(name));
    device.SetDeviceUUID(std::move(uuid));
    device.SetRack(std::move(rack));
    device.SetBlockSize(blockSize);
    device.SetBlocksCount(totalSize / blockSize);
    device.SetTransportId(std::move(transportId));
    device.SetState(state);
    device.MutableRdmaEndpoint()->CopyFrom(rdmaEndpoint);

    return device;
}

TDeviceConfig Device(
    TString name,
    TString uuid,
    NProto::EDeviceState state)
{
    return Device(
        std::move(name),
        std::move(uuid),
        "rack-1",
        DefaultBlockSize,
        10_GB,
        "",
        state);
}

NProto::TAgentConfig AgentConfig(
    ui32 nodeId,
    NProto::EAgentState state,
    std::initializer_list<TDeviceConfig> devices)
{
    NProto::TAgentConfig agent;
    agent.SetNodeId(nodeId);
    agent.SetAgentId("agent-" + ToString(nodeId));
    agent.SetState(state);

    for (const auto& device: devices) {
        agent.AddDevices()->CopyFrom(device);
    }

    return agent;
}

NProto::TAgentConfig AgentConfig(
    ui32 nodeId,
    std::initializer_list<TDeviceConfig> devices)
{
    return AgentConfig(nodeId, NProto::AGENT_STATE_ONLINE, devices);
}

NProto::TAgentConfig AgentConfig(
    ui32 nodeId,
    TString agentId,
    ui64 seqNumber,
    TVector<TDeviceConfig> devices)
{
    NProto::TAgentConfig agent;
    agent.SetNodeId(nodeId);
    agent.SetAgentId(agentId);
    agent.SetSeqNumber(seqNumber);

    for (const auto& device: devices) {
        agent.AddDevices()->CopyFrom(device);
    }

    return agent;
}

NProto::TAgentConfig AgentConfig(
    ui32 nodeId,
    TString agentId,
    TVector<TDeviceConfig> devices)
{
    return AgentConfig(nodeId, agentId, 0, devices);
}

NProto::TDiskRegistryConfig MakeConfig(
    const TVector<NProto::TAgentConfig>& agents,
    const TVector<NProto::TDeviceOverride>& deviceOverrides)
{
    NProto::TDiskRegistryConfig config;

    for (const auto& agent: agents) {
        *config.AddKnownAgents() = agent;
    }

    for (const auto& deviceOverride: deviceOverrides) {
        *config.AddDeviceOverrides() = deviceOverride;
    }

    return config;
}

NProto::TDiskRegistryConfig MakeConfig(
    ui32 version,
    const TVector<NProto::TAgentConfig>& agents)
{
    auto config = MakeConfig(agents);

    config.SetVersion(version);

    return config;
}

NProto::TDiskConfig Disk(
    const TString& diskId,
    std::initializer_list<TString> uuids,
    NProto::EDiskState state)
{
    NProto::TDiskConfig config;

    config.SetDiskId(diskId);
    config.SetBlockSize(DefaultLogicalBlockSize);
    config.SetState(state);

    for (const auto& uuid: uuids) {
        *config.AddDeviceUUIDs() = uuid;
    }

    return config;
}

TVector<NProto::TDiskConfig> MirrorDisk(
    const TString& diskId,
    TVector<TVector<TString>> uuids,
    NProto::EDiskState state)
{
    Y_VERIFY_DEBUG(uuids.size() > 0, "Mirror disk must have replicas");

    TVector<NProto::TDiskConfig> result;

    //Base disk
    NProto::TDiskConfig config;
    config.SetDiskId(diskId);
    config.SetBlockSize(DefaultLogicalBlockSize);
    config.SetState(state);
    config.SetReplicaCount(uuids.size() - 1);
    result.push_back(std::move(config));

    //Replicas
    for (size_t i = 0; i < uuids.size(); ++i) {
        NProto::TDiskConfig replicaConfig;
        replicaConfig.SetMasterDiskId(diskId);
        replicaConfig.SetDiskId(TStringBuilder() << diskId << "/" << i);
        replicaConfig.SetBlockSize(DefaultLogicalBlockSize);
        replicaConfig.SetState(state);
        for (const auto& uuid: uuids[i]) {
            *replicaConfig.AddDeviceUUIDs() = uuid;
        }
        result.push_back(std::move(replicaConfig));
    }

    return result;
}

NProto::TError AllocateMirroredDisk(
    TDiskRegistryDatabase& db,
    TDiskRegistryState& state,
    const TString& diskId,
    ui64 totalSize,
    ui32 replicaCount,
    TVector<TDeviceConfig>& devices,
    TVector<TVector<TDeviceConfig>>& replicas,
    TVector<NProto::TDeviceMigration>& migrations,
    TVector<TString>& deviceReplacementIds,
    TInstant now,
    NProto::EStorageMediaKind mediaKind)
{
    TDiskRegistryState::TAllocateDiskResult result {};

    auto error = state.AllocateDisk(
        now,
        db,
        TDiskRegistryState::TAllocateDiskParams {
            .DiskId = diskId,
            .BlockSize = DefaultLogicalBlockSize,
            .BlocksCount = totalSize / DefaultLogicalBlockSize,
            .ReplicaCount = replicaCount,
            .MediaKind = mediaKind
        },
        &result);

    devices = std::move(result.Devices);
    replicas = std::move(result.Replicas);
    migrations = std::move(result.Migrations);
    SortBy(migrations, [] (const auto& m) {
        return m.GetSourceDeviceId();
    });
    deviceReplacementIds = std::move(result.DeviceReplacementIds);

    return error;
}

NProto::TError AllocateDisk(
    TDiskRegistryDatabase& db,
    TDiskRegistryState& state,
    const TString& diskId,
    const TString& placementGroupId,
    ui32 placementPartitionIndex,
    ui64 totalSize,
    TVector<TDeviceConfig>& devices,
    TInstant now,
    NProto::EStorageMediaKind mediaKind)
{
    TDiskRegistryState::TAllocateDiskResult result {};

    auto error = state.AllocateDisk(
        now,
        db,
        TDiskRegistryState::TAllocateDiskParams {
            .DiskId = diskId,
            .PlacementGroupId = placementGroupId,
            .PlacementPartitionIndex = placementPartitionIndex,
            .BlockSize = DefaultLogicalBlockSize,
            .BlocksCount = totalSize / DefaultLogicalBlockSize,
            .ReplicaCount = 0,
            .MediaKind = mediaKind
        },
        &result);

    devices = std::move(result.Devices);

    UNIT_ASSERT_VALUES_EQUAL(0, result.Replicas.size());
    UNIT_ASSERT_VALUES_EQUAL(0, result.DeviceReplacementIds.size());

    return error;
}

NProto::TStorageServiceConfig CreateDefaultStorageConfigProto()
{
    NProto::TStorageServiceConfig config;
    config.SetMaxDisksInPlacementGroup(3);
    config.SetBrokenDiskDestructionDelay(5000);
    config.SetNonReplicatedMigrationStartAllowed(true);
    config.SetMirroredMigrationStartAllowed(true);
    config.SetAllocationUnitNonReplicatedSSD(10);

    return config;
}

TStorageConfigPtr CreateStorageConfig(NProto::TStorageServiceConfig proto)
{
    return std::make_shared<TStorageConfig>(
        std::move(proto),
        std::make_shared<TFeaturesConfig>(NProto::TFeaturesConfig())
    );
}

TStorageConfigPtr CreateStorageConfig()
{
    return CreateStorageConfig(CreateDefaultStorageConfigProto());
}

void UpdateConfig(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const TVector<NProto::TAgentConfig>& agents,
    const TVector<NProto::TDeviceOverride>& deviceOverrides)
{
    TVector<TString> affectedDisks;
    const auto error = state.UpdateConfig(
        db,
        MakeConfig(agents, deviceOverrides),
        true,   // ignoreVersion
        affectedDisks);
    UNIT_ASSERT_SUCCESS(error);
    UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
}

NProto::TError RegisterAgent(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const NProto::TAgentConfig& config,
    TInstant timestamp)
{
    TVector<TString> affectedDisks;
    TVector<TString> disksToReallocate;

    auto error = state.RegisterAgent(
        db,
        config,
        timestamp,
        &affectedDisks,
        &disksToReallocate
    );
    UNIT_ASSERT_VALUES_EQUAL(0, affectedDisks.size());
    Y_UNUSED(disksToReallocate);

    return error;
}

NProto::TError RegisterAgent(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const NProto::TAgentConfig& config)
{
    return RegisterAgent(state, db, config, Now());
}

void CleanDevices(TDiskRegistryState& state, TDiskRegistryDatabase& db)
{
    for (const auto& device: state.GetDirtyDevices()) {
        state.MarkDeviceAsClean(Now(), db, device.GetDeviceUUID());
    }
}

TVector<TString> UpdateAgentState(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const TString& agentId,
    NProto::EAgentState desiredState)
{
    TVector<TString> affectedDisks;
    const auto error = state.UpdateAgentState(
        db,
        agentId,
        desiredState,
        Now(),
        "state message",
        affectedDisks);
    UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
    return affectedDisks;
}

TVector<TString> UpdateAgentState(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const NProto::TAgentConfig& config,
    NProto::EAgentState desiredState)
{
    return UpdateAgentState(state, db, config.GetAgentId(), desiredState);
}

////////////////////////////////////////////////////////////////////////////////

TString GetReplicaTableRepr(
    const TDiskRegistryState& state,
    const TString& diskId)
{
    const auto& rt = state.GetReplicaTable();
    const auto m = rt.AsMatrix(diskId);
    TStringBuilder sb;
    for (const auto& row: m) {
        sb << "|";
        for (const auto& d: row) {
            sb << d.Id;
            if (d.IsReplacement) {
                sb << "*";
            }
            sb << "|";
        }
    }

    return sb;
}

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryState TDiskRegistryStateBuilder::Build()
{
    return TDiskRegistryState(
        std::move(StorageConfig),
        std::move(Counters),
        std::move(Config),
        std::move(Agents),
        std::move(Disks),
        std::move(PlacementGroups),
        std::move(BrokenDisks),
        std::move(DisksToReallocate),
        std::move(DiskStateUpdates),
        std::move(DiskStateSeqNo),
        std::move(DirtyDevices),
        std::move(DisksToCleanup),
        std::move(ErrorNotifications),
        std::move(OutdatedVolumeConfigs),
        std::move(SuspendedDevices),
        std::move(AutomaticallyReplacedDevices),
        std::move(DiskRegistryAgentListParams));
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::With(
    TStorageConfigPtr config)
{
    StorageConfig = std::move(config);

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithDisksToReallocate(
    TVector<TString> ids)
{
    DisksToReallocate = std::move(ids);

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithStorageConfig(
    NProto::TStorageServiceConfig proto)
{
    StorageConfig = CreateStorageConfig(std::move(proto));

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::With(
    NMonitoring::TDynamicCountersPtr counters)
{
    Counters = std::move(counters);

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::With(ui64 diskStateSeqNo)
{
    DiskStateSeqNo = diskStateSeqNo;

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithConfig(
    NProto::TDiskRegistryConfig config)
{
    Config = std::move(config);

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithConfig(
    TVector<NProto::TAgentConfig> agents)
{
    Config = MakeConfig(std::move(agents));

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithConfig(
    ui32 version,
    TVector<NProto::TAgentConfig> agents)
{
    Config = MakeConfig(std::move(agents));
    Config.SetVersion(version);

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithKnownAgents(
    TVector<NProto::TAgentConfig> agents)
{
    WithConfig(agents);
    WithAgents(agents);

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithAgents(
    TVector<NProto::TAgentConfig> agents)
{
    Agents = std::move(agents);

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithDisks(
    TVector<NProto::TDiskConfig> disks)
{
    Disks = std::move(disks);

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithDirtyDevices(
    TVector<TString> dirtyDevices)
{
    DirtyDevices.clear();
    DirtyDevices.reserve(dirtyDevices.size());
    for (auto& s: dirtyDevices) {
        DirtyDevices.emplace_back(TDirtyDevice {
            .Id = std::move(s)
        });
    }

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithPlacementGroups(
    TVector<TString> groupIds)
{
    for (auto& id: groupIds) {
        NProto::TPlacementGroupConfig config;
        config.SetGroupId(id);

        PlacementGroups.push_back(config);
    }

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::AddPlacementGroup(
    TString id,
    TVector<TString> disks)
{
    NProto::TPlacementGroupConfig config;
    config.SetGroupId(std::move(id));

    for (auto& diskId: disks) {
        config.AddDisks()->SetDiskId(std::move(diskId));
    }

    PlacementGroups.push_back(config);

    return *this;
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskRegistryStateTest
