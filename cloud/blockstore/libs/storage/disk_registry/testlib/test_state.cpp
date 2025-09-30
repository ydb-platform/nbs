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

NProto::TPlacementGroupConfig SpreadPlacementGroup(
    TString id,
    TVector<TString> disks)
{
    NProto::TPlacementGroupConfig config;
    config.SetGroupId(std::move(id));
    config.SetPlacementStrategy(
        NProto::EPlacementStrategy::PLACEMENT_STRATEGY_SPREAD);

    for (auto& diskId: disks) {
        config.AddDisks()->SetDiskId(std::move(diskId));
    }

    return config;
}

NProto::TPlacementGroupConfig PartitionPlacementGroup(
    TString id,
    TVector<TVector<TString>> partitions)
{
    NProto::TPlacementGroupConfig config;
    config.SetGroupId(std::move(id));
    config.SetPlacementStrategy(
        NProto::EPlacementStrategy::PLACEMENT_STRATEGY_PARTITION);
    config.SetPlacementPartitionCount(partitions.size());

    for (size_t i = 0; i < partitions.size(); ++i) {
        for (auto& diskId: partitions[i]) {
            auto* disk = config.AddDisks();
            disk->SetDiskId(std::move(diskId));
            disk->SetPlacementPartitionIndex(i + 1);
        }
    }

    return config;
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
    Y_DEBUG_ABORT_UNLESS(
        uuids.size() == 2 || uuids.size() == 3,
        "Mirror disk must have 2 or 3 replicas; size=%lu",
        uuids.size());

    TVector<NProto::TDiskConfig> result;

    const auto mediaKind = uuids.size() == 2 ? NProto::STORAGE_MEDIA_SSD_MIRROR2
                          : NProto::STORAGE_MEDIA_SSD_MIRROR3;

    //Base disk
    NProto::TDiskConfig config;
    config.SetDiskId(diskId);
    config.SetBlockSize(DefaultLogicalBlockSize);
    config.SetState(state);
    config.SetReplicaCount(uuids.size() - 1);
    config.SetStorageMediaKind(mediaKind);
    result.push_back(std::move(config));

    //Replicas
    for (size_t i = 0; i < uuids.size(); ++i) {
        NProto::TDiskConfig replicaConfig;
        replicaConfig.SetMasterDiskId(diskId);
        replicaConfig.SetDiskId(TStringBuilder() << diskId << "/" << i);
        replicaConfig.SetBlockSize(DefaultLogicalBlockSize);
        replicaConfig.SetStorageMediaKind(mediaKind);
        replicaConfig.SetState(state);
        for (const auto& uuid: uuids[i]) {
            *replicaConfig.AddDeviceUUIDs() = uuid;
        }
        result.push_back(std::move(replicaConfig));
    }

    return result;
}

NProto::TDiskConfig ShadowDisk(
    const TString& sourceDiskId,
    const TString& checkpointId,
    std::initializer_list<TString> uuids,
    NProto::EDiskState state)
{
    NProto::TDiskConfig config;

    config.SetDiskId(sourceDiskId + "-" + checkpointId);
    auto* checkpoint = config.MutableCheckpointReplica();
    checkpoint->SetSourceDiskId(sourceDiskId);
    checkpoint->SetCheckpointId(checkpointId);
    config.SetBlockSize(DefaultLogicalBlockSize);
    config.SetState(state);

    for (const auto& uuid: uuids) {
        *config.AddDeviceUUIDs() = uuid;
    }

    return config;
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
    NProto::EStorageMediaKind mediaKind,
    ui32 logicalBlockSize)
{
    TDiskRegistryState::TAllocateDiskResult result {};

    auto error = state.AllocateDisk(
        now,
        db,
        TDiskRegistryState::TAllocateDiskParams {
            .DiskId = diskId,
            .CloudId = "cloud-1",
            .FolderId = "folder-1",
            .BlockSize = logicalBlockSize,
            .BlocksCount = totalSize / logicalBlockSize,
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
    NProto::EStorageMediaKind mediaKind,
    TString poolName,
    ui32 logicalBlockSize)
{
    TDiskRegistryState::TAllocateDiskResult result {};

    auto error = state.AllocateDisk(
        now,
        db,
        TDiskRegistryState::TAllocateDiskParams {
            .DiskId = diskId,
            .CloudId = "cloud-1",
            .FolderId = "folder-1",
            .PlacementGroupId = placementGroupId,
            .PlacementPartitionIndex = placementPartitionIndex,
            .BlockSize = logicalBlockSize,
            .BlocksCount = totalSize / logicalBlockSize,
            .ReplicaCount = 0,
            .PoolName = poolName,
            .MediaKind = mediaKind,
        },
        &result);

    devices = std::move(result.Devices);

    UNIT_ASSERT_VALUES_EQUAL(0, result.Replicas.size());
    UNIT_ASSERT_VALUES_EQUAL(0, result.DeviceReplacementIds.size());

    return error;
}

NProto::TError AllocateCheckpoint(
    TInstant now,
    TDiskRegistryDatabase& db,
    TDiskRegistryState& state,
    const TString& sourceDiskId,
    const TString& checkpointId,
    TString* shadowDiskId,
    TVector<TDeviceConfig>* devices)
{
    TDiskRegistryState::TAllocateCheckpointResult result{};

    auto error =
        state.AllocateCheckpoint(now, db, sourceDiskId, checkpointId, &result);

    UNIT_ASSERT_VALUES_EQUAL(0, result.Replicas.size());
    UNIT_ASSERT_VALUES_EQUAL(0, result.DeviceReplacementIds.size());

    *devices = std::move(result.Devices);
    *shadowDiskId = std::move(result.ShadowDiskId);

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
    config.SetDiskRegistryAlwaysAllocatesLocalDisks(true);

    return config;
}

TStorageConfigPtr CreateStorageConfig(NProto::TStorageServiceConfig proto)
{
    return std::make_shared<TStorageConfig>(
        std::move(proto),
        std::make_shared<NFeatures::TFeaturesConfig>(
            NCloud::NProto::TFeaturesConfig())
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
    auto [r, error] = state.RegisterAgent(db, config, timestamp);
    UNIT_ASSERT_VALUES_EQUAL(0, r.AffectedDisks.size());

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

TDiskRegistryStateBuilder TDiskRegistryStateBuilder::LoadState(
    TDiskRegistryDatabase& db)
{
    TDiskRegistryStateBuilder builder;

    db.ReadDiskRegistryConfig(builder.Config);
    db.ReadDirtyDevices(builder.DirtyDevices);
    db.ReadAgents(builder.Agents);
    db.ReadDisks(builder.Disks);
    db.ReadPlacementGroups(builder.PlacementGroups);
    db.ReadBrokenDisks(builder.BrokenDisks);
    db.ReadDisksToReallocate(builder.DisksToReallocate);
    db.ReadErrorNotifications(builder.ErrorNotifications);
    db.ReadUserNotifications(builder.UserNotifications);
    db.ReadDisksToCleanup(builder.DisksToCleanup);
    db.ReadOutdatedVolumeConfigs(builder.OutdatedVolumeConfigs);
    db.ReadSuspendedDevices(builder.SuspendedDevices);
    db.ReadAutomaticallyReplacedDevices(builder.AutomaticallyReplacedDevices);
    db.ReadDiskRegistryAgentListParams(builder.DiskRegistryAgentListParams);

    return builder;
}

std::unique_ptr<TDiskRegistryState> TDiskRegistryStateBuilder::Build()
{
    return std::make_unique<TDiskRegistryState>(
        std::move(Logging),
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
        std::move(UserNotifications),
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
    TVector<TDirtyDevice> dirtyDevices)
{
    DirtyDevices = std::move(dirtyDevices);
    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithSuspendedDevices(
    TVector<TString> suspendedDevices)
{
    SuspendedDevices.clear();
    SuspendedDevices.reserve(suspendedDevices.size());
    for (auto& uuid: suspendedDevices) {
        NProto::TSuspendedDevice suspendedDevice;
        suspendedDevice.SetId(uuid);
        SuspendedDevices.push_back(std::move(suspendedDevice));
    }
    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithSpreadPlacementGroups(
    TVector<TString> groupIds)
{
    for (auto& id: groupIds) {
        NProto::TPlacementGroupConfig config;
        config.SetGroupId(std::move(id));

        PlacementGroups.push_back(config);
    }

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithPlacementGroups(
    TVector<NProto::TPlacementGroupConfig> groups)
{
    for (auto& group: groups) {
        PlacementGroups.push_back(std::move(group));
    }

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::WithErrorNotifications(
    TVector<TString> notifications)
{
    ErrorNotifications = std::move(notifications);

    return *this;
}

TDiskRegistryStateBuilder& TDiskRegistryStateBuilder::AddDevicePoolConfig(
    TString name,
    ui64 allocationUnit,
    NProto::EDevicePoolKind kind)
{
    NProto::TDevicePoolConfig& pool = *Config.AddDevicePoolConfigs();
    pool.SetName(std::move(name));
    pool.SetAllocationUnit(allocationUnit);
    pool.SetKind(kind);

    return *this;
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskRegistryStateTest
