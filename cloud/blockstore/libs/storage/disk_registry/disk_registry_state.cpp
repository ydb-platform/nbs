#include "disk_registry_state.h"

#include "disk_registry_schema.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/disk_validation.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/disk_common/monitoring_utils.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/algorithm.h>
#include <util/generic/iterator_range.h>
#include <util/generic/overloaded.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <algorithm>
#include <tuple>

namespace NCloud::NBlockStore::NStorage {

namespace {

template<typename T>
struct TTableCount;

template<typename ... Ts>
struct TTableCount<NKikimr::NIceDb::Schema::SchemaTables<Ts...>>
{
    enum { value = sizeof...(Ts) };
};

////////////////////////////////////////////////////////////////////////////////

struct TByUUID
{
    template <typename T, typename U>
    bool operator () (const T& lhs, const U& rhs) const
    {
        return lhs.GetDeviceUUID() < rhs.GetDeviceUUID();
    }
};

NProto::EDiskState ToDiskState(NProto::EAgentState agentState)
{
    switch (agentState)
    {
    case NProto::AGENT_STATE_ONLINE:
        return NProto::DISK_STATE_ONLINE;
    case NProto::AGENT_STATE_WARNING:
        return NProto::DISK_STATE_WARNING;
    case NProto::AGENT_STATE_UNAVAILABLE:
        return NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE;
    default:
        Y_ABORT("unknown agent state");
    }
}

NProto::EDiskState ToDiskState(NProto::EDeviceState deviceState)
{
    switch (deviceState)
    {
    case NProto::DEVICE_STATE_ONLINE:
        return NProto::DISK_STATE_ONLINE;
    case NProto::DEVICE_STATE_WARNING:
        return NProto::DISK_STATE_WARNING;
    case NProto::DEVICE_STATE_ERROR:
        return NProto::DISK_STATE_ERROR;
    default:
        Y_ABORT("unknown device state");
    }
}

////////////////////////////////////////////////////////////////////////////////

TString GetMirroredDiskGroupId(const TString& diskId)
{
    return diskId + "/g";
}

TString GetReplicaDiskId(const TString& diskId, ui32 i)
{
    return TStringBuilder() << diskId << "/" << i;
}

////////////////////////////////////////////////////////////////////////////////

TDuration GetInfraTimeout(
    const TStorageConfig& config,
    NProto::EAgentState agentState)
{
    if (agentState == NProto::AGENT_STATE_UNAVAILABLE) {
        return config.GetNonReplicatedInfraUnavailableAgentTimeout();
    }

    return config.GetNonReplicatedInfraTimeout();
}

////////////////////////////////////////////////////////////////////////////////

TDevicePoolConfigs CreateDevicePoolConfigs(
    const NProto::TDiskRegistryConfig& config,
    const TStorageConfig& storageConfig)
{
    NProto::TDevicePoolConfig nonrepl;
    nonrepl.SetKind(NProto::DEVICE_POOL_KIND_DEFAULT);
    nonrepl.SetAllocationUnit(
        storageConfig.GetAllocationUnitNonReplicatedSSD() * 1_GB);

    TDevicePoolConfigs result {
        { TString {}, nonrepl }
    };

    for (const auto& pool: config.GetDevicePoolConfigs()) {
        result[pool.GetName()] = pool;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool PlacementGroupMustHavePartitions(
    const NProto::TPlacementGroupConfig& placementGroup)
{
    switch (placementGroup.GetPlacementStrategy()) {
        case NProto::PLACEMENT_STRATEGY_SPREAD:
            return false;
        case NProto::PLACEMENT_STRATEGY_PARTITION:
            return true;
        default:
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

bool PartitionSuitsPlacementGroup(
    const NProto::TPlacementGroupConfig& placementGroup,
    const ui32 placementPartitionIndex)
{
    if (!PlacementGroupMustHavePartitions(placementGroup)) {
        return placementPartitionIndex == 0;
    }

    return placementPartitionIndex > 0 &&
           placementPartitionIndex <=
               placementGroup.GetPlacementPartitionCount();
}

////////////////////////////////////////////////////////////////////////////////

ui32 GetMaxDisksInPlacementGroup(
    const TStorageConfig& config,
    const NProto::TPlacementGroupConfig& g)
{
    if (g.GetSettings().GetMaxDisksInGroup()) {
        return g.GetSettings().GetMaxDisksInGroup();
    }

    if (g.GetPlacementStrategy() == NProto::PLACEMENT_STRATEGY_PARTITION) {
        return config.GetMaxDisksInPartitionPlacementGroup();
    }

    return config.GetMaxDisksInPlacementGroup();
}

////////////////////////////////////////////////////////////////////////////////

TString MakeMirroredDiskDeviceReplacementMessage(
    TStringBuf diskId,
    TStringBuf reason)
{
    TStringBuilder message;
    message << "MirroredDiskId=" << diskId << ", ReplacementReason=" << reason;
    return std::move(message);
}

////////////////////////////////////////////////////////////////////////////////

struct TDevicePoolCounters
{
    ui64 FreeBytes = 0;
    ui64 TotalBytes = 0;
    ui64 BrokenBytes = 0;
    ui64 DecommissionedBytes = 0;
    ui64 SuspendedBytes = 0;
    ui64 DirtyBytes = 0;
    ui64 AllocatedBytes = 0;
    ui64 AllocatedDevices = 0;
    ui64 DirtyDevices = 0;
    ui64 DevicesInOnlineState = 0;
    ui64 DevicesInWarningState = 0;
    ui64 DevicesInErrorState = 0;
};

void SetDevicePoolCounters(
    TDiskRegistrySelfCounters::TDevicePoolCounters& counters,
    const TDevicePoolCounters& values)
{
    counters.FreeBytes->Set(values.FreeBytes);
    counters.TotalBytes->Set(values.TotalBytes);
    counters.BrokenBytes->Set(values.BrokenBytes);
    counters.DecommissionedBytes->Set(values.DecommissionedBytes);
    counters.SuspendedBytes->Set(values.SuspendedBytes);
    counters.DirtyBytes->Set(values.DirtyBytes);
    counters.AllocatedBytes->Set(values.AllocatedBytes);
    counters.AllocatedDevices->Set(values.AllocatedDevices);
    counters.DirtyDevices->Set(values.DirtyDevices);
    counters.DevicesInOnlineState->Set(values.DevicesInOnlineState);
    counters.DevicesInWarningState->Set(values.DevicesInWarningState);
    counters.DevicesInErrorState->Set(values.DevicesInErrorState);
}

////////////////////////////////////////////////////////////////////////////////

TString GetPoolNameForCounters(
    const TString& poolName,
    const NProto::EDevicePoolKind poolKind)
{
    if (poolKind == NProto::DEVICE_POOL_KIND_LOCAL) {
        return "local";
    } else if (poolKind == NProto::DEVICE_POOL_KIND_DEFAULT) {
        return "default";
    }

    return poolName;
}

void SetDeviceErrorState(
    NProto::TDeviceConfig& device,
    TInstant timestamp,
    TString message)
{
    device.SetState(NProto::DEVICE_STATE_ERROR);
    device.SetStateTs(timestamp.MicroSeconds());
    device.SetStateMessage(std::move(message));
}

auto CollectDirtyDeviceUUIDs(const TVector<TDirtyDevice>& dirtyDevices)
{
    TVector<TString> uuids;
    uuids.reserve(dirtyDevices.size());

    for (const auto& [uuid, diskId]: dirtyDevices) {
        uuids.push_back(uuid);
    }

    return uuids;
}

auto CollectAllocatedDevices(const TVector<NProto::TDiskConfig>& disks)
{
    TVector<std::pair<TString, TString>> r;

    for (const auto& disk: disks) {
        for (const auto& uuid: disk.GetDeviceUUIDs()) {
            r.emplace_back(uuid, disk.GetDiskId());
        }
    }

    return r;
}

bool HasNewLayout(
    const NProto::TDeviceConfig& newConfig,
    const NProto::TDeviceConfig& oldConfig)
{
    auto key = [] (const NProto::TDeviceConfig& device) {
        return std::make_tuple(
            device.GetBlockSize(),
            device.GetBlocksCount(),
            device.GetUnadjustedBlockCount(),
            TStringBuf {device.GetDeviceName()}
        );
    };

    return key(oldConfig) != key(newConfig) ||
           (oldConfig.GetPhysicalOffset() &&
            oldConfig.GetPhysicalOffset() != newConfig.GetPhysicalOffset());
}

bool HasNewSerialNumber(
    const NProto::TDeviceConfig& newConfig,
    const NProto::TDeviceConfig& oldConfig)
{
    return oldConfig.GetSerialNumber() &&
           oldConfig.GetSerialNumber() != newConfig.GetSerialNumber();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString TCheckpointInfo::Id() const
{
    return MakeId(SourceDiskId, CheckpointId);
}

// static
TString TCheckpointInfo::MakeId(
    const TString& sourceDiskId,
    const TString& checkpointId)
{
    return sourceDiskId + "-" + checkpointId;
}

////////////////////////////////////////////////////////////////////////////////

ui64 TDiskInfo::GetBlocksCount() const
{
    ui64 result = 0;
    for (const auto& device: Devices) {
        const ui64 logicalBlockCount =
            device.GetBlockSize() * device.GetBlocksCount() / LogicalBlockSize;
        result += logicalBlockCount;
    }
    return result;
}

TVector<TBlockRange64> TDiskInfo::GetDeviceRanges() const
{
    TVector<TBlockRange64> result;
    result.reserve(Devices.size());
    ui64 startOffset = 0;
    for (const auto& device: Devices) {
        const ui64 logicalBlockCount =
            device.GetBlockSize() * device.GetBlocksCount() / LogicalBlockSize;
        result.push_back(
            TBlockRange64::WithLength(startOffset, logicalBlockCount));
        startOffset += logicalBlockCount;
    }
    return result;
}

TString TDiskInfo::GetPoolName() const
{
    for (const auto& replica: Replicas) {
        for (const auto& device: replica) {
            return device.GetPoolName();
        }
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryState::TDiskRegistryState(
        ILoggingServicePtr logging,
        TStorageConfigPtr storageConfig,
        NMonitoring::TDynamicCountersPtr counters,
        NProto::TDiskRegistryConfig config,
        TVector<NProto::TAgentConfig> agents,
        TVector<NProto::TDiskConfig> disks,
        TVector<NProto::TPlacementGroupConfig> placementGroups,
        TVector<TBrokenDiskInfo> brokenDisks,
        TVector<TString> disksToReallocate,
        TVector<TDiskStateUpdate> diskStateUpdates,
        ui64 diskStateSeqNo,
        TVector<TDirtyDevice> dirtyDevices,
        TVector<TString> disksToCleanup,
        TVector<TString> errorNotifications,
        TVector<NProto::TUserNotification> userNotifications,
        TVector<TString> outdatedVolumeConfigs,
        TVector<NProto::TSuspendedDevice> suspendedDevices,
        TDeque<TAutomaticallyReplacedDeviceInfo> automaticallyReplacedDevices,
        THashMap<TString, NProto::TDiskRegistryAgentParams> diskRegistryAgentListParams)
    : Log(logging->CreateLog("BLOCKSTORE_DISK_REGISTRY"))
    , StorageConfig(std::move(storageConfig))
    , Counters(counters)
    , AgentList({
        static_cast<double>(
            StorageConfig->GetNonReplicatedAgentTimeoutGrowthFactor()),
        StorageConfig->GetNonReplicatedAgentMinTimeout(),
        StorageConfig->GetNonReplicatedAgentMaxTimeout(),
        StorageConfig->GetNonReplicatedAgentDisconnectRecoveryInterval(),
        StorageConfig->GetSerialNumberValidationEnabled(),
    }, counters, std::move(agents), std::move(diskRegistryAgentListParams), Log)
    , DeviceList(
        CollectDirtyDeviceUUIDs(dirtyDevices),
        std::move(suspendedDevices),
        CollectAllocatedDevices(disks),
        StorageConfig->GetDiskRegistryAlwaysAllocatesLocalDisks())
    , BrokenDisks(std::move(brokenDisks))
    , AutomaticallyReplacedDevices(std::move(automaticallyReplacedDevices))
    , CurrentConfig(std::move(config))
    , NotificationSystem {
        StorageConfig,
        std::move(errorNotifications),
        std::move(userNotifications),
        std::move(disksToReallocate),
        std::move(diskStateUpdates),
        diskStateSeqNo,
        std::move(outdatedVolumeConfigs)
    }
{
    for (const auto& x: AutomaticallyReplacedDevices) {
        AutomaticallyReplacedDeviceIds.insert(x.DeviceId);
    }

    // Config doesn't depend on anything
    ProcessConfig(CurrentConfig);
    // fills DeviceList and shouldn't depend on anything apart from AgentList
    ProcessAgents();
    // fills disk configs and may use DeviceList
    ProcessDisks(std::move(disks));
    // fills Checkpoints and depends on Disks
    ProcessCheckpoints();
    // fills PlacementGroups and depends on Disks
    ProcessPlacementGroups(std::move(placementGroups));
    // fills DisksToCleanup and shouldn't depend on anything
    ProcessDisksToCleanup(std::move(disksToCleanup));
    // fills PendingCleanup and shouldn't depend on anything as well
    ProcessDirtyDevices(std::move(dirtyDevices));
    // fills Migrations and uses both Disks and DeviceList
    FillMigrations();

    if (Counters) {
        TVector<TString> poolNames;
        for (const auto& x: DevicePoolConfigs) {
            const auto& pool = x.second;
            poolNames.push_back(
                GetPoolNameForCounters(pool.GetName(), pool.GetKind()));
        }
        SelfCounters.Init(poolNames, Counters);
    }
}

TDiskRegistryState::~TDiskRegistryState() = default;

void TDiskRegistryState::AllowNotifications(
    const TDiskId& diskId,
    const TDiskState& disk)
{
    // We do not want to notify the user about the breakdowns of the shadow disks.
    if (disk.CheckpointReplica.GetCheckpointId()) {
        return;
    }

    Y_DEBUG_ABORT_UNLESS(IsDiskRegistryMediaKind(disk.MediaKind));
    if (!IsReliableDiskRegistryMediaKind(disk.MediaKind)) {
        NotificationSystem.AllowNotifications(diskId);
    }

    if (disk.MasterDiskId) {
        NotificationSystem.AllowNotifications(disk.MasterDiskId);
    }
}

void TDiskRegistryState::ProcessDisks(TVector<NProto::TDiskConfig> configs)
{
    for (auto& config: configs) {
        const auto& diskId = config.GetDiskId();
        auto& disk = Disks[config.GetDiskId()];
        disk.LogicalBlockSize = config.GetBlockSize();
        disk.Devices.reserve(config.DeviceUUIDsSize());
        disk.State = config.GetState();
        disk.StateTs = TInstant::MicroSeconds(config.GetStateTs());
        disk.CloudId = config.GetCloudId();
        disk.FolderId = config.GetFolderId();
        disk.UserId = config.GetUserId();
        disk.ReplicaCount = config.GetReplicaCount();
        disk.MasterDiskId = config.GetMasterDiskId();
        disk.CheckpointReplica = config.GetCheckpointReplica();
        disk.MediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED;
        disk.MigrationStartTs = TInstant::MicroSeconds(config.GetMigrationStartTs());

        for (auto& hi: *config.MutableHistory()) {
            disk.History.push_back(std::move(hi));
        }

        if (config.GetStorageMediaKind() != NProto::STORAGE_MEDIA_DEFAULT) {
            disk.MediaKind = config.GetStorageMediaKind();
        } else if (disk.ReplicaCount == 1) {
            disk.MediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR2;
        } else if (disk.ReplicaCount == 2) {
            disk.MediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR3;
        } else {
            // XXX temp hack, see NBS-4703
            bool foundRotPool = false;
            for (const auto& uuid: config.GetDeviceUUIDs()) {
                const auto* device = DeviceList.FindDevice(uuid);
                // can't use impossible events here since there are some uts
                // that configure TDiskRegistryState in a 'peculiar' way
                if (!device) {
                    ReportDiskRegistryDeviceNotFoundSoft(
                        TStringBuilder() << "ProcessDisks:DeviceId: " << uuid);
                    continue;
                }

                if (device->GetPoolName() == "rot") {
                    foundRotPool = true;
                    break;
                }
            }

            if (foundRotPool) {
                disk.MediaKind = NProto::STORAGE_MEDIA_HDD_NONREPLICATED;
            } else {
                Y_DEBUG_ABORT_UNLESS(
                    disk.ReplicaCount == 0,
                    "unexpected ReplicaCount: %d",
                    disk.ReplicaCount);
            }
        }

        AllowNotifications(diskId, disk);

        for (const auto& uuid: config.GetDeviceUUIDs()) {
            disk.Devices.push_back(uuid);

            DeviceList.MarkDeviceAllocated(diskId, uuid);
        }

        if (disk.MasterDiskId) {
            ReplicaTable.AddReplica(disk.MasterDiskId, disk.Devices);
        }

        for (const auto& id: config.GetDeviceReplacementUUIDs()) {
            disk.DeviceReplacementIds.push_back(id);
        }

        for (const auto& m: config.GetMigrations()) {
            const auto* device = DeviceList.FindDevice(m.GetSourceDeviceId());
            TString poolName;
            if (device) {
                poolName = device->GetPoolName();
            }

            SourceDeviceMigrationsInProgress[poolName].insert(
                m.GetSourceDeviceId());

            disk.MigrationTarget2Source.emplace(
                m.GetTargetDevice().GetDeviceUUID(),
                m.GetSourceDeviceId());

            disk.MigrationSource2Target.emplace(
                m.GetSourceDeviceId(),
                m.GetTargetDevice().GetDeviceUUID());

            DeviceList.MarkDeviceAllocated(
                diskId,
                m.GetTargetDevice().GetDeviceUUID());
        }

        // backward compatibility
        if (!disk.MigrationStartTs && !disk.MigrationSource2Target.empty()) {
            disk.MigrationStartTs = disk.StateTs;
        }

        if (disk.MigrationSource2Target.empty()) {
            disk.MigrationStartTs = {};
        }

        auto getSeqNo = [this](const TDiskId& diskId)
        {
            const TString diskIdToNotify = GetDiskIdToNotify(diskId);
            ui64 seqNo = NotificationSystem.GetDiskSeqNo(diskIdToNotify);
            if (!seqNo) {
                ReportDiskRegistryNoScheduledNotification(
                    TStringBuilder() << "No scheduled notification for disk "
                                     << diskIdToNotify.Quote());
                seqNo = NotificationSystem.AddReallocateRequest(diskIdToNotify);
            }

            return seqNo;
        };

        if (!config.GetFinishedMigrations().empty()) {
            const ui64 seqNo = getSeqNo(diskId);

            for (const auto& m: config.GetFinishedMigrations()) {
                const auto& uuid = m.GetDeviceId();

                DeviceList.MarkDeviceAllocated(diskId, uuid);

                disk.FinishedMigrations.push_back(
                    {.DeviceId = uuid, .SeqNo = seqNo});
            }
        }

        if (!config.GetLaggingDevices().empty()) {
            const ui64 seqNo = getSeqNo(diskId);
            for (const auto& laggingDevice: config.GetLaggingDevices()) {
                disk.LaggingDevices.emplace_back(laggingDevice, seqNo);
            }
        }
    }

    for (const auto& x: Disks) {
        if (x.second.ReplicaCount) {
            for (const auto& id: x.second.DeviceReplacementIds) {
                ReplicaTable.MarkReplacementDevice(x.first, id, true);
            }
        }
    }
}

void TDiskRegistryState::ProcessCheckpoints()
{
    for (const auto& [diskId, diskState]: Disks) {
        const auto& checkpointReplica = diskState.CheckpointReplica;

        if (!checkpointReplica.GetCheckpointId().empty()) {
            auto* sourceDisk = Disks.FindPtr(checkpointReplica.GetSourceDiskId());
            if (!sourceDisk) {
                ReportDiskRegistrySourceDiskNotFound(
                    TStringBuilder()
                    << "CheckpointId: " << checkpointReplica.GetCheckpointId()
                    << " SourceDiskId: " << checkpointReplica.GetSourceDiskId()
                    << " ReplicaDiskId: " << diskId);
                continue;
            }

            TCheckpointInfo checkpointInfo{
                checkpointReplica.GetSourceDiskId(),
                checkpointReplica.GetCheckpointId(),
                diskId};
            Checkpoints[checkpointInfo.Id()] = std::move(checkpointInfo);
        }
    }
}

void TDiskRegistryState::AddMigration(
    const TDiskState& disk,
    const TString& diskId,
    const TString& sourceDeviceId)
{
    if (disk.CheckpointReplica.GetCheckpointId()) {
        // Don't start migrations for shadow disks.
        return;
    }

    if (IsDiskRegistryLocalMediaKind(disk.MediaKind) ||
        disk.MediaKind == NProto::STORAGE_MEDIA_HDD_NONREPLICATED)
    {
        return;
    }

    if (disk.MasterDiskId) {
        // mirrored disk replica
        if (!StorageConfig->GetMirroredMigrationStartAllowed()) {
            return;
        }
    } else {
        // nonreplicated disk
        if (!StorageConfig->GetNonReplicatedMigrationStartAllowed()) {
            return;
        }
    }

    Migrations.emplace(diskId, sourceDeviceId);
}

void TDiskRegistryState::FillMigrations()
{
    for (auto& [diskId, disk]: Disks) {
        if (disk.State == NProto::DISK_STATE_ONLINE) {
            continue;
        }

        for (const auto& uuid: disk.Devices) {
            if (disk.MigrationSource2Target.contains(uuid)) {
                continue; // migration in progress
            }

            auto [agent, device] = FindDeviceLocation(uuid);

            if (!device) {
                ReportDiskRegistryDeviceNotFound(
                    TStringBuilder() << "FillMigrations:DeviceId: " << uuid);

                continue;
            }

            if (!agent) {
                ReportDiskRegistryAgentNotFound(
                    TStringBuilder() << "FillMigrations:AgentId: "
                    << device->GetAgentId());

                continue;
            }

            if (device->GetState() == NProto::DEVICE_STATE_WARNING) {
                AddMigration(disk, diskId, uuid);

                continue;
            }

            if (device->GetState() == NProto::DEVICE_STATE_ERROR) {
                continue; // skip for broken devices
            }

            if (agent->GetState() == NProto::AGENT_STATE_WARNING) {
                AddMigration(disk, diskId, uuid);
            }
        }
    }
}

void TDiskRegistryState::ProcessPlacementGroups(
    TVector<NProto::TPlacementGroupConfig> configs)
{
    for (auto& config: configs) {
        auto groupId = config.GetGroupId();

        for (const auto& disk: config.GetDisks()) {
            auto* d = Disks.FindPtr(disk.GetDiskId());

            if (!d) {
                ReportDiskRegistryDiskNotFound(TStringBuilder()
                    << "ProcessPlacementGroups:DiskId: " << disk.GetDiskId());

                continue;
            }

            d->PlacementGroupId = groupId;

            if (PlacementGroupMustHavePartitions(config)) {
                if (!PartitionSuitsPlacementGroup(
                        config,
                        disk.GetPlacementPartitionIndex()))
                {
                    ReportDiskRegistryInvalidPlacementGroupPartition(
                        TStringBuilder()
                        << "ProcessPlacementGroups:DiskId: " << disk.GetDiskId()
                        << ", PlacementGroupId: " << config.GetGroupId()
                        << ", Strategy: "
                        << NProto::EPlacementStrategy_Name(
                               config.GetPlacementStrategy())
                        << ", PlacementPartitionIndex: "
                        << disk.GetPlacementPartitionIndex());
                    continue;
                }
                d->PlacementPartitionIndex = disk.GetPlacementPartitionIndex();
            }
        }

        PlacementGroups[groupId] = std::move(config);
    }
}

void TDiskRegistryState::ProcessAgents()
{
    for (auto& agent: AgentList.GetAgents()) {
        DeviceList.UpdateDevices(agent, DevicePoolConfigs);
        TimeBetweenFailures.SetWorkTime(
            TimeBetweenFailures.GetWorkTime() +
            agent.GetTimeBetweenFailures().GetWorkTime());
        TimeBetweenFailures.SetBrokenCount(
            TimeBetweenFailures.GetBrokenCount() +
            agent.GetTimeBetweenFailures().GetBrokenCount());
    }
}

void TDiskRegistryState::ProcessDisksToCleanup(TVector<TString> disksToCleanup)
{
    for (auto& diskId: disksToCleanup) {
        DisksToCleanup.emplace(std::move(diskId));
    }
}

void TDiskRegistryState::ProcessDirtyDevices(TVector<TDirtyDevice> dirtyDevices)
{
    for (auto&& [uuid, diskId]: dirtyDevices) {
        if (!diskId.empty()) {
            auto error = AddDevicesToPendingCleanup(diskId, {std::move(uuid)});
            if (HasError(error)) {
                ReportDiskRegistryInsertToPendingCleanupFailed(
                    TStringBuilder()
                    << "An error occurred while processing dirty devices: "
                    << FormatError(error));
            }
        }
    }
}

const TVector<NProto::TAgentConfig>& TDiskRegistryState::GetAgents() const
{
    return AgentList.GetAgents();
}

NProto::TError TDiskRegistryState::ValidateAgent(
    const NProto::TAgentConfig& agent) const
{
    const auto& agentId = agent.GetAgentId();

    if (agentId.empty()) {
        return MakeError(E_ARGUMENT, "empty agent id");
    }

    if (agent.GetNodeId() == 0) {
        return MakeError(E_ARGUMENT, "empty node id");
    }

    auto* buddy = AgentList.FindAgent(agent.GetNodeId());

    if (buddy
        && buddy->GetAgentId() != agentId
        && buddy->GetState() != NProto::AGENT_STATE_UNAVAILABLE)
    {
        return MakeError(E_INVALID_STATE, TStringBuilder()
            << "Agent " << buddy->GetAgentId().Quote()
            << " already registered at node #" << agent.GetNodeId());
    }

    buddy = AgentList.FindAgent(agentId);

    if (buddy
        && buddy->GetSeqNumber() > agent.GetSeqNumber()
        && buddy->GetState() != NProto::AGENT_STATE_UNAVAILABLE)
    {
        return MakeError(E_INVALID_STATE, TStringBuilder()
            << "Agent " << buddy->GetAgentId().Quote()
            << " already registered with a greater SeqNo "
            << "(" << buddy->GetSeqNumber() << " > " << agent.GetSeqNumber() << ")");
    }

    const TKnownAgent* knownAgent = KnownAgents.FindPtr(agentId);

    if (!knownAgent) {
        return {};
    }

    TString rack;
    if (agent.DevicesSize()) {
        rack = agent.GetDevices(0).GetRack();
    }

    for (const auto& device: agent.GetDevices()) {
        // right now we suppose that each agent presents devices
        // from one rack only
        if (rack != device.GetRack()) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "all agent devices should come from the same rack, mismatch: "
                << rack << " != " << device.GetRack());
        }

        if (device.GetState() != NProto::DEVICE_STATE_ONLINE &&
            device.GetState() != NProto::DEVICE_STATE_ERROR)
        {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "unexpected state of the device " << device.GetDeviceUUID()
                << ": " << NProto::EDeviceState_Name(device.GetState())
                << " (" << static_cast<ui32>(device.GetState()) << ")");
        }
    }

    return {};
}

auto TDiskRegistryState::FindDisk(const TDeviceId& uuid) const -> TDiskId
{
    return DeviceList.FindDiskId(uuid);
}

void TDiskRegistryState::AdjustDeviceIfNeeded(
    NProto::TDeviceConfig& device,
    TInstant timestamp)
{
    if (!device.GetUnadjustedBlockCount()) {
        device.SetUnadjustedBlockCount(device.GetBlocksCount());
    }

    const auto* poolConfig = DevicePoolConfigs.FindPtr(device.GetPoolName());
    if (!poolConfig) {
        SetDeviceErrorState(device, timestamp, TStringBuilder()
            << "unknown pool: " << device.GetPoolName());

        return;
    }

    device.SetPoolKind(poolConfig->GetKind());

    const ui64 unit = poolConfig->GetAllocationUnit();
    Y_DEBUG_ABORT_UNLESS(unit != 0);

    if (device.GetState() == NProto::DEVICE_STATE_ERROR ||
        !DeviceList.FindDiskId(device.GetDeviceUUID()).empty())
    {
        return;
    }

    const auto deviceSize =
        device.GetBlockSize() * device.GetUnadjustedBlockCount();

    if (deviceSize < unit) {
        SetDeviceErrorState(device, timestamp, TStringBuilder()
            << "device is too small: " << deviceSize);

        return;
    }

    if (device.GetBlockSize() == 0 || unit % device.GetBlockSize() != 0) {
        SetDeviceErrorState(device, timestamp, TStringBuilder()
            << "bad block size: " << device.GetBlockSize());

        return;
    }

    device.SetBlocksCount(unit / device.GetBlockSize());
}

void TDiskRegistryState::RemoveAgentFromNode(
    TDiskRegistryDatabase& db,
    NProto::TAgentConfig& agent,
    TInstant timestamp,
    TVector<TDiskId>& affectedDisks,
    THashSet<TDiskId>& disksToReallocate)
{
    Y_DEBUG_ABORT_UNLESS(agent.GetState() == NProto::AGENT_STATE_UNAVAILABLE);

    const ui32 nodeId = agent.GetNodeId();
    agent.SetNodeId(0);
    ChangeAgentState(
        agent,
        NProto::AGENT_STATE_UNAVAILABLE,
        timestamp,
        "lost");

    for (auto& d: *agent.MutableDevices()) {
        d.SetNodeId(0);

        const auto& uuid = d.GetDeviceUUID();
        auto diskId = DeviceList.FindDiskId(uuid);

        if (!diskId.empty()) {
            disksToReallocate.emplace(std::move(diskId));
        }
    }

    AgentList.RemoveAgentFromNode(nodeId);
    DeviceList.UpdateDevices(agent, DevicePoolConfigs, nodeId);

    db.UpdateAgent(agent);

    ApplyAgentStateChange(db, agent, timestamp, affectedDisks);
}

auto TDiskRegistryState::RegisterAgent(
    TDiskRegistryDatabase& db,
    NProto::TAgentConfig config,
    TInstant timestamp) -> TResultOrError<TAgentRegistrationResult>
{
    if (auto error = ValidateAgent(config); HasError(error)) {
        return error;
    }

    TVector<TDiskId> affectedDisks;
    THashSet<TDiskId> disksToReallocate;
    TVector<TString> devicesToDisableIO;

    try {
        if (auto* buddy = AgentList.FindAgent(config.GetNodeId());
                buddy && buddy->GetAgentId() != config.GetAgentId())
        {
            STORAGE_INFO(
                "Agent %s occupies the same node (#%d) as the arriving agent %s. "
                "Kick out %s",
                buddy->GetAgentId().c_str(),
                config.GetNodeId(),
                config.GetAgentId().c_str(),
                buddy->GetAgentId().c_str());

            RemoveAgentFromNode(
                db,
                *buddy,
                timestamp,
                affectedDisks,
                disksToReallocate);
        }

        const auto& knownAgent = KnownAgents.Value(
            config.GetAgentId(),
            TKnownAgent {});

        auto r = AgentList.RegisterAgent(
            std::move(config),
            timestamp,
            knownAgent);

        NProto::TAgentConfig& agent = r.Agent;

        for (auto& d: *agent.MutableDevices()) {
            const auto& uuid = d.GetDeviceUUID();

            auto* oldConfig = r.OldConfigs.FindPtr(uuid);
            const auto diskId = FindDisk(uuid);

            const bool isChangeDestructive =
                oldConfig && (HasNewLayout(d, *oldConfig) ||
                              HasNewSerialNumber(d, *oldConfig));

            if (diskId && isChangeDestructive) {
                TString message =
                    TStringBuilder()
                    << "Device configuration has changed: " << *oldConfig
                    << " -> " << d << ". Affected disk: " << diskId;

                if (d.GetState() != NProto::DEVICE_STATE_ERROR ||
                    HasNewLayout(d, *oldConfig))
                {
                    ReportDiskRegistryOccupiedDeviceConfigurationHasChanged(
                        message);
                } else {
                    STORAGE_WARN(message);
                }

                SetDeviceErrorState(d, timestamp, std::move(message));

                continue;
            }

            // To prevent data leakage we should clean the available device if
            // its configuration has been changed
            if (diskId.empty() && isChangeDestructive && !IsDirtyDevice(uuid)) {
                DeviceList.MarkDeviceAsDirty(uuid);
                db.UpdateDirtyDevice(uuid, {});
            }

            AdjustDeviceIfNeeded(d, timestamp);
            if (r.NewDeviceIds.contains(uuid)) {
                SuspendDeviceIfNeeded(db, d);
            }
        }

        DeviceList.UpdateDevices(agent, DevicePoolConfigs, r.PrevNodeId);

        for (const auto& uuid: r.NewDeviceIds) {
            if (!DeviceList.FindDiskId(uuid)) {
                DeviceList.MarkDeviceAsDirty(uuid);
                db.UpdateDirtyDevice(uuid, {});
            }
        }

        ReallocateDisksWithLostOrReappearedDevices(r, disksToReallocate);

        THashSet<TDiskId> diskIds;

        for (const auto& d: agent.GetDevices()) {
            const auto& uuid = d.GetDeviceUUID();

            if (d.GetState() == NProto::DEVICE_STATE_ERROR) {
                devicesToDisableIO.push_back(uuid);
            }

            auto diskId = DeviceList.FindDiskId(uuid);

            if (diskId.empty()) {
                continue;
            }

            if (d.GetState() == NProto::DEVICE_STATE_ERROR) {
                auto& disk = Disks[diskId];

                if (disk.MasterDiskId) {
                    TryToReplaceDeviceIfAllowedWithoutDiskStateUpdate(
                        db,
                        disk,
                        diskId,
                        d.GetDeviceUUID(),
                        timestamp,
                        "device failure");
                }

                if (!RestartDeviceMigration(timestamp, db, diskId, disk, uuid)) {
                    CancelDeviceMigration(timestamp, db, diskId, disk, uuid);
                }
            }

            diskIds.emplace(std::move(diskId));
        }

        for (const auto& id: diskIds) {
            if (TryUpdateDiskState(db, id, timestamp)) {
                affectedDisks.push_back(id);
            }
            TDiskState& disk = Disks[id];
            UpdatePlacementGroup(db, id, disk, "RegisterAgent");
        }

        if (r.PrevNodeId != agent.GetNodeId()) {
            disksToReallocate.insert(diskIds.begin(), diskIds.end());
        }

        if (agent.GetState() == NProto::AGENT_STATE_UNAVAILABLE) {
            agent.SetCmsTs(0);
            ChangeAgentState(
                agent,
                NProto::AGENT_STATE_WARNING,
                timestamp,
                "back from unavailable");

            ApplyAgentStateChange(db, agent, timestamp, affectedDisks);

            disksToReallocate.insert(diskIds.begin(), diskIds.end());
        }

        UpdateAgent(db, agent);
    } catch (const TServiceError& e) {
        return MakeError(e.GetCode(), e.what());
    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }

    for (const auto& diskId: disksToReallocate) {
        AddReallocateRequest(db, diskId);
    }

    return TAgentRegistrationResult{
        .AffectedDisks = std::move(affectedDisks),
        .DisksToReallocate =
            {disksToReallocate.begin(), disksToReallocate.end()},
        .DevicesToDisableIO = std::move(devicesToDisableIO)};
}

void TDiskRegistryState::ReallocateDisksWithLostOrReappearedDevices(
    const TAgentList::TAgentRegistrationResult& r,
    THashSet<TDiskId>& disksToReallocate)
{
    const NProto::TAgentConfig& agent = r.Agent;
    for (const auto& lostDeviceId: r.LostDeviceIds) {
        auto diskId = DeviceList.FindDiskId(lostDeviceId);
        if (!diskId) {
            continue;
        }

        auto* disk = Disks.FindPtr(diskId);
        Y_DEBUG_ABORT_UNLESS(disk, "unknown disk: %s", diskId.c_str());
        if (!disk) {
            continue;
        }

        auto [_, inserted] = disk->LostDeviceIds.emplace(lostDeviceId);
        if (!inserted) {
            continue;
        }

        disksToReallocate.emplace(diskId);
    }

    for (const auto& d: agent.GetDevices()) {
        const auto& uuid = d.GetDeviceUUID();
        if (r.LostDeviceIds.contains(uuid)) {
            continue;
        }

        auto diskId = DeviceList.FindDiskId(uuid);
        if (!diskId) {
            continue;
        }

        auto* disk = Disks.FindPtr(diskId);
        Y_DEBUG_ABORT_UNLESS(disk, "unknown disk: %s", diskId.c_str());
        if (!disk) {
            continue;
        }

        if (!disk->LostDeviceIds.erase(uuid)) {
            continue;
        }

        disksToReallocate.emplace(std::move(diskId));
    }
}

auto TDiskRegistryState::GetUnavailableDevicesForDisk(
    const TString& diskId) const -> THashSet<TDeviceId>
{
    const auto* disk = Disks.FindPtr(diskId);
    Y_DEBUG_ABORT_UNLESS(disk, "unknown disk: %s", diskId.c_str());
    if (!disk) {
        return {};
    }
    Y_DEBUG_ABORT_UNLESS(!IsMasterDisk(diskId));

    THashSet<TDeviceId> unavailableDeviceIds = disk->LostDeviceIds;

    auto addDeviceIfNeeded = [&](const auto& deviceId)
    {
        auto [agentPtr, devicePtr] = FindDeviceLocation(deviceId);
        if (!agentPtr || !devicePtr ||
            agentPtr->GetState() == NProto::AGENT_STATE_UNAVAILABLE)
        {
            unavailableDeviceIds.emplace(deviceId);
        }
    };
    for (const auto& deviceId: disk->Devices) {
        addDeviceIfNeeded(deviceId);
    }

    for (const auto& [targetDeviceId, _]: disk->MigrationTarget2Source) {
        addDeviceIfNeeded(targetDeviceId);
    }

    return unavailableDeviceIds;
}

NProto::TError TDiskRegistryState::UnregisterAgent(
    TDiskRegistryDatabase& db,
    ui32 nodeId)
{
    if (!RemoveAgent(db, nodeId)) {
        return MakeError(S_ALREADY, "agent not found");
    }

    return {};
}

void TDiskRegistryState::RebuildDiskPlacementInfo(
    const TDiskState& disk,
    NProto::TPlacementGroupConfig::TDiskInfo* diskInfo) const
{
    Y_ABORT_UNLESS(diskInfo);

    THashSet<TString> racks;

    for (const auto& uuid: disk.Devices) {
        auto rack = DeviceList.FindRack(uuid);
        if (rack) {
            racks.insert(std::move(rack));
        }
    }

    for (auto& [targetId, sourceId]: disk.MigrationTarget2Source) {
        auto rack = DeviceList.FindRack(targetId);
        if (rack) {
            racks.insert(std::move(rack));
        }
    }

    diskInfo->MutableDeviceRacks()->Assign(
        std::make_move_iterator(racks.begin()),
        std::make_move_iterator(racks.end())
    );

    diskInfo->SetPlacementPartitionIndex(disk.PlacementPartitionIndex);
}

TResultOrError<NProto::TDeviceConfig> TDiskRegistryState::AllocateReplacementDevice(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    const TDeviceId& deviceReplacementId,
    const TDeviceList::TAllocationQuery& query,
    TInstant timestamp,
    TString message)
{
    if (deviceReplacementId.empty()) {
        auto device = DeviceList.AllocateDevice(diskId, query);
        if (device.GetDeviceUUID().empty()) {
            return MakeError(E_BS_DISK_ALLOCATION_FAILED, "can't allocate device");
        }

        return device;
    }

    auto [device, error] = DeviceList.AllocateSpecificDevice(
        diskId,
        deviceReplacementId,
        query);
    if (HasError(error)) {
        return MakeError(E_BS_DISK_ALLOCATION_FAILED, TStringBuilder()
            << "can't allocate specific device "
            << deviceReplacementId.Quote()
            << " : " << error.GetMessage());
    }

    // replacement device can come from dirty devices list
    db.DeleteDirtyDevice(deviceReplacementId);
    PendingCleanup.EraseDevice(deviceReplacementId);

    // replacement device can come from automatically replaced devices list
    if (IsAutomaticallyReplaced(deviceReplacementId)) {
        DeleteAutomaticallyReplacedDevice(db, deviceReplacementId);
    }

    AdjustDeviceState(
        db,
        device,
        NProto::DEVICE_STATE_ONLINE,
        timestamp,
        std::move(message));

    return device;
}

NProto::TError TDiskRegistryState::ReplaceDevice(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    const TString& deviceId,
    const TString& deviceReplacementId,
    TInstant timestamp,
    TString message,
    bool manual,
    bool* diskStateUpdated)
{
    Y_ABORT_UNLESS(diskStateUpdated);
    *diskStateUpdated = false;

    if (!diskId) {
        return MakeError(E_ARGUMENT, "empty disk id");
    }

    if (!Disks.contains(diskId)) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "unknown disk: " << diskId.Quote());
    }

    TDiskState& disk = Disks[diskId];

    auto error = ReplaceDeviceWithoutDiskStateUpdate(
        db,
        disk,
        diskId,
        deviceId,
        deviceReplacementId,
        timestamp,
        std::move(message),
        manual);

    if (HasError(error)) {
        return error;
    }

    *diskStateUpdated = TryUpdateDiskState(db, diskId, disk, timestamp);

    return {};
}

void TDiskRegistryState::TryToReplaceDeviceIfAllowedWithoutDiskStateUpdate(
    TDiskRegistryDatabase& db,
    TDiskState& disk,
    const TString& diskId,
    const TString& deviceId,
    TInstant timestamp,
    TString reason)
{
    if (!CheckIfDeviceReplacementIsAllowed(
            timestamp,
            disk.MasterDiskId,
            deviceId))
    {
        return;
    }

    auto error = ReplaceDeviceWithoutDiskStateUpdate(
        db,
        disk,
        diskId,
        deviceId,
        "",     // no replacement device
        timestamp,
        MakeMirroredDiskDeviceReplacementMessage(
            disk.MasterDiskId,
            std::move(reason)),
        false);  // manual

    if (HasError(error)) {
        ReportMirroredDiskDeviceReplacementFailure(FormatError(error));
    }
}

NProto::TError TDiskRegistryState::ReplaceDeviceWithoutDiskStateUpdate(
    TDiskRegistryDatabase& db,
    TDiskState& disk,
    const TString& diskId,
    const TString& deviceId,
    const TString& deviceReplacementId,
    TInstant timestamp,
    TString message,
    bool manual)
{
    try {
        if (!deviceId) {
            return MakeError(E_ARGUMENT, "empty device id");
        }

        if (DeviceList.FindDiskId(deviceId) != diskId) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "device does not belong to disk " << diskId.Quote());
        }

        auto it = Find(disk.Devices, deviceId);
        if (it == disk.Devices.end()) {
            auto message = ReportDiskRegistryDeviceNotFound(
                TStringBuilder() << "ReplaceDevice:DiskId: " << diskId
                << ", DeviceId: " << deviceId);

            return MakeError(E_FAIL, message);
        }

        auto [agentPtr, devicePtr] = FindDeviceLocation(deviceId);
        if (!agentPtr || !devicePtr) {
            return MakeError(E_INVALID_STATE, "can't find device");
        }

        if (!manual && !deviceReplacementId.empty()) {
            auto cleaningDiskId =
                PendingCleanup.FindDiskId(deviceReplacementId);
            if (!cleaningDiskId.empty() && cleaningDiskId != diskId) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "can't allocate specific device "
                        << deviceReplacementId.Quote() << " for disk " << diskId
                        << " since it is in pending cleanup for disk "
                        << cleaningDiskId);
            }

            if (IsDirtyDevice(deviceReplacementId)) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "can't allocate specific device "
                        << deviceReplacementId.Quote() << " for disk " << diskId
                        << " since it is dirty");
            }
        }

        const ui64 logicalBlockCount = devicePtr->GetBlockSize() * devicePtr->GetBlocksCount()
            / disk.LogicalBlockSize;

        TDeviceList::TAllocationQuery query {
            .ForbiddenRacks = CollectForbiddenRacks(diskId, disk, "ReplaceDevice"),
            .PreferredRacks = CollectPreferredRacks(diskId),
            .LogicalBlockSize = disk.LogicalBlockSize,
            .BlockCount = logicalBlockCount,
            .PoolName = devicePtr->GetPoolName(),
            .PoolKind = GetDevicePoolKind(devicePtr->GetPoolName())
        };

        if (query.PoolKind == NProto::DEVICE_POOL_KIND_LOCAL) {
            query.NodeIds = { devicePtr->GetNodeId() };
        }

        auto [targetDevice, error] = AllocateReplacementDevice(
            db,
            diskId,
            deviceReplacementId,
            query,
            timestamp,
            message);
        if (HasError(error)) {
            return error;
        }

        AdjustDeviceBlockCount(
            timestamp,
            db,
            targetDevice,
            logicalBlockCount * disk.LogicalBlockSize / targetDevice.GetBlockSize()
        );

        NProto::TDiskHistoryItem historyItem;
        historyItem.SetTimestamp(timestamp.MicroSeconds());
        historyItem.SetMessage(TStringBuilder() << "replaced device " << deviceId
            << " -> " << targetDevice.GetDeviceUUID() << ", manual=" << manual
            << (deviceReplacementId ? ", replacement device selected manually" : "")
            << ", original message=" << message.Quote());
        disk.History.push_back(historyItem);

        if (disk.MasterDiskId) {
            auto* masterDisk = Disks.FindPtr(disk.MasterDiskId);
            Y_DEBUG_ABORT_UNLESS(masterDisk);
            if (masterDisk) {
                auto it = Find(
                    masterDisk->DeviceReplacementIds.begin(),
                    masterDisk->DeviceReplacementIds.end(),
                    deviceId);

                if (it != masterDisk->DeviceReplacementIds.end()) {
                    // source device was a replacement device already, let's
                    // just change it inplace
                    *it = targetDevice.GetDeviceUUID();
                } else {
                    masterDisk->DeviceReplacementIds.push_back(
                        targetDevice.GetDeviceUUID());
                }

                *historyItem.MutableMessage() += TStringBuilder()
                    << ", replica=" << diskId;
                masterDisk->History.push_back(historyItem);

                db.UpdateDisk(BuildDiskConfig(disk.MasterDiskId, *masterDisk));

                const bool replaced = ReplicaTable.ReplaceDevice(
                    disk.MasterDiskId,
                    deviceId,
                    targetDevice.GetDeviceUUID());

                Y_DEBUG_ABORT_UNLESS(replaced);
            }
        }

        if (manual) {
            devicePtr->SetState(NProto::DEVICE_STATE_ERROR);
        } else {
            TAutomaticallyReplacedDeviceInfo deviceInfo{deviceId, timestamp};
            AutomaticallyReplacedDevices.push_back(deviceInfo);
            AutomaticallyReplacedDeviceIds.insert(deviceId);
            db.AddAutomaticallyReplacedDevice(deviceInfo);

            AutomaticReplacementTimestamps.push_back(timestamp);
        }
        devicePtr->SetStateMessage(std::move(message));
        devicePtr->SetStateTs(timestamp.MicroSeconds());

        DeviceList.UpdateDevices(*agentPtr, DevicePoolConfigs);

        DeviceList.ReleaseDevice(deviceId);
        db.UpdateDirtyDevice(deviceId, diskId);

        CancelDeviceMigration(timestamp, db, diskId, disk, deviceId);

        *it = targetDevice.GetDeviceUUID();

        UpdateAgent(db, *agentPtr);

        UpdatePlacementGroup(db, diskId, disk, "ReplaceDevice");
        UpdateAndReallocateDisk(db, diskId, disk);

        error = AddDevicesToPendingCleanup(diskId, {deviceId});
        if (HasError(error)) {
            ReportDiskRegistryInsertToPendingCleanupFailed(
                TStringBuilder() << "An error occurred while replacing device: "
                                 << FormatError(error));
        }
    } catch (const TServiceError& e) {
        return MakeError(e.GetCode(), e.what());
    }

    return {};
}

void TDiskRegistryState::AdjustDeviceBlockCount(
    TInstant now,
    TDiskRegistryDatabase& db,
    NProto::TDeviceConfig& device,
    ui64 newBlockCount)
{
    Y_UNUSED(now);
    if (newBlockCount > device.GetUnadjustedBlockCount()) {
        ReportDiskRegistryBadDeviceSizeAdjustment(
            TStringBuilder() << "AdjustDeviceBlockCount:DeviceId: "
            << device.GetDeviceUUID()
            << ", UnadjustedBlockCount: " << device.GetUnadjustedBlockCount()
            << ", newBlockCount: " << newBlockCount);

        return;
    }

    if (newBlockCount == device.GetBlocksCount()) {
        return;
    }

    auto [agent, source] = FindDeviceLocation(device.GetDeviceUUID());
    if (!agent || !source) {
        ReportDiskRegistryBadDeviceSizeAdjustment(
            TStringBuilder() << "AdjustDeviceBlockCount:DeviceId: "
            << device.GetDeviceUUID()
            << ", agent?: " << !!agent
            << ", source?: " << !!source);

        return;
    }

    source->SetBlocksCount(newBlockCount);

    UpdateAgent(db, *agent);
    DeviceList.UpdateDevices(*agent, DevicePoolConfigs);

    device = *source;
}

void TDiskRegistryState::AdjustDeviceState(
    TDiskRegistryDatabase& db,
    NProto::TDeviceConfig& device,
    NProto::EDeviceState state,
    TInstant timestamp,
    TString message)
{
    if (device.GetState() == state) {
        return;
    }

    auto [agent, source] = FindDeviceLocation(device.GetDeviceUUID());
    if (!agent || !source) {
        ReportDiskRegistryBadDeviceStateAdjustment(
            TStringBuilder() << "AdjustDeviceState:DeviceId: "
            << device.GetDeviceUUID()
            << ", agent?: " << !!agent
            << ", source?: " << !!source);
        return;
    }

    source->SetState(state);
    source->SetStateTs(timestamp.MicroSeconds());
    source->SetStateMessage(std::move(message));

    UpdateAgent(db, *agent);
    DeviceList.UpdateDevices(*agent, DevicePoolConfigs);

    device = *source;
}

ui64 TDiskRegistryState::GetDeviceBlockCountWithOverrides(
    const TDiskId& diskId,
    const NProto::TDeviceConfig& device)
{
    auto deviceBlockCount = device.GetBlocksCount();

    if (const auto* overrides = DeviceOverrides.FindPtr(diskId)) {
        const auto* overriddenBlockCount =
            overrides->Device2BlockCount.FindPtr(device.GetDeviceUUID());
        if (overriddenBlockCount) {
            deviceBlockCount = *overriddenBlockCount;
        }
    }

    return deviceBlockCount;
}

bool TDiskRegistryState::UpdatePlacementGroup(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    const TDiskState& disk,
    TStringBuf callerName)
{
    if (disk.PlacementGroupId.empty()) {
        return false;
    }

    auto* pg = PlacementGroups.FindPtr(disk.PlacementGroupId);
    if (!pg) {
        ReportDiskRegistryPlacementGroupNotFound(
            TStringBuilder() << callerName << ":DiskId: " << diskId
                << ", PlacementGroupId: " << disk.PlacementGroupId);
        return false;
    }

    auto* diskInfo = FindIfPtr(*pg->Config.MutableDisks(), [&] (auto& disk) {
        return disk.GetDiskId() == diskId;
    });

    if (!diskInfo) {
        ReportDiskRegistryPlacementGroupDiskNotFound(
            TStringBuilder() << callerName << ":DiskId: " << diskId
                << ", PlacementGroupId: " << disk.PlacementGroupId);

        return false;
    }

    RebuildDiskPlacementInfo(disk, diskInfo);

    pg->Config.SetConfigVersion(pg->Config.GetConfigVersion() + 1);
    db.UpdatePlacementGroup(pg->Config);

    return true;
}

TDeviceList::TAllocationQuery TDiskRegistryState::MakeMigrationQuery(
    const TDiskId& sourceDiskId,
    const NProto::TDeviceConfig& sourceDevice)
{
    TDiskState& disk = Disks[sourceDiskId];

    const auto logicalBlockCount =
        GetDeviceBlockCountWithOverrides(sourceDiskId, sourceDevice)
        * sourceDevice.GetBlockSize()
        / disk.LogicalBlockSize;

    TDeviceList::TAllocationQuery query {
        .ForbiddenRacks = CollectForbiddenRacks(sourceDiskId, disk, "StartDeviceMigration"),
        .PreferredRacks = CollectPreferredRacks(sourceDiskId),
        .LogicalBlockSize = disk.LogicalBlockSize,
        .BlockCount = logicalBlockCount,
        .PoolName = sourceDevice.GetPoolName(),
        .PoolKind = GetDevicePoolKind(sourceDevice.GetPoolName())
    };

    if (query.PoolKind == NProto::DEVICE_POOL_KIND_LOCAL) {
        query.NodeIds = { sourceDevice.GetNodeId() };
    }

    return query;
}

NProto::TError TDiskRegistryState::ValidateStartDeviceMigration(
    const TDiskId& sourceDiskId,
    const TString& sourceDeviceId)
{
    if (!Disks.contains(sourceDiskId)) {
        return MakeError(E_NOT_FOUND, TStringBuilder() <<
            "disk " << sourceDiskId.Quote() << " not found");
    }

    if (DeviceList.FindDiskId(sourceDeviceId) != sourceDiskId) {
        ReportDiskRegistryDeviceDoesNotBelongToDisk(
            TStringBuilder() << "StartDeviceMigration:DiskId: "
            << sourceDiskId << ", DeviceId: " << sourceDeviceId);

        return MakeError(E_ARGUMENT, TStringBuilder() <<
            "device " << sourceDeviceId.Quote() << " does not belong to "
                << sourceDiskId.Quote());
    }

    if (!DeviceList.FindDevice(sourceDeviceId)) {
        return MakeError(E_NOT_FOUND, TStringBuilder() <<
            "device " << sourceDeviceId.Quote() << " not found");
    }

    return {};
}

NProto::TDeviceConfig TDiskRegistryState::StartDeviceMigrationImpl(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& sourceDiskId,
    const TDeviceId& sourceDeviceId,
    NProto::TDeviceConfig targetDevice)
{
    TDiskState& disk = Disks[sourceDiskId];

    if (!disk.MigrationStartTs) {
        disk.MigrationStartTs = now;
    }

    const NProto::TDeviceConfig* sourceDevice =
        DeviceList.FindDevice(sourceDeviceId);

    const auto logicalBlockCount =
        GetDeviceBlockCountWithOverrides(sourceDiskId, *sourceDevice)
        * sourceDevice->GetBlockSize() / disk.LogicalBlockSize;

    AdjustDeviceBlockCount(
        now,
        db,
        targetDevice,
        logicalBlockCount * disk.LogicalBlockSize / targetDevice.GetBlockSize()
    );

    disk.MigrationTarget2Source[targetDevice.GetDeviceUUID()]
        = sourceDeviceId;
    disk.MigrationSource2Target[sourceDeviceId]
        = targetDevice.GetDeviceUUID();
    SourceDeviceMigrationsInProgress[sourceDevice->GetPoolName()].insert(
        sourceDeviceId);

    DeleteDeviceMigration(sourceDiskId, sourceDeviceId);

    NProto::TDiskHistoryItem historyItem;
    historyItem.SetTimestamp(now.MicroSeconds());
    historyItem.SetMessage(TStringBuilder() << "started migration: "
        << sourceDeviceId << " -> " << targetDevice.GetDeviceUUID());
    disk.History.push_back(std::move(historyItem));

    UpdatePlacementGroup(db, sourceDiskId, disk, "StartDeviceMigration");
    UpdateAndReallocateDisk(db, sourceDiskId, disk);

    DeviceList.MarkDeviceAllocated(sourceDiskId, targetDevice.GetDeviceUUID());

    return targetDevice;
}

/*
 *             | online | warn | unavailable |
 * online      |   -    |  1   |     2       |
 * warn        |   3    |  -   |     4       |
 * unavailable |   5    |  6   |     -       |
 * 1 - cms request - infra wants to exclude a host from our cluster for maintenance
 * 2 - agent is unavailable for some time
 * 3 - cms request - infra returns the host; blockstore-client request
 * 4 - agent is unavailable for some time
 * 5 - ERROR
 * 6 - agent is available again
 */

void TDiskRegistryState::ChangeAgentState(
    NProto::TAgentConfig& agent,
    NProto::EAgentState newState,
    TInstant now,
    TString stateMessage)
{
    const bool isChangedSate = newState != agent.GetState();
    if (isChangedSate) {
        if (newState == NProto::AGENT_STATE_UNAVAILABLE) {
            auto increment = [] (auto& mtbf, auto& workTime) {
                mtbf.SetWorkTime(mtbf.GetWorkTime() + workTime.Seconds());
                mtbf.SetBrokenCount(mtbf.GetBrokenCount() + 1);
            };

            if (agent.GetState() != NProto::AGENT_STATE_WARNING ||
                HasDependentSsdDisks(agent))
            {
                const TDuration workTime = agent.GetWorkTs()
                    ? (now - TInstant::Seconds(agent.GetWorkTs()))
                    : (now - TInstant::MicroSeconds(agent.GetStateTs()));

                increment(*agent.MutableTimeBetweenFailures(), workTime);
                increment(TimeBetweenFailures, workTime);
            }
            agent.SetWorkTs({});
        } else if (agent.GetState() == NProto::AGENT_STATE_UNAVAILABLE) {
            agent.SetWorkTs(now.Seconds());
        }
    }

    agent.SetState(newState);
    agent.SetStateTs(now.MicroSeconds());
    agent.SetStateMessage(std::move(stateMessage));
}

TResultOrError<NProto::TDeviceConfig> TDiskRegistryState::StartDeviceMigration(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& sourceDiskId,
    const TDeviceId& sourceDeviceId)
{
    try {
        if (auto error = ValidateStartDeviceMigration(
            sourceDiskId,
            sourceDeviceId); HasError(error))
        {
            return error;
        }

        TDeviceList::TAllocationQuery query =
            MakeMigrationQuery(
                sourceDiskId,
                *DeviceList.FindDevice(sourceDeviceId));

        NProto::TDeviceConfig targetDevice
            = DeviceList.AllocateDevice(sourceDiskId, query);
        if (targetDevice.GetDeviceUUID().empty()) {
            return MakeError(E_BS_DISK_ALLOCATION_FAILED, TStringBuilder() <<
                "can't allocate target for " << sourceDeviceId.Quote());
        }

        return StartDeviceMigrationImpl(
            now, db, sourceDiskId, sourceDeviceId, std::move(targetDevice));
    } catch (const TServiceError& e) {
        return MakeError(e.GetCode(), e.what());
    }
}


TResultOrError<NProto::TDeviceConfig> TDiskRegistryState::StartDeviceMigration(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& sourceDiskId,
    const TDeviceId& sourceDeviceId,
    const TDeviceId& targetDeviceId)
{
    try {
        if (auto error = ValidateStartDeviceMigration(
            sourceDiskId,
            sourceDeviceId); HasError(error))
        {
            return error;
        }

        TDeviceList::TAllocationQuery query =
            MakeMigrationQuery(
                sourceDiskId,
                *DeviceList.FindDevice(sourceDeviceId));

        const NProto::TDeviceConfig* targetDevice =
            DeviceList.FindDevice(targetDeviceId);
        if (!targetDevice) {
            return MakeError(E_NOT_FOUND, TStringBuilder() <<
                "can't find target device " << targetDeviceId.Quote());
        }

        if (!DeviceList.ValidateAllocationQuery(query, targetDeviceId)) {
            return MakeError(E_BS_DISK_ALLOCATION_FAILED, TStringBuilder()
                << "can't migrate from " << sourceDeviceId.Quote()
                << " to " << targetDeviceId.Quote());
        }

        DeviceList.MarkDeviceAllocated(sourceDiskId, targetDeviceId);
        return StartDeviceMigrationImpl(
            now, db, sourceDiskId, sourceDeviceId, *targetDevice);
    } catch (const TServiceError& e) {
        return MakeError(e.GetCode(), e.what());
    }
}

ui32 TDiskRegistryState::CalculateRackCount() const
{
    THashSet<TStringBuf> racks;

    for (const auto& agent: AgentList.GetAgents()) {
        for (const auto& device: agent.GetDevices()) {
            racks.insert(device.GetRack());
        }
    }

    return racks.size();
}

TDeque<TRackInfo> TDiskRegistryState::GatherRacksInfo(
    const TString& poolName) const
{
    TDeque<TRackInfo> racks;
    THashMap<TString, TRackInfo*> m;

    for (const auto& agent: AgentList.GetAgents()) {
        TRackInfo::TAgentInfo* agentInfo = nullptr;

        for (const auto& device: agent.GetDevices()) {
            if (device.GetPoolName() != poolName) {
                continue;
            }

            auto& rackPtr = m[device.GetRack()];
            if (!rackPtr) {
                racks.emplace_back(device.GetRack());
                rackPtr = &racks.back();
            }

            if (!agentInfo) {
                agentInfo = FindIfPtr(
                    rackPtr->AgentInfos,
                    [&] (const TRackInfo::TAgentInfo& info) {
                        return info.AgentId == agent.GetAgentId();
                    });
            }

            if (!agentInfo) {
                agentInfo = &rackPtr->AgentInfos.emplace_back(
                    agent.GetAgentId(),
                    agent.GetNodeId()
                );
            }

            ++agentInfo->TotalDevices;

            if (device.GetState() == NProto::DEVICE_STATE_ERROR) {
                ++agentInfo->BrokenDevices;
            }

            if (auto diskId = DeviceList.FindDiskId(device.GetDeviceUUID())) {
                ++agentInfo->AllocatedDevices;

                auto* disk = Disks.FindPtr(diskId);
                if (disk) {
                    if (disk->PlacementGroupId) {
                        auto& placementPartitions = rackPtr->PlacementGroups[disk->PlacementGroupId];
                        if (disk->PlacementPartitionIndex) {
                            placementPartitions.insert(disk->PlacementPartitionIndex);
                        }
                    }
                } else {
                    ReportDiskRegistryDeviceListReferencesNonexistentDisk(
                        TStringBuilder() << "GatherRacksInfo:DiskId: " << diskId
                        << ", DeviceId: " << device.GetDeviceUUID());
                }
            } else if (device.GetState() == NProto::DEVICE_STATE_ONLINE) {
                switch (agent.GetState()) {
                    case NProto::AGENT_STATE_ONLINE: {
                        if (DeviceList.IsDirtyDevice(device.GetDeviceUUID())) {
                            ++agentInfo->DirtyDevices;
                        } else {
                            ++agentInfo->FreeDevices;
                            rackPtr->FreeBytes +=
                                device.GetBlockSize() * device.GetBlocksCount();
                        }

                        break;
                    }

                    case NProto::AGENT_STATE_WARNING: {
                        ++agentInfo->WarningDevices;
                        rackPtr->WarningBytes +=
                            device.GetBlockSize() * device.GetBlocksCount();

                        break;
                    }

                    case NProto::AGENT_STATE_UNAVAILABLE: {
                        ++agentInfo->UnavailableDevices;

                        break;
                    }

                    default: {}
                }
            }

            rackPtr->TotalBytes += device.GetBlockSize() * device.GetBlocksCount();
        }
    }

    for (auto& rack: racks) {
        SortBy(rack.AgentInfos, [] (const TRackInfo::TAgentInfo& x) {
            return x.AgentId;
        });
    }

    SortBy(racks, [] (const TRackInfo& x) {
        return x.Name;
    });

    return racks;
}

THashSet<TString> TDiskRegistryState::CollectForbiddenRacks(
    const TDiskId& diskId,
    const TDiskState& disk,
    TStringBuf callerName)
{
    THashSet<TString> forbiddenRacks;
    THashSet<TString> preferredRacks;

    if (disk.PlacementGroupId.empty()) {
        return forbiddenRacks;
    }

    auto* pg = PlacementGroups.FindPtr(disk.PlacementGroupId);
    if (!pg) {
        auto message = ReportDiskRegistryPlacementGroupNotFound(
            TStringBuilder() << callerName << ":DiskId: " << diskId
            << ", PlacementGroupId: " << disk.PlacementGroupId);

        ythrow TServiceError(E_FAIL) << message;
    }

    auto* thisDisk = CollectRacks(
        diskId,
        disk.PlacementPartitionIndex,
        pg->Config,
        &forbiddenRacks,
        &preferredRacks
    );

    if (!thisDisk) {
        auto message = ReportDiskRegistryPlacementGroupDiskNotFound(
            TStringBuilder() << callerName << ":DiskId: " << diskId
            << ", PlacementGroupId: " << disk.PlacementGroupId);

        ythrow TServiceError(E_FAIL) << message;
    }

    return forbiddenRacks;
}

NProto::TPlacementGroupConfig::TDiskInfo* TDiskRegistryState::CollectRacks(
    const TString& diskId,
    ui32 placementPartitionIndex,
    NProto::TPlacementGroupConfig& placementGroup,
    THashSet<TString>* forbiddenRacks,
    THashSet<TString>* preferredRacks)
{
    NProto::TPlacementGroupConfig::TDiskInfo* thisDisk = nullptr;

    auto isThisPartition = [&] (const auto& disk) {
        if (disk.GetDiskId() == diskId) {
            return true;
        }
        if (placementGroup.GetPlacementStrategy() ==
                NProto::PLACEMENT_STRATEGY_PARTITION &&
            disk.GetPlacementPartitionIndex() == placementPartitionIndex)
        {
            return true;
        }
        return false;
    };

    for (auto& disk: *placementGroup.MutableDisks()) {
        if (disk.GetDiskId() == diskId) {
            if (thisDisk) {
                ReportDiskRegistryDuplicateDiskInPlacementGroup(
                    TStringBuilder() << "CollectRacks:PlacementGroupId: "
                    << placementGroup.GetGroupId()
                    << ", DiskId: " << diskId);
            }

            thisDisk = &disk;
        }

        auto* racks = isThisPartition(disk) ? preferredRacks : forbiddenRacks;
        for (const auto& rack: disk.GetDeviceRacks()) {
            racks->insert(rack);
        }
    }

    return thisDisk;
}

void TDiskRegistryState::CollectForbiddenRacks(
    const NProto::TPlacementGroupConfig& placementGroup,
    THashSet<TString>* forbiddenRacks)
{
    for (auto& disk: placementGroup.GetDisks()) {
        for (const auto& rack: disk.GetDeviceRacks()) {
            forbiddenRacks->insert(rack);
        }
    }
}

THashSet<TString> TDiskRegistryState::CollectPreferredRacks(
    const TDiskId& diskId) const
{
    THashSet<TString> thisDiskRacks;

    auto diskIt = Disks.find(diskId);
    if (diskIt == Disks.end()) {
        return thisDiskRacks;
    }

    for (const TString& deviceId: diskIt->second.Devices) {
        thisDiskRacks.insert(DeviceList.FindRack(deviceId));
    }

    return thisDiskRacks;
}

NProto::TError TDiskRegistryState::ValidateDiskLocation(
    const TVector<NProto::TDeviceConfig>& diskDevices,
    const TAllocateDiskParams& params) const
{
    if (!IsDiskRegistryLocalMediaKind(params.MediaKind) || diskDevices.empty())
    {
        return {};
    }

    if (!params.AgentIds.empty()) {
        const THashSet<TString> agents(
            params.AgentIds.begin(),
            params.AgentIds.end());

        for (const auto& device: diskDevices) {
            if (!agents.contains(device.GetAgentId())) {
                return MakeError(E_ARGUMENT, TStringBuilder() <<
                    "disk " << params.DiskId.Quote() << " already allocated at "
                        << device.GetAgentId());
            }
        }
    }

    return {};
}

TResultOrError<TDeviceList::TAllocationQuery> TDiskRegistryState::PrepareAllocationQuery(
    ui64 blocksToAllocate,
    const TDiskPlacementInfo& placementInfo,
    const TVector<NProto::TDeviceConfig>& diskDevices,
    const TAllocateDiskParams& params)
{
    THashSet<TString> forbiddenDiskRacks;
    THashSet<TString> preferredDiskRacks;

    if (placementInfo.PlacementGroupId) {
        auto& group = PlacementGroups[placementInfo.PlacementGroupId];

        CollectRacks(
            params.DiskId,
            placementInfo.PlacementPartitionIndex,
            group.Config,
            &forbiddenDiskRacks,
            &preferredDiskRacks);
    } else {
        preferredDiskRacks = CollectPreferredRacks(params.DiskId);
    }

    THashSet<ui32> nodeIds;
    TVector<TString> unknownAgents;

    for (const auto& id: params.AgentIds) {
        const ui32 nodeId = AgentList.FindNodeId(id);
        if (!nodeId) {
            unknownAgents.push_back(id);
        } else {
            nodeIds.insert(nodeId);
        }
    }

    if (!unknownAgents.empty()) {
        return MakeError(E_ARGUMENT, TStringBuilder() <<
            "unknown agents: " << JoinSeq(", ", unknownAgents));
    }

    NProto::EDevicePoolKind poolKind = params.PoolName.empty()
        ? NProto::DEVICE_POOL_KIND_DEFAULT
        : NProto::DEVICE_POOL_KIND_GLOBAL;

    if (IsDiskRegistryLocalMediaKind(params.MediaKind)) {
        poolKind = NProto::DEVICE_POOL_KIND_LOCAL;
    }

    if (poolKind == NProto::DEVICE_POOL_KIND_LOCAL && !diskDevices.empty()) {
        const auto nodeId = diskDevices[0].GetNodeId();

        if (!nodeIds.empty() && !nodeIds.contains(nodeId)) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "disk " << params.DiskId << " already allocated on "
                << diskDevices[0].GetAgentId());
        }

        nodeIds = { nodeId };
    }

    return TDeviceList::TAllocationQuery {
        .ForbiddenRacks = std::move(forbiddenDiskRacks),
        .PreferredRacks = std::move(preferredDiskRacks),
        .LogicalBlockSize = params.BlockSize,
        .BlockCount = blocksToAllocate,
        .PoolName = params.PoolName,
        .PoolKind = poolKind,
        .NodeIds = std::move(nodeIds),
    };
}

NProto::TError TDiskRegistryState::ValidateAllocateDiskParams(
    const TDiskState& disk,
    const TAllocateDiskParams& params) const
{
    if (disk.ReplicaCount && !params.ReplicaCount) {
        return MakeError(
            E_INVALID_STATE,
            "attempt to reallocate mirrored disk as nonrepl");
    }

    if (params.ReplicaCount && !disk.ReplicaCount && disk.Devices) {
        return MakeError(
            E_INVALID_STATE,
            "attempt to reallocate nonrepl disk as mirrored");
    }

    if (disk.LogicalBlockSize && disk.LogicalBlockSize != params.BlockSize) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "attempt to change LogicalBlockSize: "
                << disk.LogicalBlockSize << " -> " << params.BlockSize);
    }

    return {};
}

NProto::TError TDiskRegistryState::AllocateDisk(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TAllocateDiskParams& params,
    TAllocateDiskResult* result)
{
    auto& disk = Disks[params.DiskId];
    auto error = ValidateAllocateDiskParams(disk, params);

    auto originalBlockCount = GetDiskBlockCount(params.DiskId);

    if (HasError(error)) {
        if (disk.Devices.empty() && !disk.ReplicaCount) {
            Disks.erase(params.DiskId);
            AddToBrokenDisks(now, db, params.DiskId);
        }

        return error;
    }

    NProto::TError allocateError =
        params.ReplicaCount
            ? AllocateMirroredDisk(now, db, params, disk, result)
            : AllocateSimpleDisk(now, db, params, {}, disk, result);

    // If disk size changed reallocate checkpoints too.
    if (!HasError(allocateError) && originalBlockCount &&
        originalBlockCount != GetDiskBlockCount(params.DiskId))
    {
        ReallocateCheckpointByDisk(now, db, params.DiskId);
    }

    return allocateError;
}

NProto::TError TDiskRegistryState::AllocateCheckpoint(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& sourceDiskId,
    const TCheckpointId& checkpointId,
    TAllocateCheckpointResult* result)
{
    TDiskInfo diskInfo;
    auto error = GetDiskInfo(sourceDiskId, diskInfo);

    if (HasError(error)) {
        return error;
    }

    if (diskInfo.CheckpointId) {
        return MakeError(E_ARGUMENT, "Can't create checkpoint for checkpoint");
    }

    const auto shadowDiskId =
        TCheckpointInfo::MakeId(sourceDiskId, checkpointId);
    const auto checkpointMediaKind =
        GetCheckpointShadowDiskType(diskInfo.MediaKind);

    TAllocateDiskParams diskParams{
        shadowDiskId,
        diskInfo.CloudId,
        diskInfo.FolderId,
        {},   // PlacementGroupId
        0,    // PlacementPartitionIndex
        diskInfo.LogicalBlockSize,
        diskInfo.GetBlocksCount(),
        0,                        // ReplicaCount
        {},                       // MasterDiskId
        {},                       // AgentIds
        diskInfo.GetPoolName(),   // PoolName;
        checkpointMediaKind};

    if (diskParams.BlocksCount == 0 ){
        return MakeError(E_ARGUMENT, "blocks count == 0");
    }

    NProto::TCheckpointReplica checkpointParams;
    checkpointParams.SetSourceDiskId(sourceDiskId);
    checkpointParams.SetCheckpointId(checkpointId);
    checkpointParams.SetStateTs(now.MicroSeconds());
    checkpointParams.SetState(
        NProto::ECheckpointState::CHECKPOINT_STATE_CREATING);

    auto& disk = Disks[diskParams.DiskId];
    error = ValidateAllocateDiskParams(disk, diskParams);

    if (HasError(error)) {
        if (disk.Devices.empty()) {
            Disks.erase(diskParams.DiskId);
        }
        return error;
    }

    error = AllocateSimpleDisk(
        now,
        db,
        diskParams,
        checkpointParams,
        disk,
        result);
    if (HasError(error)) {
        return error;
    }

    result->ShadowDiskId = shadowDiskId;

    TCheckpointInfo checkpointInfo{
        sourceDiskId,
        checkpointId,
        shadowDiskId};
    Checkpoints[checkpointInfo.Id()] = std::move(checkpointInfo);
    return error;
}

NProto::TError TDiskRegistryState::DeallocateCheckpoint(
    TDiskRegistryDatabase& db,
    const TDiskId& sourceDiskId,
    const TCheckpointId& checkpointId,
    TDiskId* shadowDiskId)
{
    const auto id = TCheckpointInfo::MakeId(sourceDiskId, checkpointId);
    const auto* checkpointInfo = Checkpoints.FindPtr(id);
    if (!checkpointInfo) {
        return MakeError(
            S_ALREADY,
            TStringBuilder()
                << "Checkpoint " << checkpointId.Quote() << " for disk "
                << sourceDiskId.Quote() << " not found");
    }

    *shadowDiskId = checkpointInfo->ShadowDiskId;

    Checkpoints.erase(id);

    MarkDiskForCleanup(db, *shadowDiskId);
    return DeallocateDisk(db, *shadowDiskId);
}

NProto::TError TDiskRegistryState::GetCheckpointDataState(
    const TDiskId& sourceDiskId,
    const TCheckpointId& checkpointId,
    NProto::ECheckpointState* checkpointState) const
{
    const auto id = TCheckpointInfo::MakeId(sourceDiskId, checkpointId);
    const auto* checkpointInfo = Checkpoints.FindPtr(id);
    if (!checkpointInfo) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Checkpoint " << checkpointId.Quote() << " for disk "
                << sourceDiskId.Quote() << " not found");
    }

    const auto* checkpointDisk =
        Disks.FindPtr(checkpointInfo->ShadowDiskId);
    if (!checkpointDisk) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Disk " << checkpointInfo->ShadowDiskId.Quote()
                << " not found");
    }

    *checkpointState = checkpointDisk->CheckpointReplica.GetState();

    return MakeError(S_OK);
}

NProto::TError TDiskRegistryState::SetCheckpointDataState(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& sourceDiskId,
    const TCheckpointId& checkpointId,
    NProto::ECheckpointState checkpointState)
{
    const auto id = TCheckpointInfo::MakeId(sourceDiskId, checkpointId);
    const auto* checkpointInfo = Checkpoints.FindPtr(id);
    if (!checkpointInfo) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Checkpoint " << checkpointId.Quote() << " for disk "
                << sourceDiskId.Quote() << " not found");
    }

    auto* checkpointDisk = Disks.FindPtr(checkpointInfo->ShadowDiskId);
    if (!checkpointDisk) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Disk " << checkpointInfo->ShadowDiskId.Quote()
                << " not found");
    }

    if (checkpointDisk->CheckpointReplica.GetState() == checkpointState) {
        return MakeError(
            S_ALREADY,
            TStringBuilder()
                << "state for checkpoint " << checkpointId.Quote()
                << " already " << ECheckpointState_Name(checkpointState));
    }
    checkpointDisk->CheckpointReplica.SetState(checkpointState);
    checkpointDisk->CheckpointReplica.SetStateTs(now.MicroSeconds());
    db.UpdateDisk(
        BuildDiskConfig(checkpointInfo->ShadowDiskId, *checkpointDisk));
    return MakeError(S_OK);
}

bool TDiskRegistryState::IsMirroredDiskAlreadyAllocated(
    const TAllocateDiskParams& params) const
{
    const auto replicaId = GetReplicaDiskId(params.DiskId, 0);
    const auto* replica = Disks.FindPtr(replicaId);
    if (!replica) {
        return false;
    }

    ui64 size = 0;
    for (const auto& id: replica->Devices) {
        const auto* device = DeviceList.FindDevice(id);
        if (device) {
            size += device->GetBlockSize() * device->GetBlocksCount();
        }
    }

    return size >= params.BlocksCount * params.BlockSize;
}

void TDiskRegistryState::CleanupMirroredDisk(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TAllocateDiskParams& params)
{
    const auto& diskId = params.DiskId;
    const auto groupId = GetMirroredDiskGroupId(diskId);

    Disks.erase(diskId);

    for (ui32 i = 0; i < params.ReplicaCount + 1; ++i) {
        const auto replicaId = GetReplicaDiskId(diskId, i);
        auto* replica = Disks.FindPtr(replicaId);
        if (replica) {
            DeallocateSimpleDisk(db, replicaId, *replica);
        }
    }

    TVector<TString> affectedDisks;
    DestroyPlacementGroup(db, groupId, affectedDisks);

    if (affectedDisks.size()) {
        ReportMirroredDiskAllocationPlacementGroupCleanupFailure(
            TStringBuilder()
                << "AllocateMirroredDisk:PlacementGroupCleanupFailure:DiskId: "
                << diskId);
    }

    AddToBrokenDisks(now, db, diskId);
}

void TDiskRegistryState::UpdateReplicaTable(
    const TDiskId& diskId,
    const TAllocateDiskResult& r)
{
    TVector<TDeviceId> deviceIds;
    for (const auto& d: r.Devices) {
        deviceIds.push_back(d.GetDeviceUUID());
    }

    ReplicaTable.UpdateReplica(diskId, 0, deviceIds);

    for (ui32 i = 0; i < r.Replicas.size(); ++i) {
        deviceIds.clear();
        for (const auto& d: r.Replicas[i]) {
            deviceIds.push_back(d.GetDeviceUUID());
        }

        ReplicaTable.UpdateReplica(diskId, i + 1, deviceIds);
    }

    for (const auto& deviceId: r.DeviceReplacementIds) {
        ReplicaTable.MarkReplacementDevice(diskId, deviceId, true);
    }
}

NProto::TError TDiskRegistryState::CreateMirroredDiskPlacementGroup(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    return CreatePlacementGroup(
        db,
        GetMirroredDiskGroupId(diskId),
        NProto::PLACEMENT_STRATEGY_SPREAD,
        0);
}

NProto::TError TDiskRegistryState::AllocateDiskReplica(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TAllocateDiskParams& params,
    ui32 index,
    TAllocateDiskResult* result)
{
    TAllocateDiskParams subParams = params;
    subParams.ReplicaCount = 0;
    subParams.DiskId = GetReplicaDiskId(params.DiskId, index);
    subParams.PlacementGroupId = GetMirroredDiskGroupId(params.DiskId);
    subParams.MasterDiskId = params.DiskId;

    return AllocateSimpleDisk(
        now,
        db,
        subParams,
        {},
        Disks[subParams.DiskId],
        result);
}

NProto::TError TDiskRegistryState::AllocateDiskReplicas(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TAllocateDiskParams& params,
    TAllocateDiskResult* result)
{
    for (ui32 i = 0; i < params.ReplicaCount + 1; ++i) {
        TAllocateDiskResult subResult;
        auto error = AllocateDiskReplica(now, db, params, i, &subResult);

        if (HasError(error)) {
            return error;
        }

        if (i) {
            result->Replicas.push_back(std::move(subResult.Devices));
        } else {
            result->Devices = std::move(subResult.Devices);
        }
        result->Migrations.insert(
            result->Migrations.end(),
            std::make_move_iterator(subResult.Migrations.begin()),
            std::make_move_iterator(subResult.Migrations.end()));
    }

    return {};
}

NProto::TError TDiskRegistryState::AllocateMirroredDisk(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TAllocateDiskParams& params,
    TDiskState& disk,
    TAllocateDiskResult* result)
{
    const bool isNewDisk = disk.Devices.empty() && !disk.ReplicaCount;

    auto onError = [&] {
        if (isNewDisk) {
            CleanupMirroredDisk(now, db, params);
        }
    };

    const ui32 code = IsMirroredDiskAlreadyAllocated(params)
        ? S_ALREADY
        : S_OK;

    if (isNewDisk) {
        auto error = CreateMirroredDiskPlacementGroup(db, params.DiskId);
        if (HasError(error)) {
            onError();

            return error;
        }
    }

    if (auto error = AllocateDiskReplicas(now, db, params, result); HasError(error)) {
        if (!isNewDisk) {
            // TODO (NBS-3419):
            // support automatic cleanup after a failed resize
            ReportMirroredDiskAllocationCleanupFailure(TStringBuilder()
                << "AllocateMirroredDisk:ResizeCleanupFailure:DiskId: "
                << params.DiskId);
        }

        onError();

        return error;
    }

    result->DeviceReplacementIds = disk.DeviceReplacementIds;
    result->LaggingDevices = disk.LaggingDevices;

    disk.CloudId = params.CloudId;
    disk.FolderId = params.FolderId;
    disk.LogicalBlockSize = params.BlockSize;
    disk.StateTs = now;
    disk.ReplicaCount = params.ReplicaCount;
    disk.MediaKind = params.MediaKind;
    db.UpdateDisk(BuildDiskConfig(params.DiskId, disk));

    UpdateReplicaTable(params.DiskId, *result);

    return MakeError(code);
}

NProto::TError TDiskRegistryState::CheckDiskPlacementInfo(
    const TDiskPlacementInfo& info) const
{
    auto* g = PlacementGroups.FindPtr(info.PlacementGroupId);
    if (!g) {
        return MakeError(E_NOT_FOUND, TStringBuilder()
            << "placement group " << info.PlacementGroupId << " not found");
    }

    if (info.PlacementPartitionIndex && !PlacementGroupMustHavePartitions(g->Config)) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "placement partition shouldn't be specified for placement group "
            << info.PlacementGroupId.Quote());
    }

    if (!PartitionSuitsPlacementGroup(g->Config, info.PlacementPartitionIndex)) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "placement partition index "
                << info.PlacementPartitionIndex
                << " not found in placement group "
                << info.PlacementGroupId.Quote());
    }

    return {};
}

NProto::TError TDiskRegistryState::CheckPlacementGroupCapacity(
    const TString& groupId) const
{
    auto* group = PlacementGroups.FindPtr(groupId);
    if (!group) {
        return MakeError(E_NOT_FOUND, TStringBuilder()
            << "placement group " << groupId << " not found");
    }

    const auto& config = group->Config;

    if (config.DisksSize() < GetMaxDisksInPlacementGroup(*StorageConfig, config)) {
        return {};
    }

    ui32 flags = 0;
    SetProtoFlag(flags, NProto::EF_SILENT);

    return MakeError(E_BS_RESOURCE_EXHAUSTED, TStringBuilder() <<
        "max disk count in group exceeded, max: "
        << GetMaxDisksInPlacementGroup(*StorageConfig, config),
        flags);
}

void TDiskRegistryState::UpdateDiskPlacementInfo(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    const TDiskPlacementInfo& placementInfo)
{
    if (!placementInfo.PlacementGroupId) {
        return;
    }

    auto& config = PlacementGroups[placementInfo.PlacementGroupId].Config;

    NProto::TPlacementGroupConfig::TDiskInfo* thisDisk = nullptr;

    for (auto& diskInfo: *config.MutableDisks()) {
        if (diskInfo.GetDiskId() == diskId) {
            thisDisk = &diskInfo;
            break;
        }
    }

    if (!thisDisk) {
        thisDisk = config.AddDisks();
        thisDisk->SetDiskId(diskId);
    }

    auto& disk = Disks[diskId];

    disk.PlacementGroupId = placementInfo.PlacementGroupId;
    disk.PlacementPartitionIndex = placementInfo.PlacementPartitionIndex;
    RebuildDiskPlacementInfo(disk, thisDisk);

    config.SetConfigVersion(config.GetConfigVersion() + 1);
    db.UpdatePlacementGroup(config);
}

auto TDiskRegistryState::CreateDiskPlacementInfo(
    const TDiskState& disk,
    const TAllocateDiskParams& params) const -> TDiskPlacementInfo
{
    return {
        disk.Devices && disk.PlacementGroupId
            ? disk.PlacementGroupId
            : params.PlacementGroupId,
        disk.Devices && disk.PlacementPartitionIndex
            ? disk.PlacementPartitionIndex
            : params.PlacementPartitionIndex
    };
}

bool TDiskRegistryState::CanAllocateLocalDiskAfterSecureErase(
    const TVector<TString>& agentIds,
    const TString& poolName,
    const ui64 totalByteCount) const
{
    bool canAllocate = false;
    for (const TString& agentId: agentIds) {
        auto [infos, error] = QueryAvailableStorage(
            agentId,
            poolName,
            NProto::DEVICE_POOL_KIND_LOCAL);

        if (HasError(error)) {
            continue;
        }

        ui64 totalSize = totalByteCount;
        for (const auto& chunks: infos) {
            const ui64 chunksSize =
                (chunks.FreeChunks + chunks.DirtyChunks) * chunks.ChunkSize;
            STORAGE_DEBUG(
                "AgentId=%s TAgentStorageInfo={.ChunkSize=%lu .ChunkCount=%u "
                ".DirtyChunks=%u .FreeChunks=%u}",
                agentId.Quote().c_str(),
                chunks.ChunkSize,
                chunks.ChunkCount,
                chunks.DirtyChunks,
                chunks.FreeChunks);
            if (totalSize <= chunksSize) {
                STORAGE_DEBUG(
                    "Agent %s is suitable for allocation after SecureErase. "
                    "Got requested %lu bytes",
                    agentId.Quote().c_str(),
                    totalByteCount);
                canAllocate = true;
                break;
            }
            totalSize -= chunksSize;
        }
    }

    return canAllocate;
}

NProto::TError TDiskRegistryState::AllocateSimpleDisk(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TAllocateDiskParams& params,
    const NProto::TCheckpointReplica& checkpointParams,
    TDiskState& disk,
    TAllocateDiskResult* result)
{
    STORAGE_VERIFY(
        !disk.ReplicaCount && !params.ReplicaCount,
        TWellKnownEntityTypes::DISK,
        params.DiskId);

    const bool isShadowDiskAllocation =
        !checkpointParams.GetCheckpointId().empty();

    auto onError = [&] {
        const bool isNewDisk = disk.Devices.empty();

        if (isNewDisk) {
            Disks.erase(params.DiskId);

            if (!params.MasterDiskId && !isShadowDiskAllocation) {
                // failed to allocate storage for the new volume, need to
                // destroy this volume
                AddToBrokenDisks(now, db, params.DiskId);
            }
        }
    };

    if (!disk.StateTs) {
        disk.StateTs = now;
    }

    result->IOModeTs = disk.StateTs;
    result->IOMode = GetIoMode(disk, now);

    result->MuteIOErrors =
        disk.State >= NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE;

    const TDiskPlacementInfo placementInfo = CreateDiskPlacementInfo(disk, params);

    if (placementInfo.PlacementGroupId) {
        auto error = CheckDiskPlacementInfo(placementInfo);
        if (HasError(error)) {
            onError();
            return error;
        }
    }

    auto& output = result->Devices;

    if (auto error = GetDiskMigrations(disk, result->Migrations); HasError(error)) {
        onError();

        return error;
    }

    if (auto error = GetDiskDevices(params.DiskId, disk, output); HasError(error)) {
        onError();

        return error;
    }

    if (auto error = ValidateDiskLocation(output, params); HasError(error)) {
        onError();

        return error;
    }

    ui64 currentSize = 0;
    for (const auto& d: output) {
        currentSize += d.GetBlockSize() * d.GetBlocksCount();
    }

    const ui64 requestedSize = params.BlockSize * params.BlocksCount;

    if (requestedSize <= currentSize) {
        if (disk.CloudId.empty() && !params.CloudId.empty()) {
            disk.CloudId = params.CloudId;
            disk.FolderId = params.FolderId;
            db.UpdateDisk(BuildDiskConfig(params.DiskId, disk));
        }

        return MakeError(S_ALREADY, TStringBuilder() <<
            "disk " << params.DiskId.Quote() << " already exists");
    }

    const ui64 blocksToAllocate =
        ceil(double(requestedSize - currentSize) / params.BlockSize);

    if (output.empty() && placementInfo.PlacementGroupId) {
        auto error = CheckPlacementGroupCapacity(placementInfo.PlacementGroupId);
        if (HasError(error)) {
            onError();
            return error;
        }
    }

    auto [query, error] = PrepareAllocationQuery(
        blocksToAllocate,
        placementInfo,
        output,
        params);

    if (HasError(error)) {
        onError();

        return error;
    }

    auto allocatedDevices = DeviceList.AllocateDevices(params.DiskId, query);

    if (!allocatedDevices) {
        onError();

        return MakeError(E_BS_DISK_ALLOCATION_FAILED, TStringBuilder() <<
            "can't allocate disk with " << blocksToAllocate << " blocks x " <<
            params.BlockSize << " bytes");
    }

    for (const auto& device: allocatedDevices) {
        disk.Devices.push_back(device.GetDeviceUUID());
    }

    output.insert(
        output.end(),
        std::make_move_iterator(allocatedDevices.begin()),
        std::make_move_iterator(allocatedDevices.end()));

    disk.LogicalBlockSize = params.BlockSize;
    disk.CloudId = params.CloudId;
    disk.FolderId = params.FolderId;
    disk.MasterDiskId = params.MasterDiskId;
    disk.MediaKind = params.MediaKind;
    disk.CheckpointReplica.CopyFrom(checkpointParams);

    db.UpdateDisk(BuildDiskConfig(params.DiskId, disk));

    UpdateDiskPlacementInfo(db, params.DiskId, placementInfo);

    AllowNotifications(params.DiskId, disk);

    return {};
}

NProto::TError TDiskRegistryState::DeallocateDisk(
    TDiskRegistryDatabase& db,
    const TString& diskId)
{
    auto* disk = Disks.FindPtr(diskId);
    if (!disk) {
        return MakeError(S_ALREADY, TStringBuilder() <<
            "disk " << diskId.Quote() << " not found");
    }

    if (!IsReadyForCleanup(diskId)) {
        auto message = ReportNrdDestructionError(TStringBuilder()
            << "attempting to clean up unmarked disk " << diskId.Quote());

        return MakeError(E_INVALID_STATE, std::move(message));
    }

    if (disk->ReplicaCount) {
        const TString groupId = GetMirroredDiskGroupId(diskId);
        TVector<TString> affectedDisks;
        auto error = DestroyPlacementGroup(db, groupId, affectedDisks);
        if (HasError(error)) {
            return error;
        }

        for (const auto& affectedDiskId: affectedDisks) {
            Y_DEBUG_ABORT_UNLESS(affectedDiskId.StartsWith(diskId + "/"));
            error = AddDevicesToPendingCleanup(
                diskId,
                DeallocateSimpleDisk(
                    db,
                    affectedDiskId,
                    "DeallocateDisk:Replica"));
            if (HasError(error)) {
                ReportDiskRegistryInsertToPendingCleanupFailed(
                    TStringBuilder()
                    << "An error occurred while deallocating disk replica: "
                    << affectedDiskId << ". " << FormatError(error));
            }
        }

        DeleteDisk(db, diskId);
        ReplicaTable.RemoveMirroredDisk(diskId);

        return {};
    }

    auto dirtyDevices = DeallocateSimpleDisk(db, diskId, *disk);
    auto error = AddDevicesToPendingCleanup(diskId, std::move(dirtyDevices));
    if (!HasError(error)) {
        // NOTE: We must pass S_ALREADY to the higher-level code. It is used for
        // sync deallocations.
        return error;
    }
    ReportDiskRegistryInsertToPendingCleanupFailed(
        TStringBuilder() << "An error occurred while deallocating disk: "
                         << FormatError(error));
    return {};
}

bool TDiskRegistryState::CanSecureErase(
    const NProto::TDeviceConfig& device) const
{
    if (DeviceList.IsSuspendedAndNotResumingDevice(device.GetDeviceUUID())) {
        STORAGE_DEBUG(
            "Skip SecureErase for device '%s'. Device is suspended.",
            device.GetDeviceUUID().c_str());
        return false;
    }

    if (device.GetState() == NProto::DEVICE_STATE_ERROR) {
        STORAGE_DEBUG(
            "Skip SecureErase for device '%s'. Device in error state",
            device.GetDeviceUUID().c_str());
        return false;
    }

    if (IsAutomaticallyReplaced(device.GetDeviceUUID())) {
        STORAGE_DEBUG(
            "Skip SecureErase for device '%s'."
            " Device was automatically replaced recently.",
            device.GetDeviceUUID().c_str());
        return false;
    }

    const NProto::TAgentConfig* agent = FindAgent(device.GetNodeId());
    if (!agent) {
        STORAGE_DEBUG(
            "Skip SecureErase for device '%s'. Agent for node id %d not found",
            device.GetDeviceUUID().c_str(),
            device.GetNodeId());
        return false;
    }

    if (agent->GetState() == NProto::AGENT_STATE_UNAVAILABLE) {
        STORAGE_DEBUG(
            "Skip SecureErase for device '%s'. Agent is unavailable",
            device.GetDeviceUUID().c_str());
        return false;
    }

    return true;
}

bool TDiskRegistryState::CanSecureErase(const TDeviceId& uuid) const
{
    const NProto::TDeviceConfig* device = FindDevice(uuid);
    if (!device) {
        return false;
    }
    return CanSecureErase(*device);
}

bool TDiskRegistryState::HasPendingCleanup(const TDiskId& diskId) const
{
    return PendingCleanup.Contains(diskId);
}

auto TDiskRegistryState::DeallocateSimpleDisk(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    const TString& parentMethodName) -> TVector<TDeviceId>
{
    auto* disk = Disks.FindPtr(diskId);
    if (!disk) {
        ReportDiskRegistryDiskNotFound(
            TStringBuilder() << parentMethodName << ":DiskId: "
            << diskId);

        return {};
    }

    return DeallocateSimpleDisk(db, diskId, *disk);
}

auto TDiskRegistryState::DeallocateSimpleDisk(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    TDiskState& disk) -> TVector<TDeviceId>
{
    Y_ABORT_UNLESS(disk.ReplicaCount == 0);

    TVector<TDeviceId> dirtyDevices;

    for (const auto& uuid: disk.Devices) {
        if (DeviceList.ReleaseDevice(uuid)) {
            dirtyDevices.push_back(uuid);
        }
    }

    if (disk.PlacementGroupId) {
        auto* pg = PlacementGroups.FindPtr(disk.PlacementGroupId);

        if (pg) {
            auto& config = pg->Config;

            auto end = std::remove_if(
                config.MutableDisks()->begin(),
                config.MutableDisks()->end(),
                [&] (const NProto::TPlacementGroupConfig::TDiskInfo& d) {
                    return diskId == d.GetDiskId();
                }
            );

            while (config.MutableDisks()->end() > end) {
                config.MutableDisks()->RemoveLast();
            }

            config.SetConfigVersion(config.GetConfigVersion() + 1);
            db.UpdatePlacementGroup(config);
        } else {
            ReportDiskRegistryPlacementGroupNotFound(
                TStringBuilder() << "DeallocateDisk:DiskId: " << diskId
                << ", PlacementGroupId: " << disk.PlacementGroupId);
        }
    }

    for (const auto& [targetId, sourceId]: disk.MigrationTarget2Source) {
        Y_UNUSED(sourceId);

        if (DeviceList.ReleaseDevice(targetId)) {
            dirtyDevices.push_back(targetId);
        }
    }

    for (const auto& [uuid, seqNo, _]: disk.FinishedMigrations) {
        Y_UNUSED(seqNo);

        if (DeviceList.ReleaseDevice(uuid)) {
            dirtyDevices.push_back(uuid);
        }
    }

    for (const auto& uuid: dirtyDevices) {
        db.UpdateDirtyDevice(uuid, diskId);
    }

    DeleteAllDeviceMigrations(diskId);
    DeleteDisk(db, diskId);

    return dirtyDevices;
}

void TDiskRegistryState::DeleteDisk(
    TDiskRegistryDatabase& db,
    const TString& diskId)
{
    Disks.erase(diskId);
    DisksToCleanup.erase(diskId);

    NotificationSystem.DeleteDisk(db, diskId);

    db.DeleteDisk(diskId);
    db.DeleteDiskToCleanup(diskId);

    DeleteCheckpointByDisk(db, diskId);
}

void TDiskRegistryState::DeleteCheckpointByDisk(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    TVector<TDiskId> disksToDelete;
    TVector<TCheckpointId> checkpointsToDelete;
    for (const auto& [id, checkpointInfo]: Checkpoints) {
        if (checkpointInfo.SourceDiskId == diskId)
        {
            disksToDelete.push_back(checkpointInfo.ShadowDiskId);
            checkpointsToDelete.push_back(id);
        }
        if (checkpointInfo.ShadowDiskId == diskId)
        {
            checkpointsToDelete.push_back(id);
        }
    }
    for (const auto& id: checkpointsToDelete) {
        Checkpoints.erase(id);
    }
    for (const auto& diskToDelete: disksToDelete) {
        MarkDiskForCleanup(db, diskToDelete);
        DeallocateDisk(db, diskToDelete);
    }
}

void TDiskRegistryState::ReallocateCheckpointByDisk(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    TVector<TCheckpointId> checkpointsToReallocate;
    for (const auto& [id, checkpointInfo]: Checkpoints) {
        if (checkpointInfo.SourceDiskId == diskId) {
            checkpointsToReallocate.push_back(checkpointInfo.CheckpointId);
        }
    }

    for (const auto& checkpointId: checkpointsToReallocate) {
        TAllocateCheckpointResult result;
        AllocateCheckpoint(now, db, diskId, checkpointId, &result);
    }
}

void TDiskRegistryState::AddToBrokenDisks(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TString& diskId)
{
    TBrokenDiskInfo brokenDiskInfo{
        diskId,
        now + StorageConfig->GetBrokenDiskDestructionDelay()
    };
    db.AddBrokenDisk(brokenDiskInfo);
    BrokenDisks.push_back(brokenDiskInfo);
}

NProto::TDeviceConfig TDiskRegistryState::GetDevice(const TString& id) const
{
    const auto* device = DeviceList.FindDevice(id);

    if (device) {
        return *device;
    }

    return {};
}

TVector<NProto::TDeviceConfig> TDiskRegistryState::FindDevices(
    const TString& agentId,
    const TString& path) const
{
    const auto* agent = AgentList.FindAgent(agentId);
    if (!agent) {
        return {};
    }

    TVector<NProto::TDeviceConfig> devices;
    for (const auto& device: agent->GetDevices()) {
        if (device.GetDeviceName() == path) {
            devices.push_back(device);
        }
    }
    return devices;
}

TResultOrError<NProto::TDeviceConfig> TDiskRegistryState::FindDevice(
    const NProto::TDeviceConfig& deviceConfig) const
{
    const auto& deviceId = deviceConfig.GetDeviceUUID();
    if (deviceId) {
        const auto* device = DeviceList.FindDevice(deviceId);
        if (!device) {
            return MakeError(
                E_NOT_FOUND,
                TStringBuilder() << "device with id " << deviceId
                    << " not found");
        }

        return *device;
    }

    auto devices = FindDevices(
        deviceConfig.GetAgentId(),
        deviceConfig.GetDeviceName());

    if (devices.size() == 0) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder() << "device with AgentId="
                << deviceConfig.GetAgentId()
                << " and DeviceName=" << deviceConfig.GetDeviceName()
                << " not found");
    }

    if (devices.size() > 1) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "too many devices with AgentId="
                << deviceConfig.GetAgentId()
                << " and DeviceName=" << deviceConfig.GetDeviceName()
                << ": " << devices.size());
    }

    return devices.front();
}

const NProto::TDeviceConfig* TDiskRegistryState::FindDevice(
    const TString& deviceId) const
{
    return DeviceList.FindDevice(deviceId);
}

TVector<TString> TDiskRegistryState::GetDeviceIds(
    const TString& agentId,
    const TString& path) const
{
    auto devices = FindDevices(agentId, path);
    TVector<TString> deviceIds;
    deviceIds.reserve(devices.size());
    for (auto& device: devices) {
        deviceIds.push_back(*std::move(device.MutableDeviceUUID()));
    }
    return deviceIds;
}

NProto::EDeviceState TDiskRegistryState::GetDeviceState(
    const TString& deviceId) const
{
    return DeviceList.GetDeviceState(deviceId);
}

NProto::TError TDiskRegistryState::GetDependentDisks(
    const TString& agentId,
    const TString& path,
    bool ignoreReplicatedDisks,
    TVector<TDiskId>* diskIds) const
{
    diskIds->clear();
    const auto* agent = AgentList.FindAgent(agentId);
    if (!agent) {
        return MakeError(E_NOT_FOUND, agentId);
    }

    for (const auto& d: agent->GetDevices()) {
        if (path && d.GetDeviceName() != path) {
            continue;
        }

        auto diskId = FindDisk(d.GetDeviceUUID());

        if (!diskId) {
            continue;
        }

        const TDiskState* state = FindDiskState(diskId);
        if (state && state->MasterDiskId && ignoreReplicatedDisks) {
            continue;
        }

        // linear search on every iteration is ok here, diskIds size is small
        auto diskIdToNotify = GetDiskIdToNotify(diskId);
        if (Find(*diskIds, diskIdToNotify) == diskIds->end()) {
            diskIds->push_back(std::move(diskIdToNotify));
        }
    }

    return {};
}

TString TDiskRegistryState::GetDiskIdToNotify(const TString& diskId) const
{
    const auto* disk = Disks.FindPtr(diskId);
    Y_DEBUG_ABORT_UNLESS(disk, "unknown disk: %s", diskId.c_str());

    if (disk && disk->MasterDiskId) {
        return disk->MasterDiskId;
    }
    if (disk && disk->CheckpointReplica.GetCheckpointId()) {
        return disk->CheckpointReplica.GetSourceDiskId();
    }
    return diskId;
}

NProto::TError TDiskRegistryState::GetDiskDevices(
    const TDiskId& diskId,
    TVector<NProto::TDeviceConfig>& devices) const
{
    auto it = Disks.find(diskId);

    if (it == Disks.end()) {
        return MakeError(E_NOT_FOUND, TStringBuilder() <<
            "disk " << diskId.Quote() << " not found");
    }

    return GetDiskDevices(diskId, it->second, devices);
}

NProto::TError TDiskRegistryState::GetDiskDevices(
    const TDiskId& diskId,
    const TDiskState& disk,
    TVector<NProto::TDeviceConfig>& devices) const
{
    auto* overrides = DeviceOverrides.FindPtr(diskId);

    for (const auto& uuid: disk.Devices) {
        const auto* device = DeviceList.FindDevice(uuid);

        if (!device) {
            return MakeError(E_NOT_FOUND, TStringBuilder() <<
                "device " << uuid.Quote() << " not found");
        }

        devices.emplace_back(*device);
        if (overrides) {
            auto* blocksCount = overrides->Device2BlockCount.FindPtr(uuid);
            if (blocksCount) {
                devices.back().SetBlocksCount(*blocksCount);
            }
        }
    }

    return {};
}

NProto::TError TDiskRegistryState::GetDiskMigrations(
    const TDiskId& diskId,
    TVector<NProto::TDeviceMigration>& migrations) const
{
    auto it = Disks.find(diskId);

    if (it == Disks.end()) {
        return MakeError(E_NOT_FOUND, TStringBuilder() <<
            "disk " << diskId.Quote() << " not found");
    }

    return GetDiskMigrations(it->second, migrations);
}

NProto::TError TDiskRegistryState::GetDiskMigrations(
    const TDiskState& disk,
    TVector<NProto::TDeviceMigration>& migrations) const
{
    ui32 oldSize = migrations.size();
    migrations.reserve(oldSize + disk.MigrationTarget2Source.size());

    for (const auto& [targetId, sourceId]: disk.MigrationTarget2Source) {
        NProto::TDeviceMigration m;
        m.SetSourceDeviceId(sourceId);
        *m.MutableTargetDevice() = GetDevice(targetId);

        const auto& uuid = m.GetTargetDevice().GetDeviceUUID();

        if (uuid.empty()) {
            return MakeError(E_NOT_FOUND, TStringBuilder() <<
                "device " << uuid.Quote() << " not found");
        }

        migrations.push_back(std::move(m));
    }

    // for convenience
    // the logic in other components should not have critical dependencies on
    // the order of migrations
    auto b = migrations.begin();
    std::advance(b, oldSize);
    SortBy(b, migrations.end(), [] (const NProto::TDeviceMigration& m) {
        return m.GetSourceDeviceId();
    });

    return {};
}

NProto::TError TDiskRegistryState::FillAllDiskDevices(
    const TDiskId& diskId,
    const TDiskState& disk,
    TDiskInfo& diskInfo) const
{
    NProto::TError error;

    if (disk.ReplicaCount) {
        error = GetDiskDevices(GetReplicaDiskId(diskId, 0), diskInfo.Devices);
        diskInfo.Replicas.resize(disk.ReplicaCount);
        for (ui32 i = 0; i < disk.ReplicaCount; ++i) {
            if (!HasError(error)) {
                error = GetDiskDevices(
                    GetReplicaDiskId(diskId, i + 1),
                    diskInfo.Replicas[i]);
            }
        }
    } else {
        error = GetDiskDevices(diskId, disk, diskInfo.Devices);
    }

    if (!HasError(error)) {
        if (disk.ReplicaCount) {
            error = GetDiskMigrations(
                GetReplicaDiskId(diskId, 0),
                diskInfo.Migrations);
            for (ui32 i = 0; i < disk.ReplicaCount; ++i) {
                if (!HasError(error)) {
                    error = GetDiskMigrations(
                        GetReplicaDiskId(diskId, i + 1),
                        diskInfo.Migrations);
                }
            }
        } else {
            error = GetDiskMigrations(disk, diskInfo.Migrations);
        }
    }

    return error;
}

NProto::EDiskState TDiskRegistryState::GetDiskState(const TDiskId& diskId) const
{
    auto* disk = Disks.FindPtr(diskId);
    if (!disk) {
        return NProto::DISK_STATE_ERROR;
    }

    return disk->State;
}

NProto::TError TDiskRegistryState::GetShadowDiskId(
    const TDiskId& sourceDiskId,
    const TCheckpointId& checkpointId,
    TDiskId* shadowDiskId) const
{
    const auto id = TCheckpointInfo::MakeId(sourceDiskId, checkpointId);
    const auto* checkpointInfo = Checkpoints.FindPtr(id);
    if (!checkpointInfo) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Checkpoint " << checkpointId.Quote() << " for disk "
                << sourceDiskId.Quote() << " not found");
    }

    *shadowDiskId = checkpointInfo->ShadowDiskId;
    return MakeError(S_OK);
}

NProto::TError TDiskRegistryState::GetDiskInfo(
    const TString& diskId,
    TDiskInfo& diskInfo) const
{
    auto it = Disks.find(diskId);

    if (it == Disks.end()) {
        return MakeError(E_NOT_FOUND, TStringBuilder() <<
            "disk " << diskId.Quote() << " not found");
    }

    const auto& disk = it->second;

    diskInfo.CloudId = disk.CloudId;
    diskInfo.FolderId = disk.FolderId;
    diskInfo.UserId = disk.UserId;
    diskInfo.LogicalBlockSize = disk.LogicalBlockSize;
    diskInfo.State = disk.State;
    diskInfo.StateTs = disk.StateTs;
    diskInfo.PlacementGroupId = disk.PlacementGroupId;
    diskInfo.PlacementPartitionIndex = disk.PlacementPartitionIndex;
    diskInfo.FinishedMigrations = disk.FinishedMigrations;
    diskInfo.LaggingDevices = disk.LaggingDevices;
    diskInfo.DeviceReplacementIds = disk.DeviceReplacementIds;
    diskInfo.MediaKind = disk.MediaKind;
    diskInfo.MasterDiskId = disk.MasterDiskId;
    diskInfo.CheckpointId = disk.CheckpointReplica.GetCheckpointId();
    diskInfo.SourceDiskId = disk.CheckpointReplica.GetSourceDiskId();
    diskInfo.History = disk.History;
    diskInfo.MigrationStartTs = disk.MigrationStartTs;

    auto error = FillAllDiskDevices(diskId, disk, diskInfo);

    if (error.GetCode() == E_NOT_FOUND) {
        return MakeError(E_INVALID_STATE, error.GetMessage());
    }

    return error;
}

bool TDiskRegistryState::FilterDevicesForAcquire(TDiskInfo& diskInfo) const
{
    auto isUnavailableOrBroken = [&](const auto& d)
    {
        const auto* agent = AgentList.FindAgent(d.GetAgentId());

        const bool agentUnavailable =
            !agent || agent->GetState() == NProto::AGENT_STATE_UNAVAILABLE;

        const bool brokenDevice = d.GetState() == NProto::DEVICE_STATE_ERROR;

        return agentUnavailable || brokenDevice;
    };

    EraseIf(diskInfo.Devices, isUnavailableOrBroken);
    EraseIf(
        diskInfo.Migrations,
        [&](const auto& d)
        { return isUnavailableOrBroken(d.GetTargetDevice()); });

    return diskInfo.Devices.size() || diskInfo.Migrations.size();
}

bool TDiskRegistryState::FilterDevicesForRelease(TDiskInfo& diskInfo) const
{
    auto isUnavailable = [&](const auto& d)
    {
        const auto* agent = AgentList.FindAgent(d.GetAgentId());

        return !agent || agent->GetState() == NProto::AGENT_STATE_UNAVAILABLE;
    };

    EraseIf(diskInfo.Devices, isUnavailable);
    EraseIf(
        diskInfo.Migrations,
        [&](const auto& d) { return isUnavailable(d.GetTargetDevice()); });

    return diskInfo.Devices.size() || diskInfo.Migrations.size();
}

NProto::TError TDiskRegistryState::StartAcquireDisk(
    const TString& diskId,
    TDiskInfo& diskInfo)
{
    auto it = Disks.find(diskId);

    if (it == Disks.end()) {
        return MakeError(E_NOT_FOUND, TStringBuilder() <<
            "disk " << diskId.Quote() << " not found");
    }

    auto& disk = it->second;

    if (disk.AcquireInProgress) {
        return MakeError(E_REJECTED, TStringBuilder() <<
            "disk " << diskId.Quote() << " acquire in progress");
    }

    auto error = FillAllDiskDevices(diskId, disk, diskInfo);

    if (HasError(error)) {
        return error;
    }

    disk.AcquireInProgress = true;

    diskInfo.LogicalBlockSize = disk.LogicalBlockSize;

    return {};
}

void TDiskRegistryState::FinishAcquireDisk(const TString& diskId)
{
    auto it = Disks.find(diskId);

    if (it == Disks.end()) {
        return;
    }

    auto& disk = it->second;

    disk.AcquireInProgress = false;
}

bool TDiskRegistryState::IsAcquireInProgress(const TString& diskId) const
{
    auto it = Disks.find(diskId);

    if (it == Disks.end()) {
        return false;
    }

    return it->second.AcquireInProgress;
}

ui32 TDiskRegistryState::GetConfigVersion() const
{
    return CurrentConfig.GetVersion();
}

struct TDiskRegistryState::TConfigUpdateEffect
{
    TVector<TString> RemovedDevices;
    THashSet<TString> AffectedAgents;
    TVector<TString> AffectedDisks;
};

auto TDiskRegistryState::CalcConfigUpdateEffect(
    const NProto::TDiskRegistryConfig& newConfig) const
    -> TResultOrError<TConfigUpdateEffect>
{
    for (const auto& pool: newConfig.GetDevicePoolConfigs()) {
        if (pool.GetName().empty()
                && pool.GetKind() != NProto::DEVICE_POOL_KIND_DEFAULT)
        {
            return MakeError(E_ARGUMENT, "non default pool with empty name");
        }

        if (!pool.GetName().empty()
                && pool.GetKind() == NProto::DEVICE_POOL_KIND_DEFAULT)
        {
            return MakeError(E_ARGUMENT, "default pool with non empty name");
        }
    }

    THashSet<TString> allKnownDevices;
    TKnownAgents newKnownAgents;

    for (const auto& agent: newConfig.GetKnownAgents()) {
        const auto& agentId = agent.GetAgentId();
        if (newKnownAgents.contains(agentId)) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "duplicate of an agent " << agentId);
        }

        TKnownAgent& knownAgent = newKnownAgents[agentId];

        for (const auto& device: agent.GetDevices()) {
            const auto& deviceId = device.GetDeviceUUID();

            knownAgent.Devices.emplace(deviceId, device);
            auto [_, ok] = allKnownDevices.insert(deviceId);
            if (!ok) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "duplicate of a device " << deviceId);
            }
        }
    }

    TVector<TString> removedDevices;
    THashSet<TString> affectedAgents;

    for (const auto& agent: AgentList.GetAgents()) {
        const auto& agentId = agent.GetAgentId();

        for (const auto& d: agent.GetDevices()) {
            const auto& uuid = d.GetDeviceUUID();

            if (!allKnownDevices.contains(uuid)) {
                removedDevices.push_back(uuid);
                affectedAgents.insert(agentId);
            }
        }

        for (const auto& d: agent.GetUnknownDevices()) {
            const auto& uuid = d.GetDeviceUUID();

            if (allKnownDevices.contains(uuid)) {
                affectedAgents.insert(agentId);
            }
        }

        if (!newKnownAgents.contains(agentId)) {
            affectedAgents.insert(agentId);
        }
    }

    THashSet<TString> diskIds;

    for (const auto& uuid: removedDevices) {
        auto diskId = DeviceList.FindDiskId(uuid);
        if (diskId) {
            diskIds.emplace(std::move(diskId));
        }
    }

    return TConfigUpdateEffect{
        .RemovedDevices = std::move(removedDevices),
        .AffectedAgents = std::move(affectedAgents),
        .AffectedDisks = {diskIds.begin(), diskIds.end()},
    };
}

void TDiskRegistryState::SuspendDeviceIfNeeded(
    TDiskRegistryDatabase& db,
    NProto::TDeviceConfig& device)
{
    if (!StorageConfig->GetNonReplicatedDontSuspendDevices() &&
        device.GetPoolKind() == NProto::DEVICE_POOL_KIND_LOCAL)
    {
        STORAGE_INFO(
            "Suspend the new local device %s (%s)",
            device.GetDeviceUUID().c_str(),
            device.GetDeviceName().c_str());

        SuspendDevice(db, device.GetDeviceUUID());
    }
}

NProto::TError TDiskRegistryState::UpdateConfig(
    TDiskRegistryDatabase& db,
    NProto::TDiskRegistryConfig newConfig,
    bool ignoreVersion,
    TVector<TString>& affectedDisks)
{
    if (!ignoreVersion && newConfig.GetVersion() != CurrentConfig.GetVersion()) {
        return MakeError(E_ABORTED, "Wrong config version");
    }

    auto [effect, error] = CalcConfigUpdateEffect(newConfig);
    if (HasError(error)) {
        return error;
    }

    affectedDisks = std::move(effect.AffectedDisks);
    Sort(affectedDisks);

    if (!affectedDisks.empty()) {
        return MakeError(E_INVALID_STATE, "Destructive configuration change");
    }

    ForgetDevices(db, effect.RemovedDevices);

    if (Counters) {
        for (const auto& pool: newConfig.GetDevicePoolConfigs()) {
            SelfCounters.RegisterPool(
                GetPoolNameForCounters(pool.GetName(), pool.GetKind()),
                Counters);
        }
    }

    newConfig.SetVersion(CurrentConfig.GetVersion() + 1);
    ProcessConfig(newConfig);

    for (const auto& agentId: effect.AffectedAgents) {
        const auto& knownAgent = KnownAgents.Value(agentId, TKnownAgent{});

        auto [agent, newDevices] =
            AgentList.TryUpdateAgentDevices(agentId, knownAgent);

        if (!agent) {
            continue;
        }

        Sort(newDevices);

        const auto timestamp = TInstant::MicroSeconds(agent->GetStateTs());

        // Adjust & dirty new devices
        for (auto& device: *agent->MutableDevices()) {
            const auto& uuid = device.GetDeviceUUID();
            if (!std::binary_search(newDevices.begin(), newDevices.end(), uuid)) {
                continue;
            }

            AdjustDeviceIfNeeded(device, timestamp);
            SuspendDeviceIfNeeded(db, device);

            DeviceList.MarkDeviceAsDirty(uuid);
            db.UpdateDirtyDevice(uuid, {});
        }

        DeviceList.UpdateDevices(*agent, DevicePoolConfigs);
        UpdateAgent(db, *agent);
    }

    db.WriteDiskRegistryConfig(newConfig);
    CurrentConfig = std::move(newConfig);

    return {};
}

void TDiskRegistryState::ForgetDevices(
    TDiskRegistryDatabase& db,
    const TVector<TString>& ids)
{
    for (const auto& id: ids) {
        DeviceList.ForgetDevice(id);
        db.DeleteSuspendedDevice(id);
        db.DeleteDirtyDevice(id);

        DeleteAutomaticallyReplacedDevice(db, id);
    }
}

void TDiskRegistryState::RemoveAgent(
    TDiskRegistryDatabase& db,
    const NProto::TAgentConfig& agent)
{
    const auto nodeId = agent.GetNodeId();
    const auto agentId = agent.GetAgentId();

    DeviceList.RemoveDevices(agent);
    AgentList.RemoveAgent(nodeId);

    db.DeleteAgent(agentId);
}

template <typename T>
bool TDiskRegistryState::RemoveAgent(
    TDiskRegistryDatabase& db,
    const T& id)
{
    auto* agent = AgentList.FindAgent(id);
    if (!agent) {
        return false;
    }

    RemoveAgent(db, *agent);

    return true;
}

void TDiskRegistryState::ProcessConfig(const NProto::TDiskRegistryConfig& config)
{
    TKnownAgents newKnownAgents;

    for (const auto& agent: config.GetKnownAgents()) {
        TKnownAgent& knownAgent = newKnownAgents[agent.GetAgentId()];

        for (const auto& device: agent.GetDevices()) {
            knownAgent.Devices.emplace(device.GetDeviceUUID(), device);
        }
    }

    newKnownAgents.swap(KnownAgents);

    TDeviceOverrides newDeviceOverrides;

    for (const auto& deviceOverride: config.GetDeviceOverrides()) {
        auto& diskOverrides = newDeviceOverrides[deviceOverride.GetDiskId()];
        diskOverrides.Device2BlockCount[deviceOverride.GetDevice()] =
            deviceOverride.GetBlocksCount();
    }

    newDeviceOverrides.swap(DeviceOverrides);

    DevicePoolConfigs = CreateDevicePoolConfigs(config, *StorageConfig);
}

const NProto::TDiskRegistryConfig& TDiskRegistryState::GetConfig() const
{
    return CurrentConfig;
}

ui32 TDiskRegistryState::GetDiskCount() const
{
    return Disks.size();
}

TVector<TString> TDiskRegistryState::GetDiskIds() const
{
    TVector<TString> ids(Reserve(Disks.size()));

    for (const auto& kv: Disks) {
        ids.push_back(kv.first);
    }

    Sort(ids);

    return ids;
}

TVector<TString> TDiskRegistryState::GetMasterDiskIds() const
{
    TVector<TString> ids(Reserve(Disks.size()));

    for (const auto& kv: Disks) {
        if (!kv.second.MasterDiskId) {
            ids.push_back(kv.first);
        }
    }

    Sort(ids);

    return ids;
}

TVector<TString> TDiskRegistryState::GetMirroredDiskIds() const
{
    TVector<TString> ids;

    for (const auto& kv: Disks) {
        if (kv.second.ReplicaCount) {
            ids.push_back(kv.first);
        }
    }

    Sort(ids);

    return ids;
}

bool TDiskRegistryState::IsMasterDisk(const TString& diskId) const
{
    const auto* disk = Disks.FindPtr(diskId);
    return disk && disk->ReplicaCount;
}

TVector<NProto::TDeviceConfig> TDiskRegistryState::GetBrokenDevices() const
{
    return DeviceList.GetBrokenDevices();
}

TVector<NProto::TDeviceConfig> TDiskRegistryState::GetDirtyDevices() const
{
    return DeviceList.GetDirtyDevices();
}

bool TDiskRegistryState::IsKnownDevice(const TString& uuid) const
{
    return std::any_of(
        KnownAgents.begin(),
        KnownAgents.end(),
        [&] (const auto& kv) {
            return kv.second.Devices.contains(uuid);
        });
}

NProto::TError TDiskRegistryState::MarkDiskForCleanup(
    TDiskRegistryDatabase& db,
    const TString& diskId)
{
    if (!Disks.contains(diskId)) {
        return MakeError(E_NOT_FOUND, TStringBuilder() << "disk " <<
            diskId.Quote() << " not found");
    }

    db.AddDiskToCleanup(diskId);
    DisksToCleanup.insert(diskId);

    return {};
}

bool TDiskRegistryState::MarkDeviceAsDirty(
    TDiskRegistryDatabase& db,
    const TDeviceId& uuid)
{
    if (!IsKnownDevice(uuid)) {
        return false;
    }

    DeviceList.MarkDeviceAsDirty(uuid);
    db.UpdateDirtyDevice(uuid, {});

    return true;
}

TDiskRegistryState::TDiskId TDiskRegistryState::MarkDeviceAsClean(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDeviceId& uuid)
{
    auto ret = MarkDevicesAsClean(now, db, TVector<TDeviceId>{uuid});
    return ret.empty() ? "" : ret[0];
}

TVector<TDiskRegistryState::TDiskId> TDiskRegistryState::MarkDevicesAsClean(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TVector<TDeviceId>& uuids)
{
    for (const auto& uuid: uuids) {
        DeviceList.MarkDeviceAsClean(uuid);
        db.DeleteDirtyDevice(uuid);

        if (!DeviceList.IsSuspendedDevice(uuid)) {
            db.DeleteSuspendedDevice(uuid);
        }
    }

    TVector<TDiskId> ret;
    for (const auto& uuid: TryUpdateDevices(now, db, uuids)) {
        if (auto diskId = PendingCleanup.EraseDevice(uuid); !diskId.empty()) {
            ret.push_back(std::move(diskId));
        }
    }

    return ret;
}

bool TDiskRegistryState::TryUpdateDevice(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDeviceId& uuid)
{
    return !TryUpdateDevices(now, db, {uuid}).empty();
}

TVector<TDiskRegistryState::TDeviceId> TDiskRegistryState::TryUpdateDevices(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TVector<TDeviceId>& uuids)
{
    TVector<TDeviceId> ret;
    ret.reserve(uuids.size());

    TSet<TAgentId> agentsSet;
    for (const auto& uuid: uuids) {
        auto [agent, device] = FindDeviceLocation(uuid);
        if (!agent || !device) {
            STORAGE_WARN("Could not update device: %s", uuid.c_str());
            continue;
        }
        ret.push_back(uuid);
        agentsSet.emplace(agent->GetAgentId());
        AdjustDeviceIfNeeded(*device, now);
    }

    for (const auto& agentId: agentsSet) {
        auto* agent = AgentList.FindAgent(agentId);
        Y_DEBUG_ABORT_UNLESS(agent);
        if (!agent) {
            continue;
        }
        UpdateAgent(db, *agent);
        DeviceList.UpdateDevices(*agent, DevicePoolConfigs);
    }

    return ret;
}

TVector<TString> TDiskRegistryState::CollectBrokenDevices(
    const NProto::TAgentStats& stats) const
{
    TVector<TString> uuids;

    for (const auto& ds: stats.GetDeviceStats()) {
        if (!ds.GetErrors()) {
            continue;
        }

        const auto& uuid = ds.GetDeviceUUID();

        const auto* device = DeviceList.FindDevice(uuid);
        if (!device) {
            continue;
        }

        Y_DEBUG_ABORT_UNLESS(device->GetNodeId() == stats.GetNodeId());

        if (device->GetState() != NProto::DEVICE_STATE_ERROR) {
            uuids.push_back(uuid);
        }
    }

    return uuids;
}

NProto::TError TDiskRegistryState::UpdateAgentCounters(
    const NProto::TAgentStats& stats)
{
    // TODO (NBS-3280): add AgentId to TAgentStats
    const auto* agent = AgentList.FindAgent(stats.GetNodeId());

    if (!agent) {
        return MakeError(E_NOT_FOUND, "agent not found");
    }

    for (const auto& device: stats.GetDeviceStats()) {
        const auto& uuid = device.GetDeviceUUID();

        const NProto::TDeviceConfig* knownDevice = DeviceList.FindDevice(uuid);
        if (!knownDevice) {
            continue;
        }

        if (stats.GetNodeId() != knownDevice->GetNodeId()) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Unexpected device. DeviceId: " << uuid.Quote()
                    << " Sender node id: " << stats.GetNodeId()
                    << " Found node id: " << knownDevice->GetNodeId());
        }
    }

    AgentList.UpdateCounters(
        agent->GetAgentId(),
        stats,
        agent->GetTimeBetweenFailures());

    return {};
}

THashMap<TString, TBrokenGroupInfo> TDiskRegistryState::GatherBrokenGroupsInfo(
    TInstant now,
    TDuration period) const
{
    THashMap<TString, TBrokenGroupInfo> groups;

    for (const auto& [diskId, disk]: Disks) {
        if (disk.State != NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE &&
            disk.State != NProto::DISK_STATE_ERROR)
        {
            continue;
        }

        const auto& groupId = disk.PlacementGroupId;
        if (groupId.empty()) {
            continue;
        }

        const auto* pg = PlacementGroups.FindPtr(disk.PlacementGroupId);
        if (!pg) {
            ReportDiskRegistryPlacementGroupNotFound(
                TStringBuilder() << "GatherBrokenGroupsInfo:DiskId: " << diskId
                << ", PlacementGroupId: " << disk.PlacementGroupId);
            continue;
        }

        auto res = groups.try_emplace(groupId, pg->Config.GetPlacementStrategy());
        TBrokenGroupInfo& info = res.first->second;

        info.Total.Increment(diskId, disk.PlacementPartitionIndex);
        if (now - period < disk.StateTs) {
            info.Recently.Increment(diskId, disk.PlacementPartitionIndex);
        }
    }

    return groups;
}

void TDiskRegistryState::PublishCounters(TInstant now)
{
    if (!Counters) {
        return;
    }

    const auto startAt = TMonotonic::Now();
    STORAGE_LOG(TLOG_DEBUG, "DiskRegistry started PublishCounters");

    AgentList.PublishCounters(now);

    THashMap<TString, TDevicePoolCounters> poolName2Counters;

    ui32 allocatedDisks = 0;
    ui32 agentsInOnlineState = 0;
    ui32 agentsInWarningState = 0;
    ui32 agentsInUnavailableState = 0;
    ui32 disksInOnlineState = 0;
    ui32 disksInWarningState = 0;
    ui32 disksInTemporarilyUnavailableState = 0;
    ui32 disksInErrorState = 0;
    ui32 placementGroups = 0;
    ui32 fullPlacementGroups = 0;
    ui32 allocatedDisksInGroups = 0;
    ui64 unknownDevices = 0;

    for (const auto& agent: AgentList.GetAgents()) {
        const auto agentState = agent.GetState();
        switch (agentState) {
            case NProto::AGENT_STATE_ONLINE: {
                ++agentsInOnlineState;
                break;
            }
            case NProto::AGENT_STATE_WARNING: {
                ++agentsInWarningState;
                break;
            }
            case NProto::AGENT_STATE_UNAVAILABLE: {
                ++agentsInUnavailableState;
                break;
            }
            default: {}
        }

        unknownDevices += agent.UnknownDevicesSize();

        for (const auto& device: agent.GetDevices()) {
            const auto deviceState = device.GetState();
            const auto deviceBytes = device.GetBlockSize() * device.GetBlocksCount();
            const bool allocated = !DeviceList.FindDiskId(device.GetDeviceUUID()).empty();
            const bool dirty = DeviceList.IsDirtyDevice(device.GetDeviceUUID());
            const bool suspended = DeviceList.IsSuspendedDevice(device.GetDeviceUUID());

            const auto poolName = GetPoolNameForCounters(
                device.GetPoolName(),
                device.GetPoolKind());
            auto& pool = poolName2Counters[poolName];

            pool.TotalBytes += deviceBytes;

            if (allocated) {
                ++pool.AllocatedDevices;
            }

            if (dirty) {
                ++pool.DirtyDevices;
            }

            switch (deviceState) {
                case NProto::DEVICE_STATE_ONLINE: {
                    ++pool.DevicesInOnlineState;
                    break;
                }
                case NProto::DEVICE_STATE_WARNING: {
                    ++pool.DevicesInWarningState;
                    break;
                }
                case NProto::DEVICE_STATE_ERROR: {
                    ++pool.DevicesInErrorState;
                    break;
                }
                default: {}
            }

            if (suspended) {
                pool.SuspendedBytes += deviceBytes;
                continue;
            }

            if (dirty) {
                pool.DirtyBytes += deviceBytes;
                continue;
            }

            if (agentState == NProto::AGENT_STATE_UNAVAILABLE ||
                deviceState == NProto::DEVICE_STATE_ERROR)
            {
                pool.BrokenBytes += deviceBytes;
                continue;
            }

            if (allocated) {
                pool.AllocatedBytes += deviceBytes;
                continue;
            }

            if (agentState == NProto::AGENT_STATE_WARNING ||
                deviceState == NProto::DEVICE_STATE_WARNING)
            {
                pool.DecommissionedBytes += deviceBytes;
                continue;
            }

            pool.FreeBytes += deviceBytes;
        }
    }

    placementGroups = PlacementGroups.size();

    for (auto& [_, pg]: PlacementGroups) {
        allocatedDisksInGroups += pg.Config.DisksSize();

        const auto limit =
            GetMaxDisksInPlacementGroup(*StorageConfig, pg.Config);
        if (pg.Config.DisksSize() >= limit || pg.Config.DisksSize() == 0) {
            continue;
        }

        pg.BiggestDiskId = {};
        pg.BiggestDiskSize = 0;
        pg.Full = false;
        ui32 logicalBlockSize = 0;
        for (const auto& diskInfo: pg.Config.GetDisks()) {
            auto* disk = Disks.FindPtr(diskInfo.GetDiskId());

            if (!disk) {
                ReportDiskRegistryDiskNotFound(
                    TStringBuilder() << "PublishCounters:DiskId: "
                    << diskInfo.GetDiskId());

                continue;
            }

            ui64 diskSize = 0;
            for (const auto& deviceId: disk->Devices) {
                const auto& device = DeviceList.FindDevice(deviceId);

                if (!device) {
                    ReportDiskRegistryDeviceNotFound(
                        TStringBuilder() << "PublishCounters:DiskId: "
                        << diskInfo.GetDiskId()
                        << ", DeviceId: " << deviceId);

                    continue;
                }

                diskSize += device->GetBlockSize() * device->GetBlocksCount();
            }

            if (diskSize > pg.BiggestDiskSize) {
                logicalBlockSize = disk->LogicalBlockSize;
                pg.BiggestDiskId = diskInfo.GetDiskId();
                pg.BiggestDiskSize = diskSize;
            }
        }

        if (!logicalBlockSize) {
            continue;
        }

        if (StorageConfig->GetDisableFullPlacementGroupCountCalculation()) {
            continue;
        }

        THashSet<TString> forbiddenRacks;
        CollectForbiddenRacks(pg.Config, &forbiddenRacks);

        for (const auto& [_, pool]: DevicePoolConfigs) {
            if (pool.GetKind() == NProto::DEVICE_POOL_KIND_LOCAL) {
                continue;
            }

            const TDeviceList::TAllocationQuery query {
                .ForbiddenRacks = forbiddenRacks,
                .LogicalBlockSize = logicalBlockSize,
                .BlockCount = pg.BiggestDiskSize / logicalBlockSize,
                .PoolName = pool.GetName(),
                .PoolKind = pool.GetKind(),
                .NodeIds = {}
            };

            if (!DeviceList.CanAllocateDevices(query)) {
                ++fullPlacementGroups;
                pg.Full = true;
                break;
            }
        }
    }

    allocatedDisks = Disks.size();

    TDuration maxWarningTime;
    TDuration maxMigrationTime;

    for (const auto& [_, disk]: Disks) {
        switch (disk.State) {
            case NProto::DISK_STATE_ONLINE: {
                ++disksInOnlineState;
                break;
            }
            case NProto::DISK_STATE_WARNING: {
                ++disksInWarningState;

                maxWarningTime = std::max(maxWarningTime, now - disk.StateTs);

                if (disk.MigrationStartTs) {
                    maxMigrationTime = std::max(maxMigrationTime, now - disk.MigrationStartTs);
                }

                break;
            }
            case NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE: {
                ++disksInTemporarilyUnavailableState;
                break;
            }
            case NProto::DISK_STATE_ERROR: {
                ++disksInErrorState;
                break;
            }
            default: {}
        }
    }

    ui64 freeBytes = 0;
    ui64 totalBytes = 0;
    ui64 brokenBytes = 0;
    ui64 decommissionedBytes = 0;
    ui64 suspendedBytes = 0;
    ui64 dirtyBytes = 0;
    ui64 allocatedBytes = 0;
    ui64 allocatedDevices = 0;
    ui64 dirtyDevices = 0;
    ui64 devicesInOnlineState = 0;
    ui64 devicesInWarningState = 0;
    ui64 devicesInErrorState = 0;
    for (const auto& [poolName, counterValues]: poolName2Counters) {
        if (auto* pc = SelfCounters.PoolName2Counters.FindPtr(poolName)) {
            SetDevicePoolCounters(*pc, counterValues);
        }

        freeBytes += counterValues.FreeBytes;
        totalBytes += counterValues.TotalBytes;
        brokenBytes += counterValues.BrokenBytes;
        decommissionedBytes += counterValues.DecommissionedBytes;
        suspendedBytes += counterValues.SuspendedBytes;
        dirtyBytes += counterValues.DirtyBytes;
        allocatedBytes += counterValues.AllocatedBytes;
        allocatedDevices += counterValues.AllocatedDevices;
        dirtyDevices += counterValues.DirtyDevices;
        devicesInOnlineState += counterValues.DevicesInOnlineState;
        devicesInWarningState += counterValues.DevicesInWarningState;
        devicesInErrorState += counterValues.DevicesInErrorState;
    }

    SelfCounters.FreeBytes->Set(freeBytes);
    SelfCounters.TotalBytes->Set(totalBytes);
    SelfCounters.BrokenBytes->Set(brokenBytes);
    SelfCounters.DecommissionedBytes->Set(decommissionedBytes);
    SelfCounters.SuspendedBytes->Set(suspendedBytes);
    SelfCounters.DirtyBytes->Set(dirtyBytes);
    SelfCounters.AllocatedBytes->Set(allocatedBytes);
    SelfCounters.AllocatedDevices->Set(allocatedDevices);
    SelfCounters.DirtyDevices->Set(dirtyDevices);
    SelfCounters.UnknownDevices->Set(unknownDevices);
    SelfCounters.DevicesInOnlineState->Set(devicesInOnlineState);
    SelfCounters.DevicesInWarningState->Set(devicesInWarningState);
    SelfCounters.DevicesInErrorState->Set(devicesInErrorState);

    SelfCounters.AllocatedDisks->Set(allocatedDisks);
    SelfCounters.AgentsInOnlineState->Set(agentsInOnlineState);
    SelfCounters.AgentsInWarningState->Set(agentsInWarningState);
    SelfCounters.AgentsInUnavailableState->Set(agentsInUnavailableState);
    SelfCounters.DisksInOnlineState->Set(disksInOnlineState);

    SelfCounters.DisksInWarningState->Set(disksInWarningState);
    SelfCounters.MaxWarningTime->Set(maxWarningTime.Seconds());
    // XXX for backward compat with alerts
    SelfCounters.DisksInMigrationState->Set(disksInWarningState);
    SelfCounters.MaxMigrationTime->Set(maxMigrationTime.Seconds());

    ui32 migratingDeviceCount = 0;
    for (const auto& x: SourceDeviceMigrationsInProgress) {
        migratingDeviceCount += x.second.size();
    }

    SelfCounters.DevicesInMigrationState->Set(
        migratingDeviceCount + Migrations.size());
    SelfCounters.DisksInTemporarilyUnavailableState->Set(
        disksInTemporarilyUnavailableState);
    SelfCounters.DisksInErrorState->Set(disksInErrorState);
    SelfCounters.PlacementGroups->Set(placementGroups);
    SelfCounters.FullPlacementGroups->Set(fullPlacementGroups);
    SelfCounters.AllocatedDisksInGroups->Set(allocatedDisksInGroups);

    SelfCounters.MeanTimeBetweenFailures->Set(
        TimeBetweenFailures.GetBrokenCount()
        ? TimeBetweenFailures.GetWorkTime() /
            TimeBetweenFailures.GetBrokenCount()
        : 0);

    ui32 placementGroupsWithRecentlyBrokenSinglePartition = 0;
    ui32 placementGroupsWithRecentlyBrokenTwoOrMorePartitions = 0;
    ui32 placementGroupsWithBrokenSinglePartition = 0;
    ui32 placementGroupsWithBrokenTwoOrMorePartitions = 0;

    auto brokenGroups = GatherBrokenGroupsInfo(
        now,
        StorageConfig->GetPlacementGroupAlertPeriod());

    for (const auto& [groupId, bg]: brokenGroups) {
        const auto recently = bg.Recently.GetBrokenPartitionCount();
        const auto total = bg.Total.GetBrokenPartitionCount();

        placementGroupsWithRecentlyBrokenSinglePartition += recently == 1;
        placementGroupsWithRecentlyBrokenTwoOrMorePartitions += recently > 1;

        placementGroupsWithBrokenSinglePartition += total == 1;
        placementGroupsWithBrokenTwoOrMorePartitions += total > 1;
    }

    // TODO(dvrazumov): left for compatibility (NBSNEBIUS-26)
    SelfCounters.PlacementGroupsWithRecentlyBrokenSingleDisk->Set(
        placementGroupsWithRecentlyBrokenSinglePartition);

    SelfCounters.PlacementGroupsWithRecentlyBrokenTwoOrMoreDisks->Set(
        placementGroupsWithRecentlyBrokenTwoOrMorePartitions);

    SelfCounters.PlacementGroupsWithBrokenSingleDisk->Set(
        placementGroupsWithBrokenSinglePartition);

    SelfCounters.PlacementGroupsWithBrokenTwoOrMoreDisks->Set(
        placementGroupsWithBrokenTwoOrMorePartitions);
    // remove above ^^^^

    SelfCounters.PlacementGroupsWithRecentlyBrokenSinglePartition->Set(
        placementGroupsWithRecentlyBrokenSinglePartition);

    SelfCounters.PlacementGroupsWithRecentlyBrokenTwoOrMorePartitions->Set(
        placementGroupsWithRecentlyBrokenTwoOrMorePartitions);

    SelfCounters.PlacementGroupsWithBrokenSinglePartition->Set(
        placementGroupsWithBrokenSinglePartition);

    SelfCounters.PlacementGroupsWithBrokenTwoOrMorePartitions->Set(
        placementGroupsWithBrokenTwoOrMorePartitions);

    size_t cachedAcquireDevicesRequestAmount = Accumulate(
        AcquireCacheByAgentId.begin(),
        AcquireCacheByAgentId.end(),
        0,
        [](size_t sum, const auto& item) { return sum + item.second.size(); });
    SelfCounters.CachedAcquireDevicesRequestAmount->Set(
        cachedAcquireDevicesRequestAmount);

    auto replicaCountStats = ReplicaTable.CalculateReplicaCountStats();
    SelfCounters.Mirror2Disks->Set(replicaCountStats.Mirror2DiskMinus0);
    SelfCounters.Mirror2DisksMinus1->Set(replicaCountStats.Mirror2DiskMinus1);
    SelfCounters.Mirror2DisksMinus2->Set(replicaCountStats.Mirror2DiskMinus2);
    SelfCounters.Mirror3Disks->Set(replicaCountStats.Mirror3DiskMinus0);
    SelfCounters.Mirror3DisksMinus1->Set(replicaCountStats.Mirror3DiskMinus1);
    SelfCounters.Mirror3DisksMinus2->Set(replicaCountStats.Mirror3DiskMinus2);
    SelfCounters.Mirror3DisksMinus3->Set(replicaCountStats.Mirror3DiskMinus3);

    SelfCounters.AutomaticallyReplacedDevices->Set(
        AutomaticallyReplacedDevices.size());

    SelfCounters.QueryAvailableStorageErrors.Publish(now);
    SelfCounters.QueryAvailableStorageErrors.Reset();

    auto executionTime = TMonotonic::Now() - startAt;
    STORAGE_LOG(
        executionTime > TDuration::Seconds(1) ? TLOG_WARNING : TLOG_DEBUG,
        "DiskRegistry finished PublishCounters in %s",
        executionTime.ToString().c_str());
}

NProto::TError TDiskRegistryState::CreatePlacementGroup(
    TDiskRegistryDatabase& db,
    const TString& groupId,
    NProto::EPlacementStrategy placementStrategy,
    ui32 placementPartitionCount)
{
    if (PlacementGroups.contains(groupId)) {
        return MakeError(S_ALREADY);
    }

    if (placementStrategy ==
        NProto::EPlacementStrategy::PLACEMENT_STRATEGY_SPREAD)
    {
        if (placementPartitionCount) {
            return MakeError(
                E_ARGUMENT,
                "Partition count for spread placement group shouldn't be "
                "specified");
        }
    }

    if (placementStrategy ==
        NProto::EPlacementStrategy::PLACEMENT_STRATEGY_PARTITION)
    {
        if (placementPartitionCount < 2 ||
            placementPartitionCount > StorageConfig->GetMaxPlacementPartitionCount())
        {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "Partitions count must be between 2 and "
                << StorageConfig->GetMaxPlacementPartitionCount());
        }
    }

    auto& g = PlacementGroups[groupId].Config;
    g.SetGroupId(groupId);
    g.SetConfigVersion(1);
    g.SetPlacementStrategy(placementStrategy);
    g.SetPlacementPartitionCount(placementPartitionCount);
    db.UpdatePlacementGroup(g);

    return {};
}

NProto::TError TDiskRegistryState::CheckPlacementGroupVersion(
    const TString& placementGroupId,
    ui32 configVersion)
{
    auto* config = FindPlacementGroup(placementGroupId);
    if (!config) {
        return MakeError(E_NOT_FOUND, TStringBuilder()
            << "group does not exist: " << placementGroupId);
    }

    if (config->GetConfigVersion() != configVersion) {
        return MakeError(E_ABORTED, TStringBuilder()
                << "received version != expected version: "
                << configVersion << " != " << config->GetConfigVersion());
    }

    return {};
}

NProto::TError TDiskRegistryState::UpdatePlacementGroupSettings(
    TDiskRegistryDatabase& db,
    const TString& groupId,
    ui32 configVersion,
    NProto::TPlacementGroupSettings settings)
{
    auto error = CheckPlacementGroupVersion(groupId, configVersion);
    if (HasError(error)) {
        return error;
    }

    auto& config = PlacementGroups[groupId].Config;
    config.SetConfigVersion(configVersion + 1);
    config.MutableSettings()->SetMaxDisksInGroup(settings.GetMaxDisksInGroup());
    db.UpdatePlacementGroup(config);

    return {};
}

NProto::TError TDiskRegistryState::DestroyPlacementGroup(
    TDiskRegistryDatabase& db,
    const TString& groupId,
    TVector<TString>& affectedDisks)
{
    auto* g = FindPlacementGroup(groupId);
    if (!g) {
        return MakeError(S_ALREADY);
    }

    for (const auto& diskInfo: g->GetDisks()) {
        auto& diskId = diskInfo.GetDiskId();
        auto* d = Disks.FindPtr(diskId);
        if (!d) {
            ReportDiskRegistryDiskNotFound(TStringBuilder()
                << "DestroyPlacementGroup:DiskId: " << diskId
                << ", PlacementGroupId: " << groupId);

            continue;
        }

        d->PlacementGroupId.clear();

        NotificationSystem.AddOutdatedVolumeConfig(db, diskId);

        affectedDisks.emplace_back(diskId);
    }

    db.DeletePlacementGroup(g->GetGroupId());
    PlacementGroups.erase(g->GetGroupId());

    return {};
}

NProto::TError TDiskRegistryState::AlterPlacementGroupMembership(
    TDiskRegistryDatabase& db,
    const TString& groupId,
    ui32 placementPartitionIndex,
    ui32 configVersion,
    TVector<TString>& disksToAdd,
    const TVector<TString>& disksToRemove)
{
    if (auto error = CheckPlacementGroupVersion(groupId, configVersion); HasError(error)) {
        return error;
    }

    if (disksToAdd) {
        auto error = CheckDiskPlacementInfo({groupId, placementPartitionIndex});
        if (HasError(error)) {
            return error;
        }
    }

    auto newG = PlacementGroups[groupId].Config;

    if (disksToRemove) {
        auto end = std::remove_if(
            newG.MutableDisks()->begin(),
            newG.MutableDisks()->end(),
            [&] (const NProto::TPlacementGroupConfig::TDiskInfo& d) {
                return Find(disksToRemove.begin(), disksToRemove.end(), d.GetDiskId())
                    != disksToRemove.end();
            }
        );

        while (newG.MutableDisks()->end() > end) {
            newG.MutableDisks()->RemoveLast();
        }
    }

    TVector<TString> failedToAdd;
    THashMap<TString, TSet<TString>> disk2racks;
    for (const auto& diskId: disksToAdd) {
        const bool diskInCorrectLocation =
            FindIfPtr(
                newG.GetDisks(),
                [&] (const NProto::TPlacementGroupConfig::TDiskInfo& d) {
                    return (d.GetDiskId() == diskId) &&
                           (!PlacementGroupMustHavePartitions(newG) ||
                            d.GetPlacementPartitionIndex() == placementPartitionIndex);
                }
            ) != nullptr;

        if (diskInCorrectLocation) {
            continue;
        }

        const auto* disk = Disks.FindPtr(diskId);
        if (!disk) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "no such nonreplicated disk: " << diskId
                    << " - wrong media kind specified during disk creation?"
            );
        }

        if (disk->PlacementGroupId) {
            failedToAdd.push_back(diskId);
            continue;
        }
        THashSet<TString> forbiddenRacks;
        THashSet<TString> preferredRacks;
        CollectRacks(
            diskId,
            placementPartitionIndex,
            newG,
            &forbiddenRacks,
            &preferredRacks);

        // consider the racks of checked disks to be forbidden for next disks
        if (newG.GetPlacementStrategy() == NProto::PLACEMENT_STRATEGY_SPREAD) {
            for (const auto& [disk, racks]: disk2racks) {
                for (const auto& rack: racks) {
                    forbiddenRacks.insert(rack);
                }
            }
        }

        bool canAdd = true;

        for (const auto& deviceUuid: disk->Devices) {
            const auto rack = DeviceList.FindRack(deviceUuid);
            if (forbiddenRacks.contains(rack)) {
                canAdd = false;
                break;
            }
        }

        if (!canAdd) {
            failedToAdd.push_back(diskId);
            continue;
        }

        auto& racks = disk2racks[diskId];
        for (const auto& deviceUuid: disk->Devices) {
            racks.insert(DeviceList.FindRack(deviceUuid));
        }
    }

    if (failedToAdd.size()) {
        disksToAdd = std::move(failedToAdd);
        return MakeError(E_PRECONDITION_FAILED, "failed to add some disks");
    }

    if (newG.DisksSize() + disk2racks.size()
            > GetMaxDisksInPlacementGroup(*StorageConfig, newG))
    {
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_SILENT);

        return MakeError(
            E_BS_RESOURCE_EXHAUSTED,
            TStringBuilder()
                << "max disk count in group exceeded, max: "
                << GetMaxDisksInPlacementGroup(*StorageConfig, newG),
            flags);
    }

    for (const auto& [diskId, racks]: disk2racks) {
        auto& d = *newG.AddDisks();
        d.SetDiskId(diskId);
        d.SetPlacementPartitionIndex(placementPartitionIndex);
        for (const auto& rack: racks) {
            *d.AddDeviceRacks() = rack;
        }
    }
    newG.SetConfigVersion(configVersion + 1);
    db.UpdatePlacementGroup(newG);
    PlacementGroups[groupId] = std::move(newG);

    for (const auto& diskId: disksToAdd) {
        auto* d = Disks.FindPtr(diskId);

        if (!d) {
            ReportDiskRegistryDiskNotFound(TStringBuilder()
                << "AlterPlacementGroupMembership:DiskId: " << diskId
                << ", PlacementGroupId: " << groupId
                << ", PlacementPartitionIndex: " << placementPartitionIndex);

            continue;
        }

        d->PlacementGroupId = groupId;
        d->PlacementPartitionIndex = placementPartitionIndex;

        NotificationSystem.AddOutdatedVolumeConfig(db, diskId);
    }

    for (const auto& diskId: disksToRemove) {
        if (FindPtr(disksToAdd, diskId)) {
            continue;
        }

        if (auto* d = Disks.FindPtr(diskId)) {
            d->PlacementGroupId.clear();
            d->PlacementPartitionIndex = 0;

            NotificationSystem.AddOutdatedVolumeConfig(db, diskId);
        }
    }

    disksToAdd.clear();

    return {};
}

const NProto::TPlacementGroupConfig* TDiskRegistryState::FindPlacementGroup(
    const TString& groupId) const
{
    if (auto g = PlacementGroups.FindPtr(groupId)) {
        return &g->Config;
    }

    return nullptr;
}

void TDiskRegistryState::DeleteBrokenDisks(
    TDiskRegistryDatabase& db,
    TVector<TDiskId> ids)
{
    for (const auto& id: ids) {
        db.DeleteBrokenDisk(id);
    }

    Sort(ids);
    SortBy(BrokenDisks, [] (const auto& d) {
        return d.DiskId;
    });

    TVector<TBrokenDiskInfo> newList;

    std::set_difference(
        BrokenDisks.begin(),
        BrokenDisks.end(),
        ids.begin(),
        ids.end(),
        std::back_inserter(newList),
        TOverloaded {
            [] (const TBrokenDiskInfo& lhs, const auto& rhs) {
                return lhs.DiskId < rhs;
            },
            [] (const auto& lhs, const auto& rhs) {
                return lhs < rhs.DiskId;
            }});

    BrokenDisks.swap(newList);
}

void TDiskRegistryState::UpdateAndReallocateDisk(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    TDiskState& disk)
{
    db.UpdateDisk(BuildDiskConfig(diskId, disk));
    AddReallocateRequest(db, diskId);
}

ui64 TDiskRegistryState::AddReallocateRequest(
    TDiskRegistryDatabase& db,
    const TString& diskId)
{
    return NotificationSystem.AddReallocateRequest(
        db,
        GetDiskIdToNotify(diskId));
}

const THashMap<TString, ui64>& TDiskRegistryState::GetDisksToReallocate() const
{
    return NotificationSystem.GetDisksToReallocate();
}

auto TDiskRegistryState::FindDiskState(const TDiskId& diskId) const
    -> const TDiskState*
{
    auto it = Disks.find(diskId);
    if (it == Disks.end()) {
        return nullptr;
    }
    return &it->second;
}

auto TDiskRegistryState::AccessDiskState(const TDiskId& diskId) -> TDiskState*
{
    return const_cast<TDiskState*>(FindDiskState(diskId));
}

void TDiskRegistryState::RemoveFinishedMigrations(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    ui64 seqNo)
{
    auto* disk = AccessDiskState(diskId);
    if (!disk) {
        return;
    }

    if (disk->ReplicaCount) {
        for (ui32 i = 0; i < disk->ReplicaCount + 1; ++i) {
            RemoveFinishedMigrations(
                db,
                GetReplicaDiskId(diskId, i),
                seqNo);
        }

        return;
    }

    auto& migrations = disk->FinishedMigrations;

    auto it = std::remove_if(
        migrations.begin(),
        migrations.end(),
        [&] (const auto& m) {
            if (m.SeqNo > seqNo) {
                return false;
            }

            DeviceList.ReleaseDevice(m.DeviceId);
            db.UpdateDirtyDevice(m.DeviceId, diskId);
            auto error = AddDevicesToPendingCleanup(diskId, {m.DeviceId});
            if (HasError(error)) {
                ReportDiskRegistryInsertToPendingCleanupFailed(
                    TStringBuilder()
                    << "An error occurred while removing finished migrations: "
                    << FormatError(error));
            }

            return true;
        }
    );

    if (it != migrations.end()) {
        migrations.erase(it, migrations.end());
        db.UpdateDisk(BuildDiskConfig(diskId, *disk));
    }
}

void TDiskRegistryState::RemoveLaggingDevices(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    ui64 seqNo)
{
    auto* disk = AccessDiskState(diskId);
    if (!disk) {
        return;
    }

    if (disk->MasterDiskId) {
        RemoveLaggingDevices(db, disk->MasterDiskId, seqNo);
        return;
    }

    const size_t removedCount = std::erase_if(
        disk->LaggingDevices,
        [&](const TLaggingDevice& laggingDevice)
        { return laggingDevice.SeqNo <= seqNo; });
    if (removedCount) {
        db.UpdateDisk(BuildDiskConfig(diskId, *disk));
    }
}

void TDiskRegistryState::DeleteDiskToReallocate(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    ui64 seqNo)
{
    NotificationSystem.DeleteDiskToReallocate(db, diskId, seqNo);
    RemoveFinishedMigrations(db, diskId, seqNo);
    RemoveLaggingDevices(db, diskId, seqNo);
}

void TDiskRegistryState::AddUserNotification(
    TDiskRegistryDatabase& db,
    NProto::TUserNotification notification)
{
    NotificationSystem.AddUserNotification(db, std::move(notification));
}

void TDiskRegistryState::DeleteUserNotification(
    TDiskRegistryDatabase& db,
    const TString& entityId,
    ui64 seqNo)
{
    NotificationSystem.DeleteUserNotification(db, entityId, seqNo);
}

void TDiskRegistryState::GetUserNotifications(
    TVector<NProto::TUserNotification>& notifications) const
{
    return NotificationSystem.GetUserNotifications(notifications);
}

NProto::TDiskConfig TDiskRegistryState::BuildDiskConfig(
    TDiskId diskId,
    const TDiskState& diskState) const
{
    NProto::TDiskConfig config;

    config.SetDiskId(std::move(diskId));
    config.SetBlockSize(diskState.LogicalBlockSize);
    config.SetState(diskState.State);
    config.SetStateTs(diskState.StateTs.MicroSeconds());
    config.SetCloudId(diskState.CloudId);
    config.SetFolderId(diskState.FolderId);
    config.SetUserId(diskState.UserId);
    config.SetReplicaCount(diskState.ReplicaCount);
    config.SetMasterDiskId(diskState.MasterDiskId);
    config.MutableCheckpointReplica()->CopyFrom(diskState.CheckpointReplica);
    config.SetStorageMediaKind(diskState.MediaKind);
    config.SetMigrationStartTs(diskState.MigrationStartTs.MicroSeconds());

    for (const auto& [uuid, seqNo, _]: diskState.FinishedMigrations) {
        Y_UNUSED(seqNo);
        auto& m = *config.AddFinishedMigrations();
        m.SetDeviceId(uuid);
    }

    for (const auto& id: diskState.DeviceReplacementIds) {
        *config.AddDeviceReplacementUUIDs() = id;
    }

    for (const auto& laggingDevice: diskState.LaggingDevices) {
        *config.AddLaggingDevices() = laggingDevice.Device;
    }

    for (const auto& uuid: diskState.Devices) {
        *config.AddDeviceUUIDs() = uuid;
    }

    for (const auto& [targetId, sourceId]: diskState.MigrationTarget2Source) {
        auto& m = *config.AddMigrations();
        m.SetSourceDeviceId(sourceId);
        m.MutableTargetDevice()->SetDeviceUUID(targetId);
    }

    for (const auto& hi: diskState.History) {
        config.AddHistory()->CopyFrom(hi);
    }

    return config;
}

void TDiskRegistryState::DeleteDiskStateChanges(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    ui64 seqNo)
{
    db.DeleteDiskStateChanges(diskId, seqNo);
}

NProto::TError TDiskRegistryState::CheckAgentStateTransition(
    const NProto::TAgentConfig& agent,
    NProto::EAgentState newState,
    TInstant timestamp) const
{
    if (agent.GetState() == newState) {
        return MakeError(S_ALREADY);
    }

    if (agent.GetStateTs() > timestamp.MicroSeconds()) {
        return MakeError(E_INVALID_STATE, "out of order");
    }

    return {};
}

NProto::TError TDiskRegistryState::UpdateAgentState(
    TDiskRegistryDatabase& db,
    const TString& agentId,
    NProto::EAgentState newState,
    TInstant timestamp,
    TString reason,
    TVector<TDiskId>& affectedDisks)
{
    auto* agent = AgentList.FindAgent(agentId);

    if (!agent) {
        return MakeError(E_NOT_FOUND, "agent not found");
    }

    auto error = CheckAgentStateTransition(*agent, newState, timestamp);
    if (FAILED(error.GetCode())) {
        return error;
    }

    // Unavailable hosts are a problem when infra tries to request ADD_HOST on
    // an idle agent. DR waits for it to become online and blocks host
    // deployment.
    if (newState == NProto::AGENT_STATE_UNAVAILABLE &&
        agent->GetDevices().empty())
    {
        RemoveAgent(db, *agent);
        return {};
    }

    const auto cmsTs = TInstant::MicroSeconds(agent->GetCmsTs());
    const auto oldState = agent->GetState();
    const auto cmsDeadline = cmsTs + GetInfraTimeout(*StorageConfig, oldState);
    const auto cmsRequestActive = cmsTs && cmsDeadline > timestamp;

    if (!cmsRequestActive) {
        agent->SetCmsTs(0);
    }

    // when newState is less than AGENT_STATE_WARNING
    // check if agent is not scheduled for shutdown by cms
    if ((newState < NProto::EAgentState::AGENT_STATE_WARNING) && cmsRequestActive) {
        newState = oldState;
    }

    ChangeAgentState(*agent, newState, timestamp, std::move(reason));

    size_t sizeBefore = affectedDisks.size();
    ApplyAgentStateChange(db, *agent, timestamp, affectedDisks);

    if (newState == NProto::AGENT_STATE_UNAVAILABLE && newState != oldState) {
        for (auto i = sizeBefore; i < affectedDisks.size(); ++i) {
            AddReallocateRequest(db, affectedDisks[i]);
        }
    }

    return error;
}

NProto::TError TDiskRegistryState::SwitchAgentDisksToReadOnly(
    TDiskRegistryDatabase& db,
    TString agentId,
    TVector<TDiskId>& affectedDisks)
{
    TVector<TDiskId> dependentDisks;
    auto error = GetDependentDisks(agentId, "", true, &dependentDisks);
    if (error.GetCode() != S_OK) {
        return error;
    }

    for (const auto& diskId: dependentDisks) {
        AddReallocateRequest(db, diskId);
        affectedDisks.push_back(diskId);
    }
    return {};
}

void TDiskRegistryState::ApplyAgentStateChange(
    TDiskRegistryDatabase& db,
    const NProto::TAgentConfig& agent,
    TInstant timestamp,
    TVector<TDiskId>& affectedDisks)
{
    UpdateAgent(db, agent);
    DeviceList.UpdateDevices(agent, DevicePoolConfigs);

    THashSet<TString> diskIds;

    for (const auto& d: agent.GetDevices()) {
        const auto& deviceId = d.GetDeviceUUID();
        auto diskId = DeviceList.FindDiskId(deviceId);

        if (diskId.empty()) {
            continue;
        }

        auto& disk = Disks[diskId];

        diskIds.emplace(diskId);

        // check if deviceId is target for migration
        if (RestartDeviceMigration(timestamp, db, diskId, disk, deviceId)) {
            continue;
        }

        if (agent.GetState() == NProto::AGENT_STATE_WARNING) {
            if (MigrationCanBeStarted(disk, deviceId)) {
                if (!FindPtr(disk.Devices, deviceId)) {
                    ReportDiskRegistryWrongMigratedDeviceOwnership(Sprintf(
                        "ApplyAgentStateChange: device[DeviceUUID = %s] not "
                        "found in disk[DiskId "
                        "= %s]",
                        deviceId.c_str(),
                        diskId.c_str()));
                    continue;
                }

                AddMigration(disk, diskId, deviceId);
            }
        } else {
            if (agent.GetState() == NProto::AGENT_STATE_UNAVAILABLE
                    && disk.MasterDiskId)
            {
                TryToReplaceDeviceIfAllowedWithoutDiskStateUpdate(
                    db,
                    disk,
                    diskId,
                    deviceId,
                    timestamp,
                    "agent unavailable");
            }

            CancelDeviceMigration(timestamp, db, diskId, disk, deviceId);
        }
    }

    for (const auto& diskId: diskIds) {
        if (TryUpdateDiskState(db, diskId, timestamp)) {
            affectedDisks.push_back(diskId);
        }
    }
}

bool TDiskRegistryState::HasDependentSsdDisks(
    const NProto::TAgentConfig& agent) const
{
    for (const auto& d: agent.GetDevices()) {
        if (d.GetState() >= NProto::DEVICE_STATE_ERROR) {
            continue;
        }

        if (d.GetPoolKind() == NProto::DEVICE_POOL_KIND_LOCAL &&
            PendingCleanup.FindDiskId(d.GetDeviceUUID()))
        {
            return true;
        }

        const auto diskId = FindDisk(d.GetDeviceUUID());

        if (!diskId) {
            continue;
        }

        const auto* disk = Disks.FindPtr(diskId);
        if (!disk) {
            ReportDiskRegistryDiskNotFound(
                TStringBuilder() << "HasDependentSsdDisks:DiskId: " << diskId);
            continue;
        }

        if (disk->MediaKind == NProto::STORAGE_MEDIA_HDD_NONREPLICATED) {
            // existence of hdd disks should not directly block maintenance
            continue;
        }

        return true;
    }

    return false;
}

ui32 TDiskRegistryState::CountBrokenHddPlacementGroupPartitionsAfterAgentRemoval(
    const NProto::TAgentConfig& agent) const
{
    THashSet<TString> deviceIds;
    for (const auto& d: agent.GetDevices()) {
        deviceIds.insert(d.GetDeviceUUID());
    }
    return CountBrokenHddPlacementGroupPartitionsAfterDeviceRemoval(deviceIds);
}

ui32 TDiskRegistryState::CountBrokenHddPlacementGroupPartitionsAfterDeviceRemoval(
    const THashSet<TString>& deviceIds) const
{
    ui32 maxBrokenCount = 0;

    // pretty inefficient but ok for now because this method is not going to
    // be called too often - like, several times per hour in big clusters and
    // several times per day in small ones
    for (const auto& [_, pg]: PlacementGroups) {
        const auto& c = pg.Config;
        THashSet<ui32> brokenPartitions;
        bool dependsOnAffectedDevices = false;
        ui32 i = 0;
        for (const auto& diskInfo: c.GetDisks()) {
            ++i;

            auto* disk = Disks.FindPtr(diskInfo.GetDiskId());
            Y_DEBUG_ABORT_UNLESS(disk);
            if (!disk) {
                continue;
            }

            if (disk->MediaKind != NProto::STORAGE_MEDIA_HDD_NONREPLICATED) {
                continue;
            }

            // for non-PARTITION placement groups each disk is treated as a
            // separate 'partition'
            const ui32 partitionIndex =
                c.GetPlacementStrategy() == NProto::PLACEMENT_STRATEGY_PARTITION
                ? diskInfo.GetPlacementPartitionIndex() : i;

            if (brokenPartitions.contains(partitionIndex)) {
                continue;
            }

            // a disk is broken if at least one of its devices is broken
            // a device is broken if it or its agent is not online or if it or
            // its agent is a maintenance target
            bool broken = false;
            for (const auto& deviceId: disk->Devices) {
                if (deviceIds.contains(deviceId)) {
                    broken = true;
                    dependsOnAffectedDevices = true;
                    break;
                }

                const auto deviceState = DeviceList.GetDeviceState(deviceId);
                if (deviceState != NProto::DEVICE_STATE_ONLINE) {
                    broken = true;
                    break;
                }

                const auto agentId = DeviceList.FindAgentId(deviceId);
                const auto agentState = GetAgentState(agentId);
                if (agentState != NProto::AGENT_STATE_ONLINE) {
                    broken = true;
                    break;
                }
            }

            if (broken) {
                brokenPartitions.insert(partitionIndex);
            }
        }

        // we are interested only in those placement groups that depend on at
        // least one of the devices that are going to be affected by maintenance
        if (dependsOnAffectedDevices) {
            maxBrokenCount = Max<ui32>(brokenPartitions.size(), maxBrokenCount);
        }
    }

    return maxBrokenCount;
}

NProto::TError TDiskRegistryState::UpdateCmsHostState(
    TDiskRegistryDatabase& db,
    const TString& agentId,
    NProto::EAgentState newState,
    TInstant now,
    bool dryRun,
    TVector<TDiskId>& affectedDisks,
    TDuration& timeout)
{
    auto* agent = AgentList.FindAgent(agentId);

    if (!agent) {
        return MakeError(E_NOT_FOUND, "agent not found");
    }

    auto error = CheckAgentStateTransition(*agent, newState, now);
    if (FAILED(error.GetCode())) {
        return error;
    }

    TInstant cmsTs = TInstant::MicroSeconds(agent->GetCmsTs());
    if (cmsTs == TInstant::Zero()) {
        cmsTs = now;
    }

    const auto infraTimeout = GetInfraTimeout(*StorageConfig, agent->GetState());

    if (cmsTs + infraTimeout <= now
            && agent->GetState() < NProto::AGENT_STATE_UNAVAILABLE)
    {
        // restart timer
        cmsTs = now;
    }

    timeout = cmsTs + infraTimeout - now;

    const bool hasDependentDisks = HasDependentSsdDisks(*agent);
    const ui32 brokenPlacementGroupPartitions =
        CountBrokenHddPlacementGroupPartitionsAfterAgentRemoval(*agent);
    const ui32 maxBrokenHddPartitions =
        StorageConfig->GetMaxBrokenHddPlacementGroupPartitionsAfterDeviceRemoval();
    if (!hasDependentDisks
            && brokenPlacementGroupPartitions <= maxBrokenHddPartitions)
    {
        // no dependent disks => we can return this host immediately
        timeout = TDuration::Zero();
    }

    if (newState == NProto::AGENT_STATE_ONLINE
        && agent->GetState() < NProto::AGENT_STATE_UNAVAILABLE)
    {
        timeout = TDuration::Zero();
    }

    NProto::TError result;

    // Agent can return from 'unavailable' state only when it is reconnected to
    // the cluster.
    if (agent->GetState() == NProto::AGENT_STATE_UNAVAILABLE &&
        newState == NProto::AGENT_STATE_ONLINE)
    {
        timeout =
            cmsTs + StorageConfig->GetIdleAgentDeployByCmsDelay() - now;
        if (!timeout) {
            // If the timer is expired and an agent is still unavailable, then
            // the agent is most likely in the idle state and won't register in
            // the DR. Return "E_NOT_FOUND" since infra passes through it.
            result.SetCode(E_NOT_FOUND);
        }
    }

    if (timeout) {
        result = MakeError(
            E_TRY_AGAIN,
            TStringBuilder() << "time remaining: " << timeout);
    } else {
        cmsTs = TInstant::Zero();
    }

    if (dryRun) {
        return result;
    }

    if (agent->GetState() != NProto::AGENT_STATE_UNAVAILABLE) {
        ChangeAgentState(*agent, newState, now, "cms action");
    }

    agent->SetCmsTs(cmsTs.MicroSeconds());

    ApplyAgentStateChange(db, *agent, now, affectedDisks);

    if (newState != NProto::AGENT_STATE_ONLINE && !HasError(result) &&
        (!StorageConfig->GetDiskRegistryAlwaysAllocatesLocalDisks() ||
         StorageConfig->GetDiskRegistryCleanupConfigOnRemoveHost()))
    {
        CleanupAgentConfig(db, *agent);
    }

    if (newState == NProto::AGENT_STATE_ONLINE && !HasError(result)) {
        result = RegisterUnknownDevices(db, *agent, now);

        if (!HasError(result) &&
            StorageConfig->GetNonReplicatedDontSuspendDevices())
        {
            ResumeLocalDevices(db, *agent, now);
        }
    }

    return result;
}

NProto::TError TDiskRegistryState::PurgeHost(
    TDiskRegistryDatabase& db,
    const TString& agentId,
    TInstant now,
    bool dryRun,
    TVector<TDiskId>& affectedDisks)
{
    auto* agent = AgentList.FindAgent(agentId);
    if (!agent) {
        return MakeError(E_NOT_FOUND, "agent not found");
    }

    // Since "PURGE_HOST" should be called after "REMOVE_HOST", we call the
    // remove one more time to make sure. However, the result of this operation
    // should not be visible to the caller.
    TDuration timeout;
    auto removeHostError = UpdateCmsHostState(
        db,
        agentId,
        NProto::AGENT_STATE_WARNING,
        now,
        dryRun,
        affectedDisks,
        timeout);

    if (HasError(removeHostError)) {
        ReportDiskRegistryPurgeHostError();
    }

    STORAGE_LOG(
        (HasError(removeHostError) ? TLOG_ERR : TLOG_INFO),
        "Purge host %s requested. Remove host ended with the result: %s; "
        "affectedDisks: [%s]; timeout: %lu",
        agent->GetAgentId().Quote().c_str(),
        JoinSeq(", ", affectedDisks).c_str(),
        FormatError(removeHostError).c_str(),
        timeout.Seconds());

    if (dryRun) {
        return {};
    }

    CleanupAgentConfig(db, *agent);

    return {};
}

NProto::TError TDiskRegistryState::RegisterUnknownDevices(
    TDiskRegistryDatabase& db,
    NProto::TAgentConfig& agent,
    TInstant now)
{
    TVector<TDeviceId> ids;
    for (const auto& device: agent.GetUnknownDevices()) {
        ids.push_back(device.GetDeviceUUID());
    }

    STORAGE_INFO("add new devices: AgentId=" << agent.GetAgentId()
        << " Devices=" << JoinSeq(", ", ids));

    auto newConfig = GetConfig();

    NProto::TAgentConfig* knownAgent = FindIfPtr(
        *newConfig.MutableKnownAgents(),
        [&] (const auto& x) {
            return x.GetAgentId() == agent.GetAgentId();
        });
    if (!knownAgent) {
        knownAgent = newConfig.AddKnownAgents();
        knownAgent->SetAgentId(agent.GetAgentId());
    }

    for (auto& id: ids) {
        knownAgent->AddDevices()->SetDeviceUUID(id);
    }

    TVector<TString> affectedDisks;
    auto error = UpdateConfig(
        db,
        std::move(newConfig),
        false,  // ignoreVersion
        affectedDisks);

    if (!affectedDisks.empty()) {
        ReportDiskRegistryUnexpectedAffectedDisks(TStringBuilder()
            << "RegisterUnknownDevices: " << JoinSeq(" ", affectedDisks));
    }

    if (!HasError(error)) {
        for (auto& device: *agent.MutableUnknownDevices()) {
            if (device.GetState() == NProto::DEVICE_STATE_ONLINE) {
                device.SetStateMessage("New device");
            }

            device.SetStateTs(now.MicroSeconds());
        }
    }

    return error;
}

void TDiskRegistryState::ResumeLocalDevices(
    TDiskRegistryDatabase& db,
    NProto::TAgentConfig& agent,
    TInstant now)
{
    for (const auto& device: agent.GetDevices()) {
        if (device.GetPoolKind() == NProto::DEVICE_POOL_KIND_LOCAL
            && DeviceList.IsSuspendedDevice(device.GetDeviceUUID()))
        {
            STORAGE_INFO(
                "Resume the local device %s (%s)",
                device.GetDeviceUUID().c_str(),
                device.GetDeviceName().c_str());

            ResumeDevice(now, db, device.GetDeviceUUID());
        }
    }
}

void TDiskRegistryState::SuspendLocalDevices(
    TDiskRegistryDatabase& db,
    const NProto::TAgentConfig& agent)
{
    for (const auto& d: agent.GetDevices()) {
        if (d.GetPoolKind() == NProto::DEVICE_POOL_KIND_LOCAL) {
            STORAGE_INFO(
                "Suspend the local device %s (%s)",
                d.GetDeviceUUID().c_str(),
                d.GetDeviceName().c_str());

            SuspendDevice(db, d.GetDeviceUUID());
        }
    }
}

TMaybe<NProto::EAgentState> TDiskRegistryState::GetAgentState(
    const TString& agentId) const
{
    const auto* agent = AgentList.FindAgent(agentId);
    if (agent) {
        return agent->GetState();
    }

    return {};
}

TMaybe<TInstant> TDiskRegistryState::GetAgentCmsTs(
    const TString& agentId) const
{
    const auto* agent = AgentList.FindAgent(agentId);
    if (agent) {
        return TInstant::MicroSeconds(agent->GetCmsTs());
    }

    return {};
}

bool TDiskRegistryState::TryUpdateDiskState(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    TInstant timestamp)
{
    auto* d = Disks.FindPtr(diskId);

    if (!d) {
        ReportDiskRegistryDiskNotFound(TStringBuilder()
            << "TryUpdateDiskState:DiskId: " << diskId);

        return {};
    }

    return TryUpdateDiskState(
        db,
        diskId,
        *d,
        timestamp);
}

bool TDiskRegistryState::TryUpdateDiskState(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    TDiskState& disk,
    TInstant timestamp)
{
    Y_DEBUG_ABORT_UNLESS(!IsMasterDisk(diskId));

    const bool updated = TryUpdateDiskStateImpl(db, diskId, disk, timestamp);

    if (disk.MasterDiskId) {
        auto* masterDisk = Disks.FindPtr(disk.MasterDiskId);

        if (masterDisk) {
            TryUpdateDiskStateImpl(
                db,
                disk.MasterDiskId,
                *masterDisk,
                timestamp);
        } else {
            Y_DEBUG_ABORT_UNLESS(masterDisk);
            ReportDiskRegistryDiskNotFound(
                TStringBuilder()
                << "TryUpdateDiskState: DiskId: " << disk.MasterDiskId);
        }
    }

    return updated;
}

bool TDiskRegistryState::TryUpdateDiskStateImpl(
    TDiskRegistryDatabase& db,
    const TString& diskId,
    TDiskState& disk,
    TInstant timestamp)
{
    const auto newState = CalculateDiskState(diskId, disk);
    const auto oldState = disk.State;

    if (oldState == newState) {
        return false;
    }

    disk.State = newState;
    disk.StateTs = timestamp;

    NProto::TDiskHistoryItem historyItem;
    historyItem.SetTimestamp(timestamp.MicroSeconds());
    historyItem.SetMessage(
        TStringBuilder() << "state changed: "
                         << NProto::EDiskState_Name(oldState) << " ("
                         << static_cast<int>(oldState) << ") -> "
                         << NProto::EDiskState_Name(newState) << " ("
                         << static_cast<int>(newState) << ")");
    disk.History.push_back(std::move(historyItem));

    UpdateAndReallocateDisk(db, diskId, disk);

    NotificationSystem.OnDiskStateChanged(
        db,
        diskId,
        oldState,
        newState,
        timestamp);

    return true;
}

void TDiskRegistryState::CleanupAgentConfig(
    TDiskRegistryDatabase& db,
    const NProto::TAgentConfig& agent)
{
    auto error = TryToRemoveAgentDevices(db, agent.GetAgentId());
    // Do not return the error from "TryToRemoveAgentDevices()" since
    // it's internal and shouldn't block node removal.
    if (!HasError(error) || error.GetCode() == E_NOT_FOUND) {
        return;
    }

    SuspendLocalDevices(db, agent);
}

NProto::TError TDiskRegistryState::TryToRemoveAgentDevices(
    TDiskRegistryDatabase& db,
    const TAgentId& agentId)
{
    auto newConfig = GetConfig();
    auto* agents = newConfig.MutableKnownAgents();
    const auto agentIt = FindIf(
        *agents,
        [&](const auto& x) { return x.GetAgentId() == agentId; });
    if (agentIt == agents->end()) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder() << "Couldn't find agent " << agentId.Quote()
                             << " in the DR config.");
    }

    agents->erase(agentIt);
    TVector<TString> affectedDisks;
    auto error = UpdateConfig(
        db,
        std::move(newConfig),
        false,   // ignoreVersion
        affectedDisks);
    return error;
}

void TDiskRegistryState::DeleteDiskStateUpdate(
    TDiskRegistryDatabase& db,
    ui64 maxSeqNo)
{
    NotificationSystem.DeleteDiskStateUpdate(db, maxSeqNo);
}

NProto::EDiskState TDiskRegistryState::CalculateDiskState(
    const TString& diskId,
    const TDiskState& disk) const
{
    if (disk.ReplicaCount != 0) {
        for (ui32 i = 0; i < disk.ReplicaCount + 1; ++i) {
            auto replicaId = GetReplicaDiskId(diskId, i);
            if (GetDiskState(replicaId) == NProto::DISK_STATE_WARNING) {
                return NProto::DISK_STATE_WARNING;
            }
        }
        return NProto::DISK_STATE_ONLINE;
    }

    NProto::EDiskState state = NProto::DISK_STATE_ONLINE;

    for (const auto& uuid: disk.Devices) {
        const auto* device = DeviceList.FindDevice(uuid);

        if (!device) {
            return NProto::DISK_STATE_ERROR;
        }

        const auto* agent = AgentList.FindAgent(device->GetAgentId());
        if (!agent) {
            return NProto::DISK_STATE_ERROR;
        }

        state = std::max(state, ToDiskState(agent->GetState()));
        state = std::max(state, ToDiskState(device->GetState()));

        if (state == NProto::DISK_STATE_ERROR) {
            break;
        }
    }

    return state;
}

auto TDiskRegistryState::FindDeviceLocation(const TDeviceId& deviceId) const
    -> std::pair<const NProto::TAgentConfig*, const NProto::TDeviceConfig*>
{
    return const_cast<TDiskRegistryState*>(this)->FindDeviceLocation(deviceId);
}

auto TDiskRegistryState::FindDeviceLocation(const TDeviceId& deviceId)
    -> std::pair<NProto::TAgentConfig*, NProto::TDeviceConfig*>
{
    const auto agentId = DeviceList.FindAgentId(deviceId);
    if (agentId.empty()) {
        return {};
    }

    auto* agent = AgentList.FindAgent(agentId);
    if (!agent) {
        return {};
    }

    auto* device = FindIfPtr(*agent->MutableDevices(), [&] (const auto& x) {
        return x.GetDeviceUUID() == deviceId;
    });

    if (!device) {
        return {};
    }

    return {agent, device};
}

NProto::TError TDiskRegistryState::UpdateDeviceState(
    TDiskRegistryDatabase& db,
    const TString& deviceId,
    NProto::EDeviceState newState,
    TInstant now,
    TString reason,
    TDiskId& affectedDisk)
{
    auto [agentPtr, devicePtr] = FindDeviceLocation(deviceId);
    if (!agentPtr || !devicePtr) {
        return MakeError(E_NOT_FOUND, TStringBuilder() <<
            "device " << deviceId.Quote() << " not found");
    }

    auto error = CheckDeviceStateTransition(*devicePtr, newState, now);
    if (HasError(error)) {
        return error;
    }

    const auto cmsTs = TInstant::MicroSeconds(devicePtr->GetCmsTs());
    const auto cmsDeadline = cmsTs + StorageConfig->GetNonReplicatedInfraTimeout();
    const auto cmsRequestActive = cmsTs && cmsDeadline > now;
    const auto oldState = devicePtr->GetState();

    if (!cmsRequestActive) {
        devicePtr->SetCmsTs(0);
    }

    // when newState is less than DEVICE_STATE_WARNING
    // check if device is not scheduled for shutdown by cms
    if (newState < NProto::DEVICE_STATE_WARNING && cmsRequestActive) {
        newState = oldState;
    }

    devicePtr->SetState(newState);
    devicePtr->SetStateTs(now.MicroSeconds());
    devicePtr->SetStateMessage(std::move(reason));

    ApplyDeviceStateChange(db, *agentPtr, *devicePtr, now, affectedDisk);

    return error;
}

auto TDiskRegistryState::ResolveDevices(
        const TAgentId& agentId,
        const TString& path)
    -> std::pair<NProto::TAgentConfig*, TVector<NProto::TDeviceConfig*>>
{
    auto* agent = AgentList.FindAgent(agentId);
    if (!agent) {
        return {};
    }

    TVector<NProto::TDeviceConfig*> devices;
    for (auto& device: *agent->MutableDevices()) {
        if (device.GetDeviceName() == path) {
            devices.push_back(&device);
        }
    }

    return { agent, std::move(devices) };
}

auto TDiskRegistryState::AddNewDevices(
    TDiskRegistryDatabase& db,
    NProto::TAgentConfig& agent,
    const TString& path,
    TInstant now,
    bool shouldResume,
    bool dryRun) -> TUpdateCmsDeviceStateResult
{
    TVector<NProto::TDeviceConfig*> devices;

    for (auto& device: *agent.MutableUnknownDevices()) {
        if (device.GetDeviceName() == path) {
            devices.push_back(&device);
        }
    }

    if (devices.empty()) {
        return {
            .Error = MakeError(E_NOT_FOUND, TStringBuilder()
                << "Device not found (" << agent.GetAgentId() << "," << path << ')')
        };
    }

    if (dryRun) {
        return {};
    }

    TVector<TString> ids;
    for (auto* device: devices) {
        ids.push_back(device->GetDeviceUUID());

        device->SetState(NProto::DEVICE_STATE_ONLINE);
        device->SetStateMessage("cms add device action");
        device->SetStateTs(now.MicroSeconds());
    }

    STORAGE_INFO("add new devices: AgentId=" << agent.GetAgentId()
        << " Devices=" << JoinSeq(", ", ids));

    auto newConfig = GetConfig();
    NProto::TAgentConfig* knownAgent = FindIfPtr(
        *newConfig.MutableKnownAgents(),
        [&] (const auto& x) {
            return x.GetAgentId() == agent.GetAgentId();
        });
    if (!knownAgent) {
        knownAgent = newConfig.AddKnownAgents();
        knownAgent->SetAgentId(agent.GetAgentId());
    }

    for (auto& id: ids) {
        knownAgent->AddDevices()->SetDeviceUUID(id);
    }

    TVector<TString> affectedDisks;
    auto error = UpdateConfig(
        db,
        std::move(newConfig),
        false,  // ignoreVersion
        affectedDisks);

    if (!affectedDisks.empty()) {
        ReportDiskRegistryUnexpectedAffectedDisks(TStringBuilder()
            << "AddNewDevices: " << JoinSeq(" ", affectedDisks));
    }

    if (shouldResume) {
        ResumeDevices(now, db, ids);
    }

    return {
        .Error = std::move(error),
    };
}

NProto::TError TDiskRegistryState::CmsAddDevice(
    TDiskRegistryDatabase& db,
    NProto::TAgentConfig& agent,
    NProto::TDeviceConfig& device,
    TInstant now,
    bool shouldResume,
    bool dryRun,
    TDuration& timeout)
{
    NProto::TError error = CheckDeviceStateTransition(
        device,
        NProto::DEVICE_STATE_ONLINE,
        now);

    if (HasError(error)) {
        return error;
    }

    error = {};
    timeout = {};

    if (device.GetState() == NProto::DEVICE_STATE_ERROR) {
        // CMS can't return device from 'error' state.
        error = MakeError(E_INVALID_STATE, "device is in error state");
    }

    if (dryRun) {
        return error;
    }

    if (device.GetState() != NProto::DEVICE_STATE_ERROR) {
        device.SetState(NProto::DEVICE_STATE_ONLINE);
        device.SetStateMessage("cms add device action");
    }

    device.SetStateTs(now.MicroSeconds());

    TDiskId affectedDisk;
    ApplyDeviceStateChange(db, agent, device, now, affectedDisk);
    Y_UNUSED(affectedDisk);

    if (shouldResume && !HasError(error)) {
        ResumeDevice(now, db, device.GetDeviceUUID());
    }

    return error;
}

NProto::TError TDiskRegistryState::CmsRemoveDevice(
    TDiskRegistryDatabase& db,
    NProto::TAgentConfig& agent,
    NProto::TDeviceConfig& device,
    TInstant now,
    bool dryRun,
    TDiskId& affectedDisk,
    TDuration& timeout)
{
    NProto::TError error = CheckDeviceStateTransition(
        device,
        NProto::DEVICE_STATE_WARNING,
        now);

    if (HasError(error)) {
        return error;
    }

    TInstant cmsTs;
    error = {};
    timeout = {};

    const auto& deviceId = device.GetDeviceUUID();
    const auto diskId = FindDisk(deviceId);
    const bool hasDependentDisk = diskId
        && device.GetState() < NProto::DEVICE_STATE_ERROR;

    if (hasDependentDisk) {
        error = MakeError(
            E_TRY_AGAIN,
            TStringBuilder() << "have dependent disk: " << diskId);
    }

    const ui32 brokenPlacementGroupPartitions =
        CountBrokenHddPlacementGroupPartitionsAfterDeviceRemoval({deviceId});
    const ui32 maxBrokenHddPartitions =
        StorageConfig->GetMaxBrokenHddPlacementGroupPartitionsAfterDeviceRemoval();

    if (brokenPlacementGroupPartitions > maxBrokenHddPartitions) {
        error = MakeError(
            E_TRY_AGAIN,
            TStringBuilder() << "will break too many partitions: "
                << brokenPlacementGroupPartitions);
    }

    if (HasError(error)) {
        cmsTs = TInstant::MicroSeconds(device.GetCmsTs());
        if (!cmsTs || cmsTs + StorageConfig->GetNonReplicatedInfraTimeout() <= now) {
            // restart timer
            cmsTs = now;
        }

        timeout = cmsTs + StorageConfig->GetNonReplicatedInfraTimeout() - now;
    }

    if (dryRun) {
        return error;
    }

    if (device.GetState() != NProto::DEVICE_STATE_ERROR) {
        device.SetState(NProto::DEVICE_STATE_WARNING);
        device.SetStateMessage("cms remove device action");
    }

    device.SetStateTs(now.MicroSeconds());
    device.SetCmsTs(cmsTs.MicroSeconds());

    ApplyDeviceStateChange(db, agent, device, now, affectedDisk);

    return error;
}

auto TDiskRegistryState::UpdateCmsDeviceState(
    TDiskRegistryDatabase& db,
    const TAgentId& agentId,
    const TString& path,
    NProto::EDeviceState newState,
    TInstant now,
    bool shouldResume,
    bool dryRun) -> TUpdateCmsDeviceStateResult
{
    if (newState != NProto::DEVICE_STATE_ONLINE &&
        newState != NProto::DEVICE_STATE_WARNING)
    {
        return { .Error = MakeError(E_ARGUMENT, "Unexpected state") };
    }

    auto [agent, devices] = ResolveDevices(agentId, path);

    if (agent && devices.empty() && newState == NProto::DEVICE_STATE_ONLINE) {
        return AddNewDevices(db, *agent, path, now, shouldResume, dryRun);
    }

    if (!agent || devices.empty()) {
        return {
            .Error = MakeError(E_NOT_FOUND, TStringBuilder()
                << "Device not found (" << agentId << "," << path << ')')
        };
    }

    TUpdateCmsDeviceStateResult result;

    // not transactional actually but it's not a big problem
    ui32 processed = 0;
    for (; processed != devices.size(); ++processed) {
        auto& device = *devices[processed];

        if (newState == NProto::DEVICE_STATE_ONLINE) {
            result.Error = CmsAddDevice(
                db,
                *agent,
                device,
                now,
                shouldResume,
                dryRun,
                result.Timeout);

            if (HasError(result.Error)) {
                break;
            }

            continue;
        }

        TDuration timeout;
        TString affectedDisk;
        auto error = CmsRemoveDevice(
            db,
            *agent,
            device,
            now,
            dryRun,
            affectedDisk,
            timeout);

        if (!affectedDisk.empty()) {
            result.AffectedDisks.push_back(std::move(affectedDisk));
        }

        result.Timeout = Max(result.Timeout, timeout);

        if (HasError(error)) {
            result.Error = std::move(error);
            // ignoring E_TRY_AGAIN error, we touch each logical device to
            // start migrations
            if (result.Error.GetCode() != E_TRY_AGAIN) {
                break;
            }
        }
    }

    if (processed != devices.size()) {
        STORAGE_WARN(
            "UpdateCmsDeviceState stopped after processing %u devices"
            ", current deviceId: %s, error: %s",
            processed,
            devices[processed]->GetDeviceUUID().c_str(),
            FormatError(result.Error).c_str());
    }

    SortUnique(result.AffectedDisks);

    return result;
}

void TDiskRegistryState::ApplyDeviceStateChange(
    TDiskRegistryDatabase& db,
    const NProto::TAgentConfig& agent,
    const NProto::TDeviceConfig& device,
    TInstant now,
    TDiskId& affectedDisk)
{
    UpdateAgent(db, agent);
    DeviceList.UpdateDevices(agent, DevicePoolConfigs);

    const auto& uuid = device.GetDeviceUUID();
    auto diskId = DeviceList.FindDiskId(uuid);

    if (diskId.empty()) {
        return;
    }

    auto* disk = Disks.FindPtr(diskId);

    if (!disk) {
        ReportDiskRegistryDiskNotFound(TStringBuilder()
            << "ApplyDeviceStateChange:DiskId: " << diskId);

        return;
    }

    // check if uuid is target for migration
    if (RestartDeviceMigration(now, db, diskId, *disk, uuid)) {
        return;
    }

    if (device.GetState() == NProto::DEVICE_STATE_ERROR && disk->MasterDiskId) {
        TryToReplaceDeviceIfAllowedWithoutDiskStateUpdate(
            db,
            *disk,
            diskId,
            device.GetDeviceUUID(),
            now,
            "device failure");
    }

    if (TryUpdateDiskState(db, diskId, *disk, now)) {
        affectedDisk = diskId;
    }

    if (device.GetState() != NProto::DEVICE_STATE_WARNING) {
        CancelDeviceMigration(now, db, diskId, *disk, uuid);
        return;
    }

    if (MigrationCanBeStarted(*disk, uuid)) {
        AddMigration(*disk, diskId, uuid);
    }
}

bool TDiskRegistryState::RestartDeviceMigration(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    TDiskState& disk,
    const TDeviceId& targetId)
{
    auto it = disk.MigrationTarget2Source.find(targetId);

    if (it == disk.MigrationTarget2Source.end()) {
        return false;
    }

    TDeviceId sourceId = it->second;

    CancelDeviceMigration(now, db, diskId, disk, sourceId);

    AddMigration(disk, diskId, sourceId);

    return true;
}

void TDiskRegistryState::DeleteAllDeviceMigrations(const TDiskId& diskId)
{
    auto it = Migrations.lower_bound({diskId, TString {}});

    while (it != Migrations.end() && it->DiskId == diskId) {
        it = Migrations.erase(it);
    }
}

void TDiskRegistryState::DeleteDeviceMigration(
    const TDiskId& diskId,
    const TDeviceId& sourceId)
{
    Migrations.erase({ diskId, sourceId });
}

void TDiskRegistryState::CancelDeviceMigration(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    TDiskState& disk,
    const TDeviceId& sourceId)
{
    Migrations.erase(TDeviceMigration(diskId, sourceId));

    auto it = disk.MigrationSource2Target.find(sourceId);

    if (it == disk.MigrationSource2Target.end()) {
        return;
    }

    const TDeviceId targetId = it->second;

    disk.MigrationTarget2Source.erase(targetId);
    disk.MigrationSource2Target.erase(it);

    ResetMigrationStartTsIfNeeded(disk);

    // searching in all pools - in theory pool name might change between
    // the start/cancel/finish events
    for (auto& x: SourceDeviceMigrationsInProgress) {
        x.second.erase(sourceId);
    }

    const ui64 seqNo = AddReallocateRequest(db, diskId);

    disk.FinishedMigrations.push_back(
        {.DeviceId = targetId, .SeqNo = seqNo, .IsCanceled = true});

    NProto::TDiskHistoryItem historyItem;
    historyItem.SetTimestamp(now.MicroSeconds());
    historyItem.SetMessage(TStringBuilder() << "cancelled migration: "
        << sourceId << " -> " << targetId);
    disk.History.push_back(std::move(historyItem));

    db.UpdateDisk(BuildDiskConfig(diskId, disk));

    UpdatePlacementGroup(db, diskId, disk, "CancelDeviceMigration");
}

NProto::TError TDiskRegistryState::AbortMigrationAndReplaceDevice(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    TDiskState& disk,
    const TDeviceId& sourceId)
{
    Y_DEBUG_ABORT_UNLESS(!disk.MasterDiskId.empty());

    Migrations.erase(TDeviceMigration(diskId, sourceId));
    auto migrationsIt = disk.MigrationSource2Target.find(sourceId);

    // If a device migration is planned, basic device replacement is done.
    // Otherwise, if the migration is already underway, the target device will
    // become "fresh" to the volume and the source will be released and marked
    // as dirty.
    if (migrationsIt == disk.MigrationSource2Target.end()) {
        return ReplaceDeviceWithoutDiskStateUpdate(
            db,
            disk,
            diskId,
            sourceId,
            TString(),   //  deviceReplacementId
            now,
            MakeMirroredDiskDeviceReplacementMessage(
                disk.MasterDiskId,
                "AbortMigrationAndReplaceDevice"),
            false   // manual
        );
    }

    const TDeviceId targetId = migrationsIt->second;
    disk.MigrationTarget2Source.erase(targetId);
    disk.MigrationSource2Target.erase(migrationsIt);

    auto sourceDeviceIt = Find(disk.Devices, sourceId);
    Y_DEBUG_ABORT_UNLESS(sourceDeviceIt != disk.Devices.end());
    if (sourceDeviceIt == disk.Devices.end()) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Disk " << diskId << " does not contain device " << sourceId
                << ". Failed to abort migration and replace device.");
    }
    *sourceDeviceIt = targetId;

    auto* masterDiskState = Disks.FindPtr(disk.MasterDiskId);
    Y_DEBUG_ABORT_UNLESS(masterDiskState);
    if (masterDiskState) {
        Y_DEBUG_ABORT_UNLESS(
            !FindPtr(masterDiskState->DeviceReplacementIds, targetId));
        masterDiskState->DeviceReplacementIds.push_back(targetId);
    }

    ResetMigrationStartTsIfNeeded(disk);

    // Searching in all pools - in theory pool name might change between
    // the start/cancel/finish events
    for (auto& x: SourceDeviceMigrationsInProgress) {
        x.second.erase(sourceId);
    }

    const bool replaced =
        ReplicaTable.ReplaceDevice(disk.MasterDiskId, sourceId, targetId);
    Y_DEBUG_ABORT_UNLESS(replaced);

    DeviceList.ReleaseDevice(sourceId);
    db.UpdateDirtyDevice(sourceId, diskId);

    auto error = AddDevicesToPendingCleanup(disk.MasterDiskId, {sourceId});
    if (HasError(error)) {
        ReportDiskRegistryInsertToPendingCleanupFailed(
            TStringBuilder()
            << "An error occurred while aborting migration: "
            << FormatError(error));
    }

    NProto::TDiskHistoryItem historyItem;
    historyItem.SetTimestamp(now.MicroSeconds());
    historyItem.SetMessage(
        TStringBuilder() << "Migration was aborted. Device " << sourceId
                         << " was replaced by " << targetId);
    masterDiskState->History.push_back(std::move(historyItem));

    db.UpdateDisk(BuildDiskConfig(diskId, disk));
    db.UpdateDisk(BuildDiskConfig(disk.MasterDiskId, *masterDiskState));
    UpdatePlacementGroup(db, diskId, disk, "AbortMigrationAndReplaceDevice");
    return {};
}

void TDiskRegistryState::ResetMigrationStartTsIfNeeded(TDiskState& disk)
{
    if (disk.MigrationSource2Target.empty()) {
        disk.MigrationStartTs = {};
    }
}

NProto::TError TDiskRegistryState::FinishDeviceMigration(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    const TDeviceId& sourceId,
    const TDeviceId& targetId,
    TInstant timestamp,
    bool* diskStateUpdated)
{
    if (!Disks.contains(diskId)) {
        return MakeError(E_NOT_FOUND, TStringBuilder() <<
            "disk " << diskId.Quote() << " not found");
    }

    TDiskState& disk = Disks[diskId];

    auto devIt = Find(disk.Devices, sourceId);

    if (devIt == disk.Devices.end()) {
        auto message = ReportDiskRegistryWrongMigratedDeviceOwnership(
            TStringBuilder() << "FinishDeviceMigration: device "
                             << sourceId.Quote() << " not found");
        return MakeError(E_INVALID_STATE, std::move(message));
    }

    if (auto it = disk.MigrationTarget2Source.find(targetId);
        it == disk.MigrationTarget2Source.end() || it->second != sourceId)
    {
        return MakeError(E_ARGUMENT, "invalid migration");
    } else {
        disk.MigrationTarget2Source.erase(it);
        disk.MigrationSource2Target.erase(sourceId);

        ResetMigrationStartTsIfNeeded(disk);

        // searching in all pools - in theory pool name might change between
        // the start/cancel/finish events
        for (auto& x: SourceDeviceMigrationsInProgress) {
            x.second.erase(sourceId);
        }

        NProto::TDiskHistoryItem historyItem;
        historyItem.SetTimestamp(timestamp.MicroSeconds());
        historyItem.SetMessage(TStringBuilder() << "finished migration: "
            << sourceId << " -> " << targetId);
        disk.History.push_back(std::move(historyItem));
    }

    const ui64 seqNo = AddReallocateRequest(db, diskId);
    *devIt = targetId;

    disk.FinishedMigrations.push_back(
        {.DeviceId = sourceId, .SeqNo = seqNo, .IsCanceled = false});

    if (disk.MasterDiskId) {
        const bool replaced =
            ReplicaTable.ReplaceDevice(disk.MasterDiskId, sourceId, targetId);
        // targetId is actually fully initialized after migration
        ReplicaTable.MarkReplacementDevice(disk.MasterDiskId, targetId, false);

        Y_DEBUG_ABORT_UNLESS(replaced);
    }

    *diskStateUpdated = TryUpdateDiskState(db, diskId, disk, timestamp);

    db.UpdateDisk(BuildDiskConfig(diskId, disk));

    UpdatePlacementGroup(db, diskId, disk, "FinishDeviceMigration");

    return {};
}

NProto::TError TDiskRegistryState::AddLaggingDevices(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    TVector<NProto::TLaggingDevice> laggingDevices)
{
    if (!Disks.contains(diskId)) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "No such disk " << diskId);
    }

    TDiskState& diskState = Disks[diskId];
    if (!IsReliableDiskRegistryMediaKind(diskState.MediaKind) ||
        diskState.ReplicaCount == 0)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Disk " << diskId << " is not master replica. Kind = "
                << static_cast<int>(diskState.MediaKind));
    }

    // Erase devices that are already known to be lagging.
    EraseIf(
        laggingDevices,
        [&](const auto& device)
        {
            const auto& uuid = device.GetDeviceUUID();
            return AnyOf(
                diskState.LaggingDevices,
                [&uuid](const TLaggingDevice& laggingDevice)
                { return laggingDevice.Device.GetDeviceUUID() == uuid; });
        });

    if (laggingDevices.empty()) {
        return MakeError(S_FALSE, "Lagging devices list is empty");
    }

    TVector<TDeviceId> addedLaggingDevices;
    addedLaggingDevices.reserve(laggingDevices.size());
    THashSet<std::pair<TString, TDiskState*>> replicasToUpdate;
    const ui64 reallocateSeqNo = AddReallocateRequest(db, diskId);
    for (const auto& laggingDevice: laggingDevices) {
        const auto& laggingDeviceUUID = laggingDevice.GetDeviceUUID();
        diskState.LaggingDevices.emplace_back(laggingDevice, reallocateSeqNo);

        auto replicaId = FindDisk(laggingDeviceUUID);
        if (replicaId.empty()) {
            // The device is already released.
            continue;
        }
        TDiskState* replicaState = Disks.FindPtr(replicaId);
        if (!replicaState) {
            ReportDiskRegistryDiskNotFound(
                TStringBuilder()
                << "AddLaggingDevices:ReplicaId: " << replicaId);
            continue;
        }
        if (replicaState->MasterDiskId != diskId) {
            // The device is a part of another disk.
            ReportDiskRegistryDeviceDoesNotBelongToDisk(
                TStringBuilder()
                << "AddLaggingDevices:ReplicaId: " << replicaId
                << ", DeviceId: " << laggingDeviceUUID
                << ", MasterDiskId: " << replicaState->MasterDiskId);
            continue;
        }

        // The device can be either a migration source, a target of migration or
        // a regular replica device.
        const bool isMigrationSource =
            replicaState->MigrationSource2Target.contains(laggingDeviceUUID) ||
            Migrations.contains(TDeviceMigration{replicaId, laggingDeviceUUID});
        const bool isMigrationTarget =
            replicaState->MigrationTarget2Source.contains(laggingDeviceUUID);
        if (isMigrationSource) {
            // Just snap a target device into the disk and discard the lagging
            // one.
            auto error = AbortMigrationAndReplaceDevice(
                now,
                db,
                replicaId,
                *replicaState,
                laggingDeviceUUID);
            if (HasError(error)) {
                ReportDiskRegistryCouldNotAddLaggingDevice(FormatError(error));
            }
        } else if (isMigrationTarget) {
            // Restart the migration with a new device.
            const bool success = RestartDeviceMigration(
                now,
                db,
                replicaId,
                *replicaState,
                laggingDeviceUUID);
            Y_DEBUG_ABORT_UNLESS(success);
        } else {
            // Add the lagging device to the fresh devices list.
            auto it = Find(diskState.DeviceReplacementIds, laggingDeviceUUID);
            if (it == diskState.DeviceReplacementIds.end()) {
                diskState.DeviceReplacementIds.push_back(laggingDeviceUUID);
            }
            ReplicaTable.MarkReplacementDevice(diskId, laggingDeviceUUID, true);
            addedLaggingDevices.push_back(laggingDeviceUUID);
        }

        replicasToUpdate.emplace(replicaId, replicaState);
    }

    if (!addedLaggingDevices.empty()) {
        NProto::TDiskHistoryItem historyItem;
        historyItem.SetTimestamp(now.MicroSeconds());
        historyItem.SetMessage(
            TStringBuilder()
            << "Devices [" << JoinSeq(", ", addedLaggingDevices)
            << "] marked as replacements by volume request.");
        diskState.History.push_back(std::move(historyItem));
    }

    db.UpdateDisk(BuildDiskConfig(diskId, diskState));
    for (const auto& [replicaId, replicaState]: replicasToUpdate) {
        if (!TryUpdateDiskState(db, replicaId, *replicaState, now)) {
            db.UpdateDisk(BuildDiskConfig(replicaId, *replicaState));
        }
    }

    return {};
}

auto TDiskRegistryState::FindReplicaByMigration(
    const TDiskId& masterDiskId,
    const TDeviceId& sourceDeviceId,
    const TDeviceId& targetDeviceId) const -> TDiskId
{
    auto* masterDisk = Disks.FindPtr(masterDiskId);
    if (!masterDisk) {
        ReportDiskRegistryDiskNotFound(TStringBuilder()
            << "FindReplicaByMigration:MasterDiskId: " << masterDiskId);

        return {};
    }

    for (ui32 i = 0; i < masterDisk->ReplicaCount + 1; ++i) {
        auto replicaId = GetReplicaDiskId(masterDiskId, i);
        auto* replica = Disks.FindPtr(replicaId);

        if (!replica) {
            ReportDiskRegistryDiskNotFound(TStringBuilder()
                << "FindReplicaByMigration:ReplicaId: " << replicaId);

            return {};
        }

        auto* t = replica->MigrationSource2Target.FindPtr(sourceDeviceId);
        if (t && *t == targetDeviceId) {
            return replicaId;
        }
    }

    return {};
}

TVector<TDeviceMigration> TDiskRegistryState::BuildMigrationList() const
{
    const ui32 minBudget =
        StorageConfig->GetMaxNonReplicatedDeviceMigrationsInProgress();
    const ui32 budgetPercentage =
        StorageConfig->GetMaxNonReplicatedDeviceMigrationPercentageInProgress();

    TVector<TDeviceMigration> result;
    THashMap<TString, ui32> poolName2Budget;

    for (const auto& m: Migrations) {
        auto [agentPtr, devicePtr] = FindDeviceLocation(m.SourceDeviceId);
        if (!agentPtr || !devicePtr) {
            continue;
        }

        if (agentPtr->GetState() > NProto::AGENT_STATE_WARNING) {
            // skip migration from unavailable agents
            continue;
        }

        if (devicePtr->GetState() > NProto::DEVICE_STATE_WARNING) {
            // skip migration from broken devices
            continue;
        }

        const ui32* deviceCountInPoolPtr =
            DeviceList.GetPoolName2DeviceCount().FindPtr(devicePtr->GetPoolName());
        const ui32 deviceCountInPool =
            deviceCountInPoolPtr ? *deviceCountInPoolPtr : 0;

        const ui32 budget = Max(
            minBudget,
            deviceCountInPool * budgetPercentage / 100);

        const auto* inProgressPtr =
            SourceDeviceMigrationsInProgress.FindPtr(devicePtr->GetPoolName());
        const ui32 inProgress = inProgressPtr ? inProgressPtr->size() : 0;
        // limits might have changed after the start of some of our current
        // migrations - that's why inProgress can sometimes be greater than budget
        if (inProgress >= budget) {
            continue;
        }

        // lazy initialization
        if (!poolName2Budget.contains(devicePtr->GetPoolName())) {
            poolName2Budget[devicePtr->GetPoolName()] = budget - inProgress;
        }

        // the size of the migration list for each pool should not be greater
        // than the limit minus inProgress
        auto& remainingBudget = poolName2Budget[devicePtr->GetPoolName()];
        if (remainingBudget == 0) {
            continue;
        }
        --remainingBudget;

        result.push_back(m);
    }

    return result;
}

NProto::TError TDiskRegistryState::CheckDeviceStateTransition(
    const NProto::TDeviceConfig& device,
    NProto::EDeviceState newState,
    TInstant timestamp)
{
    if (device.GetState() == newState) {
        return MakeError(S_ALREADY);
    }

    if (TInstant::MicroSeconds(device.GetStateTs()) > timestamp) {
        return MakeError(E_INVALID_STATE, "out of order");
    }

    return {};
}

TString TDiskRegistryState::GetAgentId(TNodeId nodeId) const
{
    const auto* agent = AgentList.FindAgent(nodeId);

    return agent
        ? agent->GetAgentId()
        : TString();
}

NProto::TDiskRegistryStateBackup TDiskRegistryState::BackupState() const
{
    static_assert(
        TTableCount<TDiskRegistrySchema::TTables>::value == 15,
        "not all fields are processed"
    );

    NProto::TDiskRegistryStateBackup backup;

    auto transform = [] (const auto& src, auto* dst, auto func) {
        dst->Reserve(src.size());
        std::transform(
            src.cbegin(),
            src.cend(),
            RepeatedFieldBackInserter(dst),
            func
        );
    };

    auto copy = [] (const auto& src, auto* dst) {
        dst->Reserve(src.size());
        std::copy(
            src.cbegin(),
            src.cend(),
            RepeatedFieldBackInserter(dst)
        );
    };

    auto copyMap = [] (const auto& src, auto* dst) {
        for (const auto& [k, v]: src) {
            (*dst)[k] = v;
        }
    };

    transform(Disks, backup.MutableDisks(), [this] (const auto& kv) {
        return BuildDiskConfig(kv.first, kv.second);
    });

    transform(PlacementGroups, backup.MutablePlacementGroups(), [] (const auto& kv) {
        return kv.second.Config;
    });

    transform(GetDirtyDevices(), backup.MutableDirtyDevices(), [this] (auto& x) {
        NProto::TDiskRegistryStateBackup::TDirtyDevice dd;
        dd.SetId(x.GetDeviceUUID());
        dd.SetDiskId(PendingCleanup.FindDiskId(x.GetDeviceUUID()));

        return dd;
    });

    transform(BrokenDisks, backup.MutableBrokenDisks(), [] (auto& x) {
        NProto::TDiskRegistryStateBackup::TBrokenDiskInfo info;
        info.SetDiskId(x.DiskId);
        info.SetTsToDestroy(x.TsToDestroy.MicroSeconds());

        return info;
    });

    transform(GetDisksToReallocate(), backup.MutableDisksToNotify(), [] (auto& kv) {
        return kv.first;
    });

    transform(GetDiskStateUpdates(), backup.MutableDiskStateChanges(), [] (auto& x) {
        NProto::TDiskRegistryStateBackup::TDiskStateUpdate update;

        update.MutableState()->CopyFrom(x.State);
        update.SetSeqNo(x.SeqNo);

        return update;
    });

    copy(AgentList.GetAgents(), backup.MutableAgents());
    copy(DisksToCleanup, backup.MutableDisksToCleanup());

    backup.MutableUserNotifications()->Reserve(GetUserNotifications().Count);
    for (auto&& [id, data]: GetUserNotifications().Storage) {
        bool doLegacyBackup = false;
        for (const auto& notif: data.Notifications) {
            if (notif.GetSeqNo() || !notif.GetHasLegacyCopy()) {
                *backup.AddUserNotifications() = notif;
            }

            if (notif.GetHasLegacyCopy()) {
                doLegacyBackup = true;
            }
        }

        if (doLegacyBackup) {
            backup.AddErrorNotifications(id);
        }
    }

    {
        auto outdatedVolumes = GetOutdatedVolumeConfigs();

        backup.MutableOutdatedVolumeConfigs()->Assign(
            std::make_move_iterator(outdatedVolumes.begin()),
            std::make_move_iterator(outdatedVolumes.end()));
    }

    copy(GetSuspendedDevices(), backup.MutableSuspendedDevices());

    transform(
        GetAutomaticallyReplacedDevices(),
        backup.MutableAutomaticallyReplacedDevices(),
        [] (auto& x) {
            NProto::TDiskRegistryStateBackup::TAutomaticallyReplacedDeviceInfo info;
            info.SetDeviceId(x.DeviceId);
            info.SetReplacementTs(x.ReplacementTs.MicroSeconds());

            return info;
        });

    copyMap(
        AgentList.GetDiskRegistryAgentListParams(),
        backup.MutableDiskRegistryAgentListParams());

    auto config = GetConfig();
    config.SetLastDiskStateSeqNo(NotificationSystem.GetDiskStateSeqNo());

    backup.MutableConfig()->Swap(&config);

    return backup;
}

bool TDiskRegistryState::IsReadyForCleanup(const TDiskId& diskId) const
{
    return DisksToCleanup.contains(diskId);
}

TVector<TString> TDiskRegistryState::GetDisksToCleanup() const
{
    return {DisksToCleanup.begin(), DisksToCleanup.end()};
}

std::pair<TVolumeConfig, ui64> TDiskRegistryState::GetVolumeConfigUpdate(
    const TDiskId& diskId) const
{
    auto seqNo = NotificationSystem.GetOutdatedVolumeSeqNo(diskId);
    if (!seqNo) {
        return {};
    }

    auto* disk = Disks.FindPtr(diskId);
    if (!disk) {
        return {};
    }

    TVolumeConfig config;

    config.SetDiskId(diskId);
    config.SetPlacementGroupId(disk->PlacementGroupId);
    config.SetPlacementPartitionIndex(disk->PlacementPartitionIndex);

    return {std::move(config), *seqNo};
}

auto TDiskRegistryState::GetOutdatedVolumeConfigs() const -> TVector<TDiskId>
{
    return NotificationSystem.GetOutdatedVolumeConfigs();
}

void TDiskRegistryState::DeleteOutdatedVolumeConfig(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId)
{
    NotificationSystem.DeleteOutdatedVolumeConfig(db, diskId);
}

NProto::TError TDiskRegistryState::SetUserId(
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    const TString& userId)
{
    auto it = Disks.find(diskId);
    if (it == Disks.end()) {
        return MakeError(E_NOT_FOUND, TStringBuilder() <<
            "disk " << diskId.Quote() << " not found");
    }

    auto& disk = it->second;
    disk.UserId = userId;

    db.UpdateDisk(BuildDiskConfig(diskId, disk));

    return {};
}

bool TDiskRegistryState::DoesNewDiskBlockSizeBreakDevice(
    const TDiskId& diskId,
    const TDeviceId& deviceId,
    ui64 newLogicalBlockSize)
{
    const auto& disk = Disks[diskId];
    const auto device = GetDevice(deviceId);

    const auto deviceSize = device.GetBlockSize() *
        GetDeviceBlockCountWithOverrides(diskId, device);

    const ui64 oldLogicalSize = deviceSize / disk.LogicalBlockSize
        * disk.LogicalBlockSize;
    const ui64 newLogicalSize = deviceSize / newLogicalBlockSize
        * newLogicalBlockSize;

    return oldLogicalSize != newLogicalSize;
}

NProto::TError TDiskRegistryState::ValidateUpdateDiskBlockSizeParams(
    const TDiskId& diskId,
    ui32 blockSize,
    bool force)
{
    if (diskId.empty()) {
        return MakeError(E_ARGUMENT, TStringBuilder() << "diskId is required");
    }

    if (blockSize == 0) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "blockSize is required");
    }

    if (!Disks.contains(diskId)) {
        return MakeError(E_NOT_FOUND, TStringBuilder() <<
            "disk " << diskId.Quote() << " not found");
    }

    const auto& disk = Disks[diskId];

    if (blockSize == disk.LogicalBlockSize) {
        return MakeError(S_FALSE, TStringBuilder()
            << "disk " << diskId.Quote() << " already has block size equal to "
            << disk.LogicalBlockSize);
    }

    if (disk.Devices.empty()) {
        return MakeError(E_INVALID_STATE, "disk without devices");
    }

    if (force) {
        return {};
    }

    const auto forceNotice = "(use force flag to bypass this restriction)";

    ui64 devicesSize = 0;
    for (const auto& id: disk.Devices) {
        const auto device = GetDevice(id);
        if (device.GetDeviceUUID().empty()) {
            return MakeError(E_INVALID_STATE,
                TStringBuilder() << "one of the disk devices cannot be "
                "found " << forceNotice);
        }

        if (device.GetBlockSize() > blockSize) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "volume's block size (" << blockSize
                << ") is less than device's block size ("
                << device.GetBlockSize() << ")");
        }

        devicesSize += device.GetBlocksCount() * device.GetBlockSize();
    }

    const auto error = ValidateBlockSize(blockSize, disk.MediaKind);

    if (HasError(error)) {
        return MakeError(error.GetCode(), TStringBuilder()
            << error.GetMessage() << " " << forceNotice);
    }

    const TString poolName = GetDevice(disk.Devices[0]).GetPoolName();
    const ui64 volumeSize = devicesSize / disk.LogicalBlockSize * blockSize;
    const ui64 allocationUnit = GetAllocationUnit(poolName);

    if (!allocationUnit) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "zero allocation unit for pool: "
            << poolName);
    }

    if (volumeSize % allocationUnit != 0) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "volume size should be divisible by " << allocationUnit);
    }

    for (const auto& deviceId: disk.Devices) {
        if (DoesNewDiskBlockSizeBreakDevice(diskId, deviceId, blockSize)) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "Device " << deviceId.Quote()
                << " logical size " << disk.LogicalBlockSize
                << " is not equal to new logical size " << blockSize
                << ", that breaks disk " << forceNotice);
        }
    }

    return {};
}

NProto::TError TDiskRegistryState::UpdateDiskBlockSize(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    ui32 blockSize,
    bool force)
{
    const auto validateError = ValidateUpdateDiskBlockSizeParams(diskId,
        blockSize, force);
    if (HasError(validateError) || validateError.GetCode() == S_FALSE) {
        return validateError;
    }

    auto& disk = Disks[diskId];

    for (const auto& deviceId: disk.Devices) {
        if (DoesNewDiskBlockSizeBreakDevice(diskId, deviceId, blockSize)) {
            auto device = GetDevice(deviceId);

            const auto newBlocksCount = device.GetBlockSize()
                * GetDeviceBlockCountWithOverrides(diskId, device)
                / disk.LogicalBlockSize * disk.LogicalBlockSize
                / device.GetBlockSize();
            AdjustDeviceBlockCount(now, db, device, newBlocksCount);
        }
    }

    disk.LogicalBlockSize = blockSize;
    UpdateAndReallocateDisk(db, diskId, disk);

    return {};
}

auto TDiskRegistryState::QueryAvailableStorage(
    const TString& agentId,
    const TString& poolName,
    NProto::EDevicePoolKind poolKind) const
        -> TResultOrError<TVector<TAgentStorageInfo>>
{
    if (!poolName.empty()) {
        auto* poolConfig = DevicePoolConfigs.FindPtr(poolName);
        if (!poolConfig) {
            return TVector<TAgentStorageInfo> {};
        }

        if (poolConfig->GetKind() != poolKind) {
            SelfCounters.QueryAvailableStorageErrors.Increment(1);

            return MakeError(
                E_ARGUMENT,
                Sprintf(
                    "Unexpected device pool kind (actual: %d, expected: %d) "
                    "for the device pool %s.",
                    poolKind,
                    poolConfig->GetKind(),
                    poolName.Quote().c_str()
                )
            );
        }
    }

    auto* agent = AgentList.FindAgent(agentId);
    if (!agent) {
        SelfCounters.QueryAvailableStorageErrors.Increment(1);

        return MakeError(E_NOT_FOUND, TStringBuilder{} <<
            "agent " << agentId.Quote() << " not found");
    }

    if (!DeviceList.DevicesAllocationAllowed(poolKind, agent->GetState())) {
        return TVector<TAgentStorageInfo>{};
    }

    THashMap<ui64, TAgentStorageInfo> chunks;

    for (const auto& device: agent->GetDevices()) {
        if (device.GetPoolKind() != poolKind) {
            continue;
        }

        if (poolName && device.GetPoolName() != poolName) {
            continue;
        }

        if (device.GetState() != NProto::DEVICE_STATE_ONLINE) {
            continue;
        }

        if (DeviceList.IsSuspendedDevice(device.GetDeviceUUID())) {
            continue;
        }

        const ui64 au = GetAllocationUnit(device.GetPoolName());
        auto& auChunks = chunks[au];
        auChunks.ChunkSize = au;

        if (DeviceList.IsDirtyDevice(device.GetDeviceUUID())) {
            auChunks.DirtyChunks++;
        } else if (!DeviceList.IsAllocatedDevice(device.GetDeviceUUID())) {
            auChunks.FreeChunks++;
        }
        auChunks.ChunkCount++;
    }

    TVector<TAgentStorageInfo> infos;
    infos.reserve(chunks.size());

    for (auto [size, info]: chunks) {
        infos.push_back(info);
    }

    return infos;
}

NProto::TError TDiskRegistryState::AllocateDiskReplicas(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& masterDiskId,
    ui32 replicaCount)
{
    if (replicaCount == 0) {
        return MakeError(E_ARGUMENT, "replica count can't be zero");
    }

    auto* masterDisk = Disks.FindPtr(masterDiskId);
    if (!masterDisk) {
        return MakeError(E_NOT_FOUND, TStringBuilder()
            << "disk " << masterDiskId.Quote() << " is not found");
    }

    if (masterDisk->ReplicaCount == 0) {
        return MakeError(E_ARGUMENT, "unable to allocate disk replica for not "
            "a master disk");
    }

    const auto newReplicaCount = masterDisk->ReplicaCount + replicaCount;

    const auto maxReplicaCount =
        StorageConfig->GetMaxDisksInPlacementGroup() - 1;
    if (newReplicaCount > maxReplicaCount) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "mirrored disks can have maximum of " << maxReplicaCount
            << " replicas, and you are asking for " << newReplicaCount);
    }

    const auto firstReplicaDiskId = GetReplicaDiskId(masterDiskId, 0);
    const auto& firstReplica = Disks[firstReplicaDiskId];

    ui64 blocksCount = 0;
    for (const auto& deviceId: firstReplica.Devices) {
        const auto device = GetDevice(deviceId);
        blocksCount += device.GetBlockSize() * device.GetBlocksCount()
            / firstReplica.LogicalBlockSize;
    }

    auto onError = [&] (ui32 count) {
        for (ui32 i = 0; i < count; ++i) {
            const size_t idx = masterDisk->ReplicaCount + i + 1;

            auto error = AddDevicesToPendingCleanup(
                masterDiskId,
                DeallocateSimpleDisk(
                    db,
                    GetReplicaDiskId(masterDiskId, idx),
                    "AllocateDiskReplicas:Cleanup"));
            if (HasError(error)) {
                ReportDiskRegistryInsertToPendingCleanupFailed(
                    TStringBuilder() << "An error occurred while allocated "
                                        "disk replica cleanup: "
                                     << FormatError(error));
            }
        }
    };

    const TAllocateDiskParams allocateParams {
        .DiskId = masterDiskId,
        .CloudId = firstReplica.CloudId,
        .FolderId = firstReplica.FolderId,
        .BlockSize = firstReplica.LogicalBlockSize,
        .BlocksCount = blocksCount,
    };

    for (size_t i = 0; i < replicaCount; ++i) {
        const size_t idx = masterDisk->ReplicaCount + i + 1;

        TAllocateDiskResult r;
        const auto error = AllocateDiskReplica(now, db, allocateParams, idx, &r);
        if (HasError(error)) {
            onError(i);

            return error;
        }

        // TODO (NBS-3418):
        // * add device ids for new replicas to device replacements
        // * update ReplicaTable
    }

    masterDisk->ReplicaCount = newReplicaCount;
    UpdateAndReallocateDisk(db, masterDiskId, *masterDisk);

    return {};
}

NProto::TError TDiskRegistryState::AddDevicesToPendingCleanup(
    const TString& diskId,
    TVector<TDeviceId> uuids)
{
    TVector<TDeviceId> devicesAllowedToBeCleaned;
    devicesAllowedToBeCleaned.reserve(uuids.size());
    for (auto& uuid: uuids) {
        if (CanSecureErase(uuid)) {
            devicesAllowedToBeCleaned.push_back(std::move(uuid));
        }
    }
    if (devicesAllowedToBeCleaned.empty()) {
        // Devices are not going to be secure erased if they aren't allowed to.
        // Don't create a PendingCleanup entry.
        return MakeError(
            S_ALREADY,
            TStringBuilder()
                << "Disk " << diskId << " devices [" << JoinSeq(", ", uuids)
                << "] are not going to be secure erased.");
    }

    STORAGE_DEBUG(
        "Adding devices [%s] to pending cleanup",
        JoinSeq(", ", devicesAllowedToBeCleaned).c_str());
    return PendingCleanup.Insert(diskId, std::move(devicesAllowedToBeCleaned));
}

NProto::TError TDiskRegistryState::DeallocateDiskReplicas(
    TDiskRegistryDatabase& db,
    const TDiskId& masterDiskId,
    ui32 replicaCount)
{
    if (replicaCount == 0) {
        return MakeError(E_ARGUMENT, "replica count can't be zero");
    }

    auto* masterDisk = Disks.FindPtr(masterDiskId);
    if (!masterDisk) {
        return MakeError(E_NOT_FOUND, TStringBuilder()
            << "disk " << masterDiskId.Quote() << " is not found");
    }

    if (masterDisk->ReplicaCount == 0) {
        return MakeError(E_ARGUMENT, "unable to deallocate disk replica for "
            "not a master disk");
    }

    if (replicaCount > masterDisk->ReplicaCount) {
        return MakeError(E_ARGUMENT, TStringBuilder() << "deallocating "
            << replicaCount << "replicas is impossible, because disk "
            "only has " << masterDisk->ReplicaCount << " replicas");
    }
    if (replicaCount == masterDisk->ReplicaCount) {
        return MakeError(E_ARGUMENT, TStringBuilder() << "deallocating "
            << replicaCount << "replicas will make disk non mirrored, "
            "which is not supported yet");
    }

    const auto newReplicaCount = masterDisk->ReplicaCount - replicaCount;

    for (size_t i = masterDisk->ReplicaCount; i >= newReplicaCount + 1; --i) {
        const auto replicaDiskId = GetReplicaDiskId(masterDiskId, i);
        auto error = AddDevicesToPendingCleanup(
            masterDiskId,
            DeallocateSimpleDisk(db, replicaDiskId, "DeallocateDiskReplicas"));
        if (HasError(error)) {
            ReportDiskRegistryInsertToPendingCleanupFailed(
                TStringBuilder()
                << "An error occurred while deallocating "
                   "disk replica: "
                << replicaDiskId << ". " << FormatError(error));
        }

        // TODO (NBS-3418): update ReplicaTable
    }

    masterDisk->ReplicaCount = newReplicaCount;
    UpdateAndReallocateDisk(db, masterDiskId, *masterDisk);

    return {};
}

NProto::TError TDiskRegistryState::UpdateDiskReplicaCount(
    TDiskRegistryDatabase& db,
    const TDiskId& masterDiskId,
    ui32 replicaCount)
{
    const auto validateError = ValidateUpdateDiskReplicaCountParams(
        masterDiskId, replicaCount);
    if (HasError(validateError) || validateError.GetCode() == S_FALSE) {
        return validateError;
    }

    const auto& masterDisk = Disks[masterDiskId];

    if (replicaCount > masterDisk.ReplicaCount) {
        return AllocateDiskReplicas(Now(), db, masterDiskId,
            replicaCount - masterDisk.ReplicaCount);
    } else {
        return DeallocateDiskReplicas(db, masterDiskId,
            masterDisk.ReplicaCount - replicaCount);
    }
}

NProto::TError TDiskRegistryState::ValidateUpdateDiskReplicaCountParams(
    const TDiskId& masterDiskId,
    ui32 replicaCount)
{
    if (masterDiskId.empty()) {
        return MakeError(E_ARGUMENT, "disk id is required");
    }
    if (replicaCount == 0) {
        return MakeError(E_ARGUMENT,
            "unable to turn mirrored disk to a simple one");
    }

    const auto& masterDisk = Disks.FindPtr(masterDiskId);
    if (!masterDisk) {
        return MakeError(E_NOT_FOUND, TStringBuilder()
            << "disk " << masterDiskId.Quote() << " is not found");
    }

    const auto onlyMasterNotice = "The method only works with master disks";
    if (!masterDisk->MasterDiskId.empty()) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "disk " << masterDiskId.Quote() << " is not a master, it's "
            "a slave of the master disk " << masterDisk->MasterDiskId.Quote() << ". "
            << onlyMasterNotice);
    }
    if (masterDisk->ReplicaCount == 0) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "disk " << masterDiskId.Quote() << " is not a master. "
            << onlyMasterNotice);
    }

    if (replicaCount == masterDisk->ReplicaCount) {
        return MakeError(S_FALSE, TStringBuilder()
            << "disk " << masterDiskId.Quote() << " already has replicaCount "
            "equal to " << masterDisk->ReplicaCount << ", no changes will be made");
    }

    return {};
}

NProto::TError TDiskRegistryState::MarkReplacementDevice(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    const TDeviceId& deviceId,
    bool isReplacement)
{
    auto* disk = Disks.FindPtr(diskId);

    if (!disk) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder() << "Disk " << diskId << " not found");
    }
    Y_DEBUG_ABORT_UNLESS(disk->MasterDiskId.empty());

    auto it = Find(disk->DeviceReplacementIds, deviceId);

    NProto::TDiskHistoryItem historyItem;
    historyItem.SetTimestamp(now.MicroSeconds());

    if (isReplacement) {
        if (it != disk->DeviceReplacementIds.end()) {
            return MakeError(
                S_ALREADY,
                TStringBuilder() << "Device " << deviceId
                    << " already in replacement list for disk " << diskId);
        }

        disk->DeviceReplacementIds.push_back(deviceId);

        historyItem.SetMessage(TStringBuilder() << "device " << deviceId
            << " marked as a replacement device");
    } else {
        if (it == disk->DeviceReplacementIds.end()) {
            return MakeError(
                S_ALREADY,
                TStringBuilder() << "Device " << deviceId
                    << " not found in replacement list for disk " << diskId);
        }

        disk->DeviceReplacementIds.erase(it);

        historyItem.SetMessage(TStringBuilder() << "device " << deviceId
            << " no more marked as a replacement device");
    }

    disk->History.push_back(std::move(historyItem));

    UpdateAndReallocateDisk(db, diskId, *disk);
    ReplicaTable.MarkReplacementDevice(diskId, deviceId, isReplacement);

    return {};
}

void TDiskRegistryState::UpdateAgent(
    TDiskRegistryDatabase& db,
    NProto::TAgentConfig config)
{
    // don't persist unknown devices
    config.MutableUnknownDevices()->Clear();

    db.UpdateAgent(config);
}

ui64 TDiskRegistryState::GetAllocationUnit(const TString& poolName) const
{
    const auto* config = DevicePoolConfigs.FindPtr(poolName);
    if (!config) {
        return 0;
    }

    return config->GetAllocationUnit();
}

NProto::EDevicePoolKind TDiskRegistryState::GetDevicePoolKind(
    const TString& poolName) const
{
    const auto* config = DevicePoolConfigs.FindPtr(poolName);
    if (!config) {
        return NProto::DEVICE_POOL_KIND_DEFAULT;
    }

    return config->GetKind();
}

NProto::TError TDiskRegistryState::SuspendDevice(
    TDiskRegistryDatabase& db,
    const TDeviceId& id)
{
    if (id.empty()) {
        return MakeError(E_ARGUMENT, "empty device id");
    }

    DeviceList.SuspendDevice(id);

    NProto::TSuspendedDevice device;
    device.SetId(id);
    db.UpdateSuspendedDevice(std::move(device));

    return {};
}

void TDiskRegistryState::ResumeDevice(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDeviceId& id)
{
    if (DeviceList.IsSuspendedDevice(id) && DeviceList.IsDirtyDevice(id)) {
        DeviceList.ResumeAfterErase(id);

        NProto::TSuspendedDevice device;
        device.SetId(id);
        device.SetResumeAfterErase(true);
        db.UpdateSuspendedDevice(device);
    } else {
        DeviceList.ResumeDevice(id);
        db.DeleteSuspendedDevice(id);

        TryUpdateDevice(now, db, id);
    }
}

void TDiskRegistryState::ResumeDevices(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TVector<TDeviceId>& ids)
{
    for (const auto& id: ids) {
        ResumeDevice(now, db, id);
    }
}

bool TDiskRegistryState::IsSuspendedDevice(const TDeviceId& id) const
{
    return DeviceList.IsSuspendedDevice(id);
}

bool TDiskRegistryState::IsDirtyDevice(const TDeviceId& id) const
{
    return DeviceList.IsDirtyDevice(id);
}

TVector<NProto::TSuspendedDevice> TDiskRegistryState::GetSuspendedDevices() const
{
    return DeviceList.GetSuspendedDevices();
}

ui32 TDiskRegistryState::DeleteAutomaticallyReplacedDevices(
    TDiskRegistryDatabase& db,
    const TInstant until)
{
    auto it = AutomaticallyReplacedDevices.begin();
    ui32 cnt = 0;
    while (it != AutomaticallyReplacedDevices.end()
            && it->ReplacementTs <= until)
    {
        db.DeleteAutomaticallyReplacedDevice(it->DeviceId);
        AutomaticallyReplacedDeviceIds.erase(it->DeviceId);
        ++it;
        ++cnt;
    }
    AutomaticallyReplacedDevices.erase(AutomaticallyReplacedDevices.begin(), it);

    return cnt;
}

void TDiskRegistryState::DeleteAutomaticallyReplacedDevice(
    TDiskRegistryDatabase& db,
    const TDeviceId& deviceId)
{
    if (!AutomaticallyReplacedDeviceIds.erase(deviceId)) {
        return;
    }

    auto it = FindIf(AutomaticallyReplacedDevices, [&] (const auto& deviceInfo) {
        return (deviceInfo.DeviceId == deviceId);
    });
    if (it != AutomaticallyReplacedDevices.end()) {
        AutomaticallyReplacedDevices.erase(it);
    }

    db.DeleteAutomaticallyReplacedDevice(deviceId);
}

bool TDiskRegistryState::CheckIfDeviceReplacementIsAllowed(
    TInstant now,
    const TDiskId& masterDiskId,
    const TDeviceId& deviceId)
{
    while (AutomaticReplacementTimestamps) {
        if (AutomaticReplacementTimestamps[0]
                > now - TDuration::Hours(1))
        {
            break;
        }

        AutomaticReplacementTimestamps.pop_front();
    }

    const auto rateLimit =
        StorageConfig->GetMaxAutomaticDeviceReplacementsPerHour();
    if (rateLimit
            && rateLimit <= AutomaticReplacementTimestamps.size()) {
        ReportMirroredDiskDeviceReplacementRateLimitExceeded();

        STORAGE_ERROR(
            "Automatic device replacement cancelled due to high"
            " replacement rate, diskId: %s, deviceId: %s",
            masterDiskId.c_str(),
            deviceId.c_str());

        return false;
    }

    const bool canReplaceDevice = ReplicaTable.IsReplacementAllowed(
        masterDiskId,
        deviceId);

    if (!canReplaceDevice) {
        ReportMirroredDiskDeviceReplacementForbidden();

        STORAGE_ERROR(
            "Automatic device replacement not allowed by ReplicaTable"
            ", diskId: %s, deviceId: %s",
            masterDiskId.c_str(),
            deviceId.c_str());

        return false;
    }

    return true;
}

NProto::TError TDiskRegistryState::CreateDiskFromDevices(
    TInstant now,
    TDiskRegistryDatabase& db,
    bool force,
    const TDiskId& diskId,
    ui32 blockSize,
    NProto::EStorageMediaKind mediaKind,
    const TVector<NProto::TDeviceConfig>& devices,
    TAllocateDiskResult* result)
{
    Y_DEBUG_ABORT_UNLESS(result);

    if (devices.empty()) {
        return MakeError(E_ARGUMENT, "empty device list");
    }

    TVector<TString> deviceIds;
    std::optional<NProto::EDevicePoolKind> poolKind;

    for (auto& device: devices) {
        auto [config, error] = FindDevice(device);
        if (HasError(error)) {
            return error;
        }

        if (blockSize < config.GetBlockSize()) {
            return MakeError(E_ARGUMENT, TStringBuilder() <<
                "volume's block size is less than device's block size: " <<
                blockSize << " < " << config.GetBlockSize());
        }

        const auto& uuid = config.GetDeviceUUID();

        if (!poolKind) {
            poolKind = config.GetPoolKind();
        }

        if (config.GetPoolKind() != *poolKind) {
            return MakeError(E_ARGUMENT, TStringBuilder() <<
                "several device pool kinds for one disk: " <<
                static_cast<int>(config.GetPoolKind()) << " and " <<
                static_cast<int>(*poolKind));
        }

        if (!force
                && DeviceList.IsDirtyDevice(uuid)
                && !DeviceList.IsSuspendedDevice(uuid))
        {
            return MakeError(E_ARGUMENT, TStringBuilder() <<
                "device " << uuid.Quote() << " is dirty");
        }

        const auto otherDiskId = FindDisk(uuid);

        if (!otherDiskId.empty() && diskId != otherDiskId) {
            return MakeError(E_ARGUMENT, TStringBuilder() <<
                "device " << uuid.Quote() << " is allocated for disk "
                << otherDiskId.Quote());
        }

        deviceIds.push_back(uuid);
    }

    auto& disk = Disks[diskId];

    if (!disk.Devices.empty()) {
        if (disk.LogicalBlockSize == blockSize && disk.Devices == deviceIds) {
            for (const auto& uuid: disk.Devices) {
                const auto* device = DeviceList.FindDevice(uuid);
                if (!device) {
                    return MakeError(E_INVALID_STATE, TStringBuilder() <<
                        "device " << uuid.Quote() << " not found");
                }
                result->Devices.push_back(*device);
            }

            return MakeError(S_ALREADY);
        }

        return MakeError(E_ARGUMENT, TStringBuilder() <<
            "disk " << diskId.Quote() << " already exists");
    }

    disk.Devices = deviceIds;
    disk.LogicalBlockSize = blockSize;
    disk.StateTs = now;
    disk.State = CalculateDiskState(diskId, disk);
    disk.MediaKind = mediaKind;

    for (auto& uuid: deviceIds) {
        DeviceList.MarkDeviceAllocated(diskId, uuid);
        DeviceList.MarkDeviceAsClean(uuid);
        db.DeleteDirtyDevice(uuid);

        DeviceList.ResumeDevice(uuid);
        db.DeleteSuspendedDevice(uuid);

        auto [agent, device] = FindDeviceLocation(uuid);

        STORAGE_VERIFY_C(
            agent && device,
            TWellKnownEntityTypes::DISK,
            diskId,
            "device " << uuid.Quote() << " not found");

        AdjustDeviceBlockCount(now, db, *device, device->GetUnadjustedBlockCount());

        result->Devices.push_back(*device);
    }

    db.UpdateDisk(BuildDiskConfig(diskId, disk));

    return {};
}

NProto::TError TDiskRegistryState::ChangeDiskDevice(
    TInstant now,
    TDiskRegistryDatabase& db,
    const TDiskId& diskId,
    const TDeviceId& sourceDeviceId,
    const TDeviceId& targetDeviceId)
{
    TDiskState* diskState = AccessDiskState(diskId);
    if (!diskState) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            <<  "disk " << diskId.Quote() << " not found");
    }

    TDeviceId* sourceDevicePtr = FindPtr(diskState->Devices, sourceDeviceId);
    if (!sourceDevicePtr) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "disk " << diskId.Quote()
            << " didn't contain device " << sourceDeviceId.Quote());
    }

    auto* sourceDeviceConfig = DeviceList.FindDevice(sourceDeviceId);
    if (!sourceDeviceConfig) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "source device " << sourceDeviceId.Quote() << " not found");
    }

    auto* targetDeviceConfig = DeviceList.FindDevice(targetDeviceId);
    if (!targetDeviceConfig) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "target device " << targetDeviceId.Quote() << " not found");
    }

    auto targetDiskId = DeviceList.FindDiskId(targetDeviceId);
    if (targetDiskId) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "device " << targetDeviceId.Quote()
            << " is allocated for disk " << targetDiskId.Quote());
    }

    if (sourceDeviceConfig->GetPoolKind()
        != targetDeviceConfig->GetPoolKind())
    {
        return MakeError(E_ARGUMENT, "Target and source pool kind not equal");
    }

    if (sourceDeviceConfig->GetBlockSize()
        != targetDeviceConfig->GetBlockSize())
    {
        return MakeError(E_ARGUMENT, "Target and source block size not equal");
    }

    *sourceDevicePtr = targetDeviceId;
    diskState->StateTs = now;
    diskState->State = CalculateDiskState(diskId, *diskState);

    DeviceList.MarkDeviceAllocated(diskId, targetDeviceId);
    DeviceList.MarkDeviceAsClean(targetDeviceId);
    db.DeleteDirtyDevice(targetDeviceId);

    DeviceList.ResumeDevice(targetDeviceId);
    db.DeleteSuspendedDevice(targetDeviceId);

    DeviceList.ReleaseDevice(sourceDeviceId);
    db.UpdateDirtyDevice(sourceDeviceId, diskId);

    auto [targetAgent, targetDevice] = FindDeviceLocation(targetDeviceId);
    STORAGE_VERIFY_C(
        targetAgent && targetDevice,
        TWellKnownEntityTypes::DISK,
        diskId,
        TStringBuilder()
            << "target device " << targetDeviceId.Quote() << " not found");
    SetDeviceErrorState(*targetDevice, now, "replaced by private api");
    DeviceList.UpdateDevices(*targetAgent, DevicePoolConfigs);
    UpdateAgent(db, *targetAgent);

    auto [sourceAgent, sourceDevice] = FindDeviceLocation(sourceDeviceId);
    STORAGE_VERIFY_C(
        sourceAgent && sourceDevice,
        TWellKnownEntityTypes::DISK,
        diskId,
        TStringBuilder()
            << "source device " << sourceDeviceId.Quote() << " not found");
    SetDeviceErrorState(*sourceDevice, now, "replaced by private api");
    DeviceList.UpdateDevices(*sourceAgent, DevicePoolConfigs);
    UpdateAgent(db, *sourceAgent);

    diskState->State = CalculateDiskState(diskId, *diskState);
    db.UpdateDisk(BuildDiskConfig(diskId, *diskState));

    AddReallocateRequest(db, diskId);

    return {};
}

const TVector<TDiskStateUpdate>& TDiskRegistryState::GetDiskStateUpdates() const
{
    return NotificationSystem.GetDiskStateUpdates();
}

void TDiskRegistryState::SetDiskRegistryAgentListParams(
    TDiskRegistryDatabase& db,
    const TString& agentId,
    const NProto::TDiskRegistryAgentParams& params)
{
    AgentList.SetDiskRegistryAgentListParams(agentId, params);
    db.AddDiskRegistryAgentListParams(agentId, params);
}

void TDiskRegistryState::CleanupExpiredAgentListParams(
    TDiskRegistryDatabase& db,
    TInstant now)
{
    for (const auto& agentId: AgentList.CleanupExpiredAgentListParams(now)) {
        db.DeleteDiskRegistryAgentListParams(agentId);
    }
}

TVector<TString> TDiskRegistryState::GetPoolNames() const
{
    TVector<TString> poolNames;
    for (const auto& [poolName, _]: DevicePoolConfigs) {
        poolNames.push_back(poolName);
    }
    Sort(poolNames);
    return poolNames;
}

NProto::EVolumeIOMode TDiskRegistryState::GetIoMode(
    TDiskState& disk,
    TInstant now) const
{
    if (disk.State >= NProto::DISK_STATE_ERROR) {
        return NProto::VOLUME_IO_ERROR_READ_ONLY;
    }

    if (disk.State == NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE &&
        (now - disk.StateTs) >
            StorageConfig->GetNonReplicatedDiskSwitchToReadOnlyTimeout())
    {
        return NProto::VOLUME_IO_ERROR_READ_ONLY;
    }

    return NProto::VOLUME_IO_OK;
}

TVector<NProto::TAgentInfo> TDiskRegistryState::QueryAgentsInfo() const
{
    TVector<NProto::TAgentInfo> ret;
    for (const auto& agent: AgentList.GetAgents()) {
        TMap<TString, NProto::TDeviceInfo> deviceMap;
        for (const auto& device: agent.GetDevices()) {
            const bool dirty = IsDirtyDevice(device.GetDeviceUUID());
            const bool suspended = IsSuspendedDevice(device.GetDeviceUUID());
            const bool allocated = !FindDisk(device.GetDeviceUUID()).empty();
            const auto deviceBytes =
                device.GetBlockSize() * device.GetBlocksCount();

            auto [it, inserted] = deviceMap.try_emplace(device.GetDeviceName());
            auto& deviceInfo = it->second;
            if (inserted) {
                deviceInfo.SetDeviceName(device.GetDeviceName());
                deviceInfo.SetDeviceSerialNumber(device.GetSerialNumber());
            }

            deviceInfo.SetDeviceTotalSpaceInBytes(
                deviceInfo.GetDeviceTotalSpaceInBytes() + deviceBytes);

            if (dirty) {
                deviceInfo.SetDeviceDirtySpaceInBytes(
                    deviceInfo.GetDeviceDirtySpaceInBytes() + deviceBytes);
                continue;
            }

            if (suspended) {
                deviceInfo.SetDeviceSuspendedSpaceInBytes(
                    deviceInfo.GetDeviceSuspendedSpaceInBytes() + deviceBytes);
                continue;
            }

            const auto agentState = agent.GetState();
            const auto deviceState = device.GetState();
            if (agentState == NProto::AGENT_STATE_UNAVAILABLE ||
                deviceState == NProto::DEVICE_STATE_ERROR)
            {
                deviceInfo.SetDeviceBrokenSpaceInBytes(
                    deviceInfo.GetDeviceBrokenSpaceInBytes() + deviceBytes);
                continue;
            }

            if (agentState == NProto::AGENT_STATE_WARNING ||
                deviceState == NProto::DEVICE_STATE_WARNING)
            {
                deviceInfo.SetDeviceDecommissionedSpaceInBytes(
                    deviceInfo.GetDeviceDecommissionedSpaceInBytes() +
                    deviceBytes);
                continue;
            }

            if (allocated) {
                deviceInfo.SetDeviceAllocatedSpaceInBytes(
                    deviceInfo.GetDeviceAllocatedSpaceInBytes() + deviceBytes);
            } else {
                deviceInfo.SetDeviceFreeSpaceInBytes(
                    deviceInfo.GetDeviceFreeSpaceInBytes() + deviceBytes);
            }
        }

        auto& agentInfo = ret.emplace_back();
        agentInfo.SetAgentId(agent.GetAgentId());

        switch (agent.GetState()) {
            case NProto::AGENT_STATE_ONLINE:
                agentInfo.SetState(NProto::TAgentInfo::AGENT_STATE_ONLINE);
                break;
            case NProto::AGENT_STATE_WARNING:
                agentInfo.SetState(NProto::TAgentInfo::AGENT_STATE_WARNING);
                break;
            case NProto::AGENT_STATE_UNAVAILABLE:
                agentInfo.SetState(NProto::TAgentInfo::AGENT_STATE_UNAVAILABLE);
                break;
            default:
                agentInfo.SetState(NProto::TAgentInfo::AGENT_STATE_UNKNOWN);
                break;
        }
        agentInfo.SetStateMessage(agent.GetStateMessage());
        agentInfo.SetStateTs(agent.GetStateTs());

        for (auto& [name, info]: deviceMap) {
            *agentInfo.AddDevices() = std::move(info);
        }
    }

    return ret;
}

TVector<TString> TDiskRegistryState::FindOrphanDevices() const
{
    THashSet<TString> allKnownDevicesWithAgents;
    for (const auto& agent: AgentList.GetAgents()) {
        for (const auto& device: agent.GetDevices()) {
            const auto& deviceUUID = device.GetDeviceUUID();
            allKnownDevicesWithAgents.insert(deviceUUID);
        }
    }

    TVector<TString> orphanDevices;
    for (auto& deviceUUID: DeviceList.GetDirtyDevicesId()) {
        if (!allKnownDevicesWithAgents.contains(deviceUUID)) {
            orphanDevices.emplace_back(std::move(deviceUUID));
        }
    }
    for (auto& device: DeviceList.GetSuspendedDevices()) {
        if (!allKnownDevicesWithAgents.contains(device.GetId())) {
            orphanDevices.emplace_back(std::move(*device.MutableId()));
        }
    }
    for (const auto& deviceUUID: AutomaticallyReplacedDeviceIds) {
        if (!allKnownDevicesWithAgents.contains(deviceUUID)) {
            orphanDevices.emplace_back(deviceUUID);
        }
    }

    SortUnique(orphanDevices);

    return orphanDevices;
}

void TDiskRegistryState::RemoveOrphanDevices(
    TDiskRegistryDatabase& db,
    const TVector<TString>& orphanDevicesIds)
{
    ForgetDevices(db, orphanDevicesIds);
}

std::optional<ui64> TDiskRegistryState::GetDiskBlockCount(
    const TDiskId& diskId) const
{
    TDiskInfo diskInfo;
    auto error = GetDiskInfo(diskId, diskInfo);
    if (HasError(error)) {
        return std::nullopt;
    }
    return diskInfo.GetBlocksCount();
}

// static
bool TDiskRegistryState::MigrationCanBeStarted(
    const TDiskState& disk,
    const TString& deviceUUID)
{
    if (disk.MigrationSource2Target.contains(deviceUUID)) {
        // migration already started
        return false;
    }

    for (const auto& m: disk.FinishedMigrations) {
        if (m.DeviceId == deviceUUID && !m.IsCanceled) {
            // there is a finished migration for the device
            return false;
        }
    }

    return true;
}

}   // namespace NCloud::NBlockStore::NStorage
