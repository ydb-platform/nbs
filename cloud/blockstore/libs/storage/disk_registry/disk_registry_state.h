#pragma once

#include "public.h"

#include "disk_registry_state_notification.h"

#include "disk_registry_database.h"
#include "disk_registry_private.h"
#include "disk_registry_self_counters.h"

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/disk_registry/model/agent_list.h>
#include <cloud/blockstore/libs/storage/disk_registry/model/device_list.h>
#include <cloud/blockstore/libs/storage/disk_registry/model/pending_cleanup.h>
#include <cloud/blockstore/libs/storage/disk_registry/model/replica_table.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TAgentStorageInfo
{
    ui64 ChunkSize = 0;
    ui32 ChunkCount = 0;
    ui32 DirtyChunks = 0;
    ui32 FreeChunks = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TDiskInfo
{
    TVector<NProto::TDeviceConfig> Devices;
    TVector<NProto::TDeviceMigration> Migrations;
    TVector<TFinishedMigration> FinishedMigrations;
    TVector<TLaggingDevice> LaggingDevices;
    TVector<TVector<NProto::TDeviceConfig>> Replicas;
    TString MasterDiskId;
    ui32 LogicalBlockSize = 0;
    NProto::EDiskState State = NProto::DISK_STATE_ONLINE;
    TInstant StateTs;
    TString PlacementGroupId;
    ui32 PlacementPartitionIndex = 0;
    TString CloudId;
    TString FolderId;
    TString UserId;
    TVector<TString> DeviceReplacementIds;
    NProto::EStorageMediaKind MediaKind =
        NProto::STORAGE_MEDIA_SSD_NONREPLICATED;
    TString CheckpointId;
    TString SourceDiskId;
    TInstant MigrationStartTs;
    TVector<NProto::TDiskHistoryItem> History;

    ui64 GetBlocksCount() const;
    TString GetPoolName() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TRackInfo
{
    // devices on agents in unavailable/warning states are considired to
    // be unavailable/warning in this struct's counters

    struct TAgentInfo
    {
        TString AgentId;
        ui32 NodeId = 0;
        ui32 FreeDevices = 0;
        ui32 DirtyDevices = 0;
        ui32 AllocatedDevices = 0;
        ui32 WarningDevices = 0;
        ui32 UnavailableDevices = 0;
        ui32 BrokenDevices = 0;
        ui32 TotalDevices = 0;

        TAgentInfo() = default;

        TAgentInfo(TString agentId, ui32 nodeId)
            : AgentId(std::move(agentId))
            , NodeId(nodeId)
        {
        }
    };

    TString Name;
    TVector<TAgentInfo> AgentInfos;
    ui64 FreeBytes = 0;
    ui64 WarningBytes = 0;
    ui64 TotalBytes = 0;

    using TPlacementPartitionInfo = TSet<ui32>;
    TMap<TString, TPlacementPartitionInfo> PlacementGroups;

    explicit TRackInfo(TString name)
        : Name(std::move(name))
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBrokenCounter
{
public:
    explicit TBrokenCounter(NProto::EPlacementStrategy strategy)
        : Strategy(strategy)
    {}

    void Increment(const TString& diskId, ui32 partitionIndex)
    {
        BrokenDisks.insert(diskId);
        if (Strategy == NProto::EPlacementStrategy::PLACEMENT_STRATEGY_PARTITION) {
            BrokenPartitions.insert(partitionIndex);
        }
    }

    [[nodiscard]] ui32 GetBrokenPartitionCount() const
    {
        switch (Strategy) {
            case NProto::EPlacementStrategy::PLACEMENT_STRATEGY_SPREAD:
                return BrokenDisks.size();
            case NProto::EPlacementStrategy::PLACEMENT_STRATEGY_PARTITION:
                return BrokenPartitions.size();
            default:
                Y_DEBUG_ABORT_UNLESS(
                    0,
                    "Unknown partition strategy: %s",
                    EPlacementStrategy_Name(Strategy).c_str());
                return 0;
        }
    }

    [[nodiscard]] const THashSet<TString>& GetBrokenDisks() const
    {
        return BrokenDisks;
    }

private:
    NProto::EPlacementStrategy Strategy;
    THashSet<TString> BrokenDisks;
    THashSet<ui32> BrokenPartitions;
};

////////////////////////////////////////////////////////////////////////////////

struct TBrokenGroupInfo
{
    explicit TBrokenGroupInfo(NProto::EPlacementStrategy strategy)
        : Total(strategy)
        , Recently(strategy)
    {}

    TBrokenCounter Total;
    TBrokenCounter Recently;
};

////////////////////////////////////////////////////////////////////////////////

struct TCheckpointInfo
{
    TString SourceDiskId;
    TString CheckpointId;
    TString ShadowDiskId;

    TCheckpointInfo() = default;

    TCheckpointInfo(
            TString sourceDiskId,
            TString checkpointId,
            TString shadowDiskId)
        : SourceDiskId(std::move(sourceDiskId))
        , CheckpointId(std::move(checkpointId))
        , ShadowDiskId(std::move(shadowDiskId))
    {}

    static TString MakeId(
        const TString& sourceDiskId,
        const TString& checkpointId);

    TString Id() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TPlacementGroupInfo
{
    NProto::TPlacementGroupConfig Config;

    // updated asynchronously during PublishCounters()
    bool Full = false;
    ui64 BiggestDiskSize = 0;
    TString BiggestDiskId;

    TPlacementGroupInfo() = default;

    TPlacementGroupInfo(NProto::TPlacementGroupConfig config)
        : Config(std::move(config))
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryState
{
    using TAgentId = TString;
    using TDeviceId = TString;
    using TDiskId = TString;
    using TCheckpointId = TString;
    using TNodeId = ui32;

    struct TDiskPlacementInfo
    {
        TString PlacementGroupId;
        ui32 PlacementPartitionIndex = 0;
    };

    // TDiskState structure can describe one of the following entities:
    // * a nonreplicated disk
    // * a local disk
    // * a single replica of a mirrored disk
    // * a "checkpoint" replica - can be created for nonreplicated or mirrored
    //   disk
    // * a "master" disk - exists for each mirrored disk
    //
    // Master disks don't contain devices and migrations - all devices and
    // migrations are kept in TDiskStates of the replicas.
    //
    // But public methods that describe mirrored disks return all the parts
    // inside a single object - e.g. GetDiskInfo and AllocateDisk return
    // 'containers' that store replica0 devices in the 'Devices' field,
    // replica 1-N devices in the 'Replicas' field and device migration
    // instructions in the 'Migrations' field.
    struct TDiskState
    {
        TString CloudId;
        TString FolderId;
        TString UserId;
        TVector<TDeviceId> Devices;
        THashMap<TDeviceId, TDeviceId> MigrationTarget2Source;
        THashMap<TDeviceId, TDeviceId> MigrationSource2Target;
        TVector<TFinishedMigration> FinishedMigrations;

        TInstant MigrationStartTs;

        bool AcquireInProgress = false;
        ui32 LogicalBlockSize = 0;
        TString PlacementGroupId;
        ui32 PlacementPartitionIndex = 0;

        NProto::EDiskState State = NProto::DISK_STATE_ONLINE;
        TInstant StateTs;

        ui32 ReplicaCount = 0;
        TString MasterDiskId;

        // Filled if the disk is a shadow disk for the checkpoint.
        NProto::TCheckpointReplica CheckpointReplica;

        TVector<TDeviceId> DeviceReplacementIds;

        NProto::EStorageMediaKind MediaKind =
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED;

        TVector<NProto::TDiskHistoryItem> History;

        TVector<TLaggingDevice> LaggingDevices;
    };

    struct TVolumeDeviceOverrides
    {
        THashMap<TDeviceId, ui64> Device2BlockCount;
    };

    using TKnownAgents = THashMap<TAgentId, TKnownAgent>;
    using TDeviceOverrides = THashMap<TDeviceId, TVolumeDeviceOverrides>;

    using TCheckpoints = THashMap<TCheckpointId, TCheckpointInfo>;
    using TPlacementGroups = THashMap<TString, TPlacementGroupInfo>;

private:
    TLog Log;

    const TStorageConfigPtr StorageConfig;
    const NMonitoring::TDynamicCountersPtr Counters;
    mutable TDiskRegistrySelfCounters SelfCounters;

    TAgentList AgentList;
    TDeviceList DeviceList;

    THashMap<TDiskId, TDiskState> Disks;
    THashSet<TDiskId> DisksToCleanup;
    TKnownAgents KnownAgents;
    TDeviceOverrides DeviceOverrides;
    TCheckpoints Checkpoints;
    TPlacementGroups PlacementGroups;
    TVector<TBrokenDiskInfo> BrokenDisks;

    TDeque<TAutomaticallyReplacedDeviceInfo> AutomaticallyReplacedDevices;
    THashSet<TDeviceId> AutomaticallyReplacedDeviceIds;
    TDeque<TInstant> AutomaticReplacementTimestamps;

    NProto::TDiskRegistryConfig CurrentConfig;

    struct TDeviceMigrationCompare
    {
        bool operator ()(
            const TDeviceMigration& lhs,
            const TDeviceMigration& rhs) const
        {
            return std::less<>()(
                std::tie(lhs.DiskId, lhs.SourceDeviceId),
                std::tie(rhs.DiskId, rhs.SourceDeviceId));
        }
    };

    TSet<TDeviceMigration, TDeviceMigrationCompare> Migrations;
    THashMap<TString, THashSet<TDeviceId>> SourceDeviceMigrationsInProgress;

    TReplicaTable ReplicaTable{&DeviceList};

    TDevicePoolConfigs DevicePoolConfigs;

    TPendingCleanup PendingCleanup;

    NProto::TMeanTimeBetweenFailures TimeBetweenFailures;

    NDiskRegistry::TNotificationSystem NotificationSystem;

    THashMap<TString, TCachedAcquireRequests> AcquireCacheByAgentId;

public:
    TDiskRegistryState(
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
        THashMap<TString, NProto::TDiskRegistryAgentParams> diskRegistryAgentListParams);

    struct TAgentRegistrationResult
    {
        TVector<TDiskId> AffectedDisks;
        TVector<TDiskId> DisksToReallocate;
        TVector<TString> DevicesToDisableIO;
    };

    auto RegisterAgent(
        TDiskRegistryDatabase& db,
        NProto::TAgentConfig config,
        TInstant timestamp) -> TResultOrError<TAgentRegistrationResult>;

    NProto::TError UnregisterAgent(
        TDiskRegistryDatabase& db,
        ui32 nodeId);

    struct TAllocateDiskParams
    {
        TString DiskId;
        TString CloudId;
        TString FolderId;
        TString PlacementGroupId;
        ui32 PlacementPartitionIndex = 0;
        ui32 BlockSize = 0;
        ui64 BlocksCount = 0;
        ui32 ReplicaCount = 0;
        TString MasterDiskId;

        TVector<TString> AgentIds;
        TString PoolName;

        NProto::EStorageMediaKind MediaKind =
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED;
    };

    struct TAllocateDiskResult
    {
        TVector<NProto::TDeviceConfig> Devices;
        TVector<NProto::TDeviceMigration> Migrations;
        TVector<TVector<NProto::TDeviceConfig>> Replicas;
        TVector<TString> DeviceReplacementIds;
        TVector<TLaggingDevice> LaggingDevices;

        NProto::EVolumeIOMode IOMode = {};
        TInstant IOModeTs;
        bool MuteIOErrors = false;
    };

    struct TAllocateCheckpointResult: public TAllocateDiskResult
    {
        TString ShadowDiskId;
    };

    NProto::TError AllocateDisk(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TAllocateDiskParams& params,
        TAllocateDiskResult* result);

    NProto::TError DeallocateDisk(
        TDiskRegistryDatabase& db,
        const TString& diskId);

    NProto::TError AllocateCheckpoint(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& sourceDiskId,
        const TCheckpointId& checkpointId,
        TAllocateCheckpointResult* result);

    NProto::TError DeallocateCheckpoint(
        TDiskRegistryDatabase& db,
        const TDiskId& sourceDiskId,
        const TCheckpointId& checkpointId,
        TDiskId* shadowDiskId);

    NProto::TError GetCheckpointDataState(
        const TDiskId& sourceDiskId,
        const TCheckpointId& checkpointId,
        NProto::ECheckpointState* checkpointState) const;

    NProto::TError SetCheckpointDataState(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& sourceDiskId,
        const TCheckpointId& checkpointId,
        NProto::ECheckpointState checkpointState);

    [[nodiscard]] NProto::TError GetDiskDevices(
        const TDiskId& diskId,
        TVector<NProto::TDeviceConfig>& devices) const;

    NProto::TError GetDiskInfo(const TDiskId& diskId, TDiskInfo& diskInfo) const;
    NProto::EDiskState GetDiskState(const TDiskId& diskId) const;
    NProto::TError GetShadowDiskId(
        const TDiskId& sourceDiskId,
        const TCheckpointId& checkpointId,
        TDiskId* shadowDiskId) const;

    bool FilterDevicesAtUnavailableAgents(TDiskInfo& diskInfo) const;

    NProto::TError StartAcquireDisk(
        const TString& diskId,
        TDiskInfo& diskInfo);

    NProto::TError MarkDiskForCleanup(
        TDiskRegistryDatabase& db,
        const TString& diskId);

    bool HasPendingCleanup(const TDiskId& diskId) const;

    void FinishAcquireDisk(const TString& diskId);

    bool IsAcquireInProgress(const TString& diskId) const;

    const TVector<NProto::TAgentConfig>& GetAgents() const;

    NProto::TError UpdateConfig(
        TDiskRegistryDatabase& db,
        NProto::TDiskRegistryConfig config,
        bool ignoreVersion,
        TVector<TString>& affectedDisks);

    const NProto::TDiskRegistryConfig& GetConfig() const;

    ui32 GetConfigVersion() const;

    ui32 GetDiskCount() const;
    TVector<TString> GetDiskIds() const;
    TVector<TString> GetMasterDiskIds() const;
    TVector<TString> GetMirroredDiskIds() const;
    TVector<TString> GetDisksToCleanup() const;
    bool IsMasterDisk(const TString& diskId) const;

    NProto::TDeviceConfig GetDevice(const TString& id) const;
    TVector<TString> GetDeviceIds(const TString& agentId, const TString& path) const;
    NProto::EDeviceState GetDeviceState(const TString& deviceId) const;

    NProto::TError GetDependentDisks(
        const TString& agentId,
        const TString& path,
        bool ignoreReplicatedDisks,
        TVector<TDiskId>* diskIds) const;

    TVector<NProto::TDeviceConfig> GetBrokenDevices() const;

    TVector<NProto::TDeviceConfig> GetDirtyDevices() const;

    /// Mark selected device as clean and remove it
    /// from lists of suspended/dirty/pending cleanup devices
    /// @return disk id where selected device was allocated
    TDiskId MarkDeviceAsClean(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDeviceId& uuid);

    /// Mark selected devices as clean and remove them
    /// from lists of suspended/dirty/pending cleanup devices
    /// @return vector of disk ids where selected devices were allocated
    TVector<TDiskId> MarkDevicesAsClean(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TVector<TDeviceId>& uuids);
    bool MarkDeviceAsDirty(TDiskRegistryDatabase& db, const TDeviceId& uuid);

    NProto::TError CreatePlacementGroup(
        TDiskRegistryDatabase& db,
        const TString& groupId,
        NProto::EPlacementStrategy placementStrategy,
        ui32 placementPartitionCount);
    NProto::TError UpdatePlacementGroupSettings(
        TDiskRegistryDatabase& db,
        const TString& groupId,
        ui32 configVersion,
        NProto::TPlacementGroupSettings settings);
    NProto::TError DestroyPlacementGroup(
        TDiskRegistryDatabase& db,
        const TString& groupId,
        TVector<TDiskId>& affectedDisks);
    NProto::TError AlterPlacementGroupMembership(
        TDiskRegistryDatabase& db,
        const TString& groupId,
        ui32 placementPartitionIndex,
        ui32 configVersion,
        TVector<TString>& disksToAdd,
        const TVector<TString>& disksToRemove);
    const TPlacementGroups& GetPlacementGroups() const
    {
        return PlacementGroups;
    }
    const NProto::TPlacementGroupConfig* FindPlacementGroup(const TString& groupId) const;

    const TVector<TBrokenDiskInfo>& GetBrokenDisks() const
    {
        return BrokenDisks;
    }

    void DeleteBrokenDisks(
        TDiskRegistryDatabase& db,
        TVector<TDiskId> ids);

    const THashMap<TString, ui64>& GetDisksToReallocate() const;
    ui64 AddReallocateRequest(TDiskRegistryDatabase& db, const TString& diskId);

    void DeleteDiskToReallocate(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        ui64 seqNo);

    const TVector<TDiskStateUpdate>& GetDiskStateUpdates() const;

    void DeleteDiskStateUpdate(TDiskRegistryDatabase& db, ui64 maxSeqNo);

    void AddUserNotification(
        TDiskRegistryDatabase& db,
        NProto::TUserNotification notification);
    void DeleteUserNotification(
        TDiskRegistryDatabase& db,
        const TString& entityId,
        ui64 seqNo);
    void GetUserNotifications(
        TVector<NProto::TUserNotification>& notifications) const;
    decltype(auto) GetUserNotifications() const
    {
        return NotificationSystem.GetUserNotifications();
    }

    TVector<TString> CollectBrokenDevices(const NProto::TAgentStats& stats) const;
    NProto::TError UpdateAgentCounters(const NProto::TAgentStats& stats);
    void PublishCounters(TInstant now);

    void DeleteDiskStateChanges(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        ui64 seqNo);

    NProto::TError UpdateAgentState(
        TDiskRegistryDatabase& db,
        const TString& agentId,
        NProto::EAgentState newState,
        TInstant timestamp,
        TString reason,
        TVector<TDiskId>& affectedDisks);

    NProto::TError SwitchAgentDisksToReadOnly(
        TDiskRegistryDatabase& db,
        TString agentId,
        TVector<TDiskId>& affectedDisks);

    NProto::TError UpdateCmsHostState(
        TDiskRegistryDatabase& db,
        const TString& agentId,
        NProto::EAgentState state,
        TInstant now,
        bool dryRun,
        TVector<TDiskId>& affectedDisks,
        TDuration& timeout);

    NProto::TError PurgeHost(
        TDiskRegistryDatabase& db,
        const TString& agentId,
        TInstant now,
        bool dryRun,
        TVector<TDiskId>& affectedDisks);

    TMaybe<NProto::EAgentState> GetAgentState(const TString& agentId) const;
    TMaybe<TInstant> GetAgentCmsTs(const TString& agentId) const;

    NProto::TError UpdateDeviceState(
        TDiskRegistryDatabase& db,
        const TString& deviceId,
        NProto::EDeviceState state,
        TInstant now,
        TString reason,
        TDiskId& affectedDisk);

    struct TUpdateCmsDeviceStateResult
    {
        NProto::TError Error;
        TVector<TDiskId> AffectedDisks;
        TDuration Timeout;
    };

    TUpdateCmsDeviceStateResult UpdateCmsDeviceState(
        TDiskRegistryDatabase& db,
        const TAgentId& agentId,
        const TString& path,
        NProto::EDeviceState state,
        TInstant now,
        bool shouldResume,
        bool dryRun);

    NProto::TError ReplaceDevice(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        const TString& deviceId,
        const TString& deviceReplacementId,
        TInstant timestamp,
        TString message,
        bool manual,
        bool* diskStateUpdated);

    TString GetAgentId(TNodeId nodeId) const;

    ui32 CalculateRackCount() const;
    TDeque<TRackInfo> GatherRacksInfo(const TString& poolName) const;
    THashMap<TString, TBrokenGroupInfo> GatherBrokenGroupsInfo(
        TInstant now,
        TDuration period) const;

    const NProto::TAgentConfig* FindAgent(const TString& id) const
    {
        return AgentList.FindAgent(id);
    }

    const NProto::TAgentConfig* FindAgent(TNodeId nodeId) const
    {
        return AgentList.FindAgent(nodeId);
    }

    const NProto::TAgentConfig* FindDeviceAgent(const TDeviceId& uuid) const {
        return FindDeviceLocation(uuid).first;
    }

    TDiskId FindDisk(const TDeviceId& uuid) const;

    NProto::TDiskRegistryStateBackup BackupState() const;

    TResultOrError<NProto::TDeviceConfig> StartDeviceMigration(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& sourceDiskId,
        const TDeviceId& sourceDeviceId);

    TResultOrError<NProto::TDeviceConfig> StartDeviceMigration(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& sourceDiskId,
        const TDeviceId& sourceDeviceId,
        const TDeviceId& targetDeviceId);

    NProto::TError FinishDeviceMigration(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        const TDeviceId& sourceId,
        const TDeviceId& targetId,
        TInstant timestamp,
        bool* diskStateUpdated);

    TDiskId FindReplicaByMigration(
        const TDiskId& masterDiskId,
        const TDeviceId& sourceDeviceId,
        const TDeviceId& targetDeviceId) const;

    TVector<TDeviceMigration> BuildMigrationList() const;

    bool IsMigrationListEmpty() const
    {
        return Migrations.empty();
    }

    bool IsReadyForCleanup(const TDiskId& diskId) const;

    bool CanSecureErase(const TDeviceId& uuid) const;
    bool CanSecureErase(const NProto::TDeviceConfig& device) const;

    NProto::TError SetUserId(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        const TString& userId);

    TVector<TDiskId> GetOutdatedVolumeConfigs() const;

    std::pair<TVolumeConfig, ui64> GetVolumeConfigUpdate(const TDiskId& diskId) const;

    void DeleteOutdatedVolumeConfig(TDiskRegistryDatabase& db, const TDiskId& diskId);

    NProto::TError UpdateDiskBlockSize(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        ui32 blockSize,
        bool force);

    NProto::TError AllocateDiskReplicas(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& masterDiskId,
        ui32 replicaCount);

    NProto::TError DeallocateDiskReplicas(
        TDiskRegistryDatabase& db,
        const TDiskId& masterDiskId,
        ui32 replicaCount);

    NProto::TError UpdateDiskReplicaCount(
        TDiskRegistryDatabase& db,
        const TDiskId& masterDiskId,
        ui32 replicaCount);

    TResultOrError<TVector<TAgentStorageInfo>> QueryAvailableStorage(
        const TString& agentId,
        const TString& poolName,
        NProto::EDevicePoolKind poolKind) const;

    NProto::TError MarkReplacementDevice(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        const TDeviceId& deviceId,
        bool isReplacement);

    [[nodiscard]] NProto::TError AddLaggingDevices(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        TVector<NProto::TLaggingDevice> laggingDevices);

    NProto::TError SuspendDevice(TDiskRegistryDatabase& db, const TDeviceId& id);

    void SuspendDeviceIfNeeded(
        TDiskRegistryDatabase& db,
        NProto::TDeviceConfig& device);

    void ResumeDevice(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDeviceId& id);
    void ResumeDevices(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TVector<TDeviceId>& ids);
    bool IsSuspendedDevice(const TDeviceId& id) const;
    bool IsDirtyDevice(const TDeviceId& id) const;
    TVector<NProto::TSuspendedDevice> GetSuspendedDevices() const;

    const TDeque<TAutomaticallyReplacedDeviceInfo>& GetAutomaticallyReplacedDevices() const
    {
        return AutomaticallyReplacedDevices;
    }

    bool IsAutomaticallyReplaced(const TDeviceId& deviceId) const
    {
        return AutomaticallyReplacedDeviceIds.contains(deviceId);
    }

    ui32 DeleteAutomaticallyReplacedDevices(
        TDiskRegistryDatabase& db,
        const TInstant until);

    void DeleteAutomaticallyReplacedDevice(
        TDiskRegistryDatabase& db,
        const TDeviceId& deviceId);

    bool CheckIfDeviceReplacementIsAllowed(
        TInstant now,
        const TDiskId& masterDiskId,
        const TDeviceId& deviceId);

    NProto::TError CreateDiskFromDevices(
        TInstant now,
        TDiskRegistryDatabase& db,
        bool force,
        const TDiskId& diskId,
        ui32 blockSize,
        NProto::EStorageMediaKind mediaKind,
        const TVector<NProto::TDeviceConfig>& devices,
        TAllocateDiskResult* result);

    NProto::TError ChangeDiskDevice(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        const TDeviceId& sourceDeviceId,
        const TDeviceId& targetDeviceId);

    // for tests and monpages
    const TReplicaTable& GetReplicaTable() const
    {
        return ReplicaTable;
    }

    THashMap<TString, TCachedAcquireRequests>& GetAcquireCacheByAgentId()
    {
        return AcquireCacheByAgentId;
    }

    TDuration GetRejectAgentTimeout(TInstant now, const TString& agentId) const
    {
        return AgentList.GetRejectAgentTimeout(now, agentId);
    }

    void OnAgentDisconnected(TInstant now, const TString& agentId)
    {
        AgentList.OnAgentDisconnected(now, agentId);
    }

    TVector<TAgentId> GetAgentIdsWithOverriddenListParams() const
    {
        return AgentList.GetAgentIdsWithOverriddenListParams();
    }

    void SetDiskRegistryAgentListParams(
        TDiskRegistryDatabase& db,
        const TString& agentId,
        const NProto::TDiskRegistryAgentParams& params);

    void CleanupExpiredAgentListParams(
        TDiskRegistryDatabase& db,
        TInstant now);

    TVector<TString> GetPoolNames() const;

    const NProto::TDeviceConfig* FindDevice(const TDeviceId& uuid) const;

    TVector<NProto::TAgentInfo> QueryAgentsInfo() const;

    TVector<TString> FindOrphanDevices() const;

    void RemoveOrphanDevices(
        TDiskRegistryDatabase& db,
        const TVector<TString>& orphanDevicesIds);

    TAgentId GetAgentIdSuitableForLocalDiskAllocationAfterCleanup(
        const TVector<TString>& agentIds,
        const TString& poolName,
        const ui64 blocksToAllocate) const;

private:
    void ProcessConfig(const NProto::TDiskRegistryConfig& config);
    void ProcessDisks(TVector<NProto::TDiskConfig> disks);
    void ProcessCheckpoints();
    void ProcessPlacementGroups(TVector<NProto::TPlacementGroupConfig> placementGroups);
    void ProcessAgents();
    void ProcessDisksToCleanup(TVector<TString> diskIds);
    void ProcessDirtyDevices(TVector<TDirtyDevice> dirtyDevices);

    void AddMigration(
        const TDiskState& disk,
        const TString& diskId,
        const TString& sourceDeviceId);
    void FillMigrations();

    const TDiskState* FindDiskState(const TDiskId& diskId) const;
    TDiskState* AccessDiskState(const TDiskId& diskId);

    template <typename T>
    bool RemoveAgent(
        TDiskRegistryDatabase& db,
        const T& id);

    void RemoveAgent(
        TDiskRegistryDatabase& db,
        const NProto::TAgentConfig& agent);

    void RemoveAgentFromNode(
        TDiskRegistryDatabase& db,
        NProto::TAgentConfig& agent,
        TInstant timestamp,
        TVector<TDiskId>* affectedDisks,
        TVector<TDiskId>* disksToReallocate);

    [[nodiscard]] TString GetDiskIdToNotify(const TString& diskId) const;

    [[nodiscard]] NProto::TError GetDiskDevices(
        const TDiskId& diskId,
        const TDiskState& disk,
        TVector<NProto::TDeviceConfig>& devices) const;

    [[nodiscard]] NProto::TError GetDiskMigrations(
        const TDiskId& diskId,
        TVector<NProto::TDeviceMigration>& migrations) const;

    [[nodiscard]] NProto::TError GetDiskMigrations(
        const TDiskState& disk,
        TVector<NProto::TDeviceMigration>& migrations) const;

    [[nodiscard]] NProto::TError FillAllDiskDevices(
        const TDiskId& diskId,
        const TDiskState& disk,
        TDiskInfo& diskInfo) const;

    [[nodiscard]] NProto::TError ValidateAgent(
        const NProto::TAgentConfig& agent) const;

    bool IsKnownDevice(const TDeviceId& uuid) const;

    NProto::TDiskConfig BuildDiskConfig(
        TDiskId diskId,
        const TDiskState& diskState) const;

    auto FindDeviceLocation(const TDeviceId& uuid)
        -> std::pair<NProto::TAgentConfig*, NProto::TDeviceConfig*>;

    auto FindDeviceLocation(const TDeviceId& uuid) const
        -> std::pair<const NProto::TAgentConfig*, const NProto::TDeviceConfig*>;

    TVector<NProto::TDeviceConfig> FindDevices(
        const TString& agentId,
        const TString& path) const;

    TResultOrError<NProto::TDeviceConfig> FindDevice(
        const NProto::TDeviceConfig& deviceConfig) const;

    NProto::EDiskState CalculateDiskState(
        const TString& diskId,
        const TDiskState& disk) const;

    bool TryUpdateDiskState(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        TInstant timestamp);

    bool TryUpdateDiskState(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        TDiskState& disk,
        TInstant timestamp);

    bool TryUpdateDiskStateImpl(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        TDiskState& disk,
        TInstant timestamp);

    NProto::TError TryToRemoveAgentDevices(
        TDiskRegistryDatabase& db,
        const TAgentId& agentId);

    NProto::TPlacementGroupConfig::TDiskInfo* CollectRacks(
        const TString& diskId,
        ui32 placementPartitionIndex,
        NProto::TPlacementGroupConfig& placementGroup,
        THashSet<TString>* forbiddenRacks,
        THashSet<TString>* preferredRacks);

    void CollectForbiddenRacks(
        const NProto::TPlacementGroupConfig& placementGroup,
        THashSet<TString>* forbiddenRacks);

    THashSet<TString> CollectForbiddenRacks(
        const TDiskId& diskId,
        const TDiskState& disk,
        TStringBuf callerName);

    THashSet<TString> CollectPreferredRacks(const TDiskId& diskId) const;

    void RebuildDiskPlacementInfo(
        const TDiskState& disk,
        NProto::TPlacementGroupConfig::TDiskInfo* diskInfo) const;

    bool UpdatePlacementGroup(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        const TDiskState& disk,
        TStringBuf callerName);

    void UpdateDiskPlacementInfo(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        const TDiskPlacementInfo& placementInfo);

    void ApplyAgentStateChange(
        TDiskRegistryDatabase& db,
        const NProto::TAgentConfig& agent,
        TInstant timestamp,
        TVector<TDiskId>& affectedDisks);

    bool HasDependentSsdDisks(const NProto::TAgentConfig& agent) const;
    ui32 CountBrokenHddPlacementGroupPartitionsAfterAgentRemoval(
        const NProto::TAgentConfig& agent) const;
    ui32 CountBrokenHddPlacementGroupPartitionsAfterDeviceRemoval(
        const THashSet<TString>& deviceIds) const;

    NProto::TError CheckAgentStateTransition(
        const NProto::TAgentConfig& agent,
        NProto::EAgentState newState,
        TInstant timestamp) const;

    NProto::TError CheckDeviceStateTransition(
        const NProto::TDeviceConfig& device,
        NProto::EDeviceState newState,
        TInstant timestamp);

    void ApplyDeviceStateChange(
        TDiskRegistryDatabase& db,
        const NProto::TAgentConfig& agent,
        const NProto::TDeviceConfig& device,
        TInstant timestamp,
        TDiskId& affectedDisk);

    bool RestartDeviceMigration(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        TDiskState& disk,
        const TDeviceId& targetId);

    void CancelDeviceMigration(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        TDiskState& disk,
        const TDeviceId& sourceId);

    NProto::TError AbortMigrationAndReplaceDevice(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& diskId,
        TDiskState& disk,
        const TDeviceId& sourceId);

    void DeleteDeviceMigration(
        const TDiskId& diskId,
        const TDeviceId& sourceId);

    void DeleteAllDeviceMigrations(const TDiskId& diskId);

    void UpdateAndReallocateDisk(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        TDiskState& disk);

    void AdjustDeviceIfNeeded(
        NProto::TDeviceConfig& device,
        TInstant timestamp);

    void AdjustDeviceBlockCount(
        TInstant now,
        TDiskRegistryDatabase& db,
        NProto::TDeviceConfig& device,
        ui64 newBlockCount);

    void AdjustDeviceState(
        TDiskRegistryDatabase& db,
        NProto::TDeviceConfig& device,
        NProto::EDeviceState state,
        TInstant timestamp,
        TString message);

    ui64 GetDeviceBlockCountWithOverrides(
        const TDiskId& diskId,
        const NProto::TDeviceConfig& device);

    void RemoveFinishedMigrations(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        ui64 seqNo);

    void RemoveLaggingDevices(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        ui64 seqNo);

    NProto::TError ValidateUpdateDiskBlockSizeParams(
        const TDiskId& diskId,
        ui32 blockSize,
        bool force);

    NProto::TError ValidateUpdateDiskReplicaCountParams(
        const TDiskId& masterDiskId,
        ui32 replicaCount);

    bool DoesNewDiskBlockSizeBreakDevice(
        const TDiskId& diskId,
        const TDeviceId& deviceId,
        ui64 newLogicalBlockSize);

    NProto::TError ValidateDiskLocation(
        const TVector<NProto::TDeviceConfig>& diskDevices,
        const TAllocateDiskParams& params) const;

    TResultOrError<TDeviceList::TAllocationQuery> PrepareAllocationQuery(
        ui64 blocksToAllocate,
        const TDiskPlacementInfo& placementInfo,
        const TVector<NProto::TDeviceConfig>& diskDevices,
        const TAllocateDiskParams& params);

    void UpdateAgent(TDiskRegistryDatabase& db, NProto::TAgentConfig config);

    ui64 GetAllocationUnit(const TString& poolName) const;
    NProto::EDevicePoolKind GetDevicePoolKind(const TString& poolName) const;

    NProto::TError ValidateAllocateDiskParams(
        const TDiskState& disk,
        const TAllocateDiskParams& params) const;

    NProto::TError AllocateSimpleDisk(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TAllocateDiskParams& params,
        const NProto::TCheckpointReplica& checkpointParams,
        TDiskState& disk,
        TAllocateDiskResult* result);

    NProto::TError AllocateMirroredDisk(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TAllocateDiskParams& params,
        TDiskState& disk,
        TAllocateDiskResult* result);

    NProto::TError AllocateDiskReplicas(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TAllocateDiskParams& params,
        TAllocateDiskResult* result);

    NProto::TError AllocateDiskReplica(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TAllocateDiskParams& params,
        ui32 index,
        TAllocateDiskResult* result);

    TVector<TDeviceId> DeallocateSimpleDisk(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        TDiskState& disk);

    TVector<TDeviceId> DeallocateSimpleDisk(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        const TString& logPrefix);

    void DeleteDisk(TDiskRegistryDatabase& db, const TString& diskId);
    void DeleteCheckpointByDisk(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId);
    void ReallocateCheckpointByDisk(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& diskId);

    void ForgetDevices(
        TDiskRegistryDatabase& db,
        const TVector<TString>& ids);

    void AddToBrokenDisks(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TString& diskId);

    NProto::TError AddDevicesToPendingCleanup(
        const TString& diskId,
        TVector<TDeviceId> uuids);

    /// Try to update configuration of selected device and its agent
    /// in the disk registry database
    /// @return true if the device updates successfully; otherwise, return false
    bool TryUpdateDevice(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDeviceId& uuid);

    /// Try to update configuration of selected devices and their agents
    /// in the disk registry database
    /// @return List of updated devices
    TVector<TDeviceId> TryUpdateDevices(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TVector<TDeviceId>& uuids);

    TDeviceList::TAllocationQuery MakeMigrationQuery(
        const TDiskId& sourceDiskId,
        const NProto::TDeviceConfig& sourceDevice);

    NProto::TError ValidateStartDeviceMigration(
        const TDiskId& sourceDiskId,
        const TString& sourceDeviceId);

    NProto::TDeviceConfig StartDeviceMigrationImpl(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TDiskId& sourceDiskId,
        const TDeviceId& sourceDeviceId,
        NProto::TDeviceConfig targetDevice);

    void ChangeAgentState(
        NProto::TAgentConfig& agent,
        NProto::EAgentState newState,
        TInstant now,
        TString stateMessage);

    bool IsMirroredDiskAlreadyAllocated(const TAllocateDiskParams& params) const;
    void UpdateReplicaTable(const TDiskId& diskId, const TAllocateDiskResult& r);
    void CleanupMirroredDisk(
        TInstant now,
        TDiskRegistryDatabase& db,
        const TAllocateDiskParams& params);

    NProto::TError CheckPlacementGroupVersion(
        const TString& placementGroupId,
        ui32 configVersion);

    NProto::TError CheckDiskPlacementInfo(const TDiskPlacementInfo& info) const;
    NProto::TError CheckPlacementGroupCapacity(const TString& groupId) const;

    TDiskPlacementInfo CreateDiskPlacementInfo(
        const TDiskState& disk,
        const TAllocateDiskParams& params) const;

    NProto::TError CreateMirroredDiskPlacementGroup(
        TDiskRegistryDatabase& db,
        const TDiskId& diskId);

    void AllowNotifications(const TDiskId& diskId, const TDiskState& disk);

    void SuspendLocalDevices(
        TDiskRegistryDatabase& db,
        const NProto::TAgentConfig& agent);

    void ResumeLocalDevices(
        TDiskRegistryDatabase& db,
        NProto::TAgentConfig& agent,
        TInstant now);

    TResultOrError<NProto::TDeviceConfig> AllocateReplacementDevice(
        TDiskRegistryDatabase& db,
        const TString& diskId,
        const TDeviceId& deviceReplacementId,
        const TDeviceList::TAllocationQuery& query,
        TInstant timestamp,
        TString message);

    NProto::EVolumeIOMode GetIoMode(TDiskState& disk, TInstant now) const;

    auto ResolveDevices(const TAgentId& agentId, const TString& path)
        -> std::pair<NProto::TAgentConfig*, TVector<NProto::TDeviceConfig*>>;

    NProto::TError CmsAddDevice(
        TDiskRegistryDatabase& db,
        NProto::TAgentConfig& agent,
        NProto::TDeviceConfig& device,
        TInstant now,
        bool shouldResume,
        bool dryRun,
        TDuration& timeout);

    TUpdateCmsDeviceStateResult AddNewDevices(
        TDiskRegistryDatabase& db,
        NProto::TAgentConfig& agent,
        const TString& path,
        TInstant now,
        bool shouldResume,
        bool dryRun);

    NProto::TError RegisterUnknownDevices(
        TDiskRegistryDatabase& db,
        NProto::TAgentConfig& agent,
        TInstant now);

    NProto::TError CmsRemoveDevice(
        TDiskRegistryDatabase& db,
        NProto::TAgentConfig& agent,
        NProto::TDeviceConfig& device,
        TInstant now,
        bool dryRun,
        TDiskId& affectedDisk,
        TDuration& timeout);

    void ResetMigrationStartTsIfNeeded(TDiskState& disk);

    struct TConfigUpdateEffect;
    TResultOrError<TConfigUpdateEffect> CalcConfigUpdateEffect(
        const NProto::TDiskRegistryConfig& newConfig) const;

    std::optional<ui64> GetDiskBlockCount(const TDiskId& diskId) const;

    NProto::TError ReplaceDeviceWithoutDiskStateUpdate(
        TDiskRegistryDatabase& db,
        TDiskState& disk,
        const TString& diskId,
        const TString& deviceId,
        const TString& deviceReplacementId,
        TInstant timestamp,
        TString message,
        bool manual);

    void TryToReplaceDeviceIfAllowedWithoutDiskStateUpdate(
        TDiskRegistryDatabase& db,
        TDiskState& disk,
        const TString& diskId,
        const TString& deviceId,
        TInstant timestamp,
        TString reason);

    void CleanupAgentConfig(
        TDiskRegistryDatabase& db,
        const NProto::TAgentConfig& agent);

    static bool MigrationCanBeStarted(
        const TDiskState& disk,
        const TString& deviceUUID);
};

}   // namespace NCloud::NBlockStore::NStorage
