#pragma once

#include "public.h"

#include "disk_registry_state.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISK_REGISTRY_TRANSACTIONS(xxx, ...)                        \
    xxx(InitSchema,                         __VA_ARGS__)                       \
    xxx(LoadState,                          __VA_ARGS__)                       \
    xxx(AddDisk,                            __VA_ARGS__)                       \
    xxx(RemoveDisk,                         __VA_ARGS__)                       \
    xxx(AddAgent,                           __VA_ARGS__)                       \
    xxx(RemoveAgent,                        __VA_ARGS__)                       \
    xxx(UpdateConfig,                       __VA_ARGS__)                       \
    xxx(CleanupDevices,                     __VA_ARGS__)                       \
    xxx(CreatePlacementGroup,               __VA_ARGS__)                       \
    xxx(DestroyPlacementGroup,              __VA_ARGS__)                       \
    xxx(AlterPlacementGroupMembership,      __VA_ARGS__)                       \
    xxx(DeleteBrokenDisks,                  __VA_ARGS__)                       \
    xxx(AddNotifiedDisks,                   __VA_ARGS__)                       \
    xxx(DeleteNotifiedDisks,                __VA_ARGS__)                       \
    xxx(UpdateAgentState,                   __VA_ARGS__)                       \
    xxx(UpdateDeviceState,                  __VA_ARGS__)                       \
    xxx(ReplaceDevice,                      __VA_ARGS__)                       \
    xxx(UpdateCmsHostDeviceState,           __VA_ARGS__)                       \
    xxx(UpdateCmsHostState,                 __VA_ARGS__)                       \
    xxx(DeleteDiskStateUpdates,             __VA_ARGS__)                       \
    xxx(WritableState,                      __VA_ARGS__)                       \
    xxx(StartMigration,                     __VA_ARGS__)                       \
    xxx(StartForceMigration,                __VA_ARGS__)                       \
    xxx(FinishMigration,                    __VA_ARGS__)                       \
    xxx(MarkDiskForCleanup,                 __VA_ARGS__)                       \
    xxx(BackupDiskRegistryState,            __VA_ARGS__)                       \
    xxx(DeleteUserNotifications,            __VA_ARGS__)                       \
    xxx(SetUserId,                          __VA_ARGS__)                       \
    xxx(FinishVolumeConfigUpdate,           __VA_ARGS__)                       \
    xxx(UpdateDiskBlockSize,                __VA_ARGS__)                       \
    xxx(UpdateDiskReplicaCount,             __VA_ARGS__)                       \
    xxx(MarkReplacementDevice,              __VA_ARGS__)                       \
    xxx(SuspendDevice,                      __VA_ARGS__)                       \
    xxx(ResumeDevices,                      __VA_ARGS__)                       \
    xxx(UpdatePlacementGroupSettings,       __VA_ARGS__)                       \
    xxx(RestoreDiskRegistryState,           __VA_ARGS__)                       \
    xxx(CreateDiskFromDevices,              __VA_ARGS__)                       \
    xxx(ChangeDiskDevice,                   __VA_ARGS__)                       \
    xxx(RestoreDiskRegistryPart,            __VA_ARGS__)                       \
    xxx(ProcessAutomaticallyReplacedDevices,__VA_ARGS__)                       \
    xxx(UpdateDiskRegistryAgentListParams,  __VA_ARGS__)                       \
    xxx(CleanupExpiredAgentListParams,      __VA_ARGS__)                       \
    xxx(SwitchAgentDisksToReadOnly,         __VA_ARGS__)                       \
    xxx(AllocateCheckpoint,                 __VA_ARGS__)                       \
    xxx(DeallocateCheckpoint,               __VA_ARGS__)                       \
    xxx(SetCheckpointDataState,             __VA_ARGS__)                       \
    xxx(PurgeHostCms,                       __VA_ARGS__)                       \
    xxx(RemoveOrphanDevices,                __VA_ARGS__)                       \
    xxx(AddOutdatedLaggingDevices,          __VA_ARGS__)                       \
// BLOCKSTORE_DISK_REGISTRY_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

struct TTxDiskRegistry
{
    //
    // InitSchema
    //

    struct TInitSchema
    {
        const TRequestInfoPtr RequestInfo;

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // LoadState
    //

    struct TLoadState
    {
        const TRequestInfoPtr RequestInfo;
        bool RestoreState = false;
        TInstant LastBackupTime;
        TDiskRegistryStateSnapshot Snapshot;

        TLoadState() = default;

        explicit TLoadState(TRequestInfoPtr requestInfo)
            : RequestInfo{std::move(requestInfo)}
        {}

        void Clear()
        {
            RestoreState = false;
            LastBackupTime = {};
            Snapshot.Clear();
        }
    };

    //
    // AddDisk
    //

    struct TAddDisk
    {
        const TRequestInfoPtr RequestInfo;
        const TString DiskId;
        const TString CloudId;
        const TString FolderId;
        const TString PlacementGroupId;
        const ui32 PlacementPartitionIndex;
        const ui32 BlockSize;
        const ui64 BlocksCount;
        const ui32 ReplicaCount;
        const TVector<TString> AgentIds;
        const TString PoolName;
        const NProto::EStorageMediaKind MediaKind;

        NProto::TError Error;
        TVector<NProto::TDeviceConfig> Devices;
        TVector<NProto::TDeviceMigration> DeviceMigrations;
        TVector<TVector<NProto::TDeviceConfig>> Replicas;
        TVector<TString> DeviceReplacementUUIDs;
        TVector<TLaggingDevice> LaggingDevices;
        NProto::EVolumeIOMode IOMode = NProto::VOLUME_IO_OK;
        TInstant IOModeTs;
        bool MuteIOErrors = false;

        TAddDisk(
                TRequestInfoPtr requestInfo,
                TString diskId,
                TString cloudId,
                TString folderId,
                TString placementGroupId,
                ui32 placementPartitionIndex,
                ui32 blockSize,
                ui64 blocksCount,
                ui32 replicaCount,
                TVector<TString> agentIds,
                TString poolName,
                NProto::EStorageMediaKind mediaKind)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , CloudId(std::move(cloudId))
            , FolderId(std::move(folderId))
            , PlacementGroupId(std::move(placementGroupId))
            , PlacementPartitionIndex(placementPartitionIndex)
            , BlockSize(blockSize)
            , BlocksCount(blocksCount)
            , ReplicaCount(replicaCount)
            , AgentIds(std::move(agentIds))
            , PoolName(std::move(poolName))
            , MediaKind(mediaKind)
        {}

        void Clear()
        {
            Error.Clear();
            Devices.clear();
            DeviceMigrations.clear();
            Replicas.clear();
            DeviceReplacementUUIDs.clear();
            LaggingDevices.clear();
            IOMode = NProto::VOLUME_IO_OK;
            IOModeTs = {};
            MuteIOErrors = false;
        }
    };

    //
    // RemoveDisk
    //

    struct TRemoveDisk
    {
        const TRequestInfoPtr RequestInfo;
        const TString DiskId;
        const bool Sync;

        NProto::TError Error;

        TRemoveDisk(
                TRequestInfoPtr requestInfo,
                TString diskId,
                bool sync)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , Sync(sync)
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // AddAgent
    //

    struct TAddAgent
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TAgentConfig Config;
        const TInstant Timestamp;
        const NActors::TActorId RegisterActorId;

        NProto::TError Error;
        TVector<TString> AffectedDisks;
        TVector<TString> NotifiedDisks;
        TVector<TString> DevicesToDisableIO;

        TAddAgent(
                TRequestInfoPtr requestInfo,
                NProto::TAgentConfig config,
                TInstant timestamp,
                NActors::TActorId registerActorId)
            : RequestInfo(std::move(requestInfo))
            , Config(std::move(config))
            , Timestamp(timestamp)
            , RegisterActorId(registerActorId)
        {}

        void Clear()
        {
            Error.Clear();
            AffectedDisks.clear();
            NotifiedDisks.clear();
            DevicesToDisableIO.clear();
        }
    };

    //
    // RemoveAgent
    //

    struct TRemoveAgent
    {
        const TRequestInfoPtr RequestInfo;
        const ui32 NodeId;
        const TInstant Timestamp;

        NProto::TError Error;
        TVector<TString> AffectedDisks;

        TRemoveAgent(
                TRequestInfoPtr requestInfo,
                ui32 nodeId,
                TInstant timestamp)
            : RequestInfo(std::move(requestInfo))
            , NodeId(nodeId)
            , Timestamp(timestamp)
        {}

        void Clear()
        {
            Error.Clear();
            AffectedDisks.clear();
        }
    };

    //
    // UpdateConfig
    //

    struct TUpdateConfig
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TDiskRegistryConfig Config;
        const bool IgnoreVersion;

        NProto::TError Error;

        TVector<TString> AffectedDisks;

        TUpdateConfig(
                TRequestInfoPtr requestInfo,
                NProto::TDiskRegistryConfig config,
                bool ignoreVersion)
            : RequestInfo(std::move(requestInfo))
            , Config(std::move(config))
            , IgnoreVersion(ignoreVersion)
        {}

        void Clear()
        {
            Error.Clear();
            AffectedDisks.clear();
        }
    };

    //
    // CleanupDevices
    //

    struct TCleanupDevices
    {
        const TRequestInfoPtr RequestInfo;
        const TVector<TString> Devices;

        TVector<TString> SyncDeallocatedDisks;

        explicit TCleanupDevices(
                TRequestInfoPtr requestInfo,
                TVector<TString> devices)
            : RequestInfo(std::move(requestInfo))
            , Devices(std::move(devices))
        {}

        void Clear()
        {
            SyncDeallocatedDisks.clear();
        }
    };

    //
    // CreatePlacementGroup
    //

    struct TCreatePlacementGroup
    {
        const TRequestInfoPtr RequestInfo;
        const TString GroupId;
        const NProto::EPlacementStrategy PlacementStrategy;
        const ui32 PlacementPartitionCount;
        NProto::TError Error;

        explicit TCreatePlacementGroup(
                TRequestInfoPtr requestInfo,
                TString groupId,
                NProto::EPlacementStrategy placementStrategy,
                ui32 placementPartitionCount)
            : RequestInfo(std::move(requestInfo))
            , GroupId(std::move(groupId))
            , PlacementStrategy(placementStrategy)
            , PlacementPartitionCount(placementPartitionCount)
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // DestroyPlacementGroup
    //

    struct TDestroyPlacementGroup
    {
        const TRequestInfoPtr RequestInfo;
        const TString GroupId;
        NProto::TError Error;
        TVector<TString> AffectedDisks;

        explicit TDestroyPlacementGroup(
                TRequestInfoPtr requestInfo,
                TString groupId)
            : RequestInfo(std::move(requestInfo))
            , GroupId(std::move(groupId))
        {}

        void Clear()
        {
            Error.Clear();
            AffectedDisks.clear();
        }
    };

    //
    // AlterPlacementGroupMembership
    //

    struct TAlterPlacementGroupMembership
    {
        const TRequestInfoPtr RequestInfo;
        const TString GroupId;
        const ui32 PlacementPartitionIndex;
        const ui32 ConfigVersion;
        const TVector<TString> DiskIdsToAdd;
        const TVector<TString> DiskIdsToRemove;

        TVector<TString> FailedToAdd;
        NProto::TError Error;

        explicit TAlterPlacementGroupMembership(
                TRequestInfoPtr requestInfo,
                TString groupId,
                ui32 placementPartitionIndex,
                ui32 configVersion,
                TVector<TString> diskIdsToAdd,
                TVector<TString> diskIdsToRemove)
            : RequestInfo(std::move(requestInfo))
            , GroupId(std::move(groupId))
            , PlacementPartitionIndex(placementPartitionIndex)
            , ConfigVersion(configVersion)
            , DiskIdsToAdd(std::move(diskIdsToAdd))
            , DiskIdsToRemove(std::move(diskIdsToRemove))
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // DeleteBrokenDisks
    //

    struct TDeleteBrokenDisks
    {
        const TRequestInfoPtr RequestInfo;
        const TVector<TString> Disks;

        TDeleteBrokenDisks(
                TRequestInfoPtr requestInfo,
                TVector<TString> disks)
            : RequestInfo(std::move(requestInfo))
            , Disks(std::move(disks))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // AddNotifiedDisks
    //

    struct TAddNotifiedDisks
    {
        const TRequestInfoPtr RequestInfo;
        const TVector<TString> DiskIds;

        TAddNotifiedDisks(
                TRequestInfoPtr requestInfo,
                TVector<TString> diskIds)
            : RequestInfo(std::move(requestInfo))
            , DiskIds(std::move(diskIds))
        {}

        TAddNotifiedDisks(
                TRequestInfoPtr requestInfo,
                TString diskId)
            : RequestInfo(std::move(requestInfo))
            , DiskIds({ std::move(diskId) })
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // DeleteNotifiedDisks
    //

    struct TDeleteNotifiedDisks
    {
        const TRequestInfoPtr RequestInfo;
        const TVector<TDiskNotification> DiskIds;

        TDeleteNotifiedDisks(
                TRequestInfoPtr requestInfo,
                TVector<TDiskNotification> diskIds)
            : RequestInfo(std::move(requestInfo))
            , DiskIds(std::move(diskIds))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // DeleteDiskStateUpdates
    //

    struct TDeleteDiskStateUpdates
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 MaxSeqNo;

        TDeleteDiskStateUpdates(
                TRequestInfoPtr requestInfo,
                ui64 maxSeqNo)
            : RequestInfo(std::move(requestInfo))
            , MaxSeqNo(maxSeqNo)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // UpdateAgentState
    //

    struct TUpdateAgentState
    {
        const TRequestInfoPtr RequestInfo;
        const TString AgentId;
        const NProto::EAgentState State;
        const TInstant StateTs;
        const TString Reason;
        const bool IsDisableAgentRequest;

        TVector<TString> AffectedDisks;
        NProto::TError Error;

        TUpdateAgentState(
                TRequestInfoPtr requestInfo,
                TString agentId,
                NProto::EAgentState state,
                TInstant stateTs,
                TString reason,
                bool isDisableAgentRequest)
            : RequestInfo(std::move(requestInfo))
            , AgentId(std::move(agentId))
            , State(state)
            , StateTs(stateTs)
            , Reason(std::move(reason))
            , IsDisableAgentRequest(isDisableAgentRequest)
        {}

        void Clear()
        {
            AffectedDisks.clear();
            Error.Clear();
        }
    };

    //
    // UpdateDeviceState
    //

    struct TUpdateDeviceState
    {
        const TRequestInfoPtr RequestInfo;
        const TString DeviceId;
        const NProto::EDeviceState State;
        const TInstant StateTs;
        const TString Reason;

        TString AffectedDisk;
        NProto::TError Error;

        TUpdateDeviceState(
                TRequestInfoPtr requestInfo,
                TString deviceId,
                NProto::EDeviceState state,
                TInstant stateTs,
                TString reason)
            : RequestInfo(std::move(requestInfo))
            , DeviceId(std::move(deviceId))
            , State(state)
            , StateTs(stateTs)
            , Reason(std::move(reason))
        {}

        void Clear()
        {
            AffectedDisk.clear();
            Error.Clear();
        }
    };

    //
    // ReplaceDevice
    //

    struct TReplaceDevice
    {
        const TRequestInfoPtr RequestInfo;
        const TString DiskId;
        const TString DeviceId;
        const TString DeviceReplacementId;
        const TInstant Timestamp;

        NProto::TError Error;

        TReplaceDevice(
                TRequestInfoPtr requestInfo,
                TString diskId,
                TString deviceId,
                TString deviceReplacementId,
                TInstant timestamp)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , DeviceId(std::move(deviceId))
            , DeviceReplacementId(std::move(deviceReplacementId))
            , Timestamp(timestamp)
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // UpdateCmsHostDeviceState
    //

    struct TUpdateCmsHostDeviceState
    {
        const TRequestInfoPtr RequestInfo;
        const TString Host;
        const TString Path;
        const NProto::EDeviceState State;
        const bool ShouldResumeDevice;
        const bool DryRun;

        NProto::TError Error;
        TVector<TString> AffectedDisks;
        TInstant TxTs;
        TDuration Timeout;

        TUpdateCmsHostDeviceState(
                TRequestInfoPtr requestInfo,
                TString host,
                TString path,
                NProto::EDeviceState state,
                bool shouldResumeDevice,
                bool dryRun)
            : RequestInfo(std::move(requestInfo))
            , Host(std::move(host))
            , Path(std::move(path))
            , State(state)
            , ShouldResumeDevice(shouldResumeDevice)
            , DryRun(dryRun)
        {}

        void Clear()
        {
            AffectedDisks.clear();
            Error.Clear();
        }
    };

    //
    // UpdateCmsHostDeviceState
    //

    struct TUpdateCmsHostState
    {
        const TRequestInfoPtr RequestInfo;
        const TString Host;
        const NProto::EAgentState State;
        const bool DryRun;

        NProto::TError Error;
        TVector<TString> AffectedDisks;
        TInstant TxTs;
        TDuration Timeout;

        TUpdateCmsHostState(
                TRequestInfoPtr requestInfo,
                TString host,
                NProto::EAgentState state,
                bool dryRun)
            : RequestInfo(std::move(requestInfo))
            , Host(std::move(host))
            , State(state)
            , DryRun(dryRun)
        {}

        void Clear()
        {
            AffectedDisks.clear();
            Error.Clear();
        }
    };

    //
    // PurgeHostCms
    //

    struct TPurgeHostCms
    {
        const TRequestInfoPtr RequestInfo;
        const TString Host;
        const bool DryRun;

        NProto::TError Error;
        TVector<TString> AffectedDisks;

        TPurgeHostCms(
                TRequestInfoPtr requestInfo,
                TString host,
                bool dryRun)
            : RequestInfo(std::move(requestInfo))
            , Host(std::move(host))
            , DryRun(dryRun)
        {}

        void Clear()
        {
            AffectedDisks.clear();
            Error.Clear();
        }
    };

    //
    // WritableState
    //

    struct TWritableState
    {
        const TRequestInfoPtr RequestInfo;
        const bool State;

        NProto::TError Error;

        TWritableState(
                TRequestInfoPtr requestInfo,
                bool state)
            : RequestInfo(std::move(requestInfo))
            , State(state)
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // FinishMigration
    //

    struct TFinishMigration
    {
        using TMigrations = google::protobuf::RepeatedPtrField<
            NProto::TDeviceMigrationIds>;

        const TRequestInfoPtr RequestInfo;

        const TString DiskId;
        const TMigrations Migrations;
        const TInstant Timestamp;

        NProto::TError Error;

        TFinishMigration(
                TRequestInfoPtr requestInfo,
                TString diskId,
                TMigrations migrations,
                TInstant timestamp)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , Migrations(std::move(migrations))
            , Timestamp(timestamp)
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // StartMigration
    //

    struct TStartMigration
    {
        const TRequestInfoPtr RequestInfo;

        explicit TStartMigration(
                TRequestInfoPtr requestInfo)
            : RequestInfo(std::move(requestInfo))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // StartForceMigration
    //

    struct TStartForceMigration
    {
        const TRequestInfoPtr RequestInfo;

        const TString SourceDiskId;
        const TString SourceDeviceId;
        const TString TargetDeviceId;

        NProto::TError Error;

        TStartForceMigration(
                TRequestInfoPtr requestInfo,
                TString sourceDiskId,
                TString sourceDeviceId,
                TString targetDeviceId)
            : RequestInfo(std::move(requestInfo))
            , SourceDiskId(std::move(sourceDiskId))
            , SourceDeviceId(std::move(sourceDeviceId))
            , TargetDeviceId(std::move(targetDeviceId))
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // UpdateAgentState
    //

    struct TMarkDiskForCleanup
    {
        const TRequestInfoPtr RequestInfo;
        const TString DiskId;

        NProto::TError Error;

        TMarkDiskForCleanup(
                TRequestInfoPtr requestInfo,
                TString diskId)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // BackupDiskRegistryState
    //

    struct TBackupDiskRegistryState
        : TLoadState
    {
        const TString BackupFilePath;

        TBackupDiskRegistryState() = default;
        TBackupDiskRegistryState(
                TRequestInfoPtr requestInfo,
                TString backupFilePath)
            : TLoadState{std::move(requestInfo)}
            , BackupFilePath{std::move(backupFilePath)}
        {}
    };

    //
    // RestoreDiskRegistryState
    //

    struct TRestoreDiskRegistryState
    {
        const TRequestInfoPtr RequestInfo;

        TDiskRegistryStateSnapshot NewState;
        TDiskRegistryStateSnapshot CurrentState;

        TRestoreDiskRegistryState() = default;

        TRestoreDiskRegistryState(
                TRequestInfoPtr requestInfo,
                TDiskRegistryStateSnapshot newState)
            : RequestInfo{std::move(requestInfo)}
            , NewState{std::move(newState)}
        {}

        void Clear()
        {
            CurrentState.Clear();
        }
    };

    //
    // RestoreDiskRegistryPart
    //

    struct TRestoreDiskRegistryPart
    {
        using TFunction = std::function<void(TDiskRegistryDatabase&)>;

        const TRequestInfoPtr RequestInfo;
        const TRequestInfoPtr PartRequestInfo;

        TQueue<TFunction> Operations;

        TRestoreDiskRegistryPart() = default;

        TRestoreDiskRegistryPart(
                TRequestInfoPtr requestInfo,
                TRequestInfoPtr partRequestInfo,
                TQueue<TFunction> operations)
            : RequestInfo{std::move(requestInfo)}
            , PartRequestInfo{std::move(partRequestInfo)}
            , Operations{std::move(operations)}
        {}

        void Clear()
        {}
    };

    //
    // DeleteUserNotification
    //

    struct TDeleteUserNotifications
    {
        const TRequestInfoPtr RequestInfo;
        const TVector<TUserNotificationKey> Notifications;

        TDeleteUserNotifications(
                TRequestInfoPtr requestInfo,
                TVector<TUserNotificationKey> notifications)
            : RequestInfo(std::move(requestInfo))
            , Notifications(std::move(notifications))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // SetUserId
    //

    struct TSetUserId
    {
        const TRequestInfoPtr RequestInfo;
        const TString DiskId;
        const TString UserId;

        NProto::TError Error;

        TSetUserId(
                TRequestInfoPtr requestInfo,
                TString diskId,
                TString userId)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , UserId(std::move(userId))
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // FinishVolumeConfigUpdate
    //

    struct TFinishVolumeConfigUpdate
    {
        const TRequestInfoPtr RequestInfo;
        const TString DiskId;

        TFinishVolumeConfigUpdate(
                TRequestInfoPtr requestInfo,
                TString diskId)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // UpdateDiskBlockSize
    //

    struct TUpdateDiskBlockSize
    {
        const TRequestInfoPtr RequestInfo;

        const TString DiskId;
        const ui32 BlockSize;
        const bool Force;

        NProto::TError Error;

        TUpdateDiskBlockSize(
                TRequestInfoPtr requestInfo,
                TString diskId,
                ui32 blockSize,
                bool force)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , BlockSize(blockSize)
            , Force(force)
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // UpdateDiskReplicaCount
    //

    struct TUpdateDiskReplicaCount
    {
        const TRequestInfoPtr RequestInfo;

        const TString MasterDiskId;
        const ui32 ReplicaCount;

        NProto::TError Error;

        TUpdateDiskReplicaCount(
                TRequestInfoPtr requestInfo,
                TString masterDiskId,
                ui32 replicaCount)
            : RequestInfo(std::move(requestInfo))
            , MasterDiskId(std::move(masterDiskId))
            , ReplicaCount(replicaCount)
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // MarkReplacementDevice
    //

    struct TMarkReplacementDevice
    {
        const TRequestInfoPtr RequestInfo;

        const TString DiskId;
        const TString DeviceId;
        const bool IsReplacement;

        NProto::TError Error;

        TMarkReplacementDevice(
                TRequestInfoPtr requestInfo,
                TString diskId,
                TString deviceId,
                bool isReplacement)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , DeviceId(std::move(deviceId))
            , IsReplacement(isReplacement)
        {}

        void Clear()
        {
            Error.Clear();
        }

        TString ToString() const
        {
            return TStringBuilder()
                << "DiskId=" << DiskId
                << " DeviceId=" << DeviceId
                << " IsReplacement=" << IsReplacement;
        }
    };

    //
    // SuspendDevice
    //

    struct TSuspendDevice
    {
        const TRequestInfoPtr RequestInfo;

        const TString DeviceId;

        NProto::TError Error;

        TSuspendDevice(
                TRequestInfoPtr requestInfo,
                TString deviceId)
            : RequestInfo(std::move(requestInfo))
            , DeviceId(std::move(deviceId))
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // ResumeDevices
    //

    struct TResumeDevices
    {
        const TRequestInfoPtr RequestInfo;

        const TVector<TString> DeviceIds;

        TResumeDevices(
                TRequestInfoPtr requestInfo,
                TVector<TString> deviceIds)
            : RequestInfo(std::move(requestInfo))
            , DeviceIds(std::move(deviceIds))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // UpdatePlacementGroupSettings
    //

    struct TUpdatePlacementGroupSettings
    {
        const TRequestInfoPtr RequestInfo;
        const TString GroupId;
        const ui32 ConfigVersion;
        const NProto::TPlacementGroupSettings Settings;

        NProto::TError Error;

        explicit TUpdatePlacementGroupSettings(
                TRequestInfoPtr requestInfo,
                TString groupId,
                ui32 configVersion,
                NProto::TPlacementGroupSettings settings)
            : RequestInfo(std::move(requestInfo))
            , GroupId(std::move(groupId))
            , ConfigVersion(configVersion)
            , Settings(std::move(settings))
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // CreateDiskFromDevices
    //

    struct TCreateDiskFromDevices
    {
        const TRequestInfoPtr RequestInfo;

        const bool Force;
        const TString DiskId;
        const ui32 BlockSize;
        const NProto::EStorageMediaKind MediaKind;
        const TVector<NProto::TDeviceConfig> Devices;

        NProto::TError Error;
        ui64 LogicalBlockCount = 0;

        TCreateDiskFromDevices(
                TRequestInfoPtr requestInfo,
                bool force,
                TString diskId,
                ui32 blockSize,
                NProto::EStorageMediaKind mediaKind,
                TVector<NProto::TDeviceConfig> devices)
            : RequestInfo(std::move(requestInfo))
            , Force(force)
            , DiskId(std::move(diskId))
            , BlockSize(blockSize)
            , MediaKind(mediaKind)
            , Devices(std::move(devices))
        {}

        void Clear()
        {
            Error.Clear();
            LogicalBlockCount = 0;
        }

        TString ToString() const
        {
            TStringStream ss;

            ss << "Force=" << Force
                << " DiskId=" << DiskId
                << " BlockSize=" << BlockSize
                << " Devices=[ ";
            for (auto& d: Devices) {
                ss << "("
                    << d.GetAgentId() << " "
                    << d.GetDeviceName()
                    << ") ";
            }
            ss << "]";
            return ss.Str();
        }
    };

    //
    // ChangeDiskDevice
    //

    struct TChangeDiskDevice
    {
        const TRequestInfoPtr RequestInfo;

        const TString DiskId;
        const TString SourceDeviceId;
        const TString TargetDeviceId;

        NProto::TError Error;

        TChangeDiskDevice(
                TRequestInfoPtr requestInfo,
                TString diskId,
                TString sourceDeviceId,
                TString targetDeviceId)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , SourceDeviceId(std::move(sourceDeviceId))
            , TargetDeviceId(std::move(targetDeviceId))
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // ProcessAutomaticallyReplacedDevices
    //

    struct TProcessAutomaticallyReplacedDevices
    {
        const TRequestInfoPtr RequestInfo;
        const TInstant Until;
        ui32 ProcessedCount = 0;

        explicit TProcessAutomaticallyReplacedDevices(
                TRequestInfoPtr requestInfo,
                TInstant until)
            : RequestInfo(std::move(requestInfo))
            , Until(until)
        {}

        void Clear()
        {
            ProcessedCount = 0;
        }
    };

    //
    // UpdateDiskRegistryAgentListParams
    //

    struct TUpdateDiskRegistryAgentListParams
    {
        const TRequestInfoPtr RequestInfo;

        const TVector<TString> AgentIds;
        const NProto::TDiskRegistryAgentParams Params;

        NProto::TError Error;

        explicit TUpdateDiskRegistryAgentListParams(
                TRequestInfoPtr requestInfo,
                const TVector<TString>& agentIds,
                const NProto::TDiskRegistryAgentParams& params)
            : RequestInfo(std::move(requestInfo))
            , AgentIds(agentIds)
            , Params(params)
        {}

        void Clear()
        {
        }
    };

    //
    // CleanupExpiredAgentListParams
    //

    struct TCleanupExpiredAgentListParams
    {
        const TRequestInfoPtr RequestInfo;

        explicit TCleanupExpiredAgentListParams(
                TRequestInfoPtr requestInfo)
            : RequestInfo(std::move(requestInfo))
        {}

        void Clear()
        {
        }
    };

    //
    // SwitchAgentDisksToReadOnly
    //

    struct TSwitchAgentDisksToReadOnly
    {
        const TRequestInfoPtr RequestInfo;
        const TString AgentId;

        NProto::TError Error;

        TSwitchAgentDisksToReadOnly(
                TRequestInfoPtr requestInfo,
                TString agentId)
            : RequestInfo(std::move(requestInfo))
            , AgentId(std::move(agentId))
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // TAllocateCheckpoint
    //

    struct TAllocateCheckpoint
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TCheckpointReplica CheckpointReplica;

        NProto::TError Error;
        TString ShadowDiskId;

        TAllocateCheckpoint(
                TRequestInfoPtr requestInfo,
                NProto::TCheckpointReplica checkpointReplica)
            : RequestInfo(std::move(requestInfo))
            , CheckpointReplica(std::move(checkpointReplica))
        {}

        void Clear()
        {
            Error.Clear();
            ShadowDiskId.clear();
        }
    };

    //
    // TDeallocateCheckpoint
    //

    struct TDeallocateCheckpoint
    {
        const TRequestInfoPtr RequestInfo;
        const TString SourceDiskId;
        const TString CheckpointId;

        NProto::TError Error;
        TString ShadowDiskId;

        TDeallocateCheckpoint(
                TRequestInfoPtr requestInfo,
                TString sourceDiskId,
                TString checkpointId)
            : RequestInfo(std::move(requestInfo))
            , SourceDiskId(std::move(sourceDiskId))
            , CheckpointId(std::move(checkpointId))
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // SetCheckpointDataState
    //

    struct TSetCheckpointDataState
    {
        const TRequestInfoPtr RequestInfo;
        const TString SourceDiskId;
        const TString CheckpointId;
        const NProto::ECheckpointState CheckpointState;

        NProto::TError Error;

        TSetCheckpointDataState(
                TRequestInfoPtr requestInfo,
                TString sourceDiskId,
                TString checkpointId,
                NProto::ECheckpointState checkpointState)
            : RequestInfo(std::move(requestInfo))
            , SourceDiskId(std::move(sourceDiskId))
            , CheckpointId(std::move(checkpointId))
            , CheckpointState(checkpointState)
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // RemoveOrphanDevices
    //

    struct TRemoveOrphanDevices
    {
        const TVector<TString> OrphanDevices;

        explicit TRemoveOrphanDevices(TVector<TString> orphanDevices)
            : OrphanDevices(std::move(orphanDevices))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // AddOutdatedLaggingDevices
    //

    struct TAddOutdatedLaggingDevices
    {
        const TRequestInfoPtr RequestInfo;
        const TString DiskId;
        TVector<NProto::TLaggingDevice> VolumeOutdatedDevices;

        NProto::TError Error;

        TAddOutdatedLaggingDevices(
                TRequestInfoPtr requestInfo,
                TString diskId,
                TVector<NProto::TLaggingDevice> volumeLaggingDevices)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , VolumeOutdatedDevices(std::move(volumeLaggingDevices))
        {}

        void Clear()
        {
            Error.Clear();
        }
    };
};

}   // namespace NCloud::NBlockStore::NStorage
