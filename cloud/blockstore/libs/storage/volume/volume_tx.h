#pragma once

#include "volume_database.h"
#include "volume_state.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/protos/volume.pb.h>
#include <cloud/blockstore/libs/storage/protos_ydb/volume.pb.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_VOLUME_TRANSACTIONS(xxx, ...)                               \
    xxx(InitSchema,                     __VA_ARGS__)                           \
    xxx(LoadState,                      __VA_ARGS__)                           \
    xxx(UpdateConfig,                   __VA_ARGS__)                           \
    xxx(UpdateDevices,                  __VA_ARGS__)                           \
    xxx(UpdateMigrationState,           __VA_ARGS__)                           \
    xxx(AddClient,                      __VA_ARGS__)                           \
    xxx(RemoveClient,                   __VA_ARGS__)                           \
    xxx(ResetMountSeqNumber,            __VA_ARGS__)                           \
    xxx(ReadHistory,                    __VA_ARGS__)                           \
    xxx(CleanupHistory,                 __VA_ARGS__)                           \
    xxx(SavePartStats,                  __VA_ARGS__)                           \
    xxx(SaveCheckpointRequest,          __VA_ARGS__)                           \
    xxx(UpdateCheckpointRequest,        __VA_ARGS__)                           \
    xxx(UpdateShadowDiskState,          __VA_ARGS__)                           \
    xxx(UpdateUsedBlocks,               __VA_ARGS__)                           \
    xxx(WriteThrottlerState,            __VA_ARGS__)                           \
    xxx(UpdateResyncState,              __VA_ARGS__)                           \
    xxx(ToggleResync,                   __VA_ARGS__)                           \
    xxx(UpdateClientInfo,               __VA_ARGS__)                           \
    xxx(ResetStartPartitionsNeeded,     __VA_ARGS__)                           \
    xxx(UpdateVolumeParams,             __VA_ARGS__)                           \
    xxx(DeleteVolumeParams,             __VA_ARGS__)                           \
    xxx(ChangeStorageConfig,            __VA_ARGS__)                           \
    xxx(ReadMetaHistory,                __VA_ARGS__)                           \
    xxx(AddLaggingAgent,                __VA_ARGS__)                           \
    xxx(RemoveLaggingAgent,             __VA_ARGS__)                           \
    xxx(UpdateFollower,                 __VA_ARGS__)                           \
    xxx(RemoveFollower,                 __VA_ARGS__)                           \
    xxx(UpdateLeader,                   __VA_ARGS__)                           \
    xxx(RemoveLeader,                   __VA_ARGS__)                           \
// BLOCKSTORE_VOLUME_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

struct TTxVolume
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
        const TInstant OldestLogEntry;

        TMaybe<NProto::TVolumeMeta> Meta;
        TVector<TVolumeMetaHistoryItem> MetaHistory;
        TVector<TRuntimeVolumeParamsValue> VolumeParams;
        TMaybe<bool> StartPartitionsNeeded;
        THashMap<TString, TVolumeClientState> Clients;
        ui64 MountSeqNumber = 0;
        TVolumeMountHistorySlice MountHistory;
        TVector<TVolumeDatabase::TPartStats> PartStats;
        TVector<TCheckpointRequest> CheckpointRequests;
        THashMap<TString, TInstant> DeletedCheckpoints;
        TVector<ui64> OutdatedCheckpointRequestIds;
        TMaybe<TCompressedBitmap> UsedBlocks;
        TMaybe<TVolumeDatabase::TThrottlerStateInfo> ThrottlerStateInfo;
        TMaybe<NProto::TStorageServiceConfig> StorageConfig;
        TFollowerDisks FollowerDisks;
        TLeaderDisks LeaderDisks;

        explicit TLoadState(TInstant oldestLogEntry)
            : OldestLogEntry(oldestLogEntry)
        {}

        void Clear()
        {
            Meta.Clear();
            MetaHistory.clear();
            VolumeParams.clear();
            StartPartitionsNeeded.Clear();
            Clients.clear();
            MountSeqNumber = 0;
            MountHistory.Clear();
            PartStats.clear();
            CheckpointRequests.clear();
            DeletedCheckpoints.clear();
            OutdatedCheckpointRequestIds.clear();
            UsedBlocks.Clear();
            ThrottlerStateInfo.Clear();
            StorageConfig.Clear();
            FollowerDisks.clear();
            LeaderDisks.clear();
        }
    };

    //
    // UpdateConfig
    //

    struct TUpdateConfig
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 TxId;
        const NProto::TVolumeMeta Meta;
        const TVolumeMetaHistoryItem MetaHistoryItem;

        TUpdateConfig(
                TRequestInfoPtr requestInfo,
                ui64 txId,
                NProto::TVolumeMeta meta,
                TVolumeMetaHistoryItem metaHistoryItem)
            : RequestInfo(std::move(requestInfo))
            , TxId(txId)
            , Meta(std::move(meta))
            , MetaHistoryItem(std::move(metaHistoryItem))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // UpdateDevices
    //

    struct TUpdateDevices
    {
        const TRequestInfoPtr RequestInfo;
        TDevices Devices;
        TMigrations Migrations;
        TVector<TDevices> Replicas;
        TVector<TString> FreshDeviceIds;
        TVector<TString> RemovedLaggingDeviceIds;
        TVector<TString> UnavailableDeviceIds;
        NProto::EVolumeIOMode IOMode;
        TInstant IOModeTs;
        bool MuteIOErrors;

        bool LiteReallocation = false;
        TVector<NProto::TDeviceConfig> ReplacedDevices;

        TUpdateDevices(
                TDevices devices,
                TMigrations migrations,
                TVector<TDevices> replicas,
                TVector<TString> freshDeviceIds,
                TVector<TString> removedLaggingDeviceIds,
                TVector<TString> unavailableDeviceIds,
                NProto::EVolumeIOMode ioMode,
                TInstant ioModeTs,
                bool muteIOErrors)
            : TUpdateDevices(
                  TRequestInfoPtr(),
                  std::move(devices),
                  std::move(migrations),
                  std::move(replicas),
                  std::move(freshDeviceIds),
                  std::move(removedLaggingDeviceIds),
                  std::move(unavailableDeviceIds),
                  ioMode,
                  ioModeTs,
                  muteIOErrors)
        {}

        TUpdateDevices(
                TRequestInfoPtr requestInfo,
                TDevices devices,
                TMigrations migrations,
                TVector<TDevices> replicas,
                TVector<TString> freshDeviceIds,
                TVector<TString> removedLaggingDeviceIds,
                TVector<TString> unavailableDeviceIds,
                NProto::EVolumeIOMode ioMode,
                TInstant ioModeTs,
                bool muteIOErrors)
            : RequestInfo(std::move(requestInfo))
            , Devices(std::move(devices))
            , Migrations(std::move(migrations))
            , Replicas(std::move(replicas))
            , FreshDeviceIds(std::move(freshDeviceIds))
            , RemovedLaggingDeviceIds(std::move(removedLaggingDeviceIds))
            , UnavailableDeviceIds(std::move(unavailableDeviceIds))
            , IOMode(ioMode)
            , IOModeTs(ioModeTs)
            , MuteIOErrors(muteIOErrors)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // UpdateMigrationState
    //

    struct TUpdateMigrationState
    {
        const TRequestInfoPtr RequestInfo;
        ui64 MigrationIndex;

        TUpdateMigrationState(TRequestInfoPtr requestInfo, ui64 migrationIndex)
            : RequestInfo(std::move(requestInfo))
            , MigrationIndex(migrationIndex)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // AddClient
    //

    struct TAddClient
    {
        const TRequestInfoPtr RequestInfo;
        const TString DiskId;
        const NActors::TActorId PipeServerActorId;
        NProto::TVolumeClientInfo Info;

        TInstant WriterLastActivityTimestamp;
        bool WriterChanged = false;
        NProto::TError Error;
        bool ForceTabletRestart = false;

        TAddClient(
                TRequestInfoPtr requestInfo,
                TString diskId,
                const NActors::TActorId& pipeServerActorId,
                NProto::TVolumeClientInfo info)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , PipeServerActorId(pipeServerActorId)
            , Info(std::move(info))
        {}

        void Clear()
        {
            WriterLastActivityTimestamp = {};
            WriterChanged = false;
            Error.Clear();
            ForceTabletRestart = false;
        }
    };

    //
    // RemoveClient
    //

    struct TRemoveClient
    {
        const TRequestInfoPtr RequestInfo;
        const TString DiskId;
        const NActors::TActorId PipeServerActorId;
        const TString ClientId;
        const bool IsMonRequest;

        NProto::TError Error;

        TRemoveClient(
                TRequestInfoPtr requestInfo,
                TString diskId,
                const NActors::TActorId& pipeServerActorId,
                TString clientId,
                bool isMonRequest)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , PipeServerActorId(pipeServerActorId)
            , ClientId(std::move(clientId))
            , IsMonRequest(isMonRequest)
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // Reset MountSeqNumber
    //

    struct TResetMountSeqNumber
    {
        const TRequestInfoPtr RequestInfo;
        const TString ClientId;
        TMaybe<NProto::TVolumeClientInfo> ClientInfo;

        TResetMountSeqNumber(
                TRequestInfoPtr requestInfo,
                TString clientId)
            : RequestInfo(std::move(requestInfo))
            , ClientId(std::move(clientId))
        {}

        void Clear()
        {
            ClientInfo.Clear();
        }
    };

    //
    // Read History
    //

    struct TReadHistory
    {
        const TRequestInfoPtr RequestInfo;
        const THistoryLogKey Ts;
        const TInstant OldestTs;
        const size_t RecordCount;
        const bool MonRequest;

        TVolumeMountHistorySlice History;

        TReadHistory(
                TRequestInfoPtr requestInfo,
                THistoryLogKey ts,
                TInstant oldestTs,
                size_t recordCount,
                bool monRequest)
            : RequestInfo(std::move(requestInfo))
            , Ts(ts)
            , OldestTs(oldestTs)
            , RecordCount(recordCount)
            , MonRequest(monRequest)
        {}

        void Clear()
        {
            History.Clear();
        }
    };

    //
    // Cleanup History
    //

    struct TCleanupHistory
    {
        const TRequestInfoPtr RequestInfo;
        const TInstant Key;
        const ui32 ItemsToRemove;

        TVector<THistoryLogKey> OutdatedHistory;

        TCleanupHistory(
                TRequestInfoPtr requestInfo,
                TInstant key,
                ui32 itemsToRemove)
            : RequestInfo(std::move(requestInfo))
            , Key(key)
            , ItemsToRemove(itemsToRemove)
        {}

        void Clear()
        {
            OutdatedHistory.clear();
        }
    };

    //
    // Read Meta History
    //

    struct TReadMetaHistory
    {
        const TRequestInfoPtr RequestInfo;

        TVector<TVolumeMetaHistoryItem> MetaHistory;

        explicit TReadMetaHistory(TRequestInfoPtr requestInfo)
            : RequestInfo(std::move(requestInfo))
        {}

        void Clear()
        {
            MetaHistory.clear();
        }
    };

    //
    // SavePartStats
    //

    struct TSavePartStats
    {
        const TRequestInfoPtr RequestInfo;
        const TVolumeDatabase::TPartStats PartStats;

        TSavePartStats(
                TRequestInfoPtr requestInfo,
                TVolumeDatabase::TPartStats partStats)
            : RequestInfo(std::move(requestInfo))
            , PartStats(std::move(partStats))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // SaveCheckpointRequest
    //

    struct TSaveCheckpointRequest
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 RequestId;

        TSaveCheckpointRequest(
                TRequestInfoPtr requestInfo,
                ui64 requestId)
            : RequestInfo(std::move(requestInfo))
            , RequestId(requestId)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // UpdateCheckpointRequest
    //

    struct TUpdateCheckpointRequest
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 RequestId;
        const bool Completed;
        const TString ShadowDiskId;
        const EShadowDiskState ShadowDiskState;
        const std::optional<TString> ErrorMessage;

        TUpdateCheckpointRequest(
                TRequestInfoPtr requestInfo,
                ui64 requestId,
                bool completed,
                TString shadowDiskId,
                EShadowDiskState shadowDiskState,
                std::optional<TString> errorMessage)
            : RequestInfo(std::move(requestInfo))
            , RequestId(requestId)
            , Completed(completed)
            , ShadowDiskId(std::move(shadowDiskId))
            , ShadowDiskState(shadowDiskState)
            , ErrorMessage(std::move(errorMessage))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // UpdateShadowDiskRequest
    //

    struct TUpdateShadowDiskState
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 RequestId;
        const EShadowDiskState ShadowDiskState;
        const ui64 ProcessedBlockCount;

        TUpdateShadowDiskState(
                TRequestInfoPtr requestInfo,
                ui64 requestId,
                EShadowDiskState shadowDiskState,
                ui64 processedBlockCount)
            : RequestInfo(std::move(requestInfo))
            , RequestId(requestId)
            , ShadowDiskState(shadowDiskState)
            , ProcessedBlockCount(processedBlockCount)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // UpdateUsedBlocks
    //

    struct TUpdateUsedBlocks
    {
        const TRequestInfoPtr RequestInfo;
        const TVector<TBlockRange64> Ranges;

        TUpdateUsedBlocks(
                TRequestInfoPtr requestInfo,
                TVector<TBlockRange64> ranges)
            : RequestInfo(std::move(requestInfo))
            , Ranges(std::move(ranges))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // WriteThrottlerState
    //

    struct TWriteThrottlerState
    {
        const TRequestInfoPtr RequestInfo;
        const TVolumeDatabase::TThrottlerStateInfo StateInfo;

        TWriteThrottlerState(
                TRequestInfoPtr requestInfo,
                TVolumeDatabase::TThrottlerStateInfo stateInfo)
            : RequestInfo(std::move(requestInfo))
            , StateInfo(stateInfo)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // UpdateResyncState
    //

    struct TUpdateResyncState
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 ResyncIndex;

        TUpdateResyncState(TRequestInfoPtr requestInfo, ui64 resyncIndex)
            : RequestInfo(std::move(requestInfo))
            , ResyncIndex(resyncIndex)
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // ToggleResync
    //

    struct TToggleResync
    {
        const TRequestInfoPtr RequestInfo;
        const bool ResyncEnabled;
        const bool AlertResyncChecksumMismatch;
        bool ResyncWasNeeded = false;

        TToggleResync(
                TRequestInfoPtr requestInfo,
                bool resyncEnabled,
                bool alertResyncChecksumMismatch)
            : RequestInfo(std::move(requestInfo))
            , ResyncEnabled(resyncEnabled)
            , AlertResyncChecksumMismatch(alertResyncChecksumMismatch)
        {}

        void Clear()
        {
            ResyncWasNeeded = false;
        }
    };

    //
    // UpdateClientInfo
    //

    struct TUpdateClientInfo
    {
        const TRequestInfoPtr RequestInfo;
        const TString ClientId;

        TUpdateClientInfo(TRequestInfoPtr requestInfo, TString clientId)
            : RequestInfo(std::move(requestInfo))
            , ClientId(std::move(clientId))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // ResetStartPartitionsNeeded
    //

    struct TResetStartPartitionsNeeded
    {
        const TRequestInfoPtr RequestInfo;
        const ui64 PartitionTabletId;

        TResetStartPartitionsNeeded(
                TRequestInfoPtr requestInfo, ui64 PartitionTabletId)
            : RequestInfo(std::move(requestInfo))
            , PartitionTabletId(std::move(PartitionTabletId))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // UpdateVolumeParams
    //

    struct TUpdateVolumeParams
    {
        const TRequestInfoPtr RequestInfo;
        const THashMap<TString, TRuntimeVolumeParamsValue> VolumeParams;

        TUpdateVolumeParams(
                TRequestInfoPtr requestInfo,
                THashMap<TString, TRuntimeVolumeParamsValue> volumeParams)
            : RequestInfo(std::move(requestInfo))
            , VolumeParams(std::move(volumeParams))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // DeleteVolumeParams
    //

    struct TDeleteVolumeParams
    {
        const TRequestInfoPtr RequestInfo;
        const TVector<TString> Keys;

        TDeleteVolumeParams(
                TRequestInfoPtr requestInfo,
                TVector<TString> keys)
            : RequestInfo(std::move(requestInfo))
            , Keys(std::move(keys))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // ChangeStorageConfig
    //

    struct TChangeStorageConfig
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TStorageServiceConfig StorageConfigNew;
        const bool MergeWithStorageConfigFromVolumeDB;

        TMaybe<NProto::TStorageServiceConfig> StorageConfigFromDB;
        NProto::TStorageServiceConfig ResultStorageConfig;

        TChangeStorageConfig(
            TRequestInfoPtr requestInfo,
            NProto::TStorageServiceConfig storageConfig,
            bool mergeWithStorageConfigFromVolumeDB)
            : RequestInfo(std::move(requestInfo))
            , StorageConfigNew(std::move(storageConfig))
            , MergeWithStorageConfigFromVolumeDB(
                mergeWithStorageConfigFromVolumeDB)
        {}

        void Clear()
        {
            StorageConfigFromDB.Clear();
            ResultStorageConfig.Clear();
        }
    };

    //
    // AddLaggingAgent
    //

    struct TAddLaggingAgent
    {
        const TRequestInfoPtr RequestInfo;
        const NProto::TLaggingAgent Agent;

        NProto::TError Error;

        TAddLaggingAgent(
                TRequestInfoPtr requestInfo,
                NProto::TLaggingAgent agent)
            : RequestInfo(std::move(requestInfo))
            , Agent(std::move(agent))
        {}

        void Clear()
        {
            Error.Clear();
        }
    };

    //
    // RemoveLaggingAgent
    //

    struct TRemoveLaggingAgent
    {
        const TRequestInfoPtr RequestInfo;
        const TString AgentId;

        NProto::TLaggingAgent RemovedLaggingAgent;
        bool ShouldStartResync = false;

        TRemoveLaggingAgent(TRequestInfoPtr requestInfo, TString agentId)
            : RequestInfo(std::move(requestInfo))
            , AgentId(std::move(agentId))
        {}

        void Clear()
        {
            RemovedLaggingAgent.Clear();
            ShouldStartResync = false;
        }
    };

    //
    // UpdateFollower
    //

    struct TUpdateFollower
    {
        const TRequestInfoPtr RequestInfo;
        const TFollowerDiskInfo FollowerInfo;

        TUpdateFollower(
            TRequestInfoPtr requestInfo,
            TFollowerDiskInfo followerInfo)
            : RequestInfo(std::move(requestInfo))
            , FollowerInfo(std::move(followerInfo))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // RemoveFollower
    //

    struct TRemoveFollower
    {
        const TRequestInfoPtr RequestInfo;
        const TLeaderFollowerLink Link;

        TRemoveFollower(TRequestInfoPtr requestInfo, TLeaderFollowerLink link)
            : RequestInfo(std::move(requestInfo))
            , Link(std::move(link))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // UpdateLeader
    //

    struct TUpdateLeader
    {
        const TRequestInfoPtr RequestInfo;
        const TLeaderDiskInfo Leader;

        TUpdateLeader(TRequestInfoPtr requestInfo, TLeaderDiskInfo leader)
            : RequestInfo(std::move(requestInfo))
            , Leader(std::move(leader))
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // RemoveLeader
    //

    struct TRemoveLeader
    {
        const TRequestInfoPtr RequestInfo;
        const TLeaderFollowerLink Link;

        TRemoveLeader(TRequestInfoPtr requestInfo, TLeaderFollowerLink link)
            : RequestInfo(std::move(requestInfo))
            , Link(std::move(link))
        {}

        void Clear()
        {
            // nothing to do
        }
    };
};

}   // namespace NCloud::NBlockStore::NStorage
