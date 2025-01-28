#pragma once

#include "partition_info.h"
#include "public.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/metrics.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/public.h>
#include <cloud/blockstore/libs/storage/protos_ydb/volume.pb.h>
#include <cloud/blockstore/libs/storage/volume/model/client_state.h>
#include <cloud/blockstore/libs/storage/volume/model/checkpoint_light.h>
#include <cloud/blockstore/libs/storage/volume/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/volume/model/meta.h>
#include <cloud/blockstore/libs/storage/volume/model/volume_params.h>
#include <cloud/blockstore/libs/storage/volume/model/volume_throttling_policy.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/set.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TDevices = google::protobuf::RepeatedPtrField<NProto::TDeviceConfig>;
using TMigrations = google::protobuf::RepeatedPtrField<NProto::TDeviceMigration>;

////////////////////////////////////////////////////////////////////////////////

struct THistoryLogKey
{
    TInstant Timestamp;
    ui64 SeqNo = 0;

    THistoryLogKey() = default;

    THistoryLogKey(TInstant timestamp, ui64 seqNo)
        : Timestamp(timestamp)
        , SeqNo(seqNo)
    {}

    explicit THistoryLogKey(TInstant timestamp)
        : THistoryLogKey(timestamp, 0)
    {}

    bool operator == (const THistoryLogKey& rhs) const;
    bool operator != (const THistoryLogKey& rhs) const;
    bool operator < (THistoryLogKey rhs) const;
    bool operator >= (THistoryLogKey rhs) const;
};

struct THistoryLogItem
{
    THistoryLogKey Key;
    NProto::TVolumeOperation Operation;

    THistoryLogItem() = default;

    THistoryLogItem(
            THistoryLogKey key,
            NProto::TVolumeOperation operation)
        : Key(key)
        , Operation(std::move(operation))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TPartitionStatInfo
{
    const TString DiskId;
    const ui64 TabletId;

    TPartitionDiskCountersPtr LastCounters = {};
    TPartitionDiskCounters CachedCounters;
    NProto::TCachedPartStats CachedCountersProto;
    ui64 LastSystemCpu = 0;
    ui64 LastUserCpu = 0;
    NBlobMetrics::TBlobLoadMetrics LastMetrics;

    TPartitionStatInfo(
            const TString& diskId,
            const ui64 tabletId,
            EPublishingPolicy countersPolicy,
            EHistogramCounterOptions histCounterOptions)
        : DiskId(diskId)
        , TabletId(tabletId)
        , CachedCounters(countersPolicy, histCounterOptions)
    {}
};

////////////////////////////////////////////////////////////////////////////////

ui64 ComputeBlockCount(const NProto::TVolumeMeta& meta);

////////////////////////////////////////////////////////////////////////////////

struct TVolumeMountHistorySlice
{
    TVector<THistoryLogItem> Items;
    std::optional<THistoryLogKey> NextOlderRecord;

    void Clear()
    {
        Items.clear();
        NextOlderRecord.reset();
    }

    bool HasMoreItems() const
    {
        return NextOlderRecord.has_value();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCachedVolumeMountHistory
{
private:
    const ui32 Capacity = 0;

    THistoryLogKey LastLogRecord;
    TDeque<THistoryLogItem> Items;
    std::optional<THistoryLogKey> NextOlderRecord;

public:
    TCachedVolumeMountHistory() = default;

    TCachedVolumeMountHistory(
        ui32 capacity,
        TVolumeMountHistorySlice records);

    void AddHistoryLogItem(THistoryLogKey key, NProto::TVolumeOperation op);

    const TDeque<THistoryLogItem>& GetItems() const
    {
        return Items;
    }

    bool HasMoreItems() const
    {
        return NextOlderRecord.has_value();
    }

    void CleanupHistoryIfNeeded(TInstant oldest);

    THistoryLogKey AllocateHistoryLogKey(TInstant timestamp);

    TVolumeMountHistorySlice GetSlice() const
    {
        return {
            .Items{Items.begin(), Items.end()},
            .NextOlderRecord = NextOlderRecord
            };
    }

    // for testing purposes
    const std::optional<THistoryLogKey> GetNextOlderRecord() const
    {
        return NextOlderRecord;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVolumeState
{
private:
    TStorageConfigPtr StorageConfig;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    NProto::TVolumeMeta Meta;
    TVector<TVolumeMetaHistoryItem> MetaHistory;
    const NProto::TPartitionConfig* Config;
    TRuntimeVolumeParams VolumeParams;
    ui64 BlockCount = 0;
    // only for mirrored disks
    THashSet<TString> FilteredFreshDeviceIds;

    TPartitionInfoList Partitions;
    TPartitionInfo::EState PartitionsState = TPartitionInfo::UNKNOWN;
    NActors::TActorId DiskRegistryBasedPartitionActor;
    TNonreplicatedPartitionConfigPtr NonreplicatedPartitionConfig;

    TVector<TPartitionStatInfo> PartitionStatInfos;

    THashMap<TString, TVolumeClientState> ClientInfosByClientId;
    TString ReadWriteAccessClientId;
    TString LocalMountClientId;

    THashMultiMap<NActors::TActorId, TString> ClientIdsByPipeServerId;

    TThrottlerConfig ThrottlerConfig;
    TVolumeThrottlingPolicy ThrottlingPolicy;

    TCachedVolumeMountHistory MountHistory;

    ui64 MountSeqNumber = 0;

    EStorageAccessMode StorageAccessMode = EStorageAccessMode::Default;
    bool ForceRepair = false;
    bool AcceptInvalidDiskAllocationResponse = false;
    bool RejectWrite = false;

    TCheckpointStore CheckpointStore;
    std::unique_ptr<TCheckpointLight> CheckpointLight;

    std::unique_ptr<TCompressedBitmap> UsedBlocks;
    bool TrackUsedBlocks = false;
    bool MaskUnusedBlocks = false;
    bool UseRdma = false;
    bool UseFastPath = false;
    bool UseRdmaForThisVolume = false;
    bool RdmaUnavailable = false;
    TDuration MaxTimedOutDeviceStateDuration;
    bool UseIntermediateWriteBuffer = false;

    bool UseMirrorResync = false;
    bool ForceMirrorResync = false;

    NProto::TError ReadWriteError;

    bool StartPartitionsNeeded = false;

    // The number of blocks that need to be migrated to complete the migration.
    std::optional<ui64> BlockCountToMigrate;
public:
    TVolumeState(
        TStorageConfigPtr storageConfig,
        TDiagnosticsConfigPtr diagnosticsConfig,
        NProto::TVolumeMeta meta,
        TVector<TVolumeMetaHistoryItem> metaHistory,
        TVector<TRuntimeVolumeParamsValue> volumeParams,
        TThrottlerConfig throttlerConfig,
        THashMap<TString, TVolumeClientState> infos,
        TCachedVolumeMountHistory mountHistory,
        TVector<TCheckpointRequest> checkpointRequests,
        bool startPartitionsNeeded);

    const NProto::TVolumeMeta& GetMeta() const
    {
        return Meta;
    }

    const TVector<TVolumeMetaHistoryItem>& GetMetaHistory() const
    {
        return MetaHistory;
    }

    const THashSet<TString>& GetFilteredFreshDevices() const
    {
        return FilteredFreshDeviceIds;
    }

    TInstant GetCreationTs() const
    {
        return TInstant::MicroSeconds(Meta.GetVolumeConfig().GetCreationTs());
    }

    const TRuntimeVolumeParams& GetVolumeParams() const;

    TRuntimeVolumeParams& GetVolumeParams();

    void UpdateMigrationIndexInMeta(ui64 migrationIndex)
    {
        Meta.SetMigrationIndex(migrationIndex);
    }

    void SetBlockCountToMigrate(
        std::optional<ui64> blockCountToMigrate)
    {
        BlockCountToMigrate = blockCountToMigrate;
    }

    std::optional<ui64> GetBlockCountToMigrate() const
    {
        return BlockCountToMigrate;
    }

    void UpdateResyncIndexInMeta(ui64 resyncIndex)
    {
        Meta.SetResyncIndex(resyncIndex);
    }

    void SetResyncNeededInMeta(bool resyncNeeded)
    {
        Meta.SetResyncNeeded(resyncNeeded);
        Meta.SetResyncIndex(0);
    }

    void UpdateFillSeqNumberInMeta(ui64 fillSeqNumber) {
        Meta.SetFillSeqNumber(fillSeqNumber);
    }

    void AddLaggingAgent(NProto::TLaggingAgent agent);
    std::optional<NProto::TLaggingAgent> RemoveLaggingAgent(
        const TString& agentId);
    [[nodiscard]] bool HasLaggingInReplica(ui32 replicaIndex) const;
    [[nodiscard]] THashSet<TString> GetLaggingDevices() const;

    void SetStartPartitionsNeeded(bool startPartitionsNeeded)
    {
        StartPartitionsNeeded = startPartitionsNeeded;
    }

    void ResetMeta(NProto::TVolumeMeta meta);
    void AddMetaHistory(TVolumeMetaHistoryItem meta);
    void ResetThrottlingPolicy(const NProto::TVolumePerformanceProfile& pp);
    void Reset();

    //
    // Config
    //

    const NProto::TPartitionConfig& GetConfig() const
    {
        return *Config;
    }

    const TString& GetDiskId() const
    {
        return Config->GetDiskId();
    }

    const TString& GetBaseDiskId() const
    {
        return Config->GetBaseDiskId();
    }

    const TString& GetBaseDiskCheckpointId() const
    {
        return Config->GetBaseDiskCheckpointId();
    }

    ui32 GetBlockSize() const
    {
        return Config->GetBlockSize();
    }

    ui64 GetBlocksCount() const
    {
        return BlockCount;
    }

    void FillDeviceInfo(NProto::TVolume& volume) const;

    bool IsDiskRegistryMediaKind() const;

    //
    // Partitions
    //

    TPartitionInfoList& GetPartitions()
    {
        return Partitions;
    }

    TPartitionInfo* GetPartition(ui64 tabletId);
    std::optional<ui32> FindPartitionIndex(NActors::TActorId owner) const;
    std::optional<ui64> FindPartitionTabletId(NActors::TActorId owner) const;

    //
    // State
    //

    TPartitionInfo::EState GetPartitionsState() const
    {
        return PartitionsState;
    }

    bool Ready()
    {
        return UpdatePartitionsState() == TPartitionInfo::READY;
    }

    void SetPartitionsState(TPartitionInfo::EState state);
    TPartitionInfo::EState UpdatePartitionsState();

    void SetReadWriteError(NProto::TError error)
    {
        ReadWriteError = std::move(error);
    }

    const auto& GetReadWriteError() const
    {
        return ReadWriteError;
    }

    TString GetPartitionsError() const;

    void SetDiskRegistryBasedPartitionActor(
        const NActors::TActorId& actor,
        TNonreplicatedPartitionConfigPtr config);

    const NActors::TActorId& GetDiskRegistryBasedPartitionActor() const
    {
        return DiskRegistryBasedPartitionActor;
    }

    const TNonreplicatedPartitionConfigPtr& GetNonreplicatedPartitionConfig() const
    {
        return NonreplicatedPartitionConfig;
    }

    //
    // PartitionStat
    //

    EPublishingPolicy CountersPolicy() const;

    TPartitionStatInfo&
    CreatePartitionStatInfo(const TString& diskId, ui64 tabletId);

    TPartitionStatInfo* GetPartitionStatInfoByTabletId(ui64 tabletId);

    TPartitionStatInfo* GetPartitionStatByDiskId(const TString& diskId);

    std::span<const TPartitionStatInfo> GetPartitionStatInfos() const
    {
        return PartitionStatInfos;
    }

    std::span<TPartitionStatInfo> GetPartitionStatInfos()
    {
        return PartitionStatInfos;
    }

    EStorageAccessMode GetStorageAccessMode() const
    {
        return ForceRepair ? EStorageAccessMode::Repair : StorageAccessMode;
    }

    bool GetRejectWrite() const
    {
        return RejectWrite;
    }

    bool GetAcceptInvalidDiskAllocationResponse() const
    {
        return AcceptInvalidDiskAllocationResponse;
    }

    bool GetMuteIOErrors() const;

    bool GetShouldStartPartitionsForGc(TInstant referenceTimestamp) const
    {
        return StartPartitionsNeeded &&
            !HasActiveClients(referenceTimestamp);
    }

    //
    // Light checkpoints
    //

    void StartCheckpointLight();

    void CreateCheckpointLight(TString checkpointId);

    void DeleteCheckpointLight(TString checkpointId);

    void StopCheckpointLight();

    bool HasCheckpointLight() const;

    NProto::TError FindDirtyBlocksBetweenLightCheckpoints(
        TString lowCheckpointId,
        TString highCheckpointId,
        const TBlockRange64& blockRange,
        TString* mask) const;

    void MarkBlocksAsDirtyInCheckpointLight(const TBlockRange64& blockRange);

    //
    // Clients
    //

    struct TAddClientResult
    {
        NProto::TError Error;
        TVector<TString> RemovedClientIds;
        bool ForceTabletRestart = false;

        TAddClientResult() = default;

        TAddClientResult(NProto::TError error)
            : Error(std::move(error))
        {}
    };

    TAddClientResult AddClient(
        const NProto::TVolumeClientInfo& info,
        const NActors::TActorId& pipeServerActorId = {},
        const NActors::TActorId& SenderActorId = {},
        TInstant referenceTimestamp = TInstant::Now());

    TInstant GetLastActivityTimestamp(const TString& clientId) const;
    void SetLastActivityTimestamp(const TString& clientId, TInstant ts);

    bool IsClientStale(
        const TString& clientId,
        TInstant referenceTimestamp) const;

    bool IsClientStale(
        const TVolumeClientState& clientInfo,
        TInstant referenceTimestamp) const;

    bool IsClientStale(
        const NProto::TVolumeClientInfo& clientInfo,
        TInstant referenceTimestamp) const;

    const NProto::TVolumeClientInfo* GetClient(const TString& clientId) const;

    NProto::TError RemoveClient(
        const TString& clientId,
        const NActors::TActorId& pipeServerActorId);

    bool HasClients() const
    {
        return !ClientInfosByClientId.empty();
    }

    bool HasActiveClients(TInstant referenceTimestamp) const;
    bool IsPreempted(NActors::TActorId selfId) const;

    const THashMap<TString, TVolumeClientState>& GetClients() const
    {
        return ClientInfosByClientId;
    }

    THashMap<TString, TVolumeClientState>& AccessClients()
    {
        return ClientInfosByClientId;
    }

    TString GetReadWriteAccessClientId() const
    {
        return ReadWriteAccessClientId;
    }

    TString GetLocalMountClientId() const
    {
        return LocalMountClientId;
    }

    //
    // Connected services
    //

    void SetServiceDisconnected(
        const NActors::TActorId& pipeServerActorId,
        TInstant disconnectTime);

    void UnmapClientFromPipeServerId(
        const NActors::TActorId& pipeServerActorId,
        const TString& clientId);

    const THashMultiMap<NActors::TActorId, TString>& GetPipeServerId2ClientId() const;

    TVector<NActors::TActorId> ClearPipeServerIds(TInstant ts);

    //
    // Throttling
    //

    TVolumeThrottlingPolicy& AccessThrottlingPolicy()
    {
        return ThrottlingPolicy;
    }

    const TVolumeThrottlingPolicy& GetThrottlingPolicy() const
    {
        return ThrottlingPolicy;
    }

    //
    // MountSeqNumber
    //

    ui64 GetMountSeqNumber() const
    {
        return MountSeqNumber;
    }

    void SetMountSeqNumber(ui64 mountSeqNumber)
    {
        MountSeqNumber = mountSeqNumber;
    }

    //
    // Volume operation log
    //

    THistoryLogItem LogAddClient(
        TInstant timestamp,
        const NProto::TVolumeClientInfo& add,
        const NProto::TError& error,
        const NActors::TActorId& pipeServer,
        const NActors::TActorId& senderId);
    THistoryLogItem LogRemoveClient(
        TInstant timestamp,
        const TString& clientId,
        const TString& reason,
        const NProto::TError& error);

    const TCachedVolumeMountHistory& GetMountHistory() const
    {
        return MountHistory;
    }

    TCachedVolumeMountHistory& AccessMountHistory()
    {
        return MountHistory;
    }

    //
    // Checkpoint request history
    //

    TCheckpointStore& GetCheckpointStore()
    {
        return CheckpointStore;
    }

    const TCheckpointStore& GetCheckpointStore() const
    {
        return CheckpointStore;
    }

    void SetCheckpointRequestFinished(
        const TCheckpointRequest& request,
        bool success,
        TString shadowDiskId,
        EShadowDiskState shadowDiskState);

    //
    // UsedBlocks
    //

    bool GetTrackUsedBlocks() const
    {
        return TrackUsedBlocks;
    }

    bool GetMaskUnusedBlocks() const
    {
        return MaskUnusedBlocks;
    }

    bool GetUseRdma() const
    {
        return UseRdma && !RdmaUnavailable;
    }

    bool GetUseRdmaForThisVolume() const
    {
        return UseRdmaForThisVolume;
    }

    bool GetUseFastPath() const
    {
        return UseFastPath;
    }

    void SetRdmaUnavailable()
    {
        RdmaUnavailable = true;
    }

    TDuration GetMaxTimedOutDeviceStateDuration() const
    {
        return MaxTimedOutDeviceStateDuration;
    }

    bool GetUseIntermediateWriteBuffer() const
    {
        return UseIntermediateWriteBuffer;
    }

    size_t GetUsedBlockCount() const
    {
        return UsedBlocks ? UsedBlocks->Count() : 0;
    }

    const TCompressedBitmap& GetUsedBlocks() const
    {
        Y_DEBUG_ABORT_UNLESS(UsedBlocks);
        return *UsedBlocks;
    }

    TCompressedBitmap& AccessUsedBlocks()
    {
        if (!UsedBlocks) {
            Y_ABORT_UNLESS(BlockCount);
            UsedBlocks = std::make_unique<TCompressedBitmap>(BlockCount);
        }

        return *UsedBlocks;
    }

    //
    // Mirror resync
    //

    bool IsMirrorResyncNeeded() const
    {
        if (!UseMirrorResync) {
            return false;
        }

        if (ForceMirrorResync) {
            return true;
        }

        return Meta.GetResyncNeeded();
    }

    TVector<NProto::TDeviceConfig> GetAllDevicesForAcquireRelease() const;

private:
    bool CanPreemptClient(
        const TString& oldClientId,
        TInstant referenceTimestamp,
        ui64 clientMountSeqNumber);

    bool CanAcceptClient(
        ui64 newFillSeqNumber,
        ui64 proposedFillGeneration);

    bool ShouldForceTabletRestart(const NProto::TVolumeClientInfo& info) const;

    THashSet<TString> MakeFilteredDeviceIds() const;

    [[nodiscard]] bool ShouldTrackUsedBlocks() const;
};

}   // namespace NCloud::NBlockStore::NStorage
