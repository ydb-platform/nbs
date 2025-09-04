#pragma once

#include "public.h"

#include "garbage_queue.h"
#include "part2_database.h"
#include "part2_schema.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/partition2.h>
#include <cloud/blockstore/libs/storage/core/compaction_map.h>
#include <cloud/blockstore/libs/storage/core/compaction_type.h>
#include <cloud/blockstore/libs/storage/core/request_buffer.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/core/tablet.h>
#include <cloud/blockstore/libs/storage/core/write_buffer_request.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/model/channel_permissions.h>
#include <cloud/blockstore/libs/storage/partition2/model/blob_index.h>
#include <cloud/blockstore/libs/storage/partition2/model/block_index.h>
#include <cloud/blockstore/libs/storage/partition2/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/partition2/model/fresh_blocks_inflight.h>
#include <cloud/blockstore/libs/storage/partition2/model/operation_status.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/alloc.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

#include <list>
#include <utility>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

struct IFreshBlockVisitor
{
    virtual ~IFreshBlockVisitor() = default;

    virtual void Visit(const TBlock& block, TStringBuf blockContent) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IMergedBlockVisitor
{
    virtual ~IMergedBlockVisitor() = default;

    virtual void Visit(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui16 blobOffset) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationState
{
private:
    EOperationStatus Status = EOperationStatus::Idle;
    TInstant Timestamp;
    ui64 Count = 0;

public:
    TInstant GetTimestamp() const
    {
        return Timestamp;
    }

    EOperationStatus GetStatus() const
    {
        return Status;
    }

    void SetStatus(EOperationStatus status)
    {
        Status = status;
        Timestamp = TInstant::Now();

        if (Status == EOperationStatus::Started) {
            ++Count;
        }
    }

    void Dump(IOutputStream& out) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TForcedCompactionState
{
    bool IsRunning = false;
    ui32 Progress = 0;
    ui32 RangeCount = 0;
    TString OperationId;
    TOperationState State;
};

////////////////////////////////////////////////////////////////////////////////

struct TForcedCleanupState
{
    bool IsRunning = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TChannelState
{
    EChannelPermissions Permissions = EChannelPermission::UserWritesAllowed
        | EChannelPermission::SystemWritesAllowed;
    double ApproximateFreeSpaceShare = 0;
    double FreeSpaceScore = 0;
    bool ReassignRequestedByBlobStorage = false;

    std::list<NActors::IActorPtr> IORequests;
    size_t IORequestsInFlight = 0;
    size_t IORequestsQueued = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TBackpressureFeatureConfig
{
    ui64 InputLimit = 0;
    ui64 InputThreshold = 0;
    double MaxValue = 0;
};

struct TBackpressureFeaturesConfig
{
    TBackpressureFeatureConfig CompactionScoreFeatureConfig;
    TBackpressureFeatureConfig FreshByteCountFeatureConfig;
    TBackpressureFeatureConfig CleanupQueueBytesFeatureConfig;
};

////////////////////////////////////////////////////////////////////////////////

struct TFreeSpaceConfig
{
    double ChannelFreeSpaceThreshold = 0;
    double ChannelMinFreeSpace = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TIndexCachingConfig
{
    // if zone request share becomes this factor times greater than average
    // zone request share, this zone's block lists should be loaded into memory
    // and this zone's index should be converted into TMixedIndex
    ui32 ConvertToMixedIndexFactor = 0;
    // if zone request share becomes lower than this factor times average
    // zone request share, this zone's block lists should be discarded from
    // memory and this zone's index should be converted into TRangeMap
    ui32 ConvertToRangeMapFactor = 0;
    // Approximate share of disk space covered by the blobs whose block lists
    // are cached
    double BlockListCacheSizeShare = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionState
{
private:
    NProto::TPartitionMeta Meta;

    const ui64 TabletId;
    const ui32 Generation;
    const ui32 MaxBlobSize;
    const ui32 MaxRangesPerBlob;
    const ICompactionPolicyPtr CompactionPolicy;
    const TBackpressureFeaturesConfig BPConfig;
    const TFreeSpaceConfig FreeSpaceConfig;
    const TIndexCachingConfig IndexCachingConfig;

public:
    TPartitionState(
        NProto::TPartitionMeta meta,
        ui64 tabletId,
        ui32 generation,
        ui32 channelCount,
        ui32 maxBlobSize,
        ui32 maxRangesPerBlob,
        EOptimizationMode optimizationMode,
        ICompactionPolicyPtr compactionPolicy,
        TBackpressureFeaturesConfig bpConfig,
        TFreeSpaceConfig freeSpaceConfig,
        TIndexCachingConfig indexCachingConfig,
        ui32 maxIORequestsInFlight = Max(),
        ui32 reassignChannelsPercentageThreshold = 0,
        ui32 reassignFreshChannelsPercentageThreshold = 100,
        ui32 reassignMixedChannelsPercentageThreshold = 100,
        bool reassignSystemChannelsImmediately = false,
        ui32 lastStep = 0);

private:
    bool LoadStateFinished = false;

public:
    void FinishLoadState()
    {
        LoadStateFinished = true;
    }

    bool IsLoadStateFinished() const
    {
        return LoadStateFinished;
    }

    //
    // Config
    //

private:
    NProto::TPartitionConfig& Config;

public:
    const NProto::TPartitionConfig& GetConfig() const
    {
        return Config;
    }

    const TString& GetBaseDiskId() const
    {
        return Config.GetBaseDiskId();
    }

    const TString& GetBaseDiskCheckpointId() const
    {
        return Config.GetBaseDiskCheckpointId();
    }

    ui32 GetBlockSize() const
    {
        return Config.GetBlockSize();
    }

    ui32 GetMaxBlocksInBlob() const
    {
        return CalculateMaxBlocksInBlob(MaxBlobSize, Config.GetBlockSize());
    }

    ui64 GetBlockCount() const
    {
        return Config.GetBlocksCount();
    }

    bool CheckBlockRange(const TBlockRange64& range) const;

    //
    // Commits
    //

private:
    ui32 LastStep = 0;
    ui64 LastDeletionId = 0;

public:
    ui64 GetLastCommitId() const
    {
        return MakeCommitId(Generation, LastStep);
    }

    ui64 GenerateCommitId()
    {
        if (LastStep == Max<ui32>()) {
            return InvalidCommitId;
        }
        return MakeCommitId(Generation, ++LastStep);
    }

    ui64 NextDeletionId()
    {
        return ++LastDeletionId;
    }

    //
    // Channels
    //

private:
    const ui32 MaxIORequestsInFlight;
    const ui32 ReassignChannelsPercentageThreshold;
    const ui32 ReassignFreshChannelsPercentageThreshold;
    const ui32 ReassignMixedChannelsPercentageThreshold;
    const bool ReassignSystemChannelsImmediately;

    TVector<TChannelState> Channels;
    TVector<ui32> FreshChannels;
    ui32 FreshChannelSelector = -1;
    TVector<ui32> MixedChannels;
    bool HaveSeparateMixedChannels = false;
    ui32 MixedChannelSelector = -1;
    TVector<ui32> MergedChannels;
    ui32 MergedChannelSelector = -1;
    double SystemChannelSpaceScoreSum = 0;
    double DataChannelSpaceScoreSum = 0;
    double FreshChannelSpaceScoreSum = 0;
    double BackpressureDiskSpaceScore = 1;
    ui32 ChannelCount = 0;
    ui32 DataChannelCount = 0;
    ui32 FreshChannelCount = 0;
    ui32 AlmostFullChannelCount = 0;

public:
    ui32 GetChannelCount() const
    {
        return ChannelCount;
    }

    EChannelDataKind GetChannelDataKind(ui32 channel) const;
    TVector<ui32> GetChannelsByKind(
        std::function<bool(EChannelDataKind)> predicate) const;

    bool UpdatePermissions(ui32 channel, EChannelPermissions permissions);
    bool CheckPermissions(ui32 channel, EChannelPermissions permissions) const;
    bool UpdateChannelFreeSpaceShare(ui32 channel, double share);
    bool CheckChannelFreeSpaceShare(ui32 channel) const;
    bool IsCompactionAllowed() const;
    bool IsWriteAllowed(EChannelPermissions permissions) const;
    void RegisterReassignRequestFromBlobStorage(ui32 channel);
    TVector<ui32> GetChannelsToReassign() const;
    TBackpressureReport CalculateCurrentBackpressure() const;
    ui32 GetAlmostFullChannelCount() const;

    void EnqueueIORequest(ui32 channel, NActors::IActorPtr requestActor);
    NActors::IActorPtr DequeueIORequest(ui32 channel);
    void CompleteIORequest(ui32 channel);
    ui32 GetIORequestsInFlight() const;
    ui32 GetIORequestsQueued() const;

    TPartialBlobId GenerateBlobId(
        EChannelDataKind kind,
        EChannelPermissions permissions,
        ui64 commitId,
        ui32 blobSize,
        ui32 blobIndex = 0);

    ui32 PickNextChannel(
        EChannelDataKind kind,
        EChannelPermissions permissions);

private:
    void InitChannels();

    TChannelState& GetChannel(ui32 channel);
    const TChannelState* GetChannel(ui32 channel) const;

    bool UpdateChannelFreeSpaceScore(TChannelState& channelState, ui32 channel);

    //
    // Fresh blocks
    //

private:
    TBlockIndex FreshBlocks;
    TRequestBuffer<TWriteBufferRequestData> WriteBuffer;

public:
    TBlockIndex& GetFreshBlocks()
    {
        return FreshBlocks;
    }

    TRequestBuffer<TWriteBufferRequestData>& GetWriteBuffer()
    {
        return WriteBuffer;
    }

    size_t GetFreshBlocksQueued() const
    {
        return WriteBuffer.GetWeight();
    }

    size_t GetFreshBlockCount() const
    {
        return FreshBlocks.GetBlockCount();
    }

    void InitFreshBlocks(const TVector<TOwningFreshBlock>& freshBlocks);

    void WriteFreshBlock(
        const TBlock& block,
        TBlockDataRef blockContent);

    bool DeleteFreshBlock(ui32 blockIndex, ui64 commitId);

    const TFreshBlock* FindFreshBlock(ui32 blockIndex, ui64 commitId) const;

    void FindFreshBlocks(
        ui64 commitId,
        const TBlockRange32& blockRange,
        IFreshBlockVisitor& visitor) const;

    void FindFreshBlocks(
        const TBlockRange32& blockRange,
        IFreshBlockVisitor& visitor) const;

    void FindFreshBlocks(IFreshBlockVisitor& visitor) const;

private:
    void FindFreshBlocks(
        const TVector<TFreshBlock>& blocks,
        IFreshBlockVisitor& visitor) const;

    //
    // Fresh blocks inflight
    //

private:
    TFreshBlocksInFlight FreshBlocksInFlight;

public:
    void AddFreshBlocksInFlight(TBlockRange32 blockRange, ui64 commitId)
    {
        FreshBlocksInFlight.AddBlockRange(blockRange, commitId);
    }

    void RemoveFreshBlocksInFlight(TBlockRange32 blockRange, ui64 commitId)
    {
        FreshBlocksInFlight.RemoveBlockRange(blockRange, commitId);
    }

    size_t GetFreshBlockCountInFlight() const
    {
        return FreshBlocksInFlight.Size();
    }

    bool HasFreshBlocksInFlightUntil(ui64 commitId) const
    {
        return FreshBlocksInFlight.HasBlocksUntil(commitId);
    }

    //
    // Fresh block updates
    //

private:
    TFreshBlockUpdates FreshBlockUpdates;

public:
    void SetFreshBlockUpdates(TFreshBlockUpdates updates);
    void ApplyFreshBlockUpdates();

    size_t GetFreshBlockUpdateCount() const;

    void AddFreshBlockUpdate(TPartitionDatabase& db, TFreshBlockUpdate update);
    void TrimFreshBlockUpdates(TPartitionDatabase& db);

    //
    // Flush
    //

private:
    struct TFlushContext
    {
        TRequestInfoPtr RequestInfo;
        const ui64 CommitId;

        TFlushContext(
                TRequestInfoPtr requestInfo,
                ui64 commitId)
            : RequestInfo(std::move(requestInfo))
            , CommitId(commitId)
        {}
    };

    TMaybe<TFlushContext> FlushContext;
    TOperationState FlushState;

public:
    TFlushContext& GetFlushContext()
    {
        Y_ABORT_UNLESS(FlushContext);
        return *FlushContext;
    }

    template <typename ...TArgs>
    void ConstructFlushContext(TArgs&& ...args)
    {
        Y_ABORT_UNLESS(!FlushContext);
        FlushContext.ConstructInPlace(std::forward<TArgs>(args)...);
    }

    void ResetFlushContext()
    {
        Y_ABORT_UNLESS(FlushContext);
        FlushContext.Clear();
    }

    EOperationStatus GetFlushStatus() const
    {
        return FlushState.GetStatus();
    }

    void SetFlushStatus(EOperationStatus status)
    {
        FlushState.SetStatus(status);
    }

    ui64 GetLastFlushCommitId() const
    {
        return Meta.GetLastFlushCommitId();
    }

    void SetLastFlushCommitId(ui64 commitId)
    {
        Meta.SetLastFlushCommitId(commitId);
    }

    //
    // TrimFreshLog
    //

private:
    TOperationState TrimFreshLogState;
    // Declare separate variable TrimFreshLogToCommitId instead of reusing
    // LastFlushCommitId, since LastFlushCommitId is updated
    // at TxAddBlobsExecute (tx has not been committed yet).
    // There is possible data loss if tablet restarts before
    // TxComplete and blocks got trimmed.
    ui64 TrimFreshLogToCommitId = 0;
    ui64 LastTrimFreshLogToCommitId = 0;
    TDuration TrimFreshLogTimeout;

public:
    EOperationStatus GetTrimFreshLogStatus() const
    {
        return TrimFreshLogState.GetStatus();
    }

    TOperationState& GetTrimFreshLogState()
    {
        return TrimFreshLogState;
    }

    TDuration GetTrimFreshLogTimeout()
    {
        return TrimFreshLogTimeout;
    }

    void RegisterTrimFreshLogError(const NProto::TError& error, ui64 tabletId);

    void RegisterTrimFreshLogSuccess();

    ui64 GetTrimFreshLogToCommitId() const
    {
        return TrimFreshLogToCommitId;
    }

    void SetTrimFreshLogToCommitId(ui64 commitId)
    {
        TrimFreshLogToCommitId = commitId;
    }

    ui64 GetLastTrimFreshLogToCommitId() const
    {
        return LastTrimFreshLogToCommitId;
    }

    void SetLastTrimFreshLogToCommitId(ui64 commitId)
    {
        LastTrimFreshLogToCommitId = commitId;
    }


    //
    // Merged blobs
    //

private:
    mutable TBlobIndex Blobs;

public:
    TBlobIndex& GetBlobs()
    {
        return Blobs;
    }

    size_t GetMixedBlobCount() const
    {
        return 0;
    }

    size_t GetMergedBlobCount() const
    {
        return Blobs.GetGlobalBlobCount() + Blobs.GetZoneBlobCount();
    }

    void WriteBlob(
        TPartitionDatabase& db,
        const TPartialBlobId& blobId,
        TVector<TBlock>& blocks);

    bool UpdateBlob(
        TPartitionDatabase& db,
        const TPartialBlobId& blobId,
        bool fastPathAllowed,
        TVector<TBlock>& blocks);

    bool DeleteBlob(
        TPartitionDatabase& db,
        const TPartialBlobId& blobId);

    bool FindBlockList(
        TPartitionDatabase& db,
        ui32 zoneHint,
        const TPartialBlobId& blobId,
        TMaybe<TBlockList>& blocks) const;

    bool UpdateIndexStructures(
        TPartitionDatabase& db,
        TInstant now,
        const TBlockRange32& blockRange,
        TVector<TBlockRange64>* convertedToMixedIndex = nullptr,
        TVector<TBlockRange64>* convertedToRangeMap = nullptr);

    void UpdateIndex(
        const TVector<TPartitionDatabase::TBlobMeta>& blobs,
        const TVector<TBlobUpdate>& blobUpdates,
        const TVector<TPartitionDatabase::TBlobGarbage>& blobGarbage);

    void UpdateIndex(
        ui32 z,
        const TVector<TPartitionDatabase::TBlobMeta>& blobs,
        const TVector<TBlobUpdate>& blobUpdates,
        const TVector<TPartitionDatabase::TBlobGarbage>& blobGarbage);

    bool InitIndex(
        TPartitionDatabase& db,
        const TBlockRange32& blockRange);

    bool IsIndexInitialized(const TBlockRange32& blockRange);

    //
    // Blob updates by fresh
    //

private:
    TBlobUpdatesByFresh BlobUpdatesByFresh;

public:
    void SetBlobUpdatesByFresh(TBlobUpdatesByFresh blobUpdatesByFresh);
    void ApplyBlobUpdatesByFresh();
    void AddBlobUpdateByFresh(TBlobUpdate blobUpdate);
    void MoveBlobUpdatesByFreshToDb(TPartitionDatabase& db);

    //
    // Merged blocks
    //

public:
    size_t GetMixedBlockCount() const
    {
        return 0;
    }

    size_t GetMergedBlockCount() const
    {
        return Blobs.GetBlockCount();
    }

    size_t GetGarbageBlockCount() const
    {
        return Blobs.GetGarbageBlockCount();
    }

    size_t GetUsedBlockCount() const
    {
        return GetMixedBlockCount()
            + GetFreshBlockCount()
            + GetMergedBlockCount()
            - GetGarbageBlockCount();
    }

    ui32 GetMixedZoneCount() const
    {
        return Blobs.GetMixedZoneCount();
    }

    void MarkMergedBlocksDeleted(
        TPartitionDatabase& db,
        const TBlockRange32& blockRange,
        ui64 commitId);

    bool FindMergedBlocks(
        TPartitionDatabase& db,
        ui64 commitId,
        const TBlockRange32& blockRange,
        IMergedBlockVisitor& visitor) const;

    bool FindMergedBlocks(
        TPartitionDatabase& db,
        const TBlockRange32& blockRange,
        IMergedBlockVisitor& visitor) const;

    bool FindMergedBlocks(
        TPartitionDatabase& db,
        const TGarbageInfo& blobIds,
        IMergedBlockVisitor& visitor) const;

    // XXX abstraction leakage
    bool ContainsMixedZones(const TBlockRange32& range) const;

    ui32 GetZoneBlockCount() const
    {
        return Config.GetZoneBlockCount();
    }

    //
    // Cleanup/Compaction/Checkpoint request queue
    //

private:
    struct TCCCRequest
    {
        const ui64 CommitId;
        std::unique_ptr<ITransactionBase> Tx;
        std::function<void(const NActors::TActorContext&)> OnStartProcessing = {};
    };

    TDeque<TCCCRequest> CCCRequestQueue;
    bool CCCRequestInProgress = false;

public:
    auto& GetCCCRequestQueue()
    {
        return CCCRequestQueue;
    }

    bool HasCCCRequestInProgress() const
    {
        return CCCRequestInProgress;
    }

    void StartProcessingCCCRequest()
    {
        Y_ABORT_UNLESS(!CCCRequestInProgress);
        CCCRequestInProgress = true;
    }

    void StopProcessingCCCRequest()
    {
        Y_ABORT_UNLESS(CCCRequestInProgress);
        CCCRequestInProgress = false;
    }

    //
    // Checkpoints
    //

private:
    TCheckpointStorage Checkpoints;
    TCheckpointsToDelete CheckpointsToDelete;

public:
    TCheckpointStorage& GetCheckpoints()
    {
        return Checkpoints;
    }

    bool HasCheckpointsToDelete() const
    {
        return !CheckpointsToDelete.IsEmpty();
    }

    void InitCheckpoints(
        const TVector<NProto::TCheckpointMeta>& checkpoints,
        const TVector<TVector<TPartialBlobId>>& deletedCheckpointBlobIds);

    void WriteCheckpoint(const NProto::TCheckpointMeta& meta);

    bool MarkCheckpointDeleted(
        TPartitionDatabase& db,
        TInstant now,
        ui64 commitId,
        const TString& checkpointId,
        TVector<TPartialBlobId> blobIds);

    void SubtractCheckpointBlocks(ui32 erasedCheckpointBlocks);

    //
    // Cleanup
    //

private:
    TOperationState CleanupState;

public:
    EOperationStatus GetCleanupStatus() const
    {
        return CleanupState.GetStatus();
    }

    void SetCleanupStatus(EOperationStatus status)
    {
        CleanupState.SetStatus(status);
    }

    //
    // Dirty Blob Cleanup
    //

private:
    TVector<TPartialBlobId> PendingChunkToCleanup;
    TVector<TPartialBlobId> PendingBlobsToCleanup;
    ui32 PendingCleanupZoneId = InvalidZoneId;

public:
    size_t GetPendingUpdates() const
    {
        return Blobs.GetPendingUpdates();
    }

    bool HasPendingChunkToCleanup() const
    {
        return !PendingChunkToCleanup.empty();
    }

    const TVector<TPartialBlobId>& GetPendingBlobsToCleanup() const
    {
        return PendingBlobsToCleanup;
    }

    ui32 GetPendingCleanupZoneId() const
    {
        return PendingCleanupZoneId;
    }

    bool TrySelectZoneToCleanup()
    {
        Y_ABORT_UNLESS(PendingChunkToCleanup.empty());
        Y_ABORT_UNLESS(PendingBlobsToCleanup.empty());

        PendingCleanupZoneId = Blobs.SelectZoneToCleanup();
        return PendingCleanupZoneId != InvalidZoneId;
    }

    void SeparateChunkForCleanup(ui64 commitId)
    {
        Y_ABORT_UNLESS(PendingCleanupZoneId != InvalidZoneId);
        Blobs.SeparateChunkForCleanup(commitId);
    }

    void ExtractChunkToCleanup()
    {
        Y_ABORT_UNLESS(PendingCleanupZoneId != InvalidZoneId);
        PendingChunkToCleanup = Blobs.ExtractDirtyBlobs();
    }

    void ExtractBlobsFromChunkToCleanup(size_t limit);

    TVector<TBlobUpdate> FinishDirtyBlobCleanup(TPartitionDatabase& db);

    //
    // Checkpoint Blob Cleanup
    //

private:
    TVector<TPartialBlobId> PendingCheckpointBlobsToCleanup;
    ui64 CleanupCheckpointCommitId = 0;

public:
    const TVector<TPartialBlobId>& GetPendingCheckpointBlobsToCleanup() const
    {
        return PendingCheckpointBlobsToCleanup;
    }

    ui64 GetCleanupCheckpointCommitId() const
    {
        return CleanupCheckpointCommitId;
    }

    void ExtractCheckpointBlobsToCleanup(size_t limit);

    void FinishCheckpointBlobCleanup(TPartitionDatabase& db);

    //
    // Compaction
    //

private:
    TOperationState CompactionState;

    TCompactionMap CompactionMap;

public:
    EOperationStatus GetCompactionStatus(ECompactionType type) const;
    void SetCompactionStatus(ECompactionType type, EOperationStatus status);

    TCompactionMap& GetCompactionMap()
    {
        return CompactionMap;
    }

    ui32 GetLegacyCompactionScore() const
    {
        return CompactionMap.GetTop().Stat.BlobCount;
    }

    float GetCompactionScore() const
    {
        return CompactionMap.GetTop().Stat.CompactionScore.Score;
    }

    void InitCompactionMap(const TVector<TCompactionCounter>& compactionMap);

    void UpdateCompactionMap(
        TPartitionDatabase& db,
        const TVector<TBlock>& blocks);

    void ResetCompactionMap(
        TPartitionDatabase& db,
        const TVector<TBlock>& blocks,
        const ui32 blobsSkipped,
        const ui32 blocksSkipped);

    //
    // Forced Compaction
    //

private:
    TForcedCompactionState ForcedCompactionState;

public:
    bool IsForcedCompactionRunning() const
    {
        return ForcedCompactionState.IsRunning;
    }

    void StartForcedCompaction(const TString& operationId, ui32 blocksCount)
    {
        ForcedCompactionState.IsRunning = true;
        ForcedCompactionState.Progress = 0;
        ForcedCompactionState.RangeCount = blocksCount;
        ForcedCompactionState.OperationId = operationId;
    }

    void OnNewCompactionRange()
    {
        ++ForcedCompactionState.Progress;
    }

    void ResetForcedCompaction()
    {
        ForcedCompactionState.IsRunning = false;
        ForcedCompactionState.Progress = 0;
        ForcedCompactionState.RangeCount = 0;
        ForcedCompactionState.OperationId.clear();
    }

    const TForcedCompactionState& GetForcedCompactionState() const
    {
        return ForcedCompactionState;
    }

    //
    // Forced Cleanup
    //

private:
    TForcedCleanupState ForcedCleanupState;

public:
    const TForcedCleanupState& GetForcedCleanupState() const
    {
        return ForcedCleanupState;
    }

    bool IsForcedCleanupRunning() const
    {
        return ForcedCleanupState.IsRunning;
    }

    void StartForcedCleanup()
    {
        ForcedCleanupState.IsRunning = true;
    }

    void ResetForcedCleanup()
    {
        ForcedCleanupState.IsRunning = false;
    }

    //
    // Garbage
    //

private:
    TOperationState CollectGarbageState;

    TGarbageQueue GarbageQueue;
    ui32 LastCollectPerGenerationCounter = 0;
    TDuration CollectTimeout;
    bool StartupGcExecuted = false;

public:
    EOperationStatus GetCollectGarbageStatus() const
    {
        return CollectGarbageState.GetStatus();
    }

    void SetCollectGarbageStatus(EOperationStatus status)
    {
        CollectGarbageState.SetStatus(status);
    }

    TGarbageQueue& GetGarbageQueue()
    {
        return GarbageQueue;
    }

    TDuration GetCollectTimeout()
    {
        return CollectTimeout;
    }

    void RegisterCollectError()
    {
        CollectTimeout = Min(
            TDuration::Seconds(5),
            Max(TDuration::MilliSeconds(100), CollectTimeout * 2)
        );
    }

    void RegisterCollectSuccess()
    {
        CollectTimeout = {};
    }

    ui64 GetLastCollectCommitId() const
    {
        return Meta.GetLastCollectCommitId();
    }

    void SetLastCollectCommitId(ui64 commitId)
    {
        Meta.SetLastCollectCommitId(commitId);
    }

    ui32 NextCollectPerGenerationCounter()
    {
        return ++LastCollectPerGenerationCounter;
    }

    void InitGarbage(
        const TVector<TPartialBlobId>& newBlobs,
        const TVector<TPartialBlobId>& garbageBlobs)
    {
        Y_ABORT_UNLESS(GarbageQueue.AddNewBlobs(newBlobs));
        Y_ABORT_UNLESS(GarbageQueue.AddGarbageBlobs(garbageBlobs));
    }

    void AcquireCollectBarrier(ui64 commitId)
    {
        GarbageQueue.AcquireCollectBarrier(commitId);
    }

    void ReleaseCollectBarrier(ui64 commitId)
    {
        GarbageQueue.ReleaseCollectBarrier(commitId);
    }

    ui64 GetCollectCommitId() const
    {
        // should not collect after any barrier
        return Min(GetLastCommitId(), GarbageQueue.GetCollectCommitId());
    }

    bool GetStartupGcExecuted() const
    {
        return StartupGcExecuted;
    }

    void SetStartupGcExecuted()
    {
        StartupGcExecuted = true;
    }

    bool CollectGarbageHardRequested = false;

    //
    // ReadBlob
    //

private:
    ui32 ReadBlobErrorCount = 0;

public:
    ui32 IncrementReadBlobErrorCount()
    {
        return ++ReadBlobErrorCount;
    }

    //
    // Stats
    //

private:
    NProto::TPartitionStats& Stats;

public:
    const NProto::TPartitionStats& GetStats() const
    {
        return Stats;
    }

    template <typename T>
    void UpdateStats(T&& update)
    {
        update(Stats);
    }

    void WriteStats(TPartitionDatabase& db);

    void DumpHtml(IOutputStream& out) const;
    NJson::TJsonValue AsJson() const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
