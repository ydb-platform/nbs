#pragma once

#include "public.h"

#include "checkpoint.h"
#include "helpers.h"
#include "rebase_logic.h"
#include "session.h"
#include "tablet_database.h"
#include "tablet_tx.h"

#include <cloud/filestore/libs/storage/model/channel_data_kind.h>
#include <cloud/filestore/libs/storage/tablet/model/alloc.h>
#include <cloud/filestore/libs/storage/tablet/model/blob.h>
#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/tablet/model/channels.h>
#include <cloud/filestore/libs/storage/tablet/model/compaction_map.h>
#include <cloud/filestore/libs/storage/tablet/model/mixed_blocks.h>
#include <cloud/filestore/libs/storage/tablet/model/node_index_cache.h>
#include <cloud/filestore/libs/storage/tablet/model/node_session_stat.h>
#include <cloud/filestore/libs/storage/tablet/model/operation.h>
#include <cloud/filestore/libs/storage/tablet/model/public.h>
#include <cloud/filestore/libs/storage/tablet/model/range_locks.h>
#include <cloud/filestore/libs/storage/tablet/model/read_ahead.h>
#include <cloud/filestore/libs/storage/tablet/model/throttler_logger.h>
#include <cloud/filestore/libs/storage/tablet/model/truncate_queue.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>
#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/viewer/tablet_monitoring.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NProto {

////////////////////////////////////////////////////////////////////////////////

class TProfileLogRequestInfo;

} // namespace NCloud::NFileStore::NProto

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TCompactionInfo
{
    const ui32 Threshold;
    const ui32 ThresholdAverage;
    const ui32 GarbageThreshold;
    const ui32 GarbageThresholdAverage;
    const ui32 Score;
    const ui32 RangeId;
    const double GarbagePercentage;
    const double AverageScore;
    const bool NewCompactionEnabled;
    const bool ShouldCompact;

    TCompactionInfo(
            ui32 threshold,
            ui32 thresholdAverage,
            ui32 garbageThreshold,
            ui32 garbageThresholdAverage,
            ui32 score,
            ui32 rangeId,
            double garbagePercentage,
            double averageScore,
            bool newCompactionEnabled,
            bool shouldCompact)
        : Threshold(threshold)
        , ThresholdAverage(thresholdAverage)
        , GarbageThreshold(garbageThreshold)
        , GarbageThresholdAverage(garbageThresholdAverage)
        , Score(score)
        , RangeId(rangeId)
        , GarbagePercentage(garbagePercentage)
        , AverageScore(averageScore)
        , NewCompactionEnabled(newCompactionEnabled)
        , ShouldCompact(shouldCompact)
    {
    }
};

struct TCleanupInfo
{
    const ui32 Threshold;
    const ui32 ThresholdAverage;
    const ui32 Score;
    const ui32 RangeId;
    const double AverageScore;
    const ui64 LargeDeletionMarkersThreshold;
    const ui64 LargeDeletionMarkerCount;
    const ui32 PriorityRangeIdCount;
    const bool IsPriority;
    const bool NewCleanupEnabled;
    const bool ShouldCleanup;

    TCleanupInfo(
            ui32 threshold,
            ui32 thresholdAverage,
            ui32 score,
            ui32 rangeId,
            double averageScore,
            ui64 largeDeletionMarkersThreshold,
            ui64 largeDeletionMarkerCount,
            ui32 priorityRangeIdCount,
            bool isPriority,
            bool newCleanupEnabled,
            bool shouldCleanup)
        : Threshold(threshold)
        , ThresholdAverage(thresholdAverage)
        , Score(score)
        , RangeId(rangeId)
        , AverageScore(averageScore)
        , LargeDeletionMarkersThreshold(largeDeletionMarkersThreshold)
        , LargeDeletionMarkerCount(largeDeletionMarkerCount)
        , PriorityRangeIdCount(priorityRangeIdCount)
        , IsPriority(isPriority)
        , NewCleanupEnabled(newCleanupEnabled)
        , ShouldCleanup(shouldCleanup)
    {
    }
};

struct TFlushBytesStats
{
    ui64 TotalBytesFlushed = 0;
    bool ChunkCompleted = false;
};

struct TNodeToSessionCounters
{
    i64 NodesOpenForWritingBySingleSession{0};
    i64 NodesOpenForWritingByMultipleSessions{0};
    i64 NodesOpenForReadingBySingleSession{0};
    i64 NodesOpenForReadingByMultipleSessions{0};
};

struct TMiscNodeStats
{
    i64 OrphanNodesCount{0};
};

struct TWriteMixedBlocksResult
{
    ui32 GarbageBlocksCount = 0;
    bool NewBlob = false;
};

struct TDeleteMixedBlocksResult
{
    ui32 GarbageBlocksCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TIndexTabletState
{
private:
    TFileStoreAllocRegistry AllocatorRegistry;

    struct TImpl;
    std::unique_ptr<TImpl> Impl;

    ui32 Generation = 0;
    ui32 LastStep = 0;
    ui32 LastCollectCounter = 0;
    bool StartupGcExecuted = false;

    NProto::TFileSystem FileSystem;
    NProto::TFileSystemStats FileSystemStats;
    NCloud::NProto::TTabletStorageInfo TabletStorageInfo;
    TNodeToSessionCounters NodeToSessionCounters;
    ui64 DeletionMarkersThrottlingThreshold = 0;

    /*const*/ ui32 TruncateBlocksThreshold = 0;
    /*const*/ ui32 SessionHistoryEntryCount = 0;
    /*const*/ double ChannelMinFreeSpace = 0;
    /*const*/ double ChannelFreeSpaceThreshold = 1;
    /*const*/ bool LargeDeletionMarkersEnabled = false;
    /*const*/ ui64 LargeDeletionMarkerBlocks = 0;
    /*const*/ ui64 LargeDeletionMarkersThreshold = 0;
    /*const*/ ui64 LargeDeletionMarkersCleanupThreshold = 0;
    /*const*/ ui64 LargeDeletionMarkersThresholdForBackpressure = 0;

    bool StateLoaded = false;

protected:
    TString LogTag;

public:
    TIndexTabletState();
    ~TIndexTabletState();

    void UpdateLogTag(TString tag);

    void LoadState(
        ui32 generation,
        const TStorageConfig& config,
        const NProto::TFileSystem& fileSystem,
        const NProto::TFileSystemStats& fileSystemStats,
        const NCloud::NProto::TTabletStorageInfo& tabletStorageInfo,
        const TVector<TDeletionMarker>& largeDeletionMarkers,
        const TVector<ui64>& orphanNodeIds,
        const TThrottlerConfig& throttlerConfig);

    bool IsStateLoaded() const
    {
        return StateLoaded;
    }

    void CompleteStateLoad()
    {
        StateLoaded = true;
    }

    void UpdateConfig(
        TIndexTabletDatabase& db,
        const TStorageConfig& config,
        const NProto::TFileSystem& fileSystem,
        const TThrottlerConfig& throttlerConfig);

    //
    // FileSystem
    //

public:
    const NProto::TFileSystem& GetFileSystem() const
    {
        return FileSystem;
    }

    TString GetFileSystemId() const
    {
        return FileSystem.GetFileSystemId();
    }

    TString GetMainFileSystemId() const
    {
        return FileSystem.GetMainFileSystemId();
    }

    ui32 GetGeneration() const
    {
        return Generation;
    }

    ui32 GetBlockSize() const
    {
        return FileSystem.GetBlockSize();
    }

    ui64 GetBlocksCount() const
    {
        return FileSystem.GetBlocksCount();
    }

    ui64 GetNodesCount() const
    {
        if (!FileSystem.GetNodesCount()) {
            return MaxNodes;
        }

        return FileSystem.GetNodesCount();
    }

    ui64 GetCurrentCommitId() const
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

    ui64 GenerateEventId(TSession* session)
    {
        return MakeCommitId(Generation, ++session->LastEvent);
    }

    const NProto::TFileSystemStats& GetFileSystemStats() const
    {
        return FileSystemStats;
    }

    ui64 GetDeletionMarkersThrottlingThreshold() const
    {
        return DeletionMarkersThrottlingThreshold;
    }

    void SetDeletionMarkersThrottlingThreshold(ui64 count)
    {
        DeletionMarkersThrottlingThreshold = count;
    }

    const TNodeToSessionCounters& GetNodeToSessionCounters() const
    {
        return NodeToSessionCounters;
    }

    TMiscNodeStats GetMiscNodeStats() const;

    const NProto::TFileStorePerformanceProfile& GetPerformanceProfile() const;

    const TFileStoreAllocRegistry& GetFileStoreProfilingRegistry() const
    {
        return AllocatorRegistry;
    }

    IAllocator* GetAllocator(EAllocatorTag tag) const
    {
        return AllocatorRegistry.GetAllocator(tag);
    }

    bool CalculateExpectedShardCount() const;

    NProto::TError SelectShard(ui64 fileSize, TString* shardId);

    void UpdateShardStats(const TVector<TShardStats>& stats);

    //
    // FileSystem Stats
    //

public:
    void DumpStats(IOutputStream& os) const;

#define FILESTORE_DECLARE_COUNTER(name, ...)                                   \
public:                                                                        \
    ui64 Get##name() const                                                     \
    {                                                                          \
        return FileSystemStats.Get##name();                                    \
    }                                                                          \
private:                                                                       \
    void Set##name(TIndexTabletDatabase& db, ui64 value)                       \
    {                                                                          \
        FileSystemStats.Set##name(value);                                      \
        db.Write##name(value);                                                 \
    }                                                                          \
    ui64 Increment##name(TIndexTabletDatabase& db, size_t delta = 1)           \
    {                                                                          \
        ui64 value = SafeIncrement(FileSystemStats.Get##name(), delta);        \
        FileSystemStats.Set##name(value);                                      \
        db.Write##name(value);                                                 \
        return value;                                                          \
    }                                                                          \
    ui64 Decrement##name(TIndexTabletDatabase& db, size_t delta = 1)           \
    {                                                                          \
        ui64 value = SafeDecrement(FileSystemStats.Get##name(), delta);        \
        FileSystemStats.Set##name(value);                                      \
        db.Write##name(value);                                                 \
        return value;                                                          \
    }                                                                          \
// FILESTORE_DECLARE_COUNTER

FILESTORE_FILESYSTEM_STATS(FILESTORE_DECLARE_COUNTER)

#undef FILESTORE_DECLARE_COUNTER

    void ChangeNodeCounters(const TNodeToSessionStat::EKind nodeKind, i64 amount);

    //
    // Throttling
    //

public:
    const TThrottlingPolicy& GetThrottlingPolicy() const;
    TThrottlingPolicy& AccessThrottlingPolicy();
    const TThrottlerConfig& GetThrottlingConfig() const;

    //
    // Channels
    //

public:
    ui64 GetTabletChannelCount() const;
    ui64 GetConfigChannelCount() const;

    TVector<ui32> GetChannels(EChannelDataKind kind) const;
    TVector<ui32> GetUnwritableChannels() const;
    TVector<ui32> GetChannelsToMove(ui32 percentageThreshold) const;
    TVector<NCloud::NStorage::TChannelMonInfo> MakeChannelMonInfos() const;

    TChannelsStats CalculateChannelsStats() const;

    void UpdateChannelStats(
        ui32 channel,
        bool writable,
        bool toMove,
        double freeSpaceShare);

private:
    void LoadChannels();
    void UpdateChannels();

    //
    // Nodes
    //

public:
    ui64 CreateNode(
        TIndexTabletDatabase& db,
        ui64 commitId,
        const NProto::TNode& attrs);

    void UpdateNode(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const NProto::TNode& attrs,
        const NProto::TNode& prevAttrs);

    [[nodiscard]] NProto::TError RemoveNode(
        TIndexTabletDatabase& db,
        const IIndexTabletDatabase::TNode& node,
        ui64 minCommitId,
        ui64 maxCommitId);

    [[nodiscard]] NProto::TError UnlinkNode(
        TIndexTabletDatabase& db,
        ui64 parentNodeId,
        const TString& name,
        const IIndexTabletDatabase::TNode& node,
        ui64 minCommitId,
        ui64 maxCommitId);

    void UnlinkExternalNode(
        TIndexTabletDatabase& db,
        ui64 parentNodeId,
        const TString& name,
        const TString& shardId,
        const TString& shardNodeName,
        ui64 minCommitId,
        ui64 maxCommitId);

    bool ReadNode(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        TMaybe<IIndexTabletDatabase::TNode>& node);

    void RewriteNode(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const NProto::TNode& attrs);

    bool HasSpaceLeft(
        const NProto::TNode& attrs,
        ui64 newSize) const;

    bool HasBlocksLeft(
        ui32 blocks) const;

    void WriteOrphanNode(
        TIndexTabletDatabase& db,
        const TString& message,
        ui64 nodeId);

    bool HasPendingNodeCreateInShard(const TString& nodeName) const;

    void StartNodeCreateInShard(const TString& nodeName);

    void EndNodeCreateInShard(const TString& nodeName);

private:
    void UpdateUsedBlocksCount(
        TIndexTabletDatabase& db,
        ui64 currentSize,
        ui64 prevSize);

    //
    // NodeAttrs
    //

public:
    ui64 CreateNodeAttr(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        const TString& value);

    ui64 UpdateNodeAttr(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const IIndexTabletDatabase::TNodeAttr& attr,
        const TString& newValue);

    void RemoveNodeAttr(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const IIndexTabletDatabase::TNodeAttr& attr);

    bool ReadNodeAttr(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<IIndexTabletDatabase::TNodeAttr>& attr);

    bool ReadNodeAttrs(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        TVector<IIndexTabletDatabase::TNodeAttr>& attrs);

    void RewriteNodeAttr(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const IIndexTabletDatabase::TNodeAttr& attr);

    //
    // NodeRefs
    //

public:
    void CreateNodeRef(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TString& childName,
        ui64 childNodeId,
        const TString& shardId,
        const TString& shardNodeName);

    void RemoveNodeRef(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const TString& childName,
        ui64 prevChildNodeId,
        const TString& shardId,
        const TString& shardNodeName);

    bool ReadNodeRef(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<IIndexTabletDatabase::TNodeRef>& ref);

    bool ReadNodeRefs(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TString& cookie,
        TVector<IIndexTabletDatabase::TNodeRef>& refs,
        ui32 maxBytes,
        TString* next = nullptr);

    bool ReadNodeRefs(
        IIndexTabletDatabase& db,
        ui64 startNodeId,
        const TString& startCookie,
        ui64 maxCount,
        TVector<IIndexTabletDatabase::TNodeRef>& refs,
        ui64& nextNodeId,
        TString& nextCookie);

    bool PrechargeNodeRefs(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        const TString& cookie,
        ui32 bytesToPrecharge);

    void RewriteNodeRef(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const TString& childName,
        ui64 childNodeId,
        const TString& shardId,
        const TString& shardNodeName);

    bool TryLockNodeRef(TNodeRefKey key);
    void UnlockNodeRef(TNodeRefKey key);

    //
    // Sessions
    //

public:
    void LoadSessions(
        TInstant idleSessionDeadline,
        const TVector<NProto::TSession>& sessions,
        const TVector<NProto::TSessionHandle>& handles,
        const TVector<NProto::TSessionLock>& locks,
        const TVector<NProto::TDupCacheEntry>& cacheEntries,
        const TVector<NProto::TSessionHistoryEntry>& sessionsHistory,
        const NProto::TSessionOptions& sessionOptions);

    TSession* CreateSession(
        TIndexTabletDatabase& db,
        const TString& clientId,
        const TString& sessionId,
        const TString& checkpointId,
        const TString& originFqdn,
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner,
        const NProto::TSessionOptions& sessionOptions);

    void RemoveSession(
        TIndexTabletDatabase& db,
        const TString& sessionId);

    TSession* FindSession(const TString& sessionId) const;
    TSession* FindSessionByClientId(const TString& clientId) const;
    TSession* FindSession(
        const TString& clientId,
        const TString& sessionId,
        ui64 SeqNo) const;

    NActors::TActorId RecoverSession(
        TSession* session,
        ui64 sessionSeqNo,
        bool readOnly,
        const NActors::TActorId& owner);
    void OrphanSession(const NActors::TActorId& owner, TInstant deadline);
    void ResetSession(TIndexTabletDatabase& db, TSession* session, const TMaybe<TString>& state);

    TVector<TSession*> GetTimeoutedSessions(TInstant now) const;
    TVector<TSession*> GetSessionsToNotify(const NProto::TSessionEvent& event) const;
    TVector<NProtoPrivate::TTabletSessionInfo> DescribeSessions() const;

    const TSessionHistoryList& GetSessionHistoryList() const;
    void AddSessionHistoryEntry(
        TIndexTabletDatabase& db,
        const TSessionHistoryEntry& entry, size_t maxEntryCount);

    using TCreateSessionRequests =
        TVector<NProtoPrivate::TCreateSessionRequest>;
    TCreateSessionRequests BuildCreateSessionRequests(
        const THashSet<TString>& filter) const;
    TVector<TMonSessionInfo> GetActiveSessionInfos() const;
    TVector<TMonSessionInfo> GetOrphanSessionInfos() const;
    TSessionsStats CalculateSessionsStats() const;

private:
    TSession* CreateSession(
        const NProto::TSession& proto,
        TInstant deadline,
        const NProto::TSessionOptions& sessionOptions);

    TSession* CreateSession(
        const NProto::TSession& proto,
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner,
        const NProto::TSessionOptions& sessionOptions);

    void RemoveSession(TSession* session);

    //
    // Handles
    //

public:
    TSessionHandle* CreateHandle(
        TIndexTabletDatabase& db,
        TSession* session,
        ui64 nodeId,
        ui64 commitId,
        ui32 flags);

    void DestroyHandle(
        TIndexTabletDatabase& db,
        TSessionHandle* handle);

    TSessionHandle* FindHandle(ui64 handle) const;

    bool HasOpenHandles(ui64 nodeId) const;

private:
    ui64 GenerateHandle() const;

    TSessionHandle* CreateHandle(
        TSession* session,
        const NProto::TSessionHandle& proto);

    void RemoveHandle(TSessionHandle* handle);

    //
    // Locks
    //

public:
    TRangeLockOperationResult AcquireLock(
        TIndexTabletDatabase& db,
        TSession* session,
        ui64 handle,
        const TLockRange& range);

    TRangeLockOperationResult ReleaseLock(
        TIndexTabletDatabase& db,
        TSession* session,
        const TLockRange& range);

    TRangeLockOperationResult TestLock(
        TSession* session,
        const TSessionHandle* handle,
        const TLockRange& range) const;

    void ReleaseLocks(TIndexTabletDatabase& db, ui64 handle);

private:
    TSessionLock* FindLock(ui64 lockId) const;

    TRangeLockOperationResult CreateLock(
        TSession* session,
        const NProto::TSessionLock& proto,
        const TLockRange* range = nullptr);

    void RemoveLock(TSessionLock* lock);

    //
    // DupCache
    //

#define FILESTORE_DECLARE_DUPCACHE(name, ...)                                   \
public:                                                                         \
    void AddDupCacheEntry(                                                      \
        TIndexTabletDatabase& db,                                               \
        TSession* session,                                                      \
        ui64 requestId,                                                         \
        const NProto::T##name##Response& response,                              \
        ui32 maxEntries);                                                       \
                                                                                \
    bool GetDupCacheEntry(                                                      \
        const TDupCacheEntry* entry,                                            \
        NProto::T##name##Response& response);                                   \
// FILESTORE_DECLARE_DUPCACHE

FILESTORE_DUPCACHE_REQUESTS(FILESTORE_DECLARE_DUPCACHE)

#undef FILESTORE_DECLARE_DUPCACHE

    void PatchDupCacheEntry(
        TIndexTabletDatabase& db,
        const TString& sessionId,
        ui64 requestId,
        NProto::TCreateNodeResponse response);

    void PatchDupCacheEntry(
        TIndexTabletDatabase& db,
        const TString& sessionId,
        ui64 requestId,
        NProto::TRenameNodeResponse response);

    void CommitDupCacheEntry(
        const TString& sessionId,
        ui64 requestId);

    //
    // Writes
    //

public:
    bool EnqueueWriteBatch(std::unique_ptr<TWriteRequest> request);
    TWriteRequestList DequeueWriteBatch();

    bool GenerateBlobId(
        ui64 commitId,
        ui32 blobSize,
        ui32 blobIndex,
        TPartialBlobId* blobId) const;

    struct TBackpressureThresholds
    {
        ui64 Flush;
        ui64 FlushBytes;
        ui64 CompactionScore;
        ui64 CleanupScore;

        TBackpressureThresholds(
                const ui64 flush,
                const ui64 flushBytes,
                const ui64 compactionScore,
                const ui64 cleanupScore)
            : Flush(flush)
            , FlushBytes(flushBytes)
            , CompactionScore(compactionScore)
            , CleanupScore(cleanupScore)
        {
        }
    };

    using TBackpressureValues = TBackpressureThresholds;

    static bool IsWriteAllowed(
        const TBackpressureThresholds& thresholds,
        const TBackpressureValues& values,
        TString* message);

    //
    // FreshBytes
    //

public:
    void LoadFreshBytes(
        const TVector<TIndexTabletDatabase::TFreshBytesEntry>& bytes);

    void FindFreshBytes(
        IFreshBytesVisitor& visitor,
        ui64 nodeId,
        ui64 commitId,
        TByteRange byteRange) const;

    NProto::TError CheckFreshBytes(
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        TStringBuf data) const;

    void WriteFreshBytes(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        TStringBuf data);

    void WriteFreshBytesDeletionMarker(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        ui64 len);

    TFlushBytesCleanupInfo StartFlushBytes(
        TVector<TBytes>* bytes,
        TVector<TBytes>* deletionMarkers);
    TFlushBytesStats FinishFlushBytes(
        TIndexTabletDatabase& db,
        ui64 itemLimit,
        ui64 chunkId,
        NProto::TProfileLogRequestInfo& profileLogRequest);

    //
    // FreshBlocks
    //

public:
    void LoadFreshBlocks(
        const TVector<TIndexTabletDatabase::TFreshBlock>& blocks);

    void FindFreshBlocks(IFreshBlockVisitor& visitor) const;

    void FindFreshBlocks(
        IFreshBlockVisitor& visitor,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) const;

    TMaybe<TFreshBlock> FindFreshBlock(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex) const;

    void WriteFreshBlock(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        TStringBuf blockData);

    void MarkFreshBlocksDeleted(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount);

    void DeleteFreshBlocks(
        TIndexTabletDatabase& db,
        const TVector<TBlock>& blocks);

    //
    // MixedBlocks
    //

public:
    bool LoadMixedBlocks(IIndexTabletDatabase& db, ui32 rangeId);
    void ReleaseMixedBlocks(ui32 rangeId);
    void ReleaseMixedBlocks(const TSet<ui32>& ranges);

    void FindMixedBlocks(
        IMixedBlockVisitor& visitor,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) const;

    void WriteMixedBlocks(
        TIndexTabletDatabase& db,
        const TPartialBlobId& blobId,
        const TBlock& block,
        ui32 blocksCount);

    TWriteMixedBlocksResult WriteMixedBlocks(
        TIndexTabletDatabase& db,
        const TPartialBlobId& blobId,
        /*const*/ TVector<TBlock>& blocks);

    void DeleteMixedBlocks(
        TIndexTabletDatabase& db,
        const TPartialBlobId& blobId,
        const TVector<TBlock>& blocks);

    TVector<TMixedBlobMeta> GetBlobsForCompaction(ui32 rangeId) const;

    TMixedBlobMeta FindBlob(ui32 rangeId, TPartialBlobId blobId) const;

    void MarkMixedBlocksDeleted(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount);

    // returns processed deletion marker count
    ui32 CleanupBlockDeletions(
        TIndexTabletDatabase& db,
        ui32 rangeId,
        NProto::TProfileLogRequestInfo& profileLogRequest);

    bool UpdateBlockLists(
        TIndexTabletDatabase& db,
        TMixedBlobMeta& blob);

    void RewriteMixedBlocks(
        TIndexTabletDatabase& db,
        ui32 rangeId,
        /*const*/ TMixedBlobMeta& blob,
        const TMixedBlobStats& blobStats);

    TBlobMetaMapStats GetBlobMetaMapStats() const;

    ui32 GetMixedRangeIndex(ui64 nodeId, ui32 blockIndex) const;
    ui32 GetMixedRangeIndex(ui64 nodeId, ui32 blockIndex, ui32 blocksCount) const;
    ui32 GetMixedRangeIndex(const TVector<TBlock>& blocks) const;
    const IBlockLocation2RangeIndex& GetRangeIdHasher() const;

    ui32 CalculateMixedIndexRangeGarbageBlockCount(ui32 rangeId) const;

private:
    TWriteMixedBlocksResult WriteMixedBlocks(
        TIndexTabletDatabase& db,
        ui32 rangeId,
        const TPartialBlobId& blobId,
        /*const*/ TVector<TBlock>& blocks);

    TDeleteMixedBlocksResult DeleteMixedBlocks(
        TIndexTabletDatabase& db,
        ui32 rangeId,
        const TPartialBlobId& blobId,
        const TVector<TBlock>& blocks);

    TRebaseResult RebaseMixedBlocks(TVector<TBlock>& blocks) const;

    //
    // LargeBlocks
    //

public:
    void FindLargeBlocks(
        ILargeBlockVisitor& visitor,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) const;

    //
    // Garbage
    //

public:
    ui32 NextCollectCounter()
    {
        return ++LastCollectCounter;
    }

    void SetStartupGcExecuted()
    {
        StartupGcExecuted = true;
    }

    bool GetStartupGcExecuted() const
    {
        return StartupGcExecuted;
    }

    void AcquireCollectBarrier(ui64 commitId);
    bool TryReleaseCollectBarrier(ui64 commitId);
    bool IsCollectBarrierAcquired(ui64 commitId) const;

    ui64 GetCollectCommitId() const;

    void LoadGarbage(
        const TVector<TPartialBlobId>& newBlobs,
        const TVector<TPartialBlobId>& garbageBlobs);

    TVector<TPartialBlobId> GetNewBlobs(ui64 collectCommitId) const;
    TVector<TPartialBlobId> GetGarbageBlobs(ui64 collectCommitId) const;

    void DeleteGarbage(
        TIndexTabletDatabase& db,
        ui64 collectCommitId,
        const TVector<TPartialBlobId>& newBlobs,
        const TVector<TPartialBlobId>& garbageBlobs);

private:
    void AddNewBlob(TIndexTabletDatabase& db, const TPartialBlobId& blobId);
    void AddGarbageBlob(TIndexTabletDatabase& db, const TPartialBlobId& blobId);

    //
    // Checkpoints
    //

public:
    void LoadCheckpoints(const TVector<NProto::TCheckpoint>& checkpoints);

    TVector<TCheckpoint*> GetCheckpoints() const;

    TCheckpoint* FindCheckpoint(const TString& checkpointId) const;

    ui64 GetReadCommitId(const TString& checkpointId) const;

    TCheckpoint* CreateCheckpoint(
        TIndexTabletDatabase& db,
        const TString& checkpointId,
        ui64 nodeId,
        ui64 commitId);

    void MarkCheckpointDeleted(
        TIndexTabletDatabase& db,
        TCheckpoint* checkpoint);

    void RemoveCheckpointNodes(
        TIndexTabletDatabase& db,
        TCheckpoint* checkpoint,
        const TVector<ui64>& nodes);

    void RemoveCheckpointBlob(
        TIndexTabletDatabase& db,
        TCheckpoint* checkpoint,
        ui32 rangeId,
        const TPartialBlobId& blobId);

    void RemoveCheckpoint(
        TIndexTabletDatabase& db,
        TCheckpoint* checkpoint);

private:
    void AddCheckpointNode(
        TIndexTabletDatabase& db,
        ui64 checkpointId,
        ui64 nodeId);

    void AddCheckpointBlob(
        TIndexTabletDatabase& db,
        ui64 checkpointId,
        ui32 rangeId,
        const TPartialBlobId& blobId);

    //
    // Background operations
    //

public:
    TOperationState FlushState;
    TOperationState BlobIndexOpState;
    TOperationState CollectGarbageState;

private:
    TBlobIndexOpQueue BlobIndexOps;
    EBlobIndexOp CurrentBackgroundBlobIndexOp = EBlobIndexOp::Max;
    bool StartedBackgroundBlobIndexOp = false;

public:
    bool IsBlobIndexOpsQueueEmpty() const
    {
        return BlobIndexOps.Empty();
    }

    void AddBackgroundBlobIndexOp(EBlobIndexOp op)
    {
        if (CurrentBackgroundBlobIndexOp != op) {
            BlobIndexOps.Push(op);
        }
    }

    EBlobIndexOp GetCurrentBackgroundBlobIndexOp() const
    {
        return CurrentBackgroundBlobIndexOp;
    }

    bool EnqueueBackgroundBlobIndexOp()
    {
        if (BlobIndexOps.Empty()) {
            return false;
        }

        if (!BlobIndexOpState.Enqueue()) {
            return false;
        }

        Y_DEBUG_ABORT_UNLESS(!StartedBackgroundBlobIndexOp);
        CurrentBackgroundBlobIndexOp = BlobIndexOps.Pop();
        return true;
    }

    bool StartBackgroundBlobIndexOp()
    {
        Y_DEBUG_ABORT_UNLESS(CurrentBackgroundBlobIndexOp != EBlobIndexOp::Max);
        Y_DEBUG_ABORT_UNLESS(!StartedBackgroundBlobIndexOp);

        if (BlobIndexOpState.Start()) {
            StartedBackgroundBlobIndexOp = true;
            return true;
        }

        CurrentBackgroundBlobIndexOp = EBlobIndexOp::Max;
        return false;
    }

    void CompleteBlobIndexOp()
    {
        BlobIndexOpState.Complete();
        if (StartedBackgroundBlobIndexOp) {
            Y_DEBUG_ABORT_UNLESS(
                CurrentBackgroundBlobIndexOp != EBlobIndexOp::Max);
            CurrentBackgroundBlobIndexOp = EBlobIndexOp::Max;
            StartedBackgroundBlobIndexOp = false;
        }
    }

public:
    struct TPriorityRange
    {
        ui64 NodeId = 0;
        ui32 BlockIndex = 0;
        ui32 BlockCount = 0;
        ui32 RangeId = 0;
    };

private:
    mutable TDeque<TPriorityRange> PriorityRangesForCleanup;

    //
    // Compaction map
    //

public:
    void UpdateCompactionMap(
        ui32 rangeId,
        ui32 blobsCount,
        ui32 deletionsCount,
        ui32 garbageBlocksCount,
        bool compacted);

    TCompactionStats GetCompactionStats(ui32 rangeId) const;
    TCompactionCounter GetRangeToCompact() const;
    TCompactionCounter GetRangeToCleanup() const;
    TCompactionCounter GetRangeToCompactByGarbage() const;
    TMaybe<TPriorityRange> NextPriorityRangeForCleanup() const;
    ui32 GetPriorityRangeCount() const;

    TCompactionMapStats GetCompactionMapStats(ui32 topSize) const;

    TVector<ui32> GetNonEmptyCompactionRanges() const;
    TVector<ui32> GetAllCompactionRanges() const;
    TVector<TCompactionRangeInfo> GetTopRangesByCompactionScore(
        ui32 topSize) const;
    TVector<TCompactionRangeInfo> GetTopRangesByCleanupScore(
        ui32 topSize) const;
    TVector<TCompactionRangeInfo> GetTopRangesByGarbageScore(
        ui32 topSize) const;

    void LoadCompactionMap(const TVector<TCompactionRangeInfo>& compactionMap);

    //
    // Forced Compaction
    //

public:
    struct TForcedRangeOperationState
    {
        const TEvIndexTabletPrivate::EForcedRangeOperationMode Mode;
        const TVector<ui32> RangesToCompact;
        const TString OperationId;

        TInstant StartTime = TInstant::Now();
        ui32 Current = 0;

        TForcedRangeOperationState(
                TEvIndexTabletPrivate::EForcedRangeOperationMode mode,
                TVector<ui32> ranges,
                TString operationId)
            : Mode(mode)
            , RangesToCompact(std::move(ranges))
            , OperationId(std::move(operationId))
        {}

        TForcedRangeOperationState(const TForcedRangeOperationState&) = default;

        bool Progress()
        {
            return ++Current < RangesToCompact.size();
        }

        ui32 GetCurrentRange() const
        {
            return Current < RangesToCompact.size()
                ? RangesToCompact[Current] : 0;
        }
    };

private:
    struct TPendingForcedRangeOperation
    {
        TEvIndexTabletPrivate::EForcedRangeOperationMode Mode;
        TVector<ui32> Ranges;
        TString OperationId;
    };

    TVector<TPendingForcedRangeOperation> PendingForcedRangeOperations;
    TMaybe<TForcedRangeOperationState> ForcedRangeOperationState;
    TVector<TForcedRangeOperationState> CompletedForcedRangeOperations;

public:
    TString EnqueueForcedRangeOperation(
        TEvIndexTabletPrivate::EForcedRangeOperationMode mode,
        TVector<ui32> ranges);
    TPendingForcedRangeOperation DequeueForcedRangeOperation();

    void StartForcedRangeOperation(
        TEvIndexTabletPrivate::EForcedRangeOperationMode mode,
        TVector<ui32> ranges,
        TString operationId);

    void CompleteForcedRangeOperation();

    const TForcedRangeOperationState* GetForcedRangeOperationState() const
    {
        return ForcedRangeOperationState.Get();
    }

    const TForcedRangeOperationState* FindForcedRangeOperation(
        const TString& operationId) const;

    void UpdateForcedRangeOperationProgress(ui32 current)
    {
        ForcedRangeOperationState->Current =
            Max(ForcedRangeOperationState->Current, current);
    }

    bool IsForcedRangeOperationRunning() const
    {
        return ForcedRangeOperationState.Defined();
    }

    //
    // Truncate operations
    //

public:
    void EnqueueTruncateOp(ui64 nodeId, TByteRange range);
    TTruncateQueue::TEntry DequeueTruncateOp();
    bool HasPendingTruncateOps() const;

    void CompleteTruncateOp(ui64 nodeId);
    bool HasActiveTruncateOp(ui64 nodeId) const;

    void AddTruncate(TIndexTabletDatabase& db, ui64 nodeId, TByteRange range);
    void DeleteTruncate(TIndexTabletDatabase& db, ui64 nodeId);

    [[nodiscard]] NProto::TError Truncate(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        ui64 currentSize,
        ui64 targetSize);

    // Call this function only as a part of Truncate(...). The node size must be
    // changed after this call (this function guarantees that range will be
    // completely deleted). This function:
    // - aligns up range in the tail;
    // - deletes all blocks in NEW range;
    // - writes fresh bytes (zeroes) on unaligned head, if range.Offset != 0.
    [[nodiscard]] NProto::TError TruncateRange(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        TByteRange range);

    // Call this function only if you need to zero range without further
    // resizing the node. This function:
    // - writes fresh bytes (zeroes) on unaligned head, if any;
    // - writes fresh bytes (zeroes) on unaligned tail, if any.
    [[nodiscard]] NProto::TError ZeroRange(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        TByteRange range);

private:
    [[nodiscard]] NProto::TError DeleteRange(
        TIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TByteRange& range);

public:

    ////////////////////////////////////////////////////////////////////////////
    // Caching: ReadAhead, NodeIndexCache, InMemoryIndexState
    ////////////////////////////////////////////////////////////////////////////

    // Upon any completion of the RW operation this function is supposed to be
    // called in order to invalidate potentially cached data
    void InvalidateNodeCaches(ui64 nodeId);

    //
    // ReadAhead.
    //

    bool TryFillDescribeResult(
        ui64 nodeId,
        ui64 handle,
        const TByteRange& range,
        NProtoPrivate::TDescribeDataResponse* response);
    TMaybe<TByteRange> RegisterDescribe(
        ui64 nodeId,
        ui64 handle,
        const TByteRange inputRange);
    void InvalidateReadAheadCache(ui64 nodeId);
    void RegisterReadAheadResult(
        ui64 nodeId,
        ui64 handle,
        const TByteRange& range,
        const NProtoPrivate::TDescribeDataResponse& result);
    TReadAheadCacheStats CalculateReadAheadCacheStats() const;

    //
    // Node index cache
    //
    bool TryFillGetNodeAttrResult(
        ui64 parentNodeId,
        const TString& name,
        NProto::TNodeAttr* response);
    void InvalidateNodeIndexCache(ui64 parentNodeId, const TString& name);
    void InvalidateNodeIndexCache(ui64 nodeId);
    void RegisterGetNodeAttrResult(
        ui64 parentNodeId,
        const TString& name,
        const NProto::TNodeAttr& result);
    TNodeIndexCacheStats CalculateNodeIndexCacheStats() const;

    IIndexTabletDatabase& AccessInMemoryIndexState();
    void UpdateInMemoryIndexState(
        TVector<TInMemoryIndexState::TIndexStateRequest> nodeUpdates);
    void MarkNodeRefsLoadComplete();
    TInMemoryIndexStateStats GetInMemoryIndexStateStats() const;
};

}   // namespace NCloud::NFileStore::NStorage
