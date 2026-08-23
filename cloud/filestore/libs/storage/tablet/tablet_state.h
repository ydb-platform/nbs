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
#include <cloud/filestore/libs/storage/tablet/model/internal_request_id.h>
#include <cloud/filestore/libs/storage/tablet/model/mixed_blocks.h>
#include <cloud/filestore/libs/storage/tablet/model/node_access_stats.h>
#include <cloud/filestore/libs/storage/tablet/model/node_latency_stats.h>
#include <cloud/filestore/libs/storage/tablet/model/node_ref.h>
#include <cloud/filestore/libs/storage/tablet/model/node_session_stat.h>
#include <cloud/filestore/libs/storage/tablet/model/operation.h>
#include <cloud/filestore/libs/storage/tablet/model/public.h>
#include <cloud/filestore/libs/storage/tablet/model/range_locks.h>
#include <cloud/filestore/libs/storage/tablet/model/read_ahead.h>
#include <cloud/filestore/libs/storage/tablet/model/throttler_logger.h>
#include <cloud/filestore/libs/storage/tablet/model/truncate_queue.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>
#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/viewer/tablet_monitoring.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

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
    i64 DeferredNodeDestructionCount{0};
};

struct THandlesStats
{
    i64 UsedDirectHandlesCount{0};
    // TODO(2566) get rid of this counter after migration
    i64 SevenBytesHandlesCount{0};
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

enum class EBackgroundOpBackpressureStatus
{
    Normal = 1,
    CloseToThreshold = 2,
    ThresholdReached = 3,
};

struct TBackgroundOpsBackpressureStatus
{
    const EBackgroundOpBackpressureStatus Flush;
    const EBackgroundOpBackpressureStatus FlushBytes;
    const EBackgroundOpBackpressureStatus FlushBytesItemCount;
    const EBackgroundOpBackpressureStatus Compaction;
    const EBackgroundOpBackpressureStatus Cleanup;
    const EBackgroundOpBackpressureStatus CollectGarbage;
};

////////////////////////////////////////////////////////////////////////////////

// Stores deferred ConfirmAddData request info until unconfirmed data is either
// indexed by AddBlob or rejected.
struct TPendingConfirmAddData
{
    NActors::TActorId Sender;
    ui64 Cookie = 0;
    TInstant DeferredTs;
    TCallContextPtr CallContext;
    NProto::TProfileLogRequestInfo ProfileLogRequest;
};

////////////////////////////////////////////////////////////////////////////////

struct TTrackedUnconfirmedData
{
    NProto::TUnconfirmedData Data;
    TString SessionId;
    // Tablet-pipe server actor that accepted GenerateBlobIds for this data.
    NActors::TActorId PipeServerId;
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
    ui32 LastCollectPerGenerationCounter = 0;
    bool StartupGcExecuted = false;

    NProto::TFileSystem FileSystem;
    NProto::TFileSystemStats FileSystemStats;
    NCloud::NProto::TTabletStorageInfo TabletStorageInfo;
    TNodeToSessionCounters NodeToSessionCounters;
    ui64 MinDeletionMarkersCountSinceTabletStart = 0;

    /*const*/ ui32 TruncateBlocksThreshold = 0;
    /*const*/ ui32 SessionHistoryEntryCount = 0;
    /*const*/ double ChannelMinFreeSpace = 0;
    /*const*/ double ChannelFreeSpaceThreshold = 1;
    /*const*/ bool LargeDeletionMarkersEnabled = false;
    /*const*/ ui64 LargeDeletionMarkerBlocks = 0;
    /*const*/ ui64 LargeDeletionMarkersThreshold = 0;
    /*const*/ ui64 LargeDeletionMarkersCleanupThreshold = 0;
    /*const*/ ui64 LargeDeletionMarkersThresholdForBackpressure = 0;

    /*const*/ ui32 MaxTabletStep = Max<ui32>();

    bool CompressNodeRef = false;

    bool StateLoaded = false;

protected:
    TString LogTag;

    // Data for which internal AddDataUnconfirmed tx is still executing.
    THashMap<ui64, TTrackedUnconfirmedData> UnconfirmedDataInProgress;
    // Data written to local db but not yet confirmed/indexed
    THashMap<ui64, TTrackedUnconfirmedData> UnconfirmedData;
    // Data confirmed but not yet added to index
    THashMap<ui64, TTrackedUnconfirmedData> ConfirmedData;

    // CommitIds of writes still to confirm during startup recovery, in commitId
    // order. They are confirmed one at a time, so a single AddBlob
    // is ever in flight up to SafePoint and page faults cannot reorder TXes
    TDeque<ui64> RecoveredDataToConfirm;

    // CommitIds scheduled for unconfirmed-data deletion and waiting for
    // completion.
    THashSet<ui64> DeletionQueue;
    // ConfirmAddData requests that arrived before internal AddData completed.
    // Used for all requests until (#5353)
    THashMap<ui64, TVector<TPendingConfirmAddData>> PendingConfirmation;

    // Recovery gate for data operations: true when startup unconfirmed flow
    // has completed recovery confirmation.
    bool UnconfirmedRecoveryReady = false;

protected:
    void SetUnconfirmedRecoveryReady(bool value);

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
        const TVector<ui64>& deferredNodeDestructionIds,
        const TVector<NProto::TOpLogEntry>& opLog,
        const TVector<NProtoPrivate::TResponseLogEntry>& responseLog,
        const TThrottlerConfig& throttlerConfig);

    bool IsStateLoaded() const
    {
        return StateLoaded;
    }

    void CompleteStateLoad()
    {
        StateLoaded = true;
    }

    bool UpdateAccessStats(ui64 nodeId, TInstant now);

    TVector<TNodeAccessStats> GetNodeAccessStats(TInstant now, ui32 n) const;

    void UpdateConfig(
        IIndexTabletDatabase& db,
        const TStorageConfig& config,
        const NProto::TFileSystem& fileSystem,
        const TThrottlerConfig& throttlerConfig);

    void SetFrozen(IIndexTabletDatabase& db, bool frozen);

    void SetCompressNodeRef(IIndexTabletDatabase& db, bool compressNodeRef);

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

    TString GetCloudId() const
    {
        return FileSystem.GetCloudId();
    }

    TString GetFolderId() const
    {
        return FileSystem.GetFolderId();
    }

    TString GetMainFileSystemId() const
    {
        // As of now TFileSystem::MainFileSystemId is empty for the main
        // filesystem. It should be fixed. TODO(#6065)
        STORAGE_VERIFY_DEBUG(
            FileSystem.GetShardNo() == 0 || FileSystem.GetMainFileSystemId(),
            FileSystem.GetFileSystemId(),
            TWellKnownEntityTypes::FILESYSTEM);

        return FileSystem.GetMainFileSystemId()
                   ? FileSystem.GetMainFileSystemId()
                   : FileSystem.GetFileSystemId();
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

    bool GetCompressNodeRef() const
    {
        return CompressNodeRef || FileSystem.GetCompressNodeRef();
    }

    ui64 GetCurrentCommitId() const
    {
        return MakeCommitId(Generation, LastStep);
    }

    ui64 GenerateCommitId()
    {
        if (LastStep == MaxTabletStep) {
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

    ui64 GetMinDeletionMarkersCountSinceTabletStart() const
    {
        return MinDeletionMarkersCountSinceTabletStart;
    }

    void UpdateMinDeletionMarkersCountSinceTabletStart()
    {
        MinDeletionMarkersCountSinceTabletStart = Min(
            MinDeletionMarkersCountSinceTabletStart,
            FileSystemStats.GetDeletionMarkersCount()
        );
    }

    const TNodeToSessionCounters& GetNodeToSessionCounters() const
    {
        return NodeToSessionCounters;
    }

    bool UpdateLatencyStats(
        ui64 nodeId,
        EFileStoreRequest requestType,
        TInstant now,
        TDuration latency);

    TVector<TNodeLatencyStats> GetLatencyStats(TInstant now, ui32 n) const;

    TMiscNodeStats GetMiscNodeStats() const;
    THandlesStats GetHandlesStats() const;

    const NProto::TFileStorePerformanceProfile& GetPerformanceProfile() const;

    const TFileStoreAllocRegistry& GetFileStoreProfilingRegistry() const
    {
        return AllocatorRegistry;
    }

    IAllocator* GetAllocator(EAllocatorTag tag) const
    {
        return AllocatorRegistry.GetAllocator(tag);
    }

    ui64 CalculateMinExpectedShardCount(ui32 maxShardCount) const;

    void InitInMemoryIndexState(const TStorageConfig& config);

    NProto::TError SelectShard(
        NProto::ENodeType nodeType,
        ui64 fileSize,
        TString* shardId);

    void InitShardBalancer(const TStorageConfig& config);

    NProto::TError UpdateShardBalancer(const TVector<TShardStats>& stats);

    TVector<TShardStats> MakeOrderedShardList() const;

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
    void Set##name(IIndexTabletDatabase& db, ui64 value)                       \
    {                                                                          \
        FileSystemStats.Set##name(value);                                      \
        db.Write##name(value);                                                 \
    }                                                                          \
    ui64 Increment##name(IIndexTabletDatabase& db, size_t delta = 1)           \
    {                                                                          \
        ui64 value = SafeIncrement(FileSystemStats.Get##name(), delta);        \
        FileSystemStats.Set##name(value);                                      \
        db.Write##name(value);                                                 \
        return value;                                                          \
    }                                                                          \
    ui64 Decrement##name(IIndexTabletDatabase& db, size_t delta = 1)           \
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
    void CreateNodeWithId(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const NProto::TNode& attrs);

    ui64 CreateNode(
        IIndexTabletDatabase& db,
        ui64 commitId,
        const NProto::TNode& attrs);

    void UpdateNode(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const NProto::TNode& attrs,
        const NProto::TNode& prevAttrs);

    [[nodiscard]] NProto::TError RemoveNode(
        IIndexTabletDatabase& db,
        const INodeIndexTabletDatabase::TNode& node,
        ui64 minCommitId,
        ui64 maxCommitId);

    // True if unlinking this node destroys it, i.e. it has no other links and
    // no open handles.
    bool UnlinkDestroysNode(const INodeIndexTabletDatabase::TNode& node) const;

    // If deferDestruction is set, a node that loses its last reference is kept
    // in the index and registered for a deferred destruction instead of being
    // removed right away.
    [[nodiscard]] NProto::TError UnlinkNode(
        IIndexTabletDatabase& db,
        ui64 parentNodeId,
        const TString& name,
        const INodeIndexTabletDatabase::TNode& node,
        ui64 minCommitId,
        ui64 maxCommitId,
        bool removeNodeRef,
        bool deferDestruction);

    void UnlinkExternalNode(
        IIndexTabletDatabase& db,
        ui64 parentNodeId,
        const TString& name,
        const TString& shardId,
        const TString& shardNodeName,
        ui64 minCommitId,
        ui64 maxCommitId);

    bool ReadNode(
        INodeIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        TMaybe<INodeIndexTabletDatabase::TNode>& node);

    void RewriteNode(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const NProto::TNode& attrs);

    void WriteOrphanNode(
        IIndexTabletDatabase& db,
        const TString& message,
        ui64 nodeId);

    //
    // DeferredNodeDestruction
    //

    void AddDeferredNodeDestruction(IIndexTabletDatabase& db, ui64 nodeId);
    void RemoveDeferredNodeDestruction(IIndexTabletDatabase& db, ui64 nodeId);
    bool HasDeferredNodeDestruction(ui64 nodeId) const;
    ui64 GetDeferredNodeDestructionCount() const;
    TVector<ui64> GetDeferredNodeDestructionIds(ui64 maxCount) const;

    bool HasPendingNodeCreateInShard(const TString& nodeName) const;

    void StartNodeCreateInShard(const TString& nodeName);

    void EndNodeCreateInShard(const TString& nodeName);

private:
    void UpdateUsedBlocksCount(
        IIndexTabletDatabase& db,
        ui64 currentSize,
        ui64 prevSize);

    // Applies a signed usage delta to quotaId's counters, both in-memory and
    // persisted. A no-op for quotaId == 0 or a zero delta.
    void UpdateQuotaUsage(
        IIndexTabletDatabase& db,
        ui32 quotaId,
        i64 bytesDelta,
        i64 nodesDelta);

    //
    // NodeAttrs
    //

public:
    ui64 CreateNodeAttr(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        const TString& value);

    ui64 UpdateNodeAttr(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const INodeIndexTabletDatabase::TNodeAttr& attr,
        const TString& newValue);

    void RemoveNodeAttr(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const INodeIndexTabletDatabase::TNodeAttr& attr);

    bool ReadNodeAttr(
        INodeIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<INodeIndexTabletDatabase::TNodeAttr>& attr);

    bool ReadNodeAttrs(
        INodeIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        TVector<INodeIndexTabletDatabase::TNodeAttr>& attrs);

    void RewriteNodeAttr(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const INodeIndexTabletDatabase::TNodeAttr& attr);


    //
    // hasXAttrs
    //

public:
    enum class EHasXAttrs : ui64 {
        Unknown = 0,
        True = 1,
        False = 2
    };

    void WriteHasXAttrs(IIndexTabletDatabase& db, EHasXAttrs hasXAttrs);

    //
    // NodeRefs
    //

public:
    void CreateNodeRef(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TString& childName,
        ui64 childNodeId,
        const TString& shardId,
        const TString& shardNodeName,
        bool markExhaustive = false);

    void RemoveNodeRef(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const TString& childName,
        ui64 prevChildNodeId,
        const TString& shardId,
        const TString& shardNodeName);

    bool ReadNodeRef(
        INodeIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<INodeIndexTabletDatabase::TNodeRef>& ref);

    bool ReadNodeRefs(
        INodeIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TString& cookie,
        TVector<INodeIndexTabletDatabase::TNodeRef>& refs,
        ui32 maxBytes,
        TString* next = nullptr,
        bool noAutoPrecharge = false,
        NProto::EListNodesSizeMode sizeMode = NProto::LNSM_NAME_ONLY);

    bool ReadNodeRefs(
        INodeIndexTabletDatabase& db,
        ui64 startNodeId,
        const TString& startCookie,
        ui64 maxCount,
        TVector<INodeIndexTabletDatabase::TNodeRef>& refs,
        ui64& nextNodeId,
        TString& nextCookie);

    bool PrechargeNodeRefs(
        INodeIndexTabletDatabase& db,
        ui64 nodeId,
        const TString& cookie,
        ui64 rowsToPrecharge,
        ui64 bytesToPrecharge);

    void RewriteNodeRef(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const TString& childName,
        ui64 childNodeId,
        const TString& shardId,
        const TString& shardNodeName);

    bool TryLockNodeRef(TNodeRefKey key);
    void UnlockNodeRef(const TNodeRefKey& key);
    bool IsNodeRefLocked(const TNodeRefKey& key) const;
    using TNodeRefLockVisitor = std::function<void(const TNodeRefKey&)>;
    void VisitNodeRefLocks(const TNodeRefLockVisitor& visitor) const;

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
        IIndexTabletDatabase& db,
        const TString& clientId,
        const TString& sessionId,
        const TString& checkpointId,
        const TString& originFqdn,
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner,
        const NProto::TSessionOptions& sessionOptions);

    void RemoveSession(
        IIndexTabletDatabase& db,
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
    void RegisterSessionByPipeServer(
        const NActors::TActorId& pipeServer,
        const TString& sessionId);
    void UnregisterSessionByPipeServer(const TString& sessionId);
    const TVector<TString>& FindSessionIdsByPipeServer(
        const NActors::TActorId& pipeServer) const;
    void RemoveSessionByPipeServer(const NActors::TActorId& pipeServer);
    void OrphanSession(const NActors::TActorId& owner, TInstant inactivityDeadline);
    void ResetSession(IIndexTabletDatabase& db, TSession* session, const TMaybe<TString>& state);

    TVector<TSession*> GetTimedOutSessions(TInstant now) const;
    TVector<TSession*> GetSessionsToNotify(const NProto::TSessionEvent& event) const;
    TVector<NProtoPrivate::TTabletSessionInfo> DescribeSessions() const;

    const TSessionHistoryList& GetSessionHistoryList() const;
    void AddSessionHistoryEntry(
        IIndexTabletDatabase& db,
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
        TInstant inactivityDeadline,
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
        IIndexTabletDatabase& db,
        TSession* session,
        ui64 nodeId,
        ui64 commitId,
        ui32 flags);

    // Registers a handle with an explicitly specified id. Registration is
    // idempotent if the handle already belongs to the given session and node.
    // Returns an error on a collision or if the handle cannot be created.
    [[nodiscard]] NProto::TError RegisterHandle(
        IIndexTabletDatabase& db,
        TSession* session,
        ui64 handleId,
        ui64 nodeId,
        ui64 commitId,
        ui32 flags);

    TSessionHandle* UnsafeCreateHandle(
        IIndexTabletDatabase& db,
        TSession* session,
        ui64 handleId,
        ui64 nodeId,
        ui64 commitId,
        ui32 flags);

    void DestroyHandle(
        IIndexTabletDatabase& db,
        TSessionHandle* handle);

    TSessionHandle* FindHandle(ui64 handle) const;

    bool HasPendingCreateHandleCommit(ui64 handle) const;

    void StartCreateHandleCommit(ui64 handle);

    void EndCreateHandleCommit(ui64 handle);

    bool HasOpenHandles(ui64 nodeId) const;

private:
    ui64 GenerateHandle() const;

    TSessionHandle* CreateHandle(
        IIndexTabletDatabase& db,
        TSession* session,
        ui64 handleId,
        ui64 nodeId,
        ui64 commitId,
        ui32 flags);

    TSessionHandle* CreateHandle(
        TSession* session,
        const NProto::TSessionHandle& proto);

    void RemoveHandle(TSessionHandle* handle);

    //
    // Locks
    //

public:
    TRangeLockOperationResult AcquireLock(
        IIndexTabletDatabase& db,
        TSession* session,
        ui64 handle,
        const TLockRange& range);

    TRangeLockOperationResult ReleaseLock(
        IIndexTabletDatabase& db,
        TSession* session,
        const TLockRange& range);

    TRangeLockOperationResult TestLock(
        TSession* session,
        const TSessionHandle* handle,
        const TLockRange& range) const;

    void ReleaseLocks(IIndexTabletDatabase& db, ui64 handle);

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
        IIndexTabletDatabase& db,                                               \
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
        IIndexTabletDatabase& db,
        const TString& sessionId,
        ui64 requestId,
        NProto::TCreateNodeResponse response);

    void PatchDupCacheEntry(
        IIndexTabletDatabase& db,
        const TString& sessionId,
        ui64 requestId,
        NProto::TRenameNodeResponse response);

    void CommitDupCacheEntry(
        const TString& sessionId,
        ui64 requestId);

    //
    // OpLog
    //

public:
    void WriteOpLogEntry(
        IIndexTabletDatabase& db,
        const NProto::TOpLogEntry& e);

    void DeleteOpLogEntry(IIndexTabletDatabase& db, ui64 entryId);

    ui64 GetOpLogEntryCount() const;

    //
    // ResponseLog
    //

public:
    const NProtoPrivate::TResponseLogEntry* LookupResponseLogEntry(
        ui64 clientTabletId,
        ui64 requestId) const;

    void WriteResponseLogEntry(
        IIndexTabletDatabase& db,
        const NProtoPrivate::TResponseLogEntry& e);

    void CommitResponseLogEntry(NProtoPrivate::TResponseLogEntry e);

    void DeleteResponseLogEntry(
        IIndexTabletDatabase& db,
        ui64 clientTabletId,
        ui64 requestId);

    ui64 GetResponseLogEntryCount() const;

    TVector<TInternalRequestId> ListOldResponseLogEntries(
        TInstant minTimestamp);

    //
    // Writes
    //

public:
    bool GenerateBlobId(
        ui64 commitId,
        ui32 blobSize,
        ui32 blobIndex,
        TPartialBlobId* blobId) const;

    struct TBackpressureThresholds
    {
        const ui64 Flush;
        const ui64 FlushBytes;
        const ui64 FlushBytesItemCount;
        const ui64 CompactionScore;
        const ui64 CleanupScore;
        const ui64 CollectGarbage;
    };

    using TBackpressureValues = TBackpressureThresholds;

    static bool IsWriteAllowed(
        const TBackpressureThresholds& thresholds,
        const TBackpressureValues& values,
        TString* message);

    //
    // UnconfirmedData / ConfirmedData
    //

public:
    void ConfirmedDataAdded(IIndexTabletDatabase& db, ui64 commitId);

    void LoadUnconfirmedData(
        TVector<TIndexTabletDatabase::TUnconfirmedDataEntry> entries);

    bool HasDataOverlapWithUnconfirmed(
        ui64 nodeId,
        const TByteRange& requestRange) const;

    // range.End() == Max<ui64>() marks a write that may change the file size.
    void ActivateCacheReadBypass(
        ui64 nodeId,
        ui64 commitId,
        const TByteRange& range);
    void DeactivateCacheReadBypass(ui64 nodeId, ui64 commitId);
    ui64 GetReadNodeCacheBypassCount() const;
    ui64 GetReadAheadCacheBypassCount() const;

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
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        TStringBuf data);

    void WriteFreshBytesDeletionMarker(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        ui64 len);

    TFlushBytesCleanupInfo StartFlushBytes(
        TVector<TBytes>* bytes,
        TVector<TBytes>* deletionMarkers);
    TFlushBytesStats FinishFlushBytes(
        IIndexTabletDatabase& db,
        ui64 itemLimit,
        ui64 chunkId,
        NProto::TProfileLogRequestInfo& profileLogRequest);

    ui32 GetFreshBytesItemCount() const;

private:
    void UpdateFreshBytesItemCount();

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
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        TStringBuf blockData);

    void MarkFreshBlocksDeleted(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount);

    void DeleteFreshBlocks(
        IIndexTabletDatabase& db,
        const TVector<TBlock>& blocks);

    //
    // MixedBlocks
    //

public:
    bool LoadMixedBlocks(INodeIndexTabletDatabase& db, ui32 rangeId);
    void ReleaseMixedBlocks(ui32 rangeId);
    void ReleaseMixedBlocks(const TSet<ui32>& ranges);

    void FindMixedBlocks(
        IMixedBlockVisitor& visitor,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) const;

    void WriteMixedBlocks(
        IIndexTabletDatabase& db,
        const TPartialBlobId& blobId,
        const TBlock& block,
        ui32 blocksCount);

    TWriteMixedBlocksResult WriteMixedBlocks(
        IIndexTabletDatabase& db,
        const TPartialBlobId& blobId,
        /*const*/ TVector<TBlock>& blocks);

    void DeleteMixedBlocks(
        IIndexTabletDatabase& db,
        const TPartialBlobId& blobId,
        const TVector<TBlock>& blocks);

    TVector<TMixedBlobMeta> GetBlobsForCompaction(ui32 rangeId) const;

    TMixedBlobMeta FindBlob(ui32 rangeId, TPartialBlobId blobId) const;

    void MarkMixedBlocksDeleted(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount);

    // returns processed deletion marker count
    ui32 CleanupBlockDeletions(
        IIndexTabletDatabase& db,
        ui32 rangeId,
        NProto::TProfileLogRequestInfo& profileLogRequest);

    bool UpdateBlockLists(
        IIndexTabletDatabase& db,
        TMixedBlobMeta& blob);

    void RewriteMixedBlocks(
        IIndexTabletDatabase& db,
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
        IIndexTabletDatabase& db,
        ui32 rangeId,
        const TPartialBlobId& blobId,
        /*const*/ TVector<TBlock>& blocks);

    TDeleteMixedBlocksResult DeleteMixedBlocks(
        IIndexTabletDatabase& db,
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
    ui32 NextCollectPerGenerationCounter()
    {
        return ++LastCollectPerGenerationCounter;
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
        IIndexTabletDatabase& db,
        ui64 collectCommitId,
        const TVector<TPartialBlobId>& newBlobs,
        const TVector<TPartialBlobId>& garbageBlobs);

private:
    void AddNewBlob(IIndexTabletDatabase& db, const TPartialBlobId& blobId);
    void AddGarbageBlob(IIndexTabletDatabase& db, const TPartialBlobId& blobId);

    //
    // Checkpoints
    //

public:
    void LoadCheckpoints(const TVector<NProto::TCheckpoint>& checkpoints);

    TVector<TCheckpoint*> GetCheckpoints() const;

    TCheckpoint* FindCheckpoint(const TString& checkpointId) const;

    ui64 GetReadCommitId(const TString& checkpointId) const;

    TCheckpoint* CreateCheckpoint(
        IIndexTabletDatabase& db,
        const TString& checkpointId,
        ui64 nodeId,
        ui64 commitId);

    void MarkCheckpointDeleted(
        IIndexTabletDatabase& db,
        TCheckpoint* checkpoint);

    void RemoveCheckpointNodes(
        IIndexTabletDatabase& db,
        TCheckpoint* checkpoint,
        const TVector<ui64>& nodes);

    void RemoveCheckpointBlob(
        IIndexTabletDatabase& db,
        TCheckpoint* checkpoint,
        ui32 rangeId,
        const TPartialBlobId& blobId);

    void RemoveCheckpoint(
        IIndexTabletDatabase& db,
        TCheckpoint* checkpoint);

private:
    void AddCheckpointNode(
        IIndexTabletDatabase& db,
        ui64 checkpointId,
        ui64 nodeId);

    void AddCheckpointBlob(
        IIndexTabletDatabase& db,
        ui64 checkpointId,
        ui32 rangeId,
        const TPartialBlobId& blobId);

    //
    // Quotas
    //

public:
    void LoadQuotas(const TVector<NProto::TQuota>& quotas);

    TVector<NProto::TQuota> GetQuotas() const;

    const NProto::TQuota* FindQuota(ui32 quotaId) const;

    const NProto::TQuota& SetQuota(
        IIndexTabletDatabase& db,
        ui32 quotaId,
        ui64 maxBytes,
        ui64 maxNodes,
        TInstant now);

    void DeleteQuota(IIndexTabletDatabase& db, ui32 quotaId);

    void LoadQuotaUsages(const TVector<TQuotaUsage>& usages);

    TVector<TQuotaUsage> GetQuotaUsages() const;

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

    bool AdvanceBackgroundBlobIndexOp()
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

    void AddTruncate(IIndexTabletDatabase& db, ui64 nodeId, TByteRange range);
    void DeleteTruncate(IIndexTabletDatabase& db, ui64 nodeId);

    [[nodiscard]] NProto::TError Truncate(
        IIndexTabletDatabase& db,
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
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        TByteRange range);

    // Call this function only if you need to zero range without further
    // resizing the node. This function:
    // - writes fresh bytes (zeroes) on unaligned head, if any;
    // - writes fresh bytes (zeroes) on unaligned tail, if any.
    [[nodiscard]] NProto::TError ZeroRange(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        TByteRange range);

private:
    [[nodiscard]] NProto::TError DeleteRange(
        IIndexTabletDatabase& db,
        ui64 nodeId,
        ui64 commitId,
        const TByteRange& range);

public:

    ////////////////////////////////////////////////////////////////////////////
    // Caching: ReadAhead, InMemoryIndexState
    ////////////////////////////////////////////////////////////////////////////

    //
    // ReadAhead.
    //

    bool TryFillDescribeResult(
        ui64 nodeId,
        ui64 handle,
        ui64 commitId,
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
    // In-memory index state.
    //

    INodeIndexTabletDatabase* AccessInMemoryIndexState();
    void UpdateInMemoryIndexState(
        const TVector<IInMemoryIndexState::TIndexStateRequest>& nodeUpdates);
    void MarkNodeRefsLoadComplete();
    void MarkNodeRefsExhaustive(ui64 nodeId);
    TInMemoryIndexStateStats GetInMemoryIndexStateStats() const;
};

}   // namespace NCloud::NFileStore::NStorage
