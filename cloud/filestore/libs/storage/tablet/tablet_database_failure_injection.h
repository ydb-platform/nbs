#pragma once

#include "tablet_database.h"

#include <cloud/filestore/libs/storage/core/tablet_tx_rescheduler.h>


namespace NCloud::NFileStore::NStorage {

class TIndexTabletDatabaseWithFailureInjection : public IIndexTabletDatabase
{
    std::unique_ptr<IIndexTabletDatabase> Real;

    ITxReschedulerPtr TestReadRescheduler;

    bool ShouldFailReadInTest() {
        return TestReadRescheduler && TestReadRescheduler->ShouldReschedule();
    }

public:
    TIndexTabletDatabaseWithFailureInjection(
        std::unique_ptr<IIndexTabletDatabase> real,
        ITxReschedulerPtr rescheduler)
        : Real(std::move(real))
        , TestReadRescheduler(std::move(rescheduler))
    {}

    void InitSchema() override;

    //
    // FileSystem
    //

    void WriteFileSystem(const NProto::TFileSystem& fileSystem) override;
    bool ReadFileSystem(NProto::TFileSystem& fileSystem) override;
    bool ReadFileSystemStats(NProto::TFileSystemStats& stats) override;

#define FILESTORE_DECLARE_STATS(name, ...)                                     \
    void Write##name(ui64 value) override;                                     \
// FILESTORE_DECLARE_STATS

FILESTORE_FILESYSTEM_STATS(FILESTORE_DECLARE_STATS)

#undef FILESTORE_DECLARE_STATS
    void WriteStorageConfig(
        const NProto::TStorageConfig& storageConfig) override;
    bool ReadStorageConfig(
        TMaybe<NProto::TStorageConfig>& storageConfig) override;

    bool ReadTabletStorageInfo(
        NCloud::NProto::TTabletStorageInfo& tabletStorageInfo) override;
    void WriteTabletStorageInfo(
        const NCloud::NProto::TTabletStorageInfo& tabletStorageInfo) override;

    //
    // Nodes
    //

    void WriteNode(
        ui64 nodeId,
        ui64 commitId,
        const NProto::TNode& attrs) override;
    void DeleteNode(ui64 nodeId) override;
    bool ReadNode(
        ui64 nodeId,
        ui64 commitId,
        TMaybe<TNode>& node) override;
    bool ReadNodes(
        ui64 startNodeId,
        ui64 maxNodes,
        ui64& nextNodeId,
        TVector<TNode>& nodes) override;

    //
    // Nodes_Ver
    //

    void WriteNodeVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const NProto::TNode& attrs) override;
    void DeleteNodeVer(ui64 nodeId, ui64 commitId) override;
    bool ReadNodeVer(
        ui64 nodeId,
        ui64 commitId,
        TMaybe<TNode>& node) override;

    //
    // NodeAttrs
    //

    void WriteNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        const TString& value,
        ui64 version) override;
    void DeleteNodeAttr(ui64 nodeId, const TString& name) override;
    bool ReadNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) override;
    bool ReadNodeAttrs(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeAttr>& attrs) override;

    //
    // NodeAttrs_Ver
    //

    void WriteNodeAttrVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const TString& name,
        const TString& value,
        ui64 version) override;
    void DeleteNodeAttrVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name) override;
    bool ReadNodeAttrVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) override;
    bool ReadNodeAttrVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeAttr>& attrs) override;

    //
    // NodeRefs
    //

    void WriteNodeRef(
        const TNodeRef& nodeRef,
        bool markExhaustive) override;
    void DeleteNodeRef(ui64 nodeId, const TString& name) override;
    bool ReadNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) override;
    bool ReadNodeRefs(
        ui64 nodeId,
        ui64 commitId,
        const TString& cookie,
        TVector<TNodeRef>& refs,
        ui32 maxBytes,
        TString* next,
        ui32* skippedRefs,
        bool noAutoPrecharge,
        NProto::EListNodesSizeMode sizeMode) override;
    bool ReadNodeRefs(
        ui64 startNodeId,
        const TString& startCookie,
        ui64 maxCount,
        TVector<TNodeRef>& refs,
        ui64& nextNodeId,
        TString& nextCookie) override;
    bool PrechargeNodeRefs(
        ui64 nodeId,
        const TString& cookie,
        ui64 rowsToPrecharge,
        ui64 bytesToPrecharge) override;

    //
    // NodeRefs_Ver
    //

    void WriteNodeRefVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const TString& name,
        ui64 childNode,
        const TString& shardId,
        const TString& shardNodeName) override;
    void DeleteNodeRefVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name) override;
    bool ReadNodeRefVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) override;
    bool ReadNodeRefVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeRef>& refs) override;

    //
    // TruncateQueue
    //

    void WriteTruncateQueueEntry(ui64 nodeId, TByteRange range) override;
    void DeleteTruncateQueueEntry(ui64 id) override;
    bool ReadTruncateQueue(
        TVector<NProto::TTruncateEntry>& entries) override;

    //
    // Sessions
    //

    void WriteSession(const NProto::TSession& session) override;
    void DeleteSession(const TString& sessionId) override;
    bool ReadSessions(TVector<NProto::TSession>& sessions) override;

    //
    // SessionHandles
    //

    void WriteSessionHandle(const NProto::TSessionHandle& handle) override;
    void DeleteSessionHandle(
        const TString& sessionId,
        ui64 handle) override;
    bool ReadSessionHandles(
        TVector<NProto::TSessionHandle>& handles) override;
    bool ReadSessionHandles(
        const TString& sessionId,
        TVector<NProto::TSessionHandle>& handles) override;

    //
    // SessionLocks
    //

    void WriteSessionLock(const NProto::TSessionLock& lock) override;
    void DeleteSessionLock(
        const TString& sessionId,
        ui64 lockId) override;
    bool ReadSessionLocks(TVector<NProto::TSessionLock>& locks) override;
    bool ReadSessionLocks(
        const TString& sessionId,
        TVector<NProto::TSessionLock>& locks) override;

    //
    // SessionDuplicateCache
    //

    void WriteSessionDupCacheEntry(
        const NProto::TDupCacheEntry& entry) override;
    void DeleteSessionDupCacheEntry(
        const TString& sessionId,
        ui64 entryId) override;
    bool ReadSessionDupCacheEntries(
        TVector<NProto::TDupCacheEntry>& entries) override;

    //
    // SessionHistory
    //

    void WriteSessionHistoryEntry(
        const NProto::TSessionHistoryEntry& entry) override;
    void DeleteSessionHistoryEntry(ui64 entryId) override;
    bool ReadSessionHistoryEntries(
        TVector<NProto::TSessionHistoryEntry>& entries) override;

    //
    // FreshBytes
    //

    void WriteFreshBytes(
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        TStringBuf data) override;
    void WriteFreshBytesDeletionMarker(
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        ui64 len) override;
    void DeleteFreshBytes(
        ui64 nodeId,
        ui64 commitId,
        ui64 offset) override;
    bool ReadFreshBytes(TVector<TFreshBytesEntry>& bytes) override;

    //
    // FreshBlocks
    //

    void WriteFreshBlock(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        TStringBuf blockData) override;
    void MarkFreshBlockDeleted(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        ui32 blockIndex) override;
    void DeleteFreshBlock(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex) override;
    bool ReadFreshBlocks(TVector<TFreshBlock>& blocks) override;

    //
    // MixedBlocks
    //

    void WriteMixedBlocks(
        ui32 rangeId,
        const TPartialBlobId& blobId,
        const TBlockList& blockList,
        ui32 garbageBlocks,
        ui32 checkpointBlocks) override;
    void DeleteMixedBlocks(
        ui32 rangeId,
        const TPartialBlobId& blobId) override;
    bool ReadMixedBlocks(
        ui32 rangeId,
        const TPartialBlobId& blobId,
        TMaybe<TMixedBlob>& blob,
        IAllocator* alloc) override;
    bool ReadMixedBlocks(
        ui32 rangeId,
        TVector<TMixedBlob>& blobs,
        IAllocator* alloc) override;

    //
    // DeletionMarkers
    //

    void WriteDeletionMarkers(
        ui32 rangeId,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) override;
    void DeleteDeletionMarker(
        ui32 rangeId,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex) override;
    bool ReadDeletionMarkers(
        ui32 rangeId,
        TVector<TDeletionMarker>& deletionMarkers) override;

    //
    // LargeDeletionMarkers
    //

    void WriteLargeDeletionMarkers(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) override;
    void DeleteLargeDeletionMarker(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex) override;
    bool ReadLargeDeletionMarkers(
        TVector<TDeletionMarker>& deletionMarkers) override;

    //
    // OrphanNodes
    //

    void WriteOrphanNode(ui64 nodeId) override;
    void DeleteOrphanNode(ui64 nodeId) override;
    bool ReadOrphanNodes(TVector<ui64>& nodeIds) override;

    //
    // NewBlobs
    //

    void WriteNewBlob(const TPartialBlobId& blobId) override;
    void DeleteNewBlob(const TPartialBlobId& blobId) override;
    bool ReadNewBlobs(TVector<TPartialBlobId>& blobIds) override;

    //
    // GarbageBlobs
    //

    void WriteGarbageBlob(const TPartialBlobId& blobId) override;
    void DeleteGarbageBlob(const TPartialBlobId& blobId) override;
    bool ReadGarbageBlobs(TVector<TPartialBlobId>& blobIds) override;

    //
    // Checkpoints
    //

    void WriteCheckpoint(const NProto::TCheckpoint& checkpoint) override;
    void DeleteCheckpoint(const TString& checkpointId) override;
    bool ReadCheckpoints(
        TVector<NProto::TCheckpoint>& checkpoints) override;

    //
    // Quotas
    //

    void WriteQuota(const NProto::TQuota& quota) override;
    void DeleteQuota(ui32 quotaId) override;
    bool ReadQuotas(TVector<NProto::TQuota>& quotas) override;

    //
    // QuotaUsage
    //

    void WriteQuotaUsage(
        ui32 quotaId,
        ui64 usedBytes,
        ui64 usedNodes) override;
    void DeleteQuotaUsage(ui32 quotaId) override;
    bool ReadQuotaUsages(TVector<TQuotaUsage>& usages) override;

    //
    // CheckpointNodes
    //

    void WriteCheckpointNode(ui64 checkpointId, ui64 nodeId) override;
    void DeleteCheckpointNode(ui64 checkpointId, ui64 nodeId) override;
    bool ReadCheckpointNodes(
        ui64 checkpointId,
        TVector<ui64>& nodes,
        size_t maxCount) override;

    //
    // CheckpointBlobs
    //

    void WriteCheckpointBlob(
        ui64 checkpointId,
        ui32 rangeId,
        const TPartialBlobId& blobId) override;
    void DeleteCheckpointBlob(
        ui64 checkpointId,
        ui32 rangeId,
        const TPartialBlobId& blobId) override;
    bool ReadCheckpointBlobs(
        ui64 checkpointId,
        TVector<TCheckpointBlob>& blobs,
        size_t maxCount) override;

    //
    // CompactionMap
    //

    void ForceWriteCompactionMap(
        ui32 rangeId,
        ui32 blobsCount,
        ui32 deletionsCount,
        ui32 garbageBlocksCount) override;
    void WriteCompactionMap(
        ui32 rangeId,
        ui32 blobsCount,
        ui32 deletionsCount,
        ui32 garbageBlocksCount) override;
    bool ReadCompactionMap(
        TVector<TCompactionRangeInfo>& compactionMap) override;

    bool ReadCompactionMap(
        TVector<TCompactionRangeInfo>& compactionMap,
        ui32 firstRangeId,
        ui32 rangeCount,
        bool prechargeAll) override;

    //
    // OpLog
    //

    void WriteOpLogEntry(const NProto::TOpLogEntry& entry) override;
    void DeleteOpLogEntry(ui64 entryId) override;
    bool ReadOpLogEntry(
        ui64 entryId,
        TMaybe<NProto::TOpLogEntry>& entry) override;
    bool ReadOpLog(TVector<NProto::TOpLogEntry>& opLog) override;

    //
    // ResponseLog
    //

    void WriteResponseLogEntry(
        const NProtoPrivate::TResponseLogEntry& entry) override;
    void DeleteResponseLogEntry(
        ui64 clientTabletId,
        ui64 requestId) override;
    bool ReadResponseLogEntry(
        ui64 clientTabletId,
        ui64 requestId,
        TMaybe<NProtoPrivate::TResponseLogEntry>& entry) override;
    bool ReadResponseLog(
        TVector<NProtoPrivate::TResponseLogEntry>& responseLog) override;

    //
    // UnconfirmedData
    //

    void WriteUnconfirmedData(
        ui64 commitId,
        const NProto::TUnconfirmedData& data) override;
    void DeleteUnconfirmedData(ui64 commitId) override;
    bool ReadUnconfirmedData(
        TVector<TUnconfirmedDataEntry>& entries) override;
};

} // namespace NCloud::NFileStore::NStorage
