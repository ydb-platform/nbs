#pragma once

#include <cloud/filestore/libs/storage/tablet/model/block_list.h>
#include <cloud/filestore/libs/storage/tablet/model/compaction_map.h>
#include <cloud/filestore/libs/storage/tablet/model/deletion_markers.h>
#include <cloud/filestore/libs/storage/tablet/model/quota.h>
#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

namespace NCloud::NProto {

class TTabletStorageInfo;

} // namespace NCloud::NProto

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_FILESYSTEM_STATS(xxx, ...)                                   \
    xxx(LastNodeId,             __VA_ARGS__)                                   \
    xxx(LastLockId,             __VA_ARGS__)                                   \
    xxx(LastCollectCommitId,    __VA_ARGS__)                                   \
    xxx(LastXAttr,              __VA_ARGS__)                                   \
    xxx(HasXAttrs,              __VA_ARGS__)                                   \
                                                                               \
    xxx(UsedNodesCount,         __VA_ARGS__)                                   \
    xxx(UsedSessionsCount,      __VA_ARGS__)                                   \
    xxx(UsedHandlesCount,       __VA_ARGS__)                                   \
    xxx(UsedLocksCount,         __VA_ARGS__)                                   \
    xxx(UsedBlocksCount,        __VA_ARGS__)                                   \
                                                                               \
    xxx(FreshBlocksCount,           __VA_ARGS__)                               \
    xxx(MixedBlocksCount,           __VA_ARGS__)                               \
    xxx(MixedBlobsCount,            __VA_ARGS__)                               \
    xxx(DeletionMarkersCount,       __VA_ARGS__)                               \
    xxx(GarbageQueueSize,           __VA_ARGS__)                               \
    xxx(GarbageBlocksCount,         __VA_ARGS__)                               \
    xxx(CheckpointNodesCount,       __VA_ARGS__)                               \
    xxx(CheckpointBlocksCount,      __VA_ARGS__)                               \
    xxx(CheckpointBlobsCount,       __VA_ARGS__)                               \
    xxx(FreshBytesCount,            __VA_ARGS__)                               \
    xxx(AttrsUsedBytesCount,        __VA_ARGS__)                               \
    xxx(DeletedFreshBytesCount,     __VA_ARGS__)                               \
    xxx(LargeDeletionMarkersCount,  __VA_ARGS__)                               \
// FILESTORE_FILESYSTEM_STATS

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief This interface contains a subset of the methods that can be performed
 * over the localDB tables. Those are all the operations, that are performed
 * with the following tables (a.k.a inode index):
 *  - Nodes
 *  - Nodes_Ver
 *  - NodeAttrs
 *  - NodeAttrs_Ver
 *  - NodeRefs
 *  - NodeRefs_Ver
 *  - CheckpointNodes
 *
 * Also this interface contains methods related to data index: ReadMixedBlocks
 * and ReadDeletionMarkers which are not supposed to be used in the inode index.
 * But they are needed for the ReadData operation.
 */
class INodeIndexTabletDatabase
{
public:
    struct TNode
    {
        ui64 NodeId;
        NProto::TNode Attrs;
        ui64 MinCommitId;
        ui64 MaxCommitId;
    };

    struct TNodeRef
    {
        ui64 NodeId;
        TString Name;
        ui64 ChildNodeId;
        TString ShardId;
        TString ShardNodeName;
        ui64 MinCommitId;
        ui64 MaxCommitId;

        // There are two types of node refs: those that point to nodes in the
        // same filesystem as the parent and those that point to nodes in
        // another filesystem. The latter ones have ShardId and ShardNodeName
        // specified instead of ChildNodeId
        bool IsExternal() const
        {
            return !ShardId.empty();
        }

        // Calculates byte size for the entire NodeRefs row as defined in
        // TIndexTabletSchema::NodeRefs (see tablet_schema.h).
        ui32 CalculateByteSize() const
        {
            return sizeof(NodeId) + sizeof(ChildNodeId) + sizeof(MinCommitId) +
                   sizeof(MaxCommitId) + Name.size() + ShardId.size() +
                   ShardNodeName.size();
        }

        bool TryToEncodeShardId(const TString& mainFs);
        bool TryToDecodeShardId(const TString& mainFs);
    };

    struct TNodeAttr
    {
        ui64 NodeId;
        TString Name;
        TString Value;
        ui64 MinCommitId;
        ui64 MaxCommitId;
        ui64 Version;
    };

    virtual ~INodeIndexTabletDatabase() = default;

    //
    // Nodes
    //

    virtual bool ReadNode(ui64 nodeId, ui64 commitId, TMaybe<TNode>& node) = 0;

    virtual bool ReadNodes(
        ui64 startNodeId,
        ui64 maxNodes,
        ui64& nextNodeId,
        TVector<TNode>& nodes) = 0;

    //
    // Nodes_Ver
    //

    virtual bool ReadNodeVer(
        ui64 nodeId,
        ui64 commitId,
        TMaybe<TNode>& node) = 0;

    //
    // NodeAttrs
    //

    virtual bool ReadNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) = 0;

    virtual bool ReadNodeAttrs(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeAttr>& attrs) = 0;

    //
    // NodeAttrs_Ver
    //

    virtual bool ReadNodeAttrVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) = 0;

    virtual bool ReadNodeAttrVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeAttr>& attrs) = 0;

    //
    // NodeRefs
    //

    virtual bool ReadNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) = 0;

    virtual bool ReadNodeRefs(
        ui64 nodeId,
        ui64 commitId,
        const TString& cookie,
        TVector<TNodeRef>& refs,
        ui32 maxBytes,
        TString* next = nullptr,
        ui32* skippedRefs = nullptr,
        bool noAutoPrecharge = false,
        NProto::EListNodesSizeMode sizeMode = NProto::LNSM_NAME_ONLY) = 0;

    /**
     * @brief read at most maxCount node refs starting from key
     * (startNodeId, startCookie). Populates refs with the nodeRefs that have
     * been read. If there are more nodeRefs to read, nextNodeId and nextCookie
     * will be populated with the key to continue reading from
     */
    virtual bool ReadNodeRefs(
        ui64 startNodeId,
        const TString& startCookie,
        ui64 maxCount,
        TVector<INodeIndexTabletDatabase::TNodeRef>& refs,
        ui64& nextNodeId,
        TString& nextCookie) = 0;

    virtual bool PrechargeNodeRefs(
        ui64 nodeId,
        const TString& cookie,
        ui64 rowsToPrecharge,
        ui64 bytesToPrecharge) = 0;

    //
    // NodeRefs_Ver
    //

    virtual bool ReadNodeRefVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) = 0;

    virtual bool ReadNodeRefVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeRef>& refs) = 0;

    //
    // CheckpointNodes
    //

    virtual bool ReadCheckpointNodes(
        ui64 checkpointId,
        TVector<ui64>& nodes,
        size_t maxCount) = 0;

    //
    // MixedIndex
    //

    struct TMixedBlob
    {
        TPartialBlobId BlobId;
        TBlockList BlockList;
        ui32 GarbageBlocks;
        ui32 CheckpointBlocks;
    };

    virtual bool ReadMixedBlocks(
        ui32 rangeId,
        TVector<TMixedBlob>& blobs,
        IAllocator* alloc) = 0;

    virtual bool ReadDeletionMarkers(
        ui32 rangeId,
        TVector<TDeletionMarker>& deletionMarkers) = 0;
};

/**
 * @brief This interface exposes all operations supported by the index db.
 */
class IIndexTabletDatabase: public INodeIndexTabletDatabase
{
public:
    virtual void InitSchema() = 0;

    //
    // FileSystem
    //

    virtual void WriteFileSystem(const NProto::TFileSystem& fileSystem) = 0;
    virtual bool ReadFileSystem(NProto::TFileSystem& fileSystem) = 0;
    virtual bool ReadFileSystemStats(NProto::TFileSystemStats& stats) = 0;

#define FILESTORE_DECLARE_STATS(name, ...)    \
    virtual void Write##name(ui64 value) = 0; \
    // FILESTORE_DECLARE_STATS

    FILESTORE_FILESYSTEM_STATS(FILESTORE_DECLARE_STATS)

#undef FILESTORE_DECLARE_STATS

    virtual void WriteStorageConfig(
        const NProto::TStorageConfig& storageConfig) = 0;
    virtual bool ReadStorageConfig(
        TMaybe<NProto::TStorageConfig>& storageConfig) = 0;

    virtual bool ReadTabletStorageInfo(
        NCloud::NProto::TTabletStorageInfo& tabletStorageInfo) = 0;
    virtual void WriteTabletStorageInfo(
        const NCloud::NProto::TTabletStorageInfo& tabletStorageInfo) = 0;

    //
    // Nodes
    //

    virtual void
    WriteNode(ui64 nodeId, ui64 commitId, const NProto::TNode& attrs) = 0;
    virtual void DeleteNode(ui64 nodeId) = 0;

    //
    // Nodes_Ver
    //

    virtual void WriteNodeVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const NProto::TNode& attrs) = 0;
    virtual void DeleteNodeVer(ui64 nodeId, ui64 commitId) = 0;

    //
    // NodeAttrs
    //

    virtual void WriteNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        const TString& value,
        ui64 version) = 0;
    virtual void DeleteNodeAttr(ui64 nodeId, const TString& name) = 0;

    //
    // NodeAttrs_Ver
    //

    virtual void WriteNodeAttrVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const TString& name,
        const TString& value,
        ui64 version) = 0;
    virtual void
    DeleteNodeAttrVer(ui64 nodeId, ui64 commitId, const TString& name) = 0;

    //
    // NodeRefs
    //

    virtual void WriteNodeRef(const TNodeRef& nodeRef, bool markExhaustive) = 0;
    virtual void DeleteNodeRef(ui64 nodeId, const TString& name) = 0;

    //
    // NodeRefs_Ver
    //

    virtual void WriteNodeRefVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const TString& name,
        ui64 childNode,
        const TString& shardId,
        const TString& shardNodeName) = 0;
    virtual void
    DeleteNodeRefVer(ui64 nodeId, ui64 commitId, const TString& name) = 0;

    //
    // TruncateQueue
    //

    virtual void WriteTruncateQueueEntry(ui64 nodeId, TByteRange range) = 0;
    virtual void DeleteTruncateQueueEntry(ui64 id) = 0;
    virtual bool ReadTruncateQueue(
        TVector<NProto::TTruncateEntry>& entries) = 0;

    //
    // Sessions
    //

    virtual void WriteSession(const NProto::TSession& session) = 0;
    virtual void DeleteSession(const TString& sessionId) = 0;
    virtual bool ReadSessions(TVector<NProto::TSession>& sessions) = 0;

    //
    // SessionHandles
    //

    virtual void WriteSessionHandle(const NProto::TSessionHandle& handle) = 0;
    virtual void DeleteSessionHandle(const TString& sessionId, ui64 handle) = 0;
    virtual bool ReadSessionHandles(
        TVector<NProto::TSessionHandle>& handles) = 0;

    virtual bool ReadSessionHandles(
        const TString& sessionId,
        TVector<NProto::TSessionHandle>& handles) = 0;

    //
    // SessionLocks
    //

    virtual void WriteSessionLock(const NProto::TSessionLock& lock) = 0;
    virtual void DeleteSessionLock(const TString& sessionId, ui64 lockId) = 0;
    virtual bool ReadSessionLocks(TVector<NProto::TSessionLock>& locks) = 0;

    virtual bool ReadSessionLocks(
        const TString& sessionId,
        TVector<NProto::TSessionLock>& locks) = 0;

    //
    // SessionDuplicateCache
    //

    virtual void WriteSessionDupCacheEntry(
        const NProto::TDupCacheEntry& entry) = 0;
    virtual void DeleteSessionDupCacheEntry(
        const TString& sessionId,
        ui64 entryId) = 0;
    virtual bool ReadSessionDupCacheEntries(
        TVector<NProto::TDupCacheEntry>& entries) = 0;

    //
    // SessionHistory
    //

    virtual void WriteSessionHistoryEntry(
        const NProto::TSessionHistoryEntry& entry) = 0;
    virtual void DeleteSessionHistoryEntry(ui64 entryId) = 0;
    virtual bool ReadSessionHistoryEntries(
        TVector<NProto::TSessionHistoryEntry>& entries) = 0;

    //
    // FreshBytes
    //

    struct TFreshBytesEntry
    {
        ui64 NodeId;
        ui64 MinCommitId;
        ui64 Offset;
        TString Data;
        ui64 Len;
    };

    virtual void WriteFreshBytes(
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        TStringBuf data) = 0;
    virtual void WriteFreshBytesDeletionMarker(
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        ui64 len) = 0;
    virtual void DeleteFreshBytes(ui64 nodeId, ui64 commitId, ui64 offset) = 0;
    virtual bool ReadFreshBytes(TVector<TFreshBytesEntry>& bytes) = 0;

    //
    // FreshBlocks
    //

    struct TFreshBlock
    {
        ui64 NodeId;
        ui32 BlockIndex;
        ui64 MinCommitId;
        ui64 MaxCommitId;
        TString BlockData;
    };

    virtual void WriteFreshBlock(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        TStringBuf blockData) = 0;
    virtual void MarkFreshBlockDeleted(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        ui32 blockIndex) = 0;
    virtual void
    DeleteFreshBlock(ui64 nodeId, ui64 commitId, ui32 blockIndex) = 0;
    virtual bool ReadFreshBlocks(TVector<TFreshBlock>& blocks) = 0;

    //
    // MixedBlocks
    //

    virtual void WriteMixedBlocks(
        ui32 rangeId,
        const TPartialBlobId& blobId,
        const TBlockList& blockList,
        ui32 garbageBlocks,
        ui32 checkpointBlocks) = 0;
    virtual void DeleteMixedBlocks(
        ui32 rangeId,
        const TPartialBlobId& blobId) = 0;
    virtual bool ReadMixedBlocks(
        ui32 rangeId,
        const TPartialBlobId& blobId,
        TMaybe<TMixedBlob>& blob,
        IAllocator* alloc) = 0;
    using INodeIndexTabletDatabase::ReadMixedBlocks;

    //
    // DeletionMarkers
    //

    virtual void WriteDeletionMarkers(
        ui32 rangeId,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) = 0;
    virtual void DeleteDeletionMarker(
        ui32 rangeId,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex) = 0;

    //
    // LargeDeletionMarkers
    //

    virtual void WriteLargeDeletionMarkers(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) = 0;
    virtual void
    DeleteLargeDeletionMarker(ui64 nodeId, ui64 commitId, ui32 blockIndex) = 0;
    virtual bool ReadLargeDeletionMarkers(
        TVector<TDeletionMarker>& deletionMarkers) = 0;

    //
    // OrphanNodes
    //

    virtual void WriteOrphanNode(ui64 nodeId) = 0;
    virtual void DeleteOrphanNode(ui64 nodeId) = 0;
    virtual bool ReadOrphanNodes(TVector<ui64>& nodeIds) = 0;

    //
    // NewBlobs
    //

    virtual void WriteNewBlob(const TPartialBlobId& blobId) = 0;
    virtual void DeleteNewBlob(const TPartialBlobId& blobId) = 0;
    virtual bool ReadNewBlobs(TVector<TPartialBlobId>& blobIds) = 0;

    //
    // GarbageBlobs
    //

    virtual void WriteGarbageBlob(const TPartialBlobId& blobId) = 0;
    virtual void DeleteGarbageBlob(const TPartialBlobId& blobId) = 0;
    virtual bool ReadGarbageBlobs(TVector<TPartialBlobId>& blobIds) = 0;

    //
    // Checkpoints
    //

    virtual void WriteCheckpoint(const NProto::TCheckpoint& checkpoint) = 0;
    virtual void DeleteCheckpoint(const TString& checkpointId) = 0;
    virtual bool ReadCheckpoints(TVector<NProto::TCheckpoint>& checkpoints) = 0;

    //
    // Quotas
    //

    virtual void WriteQuota(const NProto::TQuota& quota) = 0;
    virtual void DeleteQuota(ui32 quotaId) = 0;
    virtual bool ReadQuotas(TVector<NProto::TQuota>& quotas) = 0;

    //
    // QuotaUsage
    //

    virtual void WriteQuotaUsage(
        ui32 quotaId,
        ui64 usedBytes,
        ui64 usedNodes) = 0;
    virtual void DeleteQuotaUsage(ui32 quotaId) = 0;
    virtual bool ReadQuotaUsages(TVector<TQuotaUsage>& usages) = 0;

    //
    // CheckpointNodes
    //

    virtual void WriteCheckpointNode(ui64 checkpointId, ui64 nodeId) = 0;
    virtual void DeleteCheckpointNode(ui64 checkpointId, ui64 nodeId) = 0;

    //
    // CheckpointBlobs
    //

    struct TCheckpointBlob
    {
        ui32 RangeId = 0;
        TPartialBlobId BlobId;
    };

    virtual void WriteCheckpointBlob(
        ui64 checkpointId,
        ui32 rangeId,
        const TPartialBlobId& blobId) = 0;
    virtual void DeleteCheckpointBlob(
        ui64 checkpointId,
        ui32 rangeId,
        const TPartialBlobId& blobId) = 0;
    virtual bool ReadCheckpointBlobs(
        ui64 checkpointId,
        TVector<TCheckpointBlob>& blobs,
        size_t maxCount) = 0;

    //
    // CompactionMap
    //

    virtual void ForceWriteCompactionMap(
        ui32 rangeId,
        ui32 blobsCount,
        ui32 deletionsCount,
        ui32 garbageBlocksCount) = 0;
    virtual void WriteCompactionMap(
        ui32 rangeId,
        ui32 blobsCount,
        ui32 deletionsCount,
        ui32 garbageBlocksCount) = 0;
    virtual bool ReadCompactionMap(
        TVector<TCompactionRangeInfo>& compactionMap) = 0;

    virtual bool ReadCompactionMap(
        TVector<TCompactionRangeInfo>& compactionMap,
        ui32 firstRangeId,
        ui32 rangeCount,
        bool prechargeAll) = 0;

    //
    // OpLog
    //

    virtual void WriteOpLogEntry(const NProto::TOpLogEntry& entry) = 0;
    virtual void DeleteOpLogEntry(ui64 entryId) = 0;
    virtual bool ReadOpLogEntry(
        ui64 entryId,
        TMaybe<NProto::TOpLogEntry>& entry) = 0;
    virtual bool ReadOpLog(TVector<NProto::TOpLogEntry>& opLog) = 0;

    //
    // ResponseLog
    //

    virtual void WriteResponseLogEntry(
        const NProtoPrivate::TResponseLogEntry& entry) = 0;
    virtual void DeleteResponseLogEntry(
        ui64 clientTabletId,
        ui64 requestId) = 0;
    virtual bool ReadResponseLogEntry(
        ui64 clientTabletId,
        ui64 requestId,
        TMaybe<NProtoPrivate::TResponseLogEntry>& entry) = 0;

    virtual bool ReadResponseLog(
        TVector<NProtoPrivate::TResponseLogEntry>& responseLog) = 0;

    //
    // UnconfirmedData
    //

    struct TUnconfirmedDataEntry
    {
        ui64 CommitId = 0;
        NProto::TUnconfirmedData Data;
    };

    virtual void WriteUnconfirmedData(
        ui64 commitId,
        const NProto::TUnconfirmedData& data) = 0;
    virtual void DeleteUnconfirmedData(ui64 commitId) = 0;
    virtual bool ReadUnconfirmedData(
        TVector<TUnconfirmedDataEntry>& entries) = 0;
};

}   // namespace NCloud::NFileStore::NStorage
