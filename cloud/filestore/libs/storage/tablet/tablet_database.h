#pragma once

#include "public.h"

#include "tablet_state_cache.h"
#include "tablet_state_iface.h"

#include <cloud/filestore/config/storage.pb.h>
#include <cloud/filestore/libs/storage/tablet/model/block_list.h>
#include <cloud/filestore/libs/storage/tablet/model/compaction_map.h>
#include <cloud/filestore/libs/storage/tablet/model/deletion_markers.h>
#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>
#include <cloud/storage/core/protos/tablet.pb.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

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

#define FILESTORE_DUPCACHE_REQUESTS(xxx, ...)                                  \
    xxx(CreateHandle,   __VA_ARGS__)                                           \
    xxx(CreateNode,     __VA_ARGS__)                                           \
    xxx(RenameNode,     __VA_ARGS__)                                           \
    xxx(UnlinkNode,     __VA_ARGS__)                                           \
// FILESTORE_DUPCACHE_REQUESTS

////////////////////////////////////////////////////////////////////////////////

class TIndexTabletDatabase
    : public IIndexTabletDatabase
    , public NKikimr::NIceDb::TNiceDb
{
public:
    TIndexTabletDatabase(NKikimr::NTable::TDatabase& database)
        : NKikimr::NIceDb::TNiceDb(database)
    {}

    void InitSchema(bool useNoneCompactionPolicy);

    //
    // FileSystem
    //

    void WriteFileSystem(const NProto::TFileSystem& fileSystem);
    bool ReadFileSystem(NProto::TFileSystem& fileSystem);
    bool ReadFileSystemStats(NProto::TFileSystemStats& stats);

    void WriteStorageConfig(const NProto::TStorageConfig& storageConfig);
    bool ReadStorageConfig(TMaybe<NProto::TStorageConfig>& storageConfig);

#define FILESTORE_DECLARE_STATS(name, ...)                                     \
    void Write##name(ui64 value);                                              \
// FILESTORE_DECLARE_STATS

FILESTORE_FILESYSTEM_STATS(FILESTORE_DECLARE_STATS)

#undef FILESTORE_DECLARE_STATS

    bool ReadTabletStorageInfo(
        NCloud::NProto::TTabletStorageInfo& tabletStorageInfo);
    void WriteTabletStorageInfo(
        const NCloud::NProto::TTabletStorageInfo& tabletStorageInfo);

    //
    // Nodes
    //

    virtual void WriteNode(
        ui64 nodeId,
        ui64 commitId,
        const NProto::TNode& attrs);
    virtual void DeleteNode(ui64 nodeId);
    virtual bool ReadNode(
        ui64 nodeId,
        ui64 commitId,
        TMaybe<TNode>& node) override;
    virtual bool ReadNodes(
        ui64 startNodeId,
        ui64 maxNodes,
        ui64& nextNodeId,
        TVector<TNode>& nodes) override;

    //
    // Nodes_Ver
    //

    virtual void WriteNodeVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const NProto::TNode& attrs);

    virtual void DeleteNodeVer(ui64 nodeId, ui64 commitId);

    virtual bool ReadNodeVer(
        ui64 nodeId,
        ui64 commitId,
        TMaybe<TNode>& node) override;

    //
    // NodeAttrs
    //

    virtual void WriteNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        const TString& value,
        ui64 version);

    virtual void DeleteNodeAttr(ui64 nodeId, const TString& name);

    virtual bool ReadNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) override;

    virtual bool ReadNodeAttrs(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeAttr>& attrs) override;

    //
    // NodeAttrs_Ver
    //

    virtual void WriteNodeAttrVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const TString& name,
        const TString& value,
        ui64 version);

    virtual void DeleteNodeAttrVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name);

    virtual bool ReadNodeAttrVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) override;

    virtual bool ReadNodeAttrVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeAttr>& attrs) override;

    //
    // NodeRefs
    //

    virtual void WriteNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        ui64 childNode,
        const TString& shardId,
        const TString& shardNodeName);

    virtual void DeleteNodeRef(ui64 nodeId, const TString& name);

    virtual bool ReadNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) override;

    virtual bool ReadNodeRefs(
        ui64 nodeId,
        ui64 commitId,
        const TString& cookie,
        TVector<TNodeRef>& refs,
        ui32 maxBytes,
        TString* next,
        ui32* skippedRefs) override;

    virtual bool ReadNodeRefs(
        ui64 startNodeId,
        const TString& startCookie,
        ui64 maxCount,
        TVector<TNodeRef>& refs,
        ui64& nextNodeId,
        TString& nextCookie) override;

    virtual bool PrechargeNodeRefs(
        ui64 nodeId,
        const TString& cookie,
        ui64 rowsToPrecharge,
        ui64 bytesToPrecharge) override;

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
        const TString& shardNodeName);

    virtual void DeleteNodeRefVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name);

    virtual bool ReadNodeRefVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) override;

    virtual bool ReadNodeRefVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeRef>& refs) override;

    //
    // TruncateQueue
    //

    void WriteTruncateQueueEntry(ui64 nodeId, TByteRange range);
    void DeleteTruncateQueueEntry(ui64 id);
    bool ReadTruncateQueue(TVector<NProto::TTruncateEntry>& entries);

    //
    // Sessions
    //

    void WriteSession(const NProto::TSession& session);
    void DeleteSession(const TString& sessionId);
    bool ReadSessions(TVector<NProto::TSession>& sessions);

    //
    // SessionHandles
    //

    void WriteSessionHandle(const NProto::TSessionHandle& handle);
    void DeleteSessionHandle(const TString& sessionId, ui64 handle);
    bool ReadSessionHandles(TVector<NProto::TSessionHandle>& handles);

    bool ReadSessionHandles(
        const TString& sessionId,
        TVector<NProto::TSessionHandle>& handles);

    //
    // SessionLocks
    //

    void WriteSessionLock(const NProto::TSessionLock& lock);
    void DeleteSessionLock(const TString& sessionId, ui64 lockId);
    bool ReadSessionLocks(TVector<NProto::TSessionLock>& locks);

    bool ReadSessionLocks(
        const TString& sessionId,
        TVector<NProto::TSessionLock>& locks);

    //
    // SessionDuplicateCache
    //

    void WriteSessionDupCacheEntry(const NProto::TDupCacheEntry& entry);
    void DeleteSessionDupCacheEntry(const TString& sessionId, ui64 entryId);
    bool ReadSessionDupCacheEntries(TVector<NProto::TDupCacheEntry>& entries);


    //
    // SessionHistory
    //

    void WriteSessionHistoryEntry(const NProto::TSessionHistoryEntry& entry);
    void DeleteSessionHistoryEntry(ui64 entryId);
    bool ReadSessionHistoryEntries(TVector<NProto::TSessionHistoryEntry>& entries);


    //
    // FreshBytes
    //

    void WriteFreshBytes(
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        TStringBuf data);

    void WriteFreshBytesDeletionMarker(
        ui64 nodeId,
        ui64 commitId,
        ui64 offset,
        ui64 len);

    void DeleteFreshBytes(ui64 nodeId, ui64 commitId, ui64 offset);

    struct TFreshBytesEntry
    {
        ui64 NodeId;
        ui64 MinCommitId;
        ui64 Offset;
        TString Data;
        ui64 Len;
    };

    bool ReadFreshBytes(TVector<TFreshBytesEntry>& bytes);

    //
    // FreshBlocks
    //

    void WriteFreshBlock(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        TStringBuf blockData);

    void MarkFreshBlockDeleted(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        ui32 blockIndex);

    void DeleteFreshBlock(ui64 nodeId, ui64 commitId, ui32 blockIndex);

    struct TFreshBlock
    {
        ui64 NodeId;
        ui32 BlockIndex;
        ui64 MinCommitId;
        ui64 MaxCommitId;
        TString BlockData;
    };

    bool ReadFreshBlocks(TVector<TFreshBlock>& blocks);

    //
    // MixedBlocks
    //

    void WriteMixedBlocks(
        ui32 rangeId,
        const TPartialBlobId& blobId,
        const TBlockList& blockList,
        ui32 garbageBlocks,
        ui32 checkpointBlocks);

    void DeleteMixedBlocks(ui32 rangeId, const TPartialBlobId& blobId);

    bool ReadMixedBlocks(
        ui32 rangeId,
        const TPartialBlobId& blobId,
        TMaybe<TMixedBlob>& blob,
        IAllocator* alloc);

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
        ui32 blocksCount);

    void DeleteDeletionMarker(
        ui32 rangeId,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex);

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
        ui32 blocksCount);

    void DeleteLargeDeletionMarker(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex);

    bool ReadLargeDeletionMarkers(TVector<TDeletionMarker>& deletionMarkers);

    //
    // OrphanNodes
    //

    void WriteOrphanNode(ui64 nodeId);
    void DeleteOrphanNode(ui64 nodeId);
    bool ReadOrphanNodes(TVector<ui64>& nodeIds);

    //
    // NewBlobs
    //

    void WriteNewBlob(const TPartialBlobId& blobId);
    void DeleteNewBlob(const TPartialBlobId& blobId);
    bool ReadNewBlobs(TVector<TPartialBlobId>& blobIds);

    //
    // GarbageBlobs
    //

    void WriteGarbageBlob(const TPartialBlobId& blobId);
    void DeleteGarbageBlob(const TPartialBlobId& blobId);
    bool ReadGarbageBlobs(TVector<TPartialBlobId>& blobIds);

    //
    // Checkpoints
    //

    void WriteCheckpoint(const NProto::TCheckpoint& checkpoint);
    void DeleteCheckpoint(const TString& checkpointId);
    bool ReadCheckpoints(TVector<NProto::TCheckpoint>& checkpoints);

    //
    // CheckpointNodes
    //

    void WriteCheckpointNode(ui64 checkpointId, ui64 nodeId);
    void DeleteCheckpointNode(ui64 checkpointId, ui64 nodeId);

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
        const TPartialBlobId& blobId);

    void DeleteCheckpointBlob(
        ui64 checkpointId,
        ui32 rangeId,
        const TPartialBlobId& blobId);

    struct TCheckpointBlob
    {
        ui32 RangeId = 0;
        TPartialBlobId BlobId;
    };

    bool ReadCheckpointBlobs(
        ui64 checkpointId,
        TVector<TCheckpointBlob>& blobs,
        size_t maxCount = 100);

    //
    // CompactionMap
    //

    void ForceWriteCompactionMap(
        ui32 rangeId,
        ui32 blobsCount,
        ui32 deletionsCount,
        ui32 garbageBlocksCount);
    void WriteCompactionMap(
        ui32 rangeId,
        ui32 blobsCount,
        ui32 deletionsCount,
        ui32 garbageBlocksCount);
    bool ReadCompactionMap(TVector<TCompactionRangeInfo>& compactionMap);
    bool ReadCompactionMap(
        TVector<TCompactionRangeInfo>& compactionMap,
        ui32 firstRangeId,
        ui32 rangeCount,
        bool prechargeAll);

    //
    // OpLog
    //

    void WriteOpLogEntry(const NProto::TOpLogEntry& entry);
    void DeleteOpLogEntry(ui64 entryId);
    bool ReadOpLog(TVector<NProto::TOpLogEntry>& opLog);
};

////////////////////////////////////////////////////////////////////////////////

// This is a proxy class that forwards all calls to the underlying
// TIndexTabletDatabase, but also records all inode-relevant changes
class TIndexTabletDatabaseProxy: public TIndexTabletDatabase
{
public:
    TIndexTabletDatabaseProxy(
        NKikimr::NTable::TDatabase& database,
        TVector<TInMemoryIndexState::TIndexStateRequest>& nodeUpdates);

    //
    // Nodes
    //

    bool ReadNode(
        ui64 nodeId,
        ui64 commitId,
        TMaybe<TNode>& node) final;

    bool ReadNodes(
        ui64 startNodeId,
        ui64 maxNodes,
        ui64& nextNodeId,
        TVector<TNode>& nodes) final;

    void WriteNode(
        ui64 nodeId,
        ui64 commitId,
        const NProto::TNode& attrs) override;

    void DeleteNode(ui64 nodeId) override;

    //
    // Nodes_Ver
    //

    void WriteNodeVer(
        ui64 nodeId,
        ui64 minCommitId,
        ui64 maxCommitId,
        const NProto::TNode& attrs) override;

    void DeleteNodeVer(ui64 nodeId, ui64 commitId) override;

    //
    // NodeAttrs
    //

    bool ReadNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) override;

    void WriteNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        const TString& value,
        ui64 version) override;

    void DeleteNodeAttr(ui64 nodeId, const TString& name) override;

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

    //
    // NodeRefs
    //

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
        TString* next = nullptr,
        ui32* skippedRefs = nullptr) override;

    bool ReadNodeRefs(
        ui64 startNodeId,
        const TString& startCookie,
        ui64 maxCount,
        TVector<TNodeRef>& refs,
        ui64& nextNodeId,
        TString& nextCookie) override;

    void WriteNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        ui64 childNode,
        const TString& shardId,
        const TString& shardNodeName) override;

    void DeleteNodeRef(ui64 nodeId, const TString& name) override;

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

private:
    TVector<TInMemoryIndexState::TIndexStateRequest>& NodeUpdates;

    static TInMemoryIndexState::TWriteNodeRefsRequest
    ExtractWriteNodeRefsFromNodeRef(const TNodeRef& ref);
};

}   // namespace NCloud::NFileStore::NStorage
