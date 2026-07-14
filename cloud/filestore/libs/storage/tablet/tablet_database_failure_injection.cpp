#include "tablet_database_failure_injection.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletDatabaseFailureInjection::InitSchema()
{
    Real->InitSchema();
}

void TIndexTabletDatabaseFailureInjection::WriteFileSystem(
    const NProto::TFileSystem& fileSystem)
{
    Real->WriteFileSystem(fileSystem);
}

bool TIndexTabletDatabaseFailureInjection::ReadFileSystem(
    NProto::TFileSystem& fileSystem)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadFileSystem(fileSystem);
}

bool TIndexTabletDatabaseFailureInjection::ReadFileSystemStats(
    NProto::TFileSystemStats& stats)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadFileSystemStats(stats);
}

#define FILESTORE_IMPLEMENT_STATS(name, ...)                                   \
    void TIndexTabletDatabaseFailureInjection::Write##name(ui64 value)         \
    {                                                                          \
        Real->Write##name(value);                                               \
    }                                                                          \
// FILESTORE_IMPLEMENT_STATS

FILESTORE_FILESYSTEM_STATS(FILESTORE_IMPLEMENT_STATS)

#undef FILESTORE_IMPLEMENT_STATS

void TIndexTabletDatabaseFailureInjection::WriteStorageConfig(
    const NProto::TStorageConfig& storageConfig)
{
    Real->WriteStorageConfig(storageConfig);
}

bool TIndexTabletDatabaseFailureInjection::ReadStorageConfig(
    TMaybe<NProto::TStorageConfig>& storageConfig)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadStorageConfig(storageConfig);
}

bool TIndexTabletDatabaseFailureInjection::ReadTabletStorageInfo(
    NCloud::NProto::TTabletStorageInfo& tabletStorageInfo)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadTabletStorageInfo(tabletStorageInfo);
}

void TIndexTabletDatabaseFailureInjection::WriteTabletStorageInfo(
    const NCloud::NProto::TTabletStorageInfo& tabletStorageInfo)
{
    Real->WriteTabletStorageInfo(tabletStorageInfo);
}

void TIndexTabletDatabaseFailureInjection::WriteNode(
    ui64 nodeId,
    ui64 commitId,
    const NProto::TNode& attrs)
{
    Real->WriteNode(nodeId, commitId, attrs);
}

void TIndexTabletDatabaseFailureInjection::DeleteNode(ui64 nodeId)
{
    Real->DeleteNode(nodeId);
}

bool TIndexTabletDatabaseFailureInjection::ReadNode(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNode(nodeId, commitId, node);
}

bool TIndexTabletDatabaseFailureInjection::ReadNodes(
    ui64 startNodeId,
    ui64 maxNodes,
    ui64& nextNodeId,
    TVector<TNode>& nodes)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodes(
        startNodeId,
        maxNodes,
        nextNodeId,
        nodes);
}

void TIndexTabletDatabaseFailureInjection::WriteNodeVer(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const NProto::TNode& attrs)
{
    Real->WriteNodeVer(nodeId, minCommitId, maxCommitId, attrs);
}

void TIndexTabletDatabaseFailureInjection::DeleteNodeVer(
    ui64 nodeId,
    ui64 commitId)
{
    Real->DeleteNodeVer(nodeId, commitId);
}

bool TIndexTabletDatabaseFailureInjection::ReadNodeVer(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeVer(nodeId, commitId, node);
}

void TIndexTabletDatabaseFailureInjection::WriteNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    const TString& value,
    ui64 version)
{
    Real->WriteNodeAttr(nodeId, commitId, name, value, version);
}

void TIndexTabletDatabaseFailureInjection::DeleteNodeAttr(
    ui64 nodeId,
    const TString& name)
{
    Real->DeleteNodeAttr(nodeId, name);
}

bool TIndexTabletDatabaseFailureInjection::ReadNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeAttr(nodeId, commitId, name, attr);
}

bool TIndexTabletDatabaseFailureInjection::ReadNodeAttrs(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeAttr>& attrs)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeAttrs(nodeId, commitId, attrs);
}

void TIndexTabletDatabaseFailureInjection::WriteNodeAttrVer(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const TString& name,
    const TString& value,
    ui64 version)
{
    Real->WriteNodeAttrVer(
        nodeId,
        minCommitId,
        maxCommitId,
        name,
        value,
        version);
}

void TIndexTabletDatabaseFailureInjection::DeleteNodeAttrVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name)
{
    Real->DeleteNodeAttrVer(nodeId, commitId, name);
}

bool TIndexTabletDatabaseFailureInjection::ReadNodeAttrVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeAttrVer(nodeId, commitId, name, attr);
}

bool TIndexTabletDatabaseFailureInjection::ReadNodeAttrVers(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeAttr>& attrs)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeAttrVers(nodeId, commitId, attrs);
}

void TIndexTabletDatabaseFailureInjection::WriteNodeRef(
    const TNodeRef& nodeRef,
    bool markExhaustive)
{
    Real->WriteNodeRef(nodeRef, markExhaustive);
}

void TIndexTabletDatabaseFailureInjection::DeleteNodeRef(
    ui64 nodeId,
    const TString& name)
{
    Real->DeleteNodeRef(nodeId, name);
}

bool TIndexTabletDatabaseFailureInjection::ReadNodeRef(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeRef(nodeId, commitId, name, ref);
}

bool TIndexTabletDatabaseFailureInjection::ReadNodeRefs(
    ui64 nodeId,
    ui64 commitId,
    const TString& cookie,
    TVector<TNodeRef>& refs,
    ui32 maxBytes,
    TString* next,
    ui32* skippedRefs,
    bool noAutoPrecharge,
    NProto::EListNodesSizeMode sizeMode)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeRefs(
        nodeId,
        commitId,
        cookie,
        refs,
        maxBytes,
        next,
        skippedRefs,
        noAutoPrecharge,
        sizeMode);
}

bool TIndexTabletDatabaseFailureInjection::ReadNodeRefs(
    ui64 startNodeId,
    const TString& startCookie,
    ui64 maxCount,
    TVector<TNodeRef>& refs,
    ui64& nextNodeId,
    TString& nextCookie)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeRefs(
        startNodeId,
        startCookie,
        maxCount,
        refs,
        nextNodeId,
        nextCookie);
}

bool TIndexTabletDatabaseFailureInjection::PrechargeNodeRefs(
    ui64 nodeId,
    const TString& cookie,
    ui64 rowsToPrecharge,
    ui64 bytesToPrecharge)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->PrechargeNodeRefs(
        nodeId,
        cookie,
        rowsToPrecharge,
        bytesToPrecharge);
}

void TIndexTabletDatabaseFailureInjection::WriteNodeRefVer(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const TString& name,
    ui64 childNode,
    const TString& shardId,
    const TString& shardNodeName)
{
    Real->WriteNodeRefVer(
        nodeId,
        minCommitId,
        maxCommitId,
        name,
        childNode,
        shardId,
        shardNodeName);
}

void TIndexTabletDatabaseFailureInjection::DeleteNodeRefVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name)
{
    Real->DeleteNodeRefVer(nodeId, commitId, name);
}

bool TIndexTabletDatabaseFailureInjection::ReadNodeRefVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeRefVer(nodeId, commitId, name, ref);
}

bool TIndexTabletDatabaseFailureInjection::ReadNodeRefVers(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeRef>& refs)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeRefVers(nodeId, commitId, refs);
}

void TIndexTabletDatabaseFailureInjection::WriteTruncateQueueEntry(
    ui64 nodeId,
    TByteRange range)
{
    Real->WriteTruncateQueueEntry(nodeId, range);
}

void TIndexTabletDatabaseFailureInjection::DeleteTruncateQueueEntry(ui64 id)
{
    Real->DeleteTruncateQueueEntry(id);
}

bool TIndexTabletDatabaseFailureInjection::ReadTruncateQueue(
    TVector<NProto::TTruncateEntry>& entries)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadTruncateQueue(entries);
}

void TIndexTabletDatabaseFailureInjection::WriteSession(
    const NProto::TSession& session)
{
    Real->WriteSession(session);
}

void TIndexTabletDatabaseFailureInjection::DeleteSession(
    const TString& sessionId)
{
    Real->DeleteSession(sessionId);
}

bool TIndexTabletDatabaseFailureInjection::ReadSessions(
    TVector<NProto::TSession>& sessions)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessions(sessions);
}

void TIndexTabletDatabaseFailureInjection::WriteSessionHandle(
    const NProto::TSessionHandle& handle)
{
    Real->WriteSessionHandle(handle);
}

void TIndexTabletDatabaseFailureInjection::DeleteSessionHandle(
    const TString& sessionId,
    ui64 handle)
{
    Real->DeleteSessionHandle(sessionId, handle);
}

bool TIndexTabletDatabaseFailureInjection::ReadSessionHandles(
    TVector<NProto::TSessionHandle>& handles)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionHandles(handles);
}

bool TIndexTabletDatabaseFailureInjection::ReadSessionHandles(
    const TString& sessionId,
    TVector<NProto::TSessionHandle>& handles)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionHandles(sessionId, handles);
}

void TIndexTabletDatabaseFailureInjection::WriteSessionLock(
    const NProto::TSessionLock& lock)
{
    Real->WriteSessionLock(lock);
}

void TIndexTabletDatabaseFailureInjection::DeleteSessionLock(
    const TString& sessionId,
    ui64 lockId)
{
    Real->DeleteSessionLock(sessionId, lockId);
}

bool TIndexTabletDatabaseFailureInjection::ReadSessionLocks(
    TVector<NProto::TSessionLock>& locks)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionLocks(locks);
}

bool TIndexTabletDatabaseFailureInjection::ReadSessionLocks(
    const TString& sessionId,
    TVector<NProto::TSessionLock>& locks)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionLocks(sessionId, locks);
}

void TIndexTabletDatabaseFailureInjection::WriteSessionDupCacheEntry(
    const NProto::TDupCacheEntry& entry)
{
    Real->WriteSessionDupCacheEntry(entry);
}

void TIndexTabletDatabaseFailureInjection::DeleteSessionDupCacheEntry(
    const TString& sessionId,
    ui64 entryId)
{
    Real->DeleteSessionDupCacheEntry(sessionId, entryId);
}

bool TIndexTabletDatabaseFailureInjection::ReadSessionDupCacheEntries(
    TVector<NProto::TDupCacheEntry>& entries)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionDupCacheEntries(entries);
}

void TIndexTabletDatabaseFailureInjection::WriteSessionHistoryEntry(
    const NProto::TSessionHistoryEntry& entry)
{
    Real->WriteSessionHistoryEntry(entry);
}

void TIndexTabletDatabaseFailureInjection::DeleteSessionHistoryEntry(
    ui64 entryId)
{
    Real->DeleteSessionHistoryEntry(entryId);
}

bool TIndexTabletDatabaseFailureInjection::ReadSessionHistoryEntries(
    TVector<NProto::TSessionHistoryEntry>& entries)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionHistoryEntries(entries);
}

void TIndexTabletDatabaseFailureInjection::WriteFreshBytes(
    ui64 nodeId,
    ui64 commitId,
    ui64 offset,
    TStringBuf data)
{
    Real->WriteFreshBytes(nodeId, commitId, offset, data);
}

void TIndexTabletDatabaseFailureInjection::WriteFreshBytesDeletionMarker(
    ui64 nodeId,
    ui64 commitId,
    ui64 offset,
    ui64 len)
{
    Real->WriteFreshBytesDeletionMarker(nodeId, commitId, offset, len);
}

void TIndexTabletDatabaseFailureInjection::DeleteFreshBytes(
    ui64 nodeId,
    ui64 commitId,
    ui64 offset)
{
    Real->DeleteFreshBytes(nodeId, commitId, offset);
}

bool TIndexTabletDatabaseFailureInjection::ReadFreshBytes(
    TVector<TFreshBytesEntry>& bytes)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadFreshBytes(bytes);
}

void TIndexTabletDatabaseFailureInjection::WriteFreshBlock(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    TStringBuf blockData)
{
    Real->WriteFreshBlock(nodeId, commitId, blockIndex, blockData);
}

void TIndexTabletDatabaseFailureInjection::MarkFreshBlockDeleted(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    ui32 blockIndex)
{
    Real->MarkFreshBlockDeleted(
        nodeId,
        minCommitId,
        maxCommitId,
        blockIndex);
}

void TIndexTabletDatabaseFailureInjection::DeleteFreshBlock(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex)
{
    Real->DeleteFreshBlock(nodeId, commitId, blockIndex);
}

bool TIndexTabletDatabaseFailureInjection::ReadFreshBlocks(TVector<TFreshBlock>& blocks)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadFreshBlocks(blocks);
}

void TIndexTabletDatabaseFailureInjection::WriteMixedBlocks(
    ui32 rangeId,
    const TPartialBlobId& blobId,
    const TBlockList& blockList,
    ui32 garbageBlocks,
    ui32 checkpointBlocks)
{
    Real->WriteMixedBlocks(
        rangeId,
        blobId,
        blockList,
        garbageBlocks,
        checkpointBlocks);
}

void TIndexTabletDatabaseFailureInjection::DeleteMixedBlocks(
    ui32 rangeId,
    const TPartialBlobId& blobId)
{
    Real->DeleteMixedBlocks(rangeId, blobId);
}

bool TIndexTabletDatabaseFailureInjection::ReadMixedBlocks(
    ui32 rangeId,
    const TPartialBlobId& blobId,
    TMaybe<TMixedBlob>& blob,
    IAllocator* alloc)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadMixedBlocks(rangeId, blobId, blob, alloc);
}

bool TIndexTabletDatabaseFailureInjection::ReadMixedBlocks(
    ui32 rangeId,
    TVector<TMixedBlob>& blobs,
    IAllocator* alloc)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadMixedBlocks(rangeId, blobs, alloc);
}

void TIndexTabletDatabaseFailureInjection::WriteDeletionMarkers(
    ui32 rangeId,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount)
{
    Real->WriteDeletionMarkers(
        rangeId,
        nodeId,
        commitId,
        blockIndex,
        blocksCount);
}

void TIndexTabletDatabaseFailureInjection::DeleteDeletionMarker(
    ui32 rangeId,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex)
{
    Real->DeleteDeletionMarker(rangeId, nodeId, commitId, blockIndex);
}

bool TIndexTabletDatabaseFailureInjection::ReadDeletionMarkers(
    ui32 rangeId,
    TVector<TDeletionMarker>& deletionMarkers)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadDeletionMarkers(rangeId, deletionMarkers);
}

void TIndexTabletDatabaseFailureInjection::WriteLargeDeletionMarkers(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount)
{
    Real->WriteLargeDeletionMarkers(
        nodeId,
        commitId,
        blockIndex,
        blocksCount);
}

void TIndexTabletDatabaseFailureInjection::DeleteLargeDeletionMarker(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex)
{
    Real->DeleteLargeDeletionMarker(nodeId, commitId, blockIndex);
}

bool TIndexTabletDatabaseFailureInjection::ReadLargeDeletionMarkers(
    TVector<TDeletionMarker>& deletionMarkers)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadLargeDeletionMarkers(deletionMarkers);
}

void TIndexTabletDatabaseFailureInjection::WriteOrphanNode(ui64 nodeId)
{
    Real->WriteOrphanNode(nodeId);
}

void TIndexTabletDatabaseFailureInjection::DeleteOrphanNode(ui64 nodeId)
{
    Real->DeleteOrphanNode(nodeId);
}

bool TIndexTabletDatabaseFailureInjection::ReadOrphanNodes(TVector<ui64>& nodeIds)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadOrphanNodes(nodeIds);
}

void TIndexTabletDatabaseFailureInjection::WriteNewBlob(
    const TPartialBlobId& blobId)
{
    Real->WriteNewBlob(blobId);
}

void TIndexTabletDatabaseFailureInjection::DeleteNewBlob(
    const TPartialBlobId& blobId)
{
    Real->DeleteNewBlob(blobId);
}

bool TIndexTabletDatabaseFailureInjection::ReadNewBlobs(TVector<TPartialBlobId>& blobIds)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNewBlobs(blobIds);
}

void TIndexTabletDatabaseFailureInjection::WriteGarbageBlob(
    const TPartialBlobId& blobId)
{
    Real->WriteGarbageBlob(blobId);
}

void TIndexTabletDatabaseFailureInjection::DeleteGarbageBlob(
    const TPartialBlobId& blobId)
{
    Real->DeleteGarbageBlob(blobId);
}

bool TIndexTabletDatabaseFailureInjection::ReadGarbageBlobs(
    TVector<TPartialBlobId>& blobIds)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadGarbageBlobs(blobIds);
}

void TIndexTabletDatabaseFailureInjection::WriteCheckpoint(
    const NProto::TCheckpoint& checkpoint)
{
    Real->WriteCheckpoint(checkpoint);
}

void TIndexTabletDatabaseFailureInjection::DeleteCheckpoint(
    const TString& checkpointId)
{
    Real->DeleteCheckpoint(checkpointId);
}

bool TIndexTabletDatabaseFailureInjection::ReadCheckpoints(
    TVector<NProto::TCheckpoint>& checkpoints)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadCheckpoints(checkpoints);
}

void TIndexTabletDatabaseFailureInjection::WriteCheckpointNode(
    ui64 checkpointId,
    ui64 nodeId)
{
    Real->WriteCheckpointNode(checkpointId, nodeId);
}

void TIndexTabletDatabaseFailureInjection::DeleteCheckpointNode(
    ui64 checkpointId,
    ui64 nodeId)
{
    Real->DeleteCheckpointNode(checkpointId, nodeId);
}

bool TIndexTabletDatabaseFailureInjection::ReadCheckpointNodes(
    ui64 checkpointId,
    TVector<ui64>& nodes,
    size_t maxCount)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadCheckpointNodes(
        checkpointId,
        nodes,
        maxCount);
}

void TIndexTabletDatabaseFailureInjection::WriteCheckpointBlob(
    ui64 checkpointId,
    ui32 rangeId,
    const TPartialBlobId& blobId)
{
    Real->WriteCheckpointBlob(checkpointId, rangeId, blobId);
}

void TIndexTabletDatabaseFailureInjection::DeleteCheckpointBlob(
    ui64 checkpointId,
    ui32 rangeId,
    const TPartialBlobId& blobId)
{
    Real->DeleteCheckpointBlob(checkpointId, rangeId, blobId);
}

bool TIndexTabletDatabaseFailureInjection::ReadCheckpointBlobs(
    ui64 checkpointId,
    TVector<TCheckpointBlob>& blobs,
    size_t maxCount)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadCheckpointBlobs(
        checkpointId,
        blobs,
        maxCount);
}

void TIndexTabletDatabaseFailureInjection::ForceWriteCompactionMap(
    ui32 rangeId,
    ui32 blobsCount,
    ui32 deletionsCount,
    ui32 garbageBlocksCount)
{
    Real->ForceWriteCompactionMap(
        rangeId,
        blobsCount,
        deletionsCount,
        garbageBlocksCount);
}

void TIndexTabletDatabaseFailureInjection::WriteCompactionMap(
    ui32 rangeId,
    ui32 blobsCount,
    ui32 deletionsCount,
    ui32 garbageBlocksCount)
{
    Real->WriteCompactionMap(
        rangeId,
        blobsCount,
        deletionsCount,
        garbageBlocksCount);
}

bool TIndexTabletDatabaseFailureInjection::ReadCompactionMap(
    TVector<TCompactionRangeInfo>& compactionMap)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadCompactionMap(compactionMap);
}

bool TIndexTabletDatabaseFailureInjection::ReadCompactionMap(
    TVector<TCompactionRangeInfo>& compactionMap,
    ui32 firstRangeId,
    ui32 rangeCount,
    bool prechargeAll)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadCompactionMap(
        compactionMap,
        firstRangeId,
        rangeCount,
        prechargeAll);
}

void TIndexTabletDatabaseFailureInjection::WriteOpLogEntry(
    const NProto::TOpLogEntry& entry)
{
    Real->WriteOpLogEntry(entry);
}

void TIndexTabletDatabaseFailureInjection::DeleteOpLogEntry(ui64 entryId)
{
    Real->DeleteOpLogEntry(entryId);
}

bool TIndexTabletDatabaseFailureInjection::ReadOpLogEntry(
    ui64 entryId,
    TMaybe<NProto::TOpLogEntry>& entry)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadOpLogEntry(entryId, entry);
}

bool TIndexTabletDatabaseFailureInjection::ReadOpLog(TVector<NProto::TOpLogEntry>& opLog)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadOpLog(opLog);
}

void TIndexTabletDatabaseFailureInjection::WriteResponseLogEntry(
    const NProtoPrivate::TResponseLogEntry& entry)
{
    Real->WriteResponseLogEntry(entry);
}

void TIndexTabletDatabaseFailureInjection::DeleteResponseLogEntry(
    ui64 clientTabletId,
    ui64 requestId)
{
    Real->DeleteResponseLogEntry(clientTabletId, requestId);
}

bool TIndexTabletDatabaseFailureInjection::ReadResponseLogEntry(
    ui64 clientTabletId,
    ui64 requestId,
    TMaybe<NProtoPrivate::TResponseLogEntry>& entry)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadResponseLogEntry(
        clientTabletId,
        requestId,
        entry);
}

bool TIndexTabletDatabaseFailureInjection::ReadResponseLog(
    TVector<NProtoPrivate::TResponseLogEntry>& responseLog)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadResponseLog(responseLog);
}

void TIndexTabletDatabaseFailureInjection::WriteUnconfirmedData(
    ui64 commitId,
    const NProto::TUnconfirmedData& data)
{
    Real->WriteUnconfirmedData(commitId, data);
}

void TIndexTabletDatabaseFailureInjection::DeleteUnconfirmedData(ui64 commitId)
{
    Real->DeleteUnconfirmedData(commitId);
}

bool TIndexTabletDatabaseFailureInjection::ReadUnconfirmedData(
    TVector<TUnconfirmedDataEntry>& entries)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadUnconfirmedData(entries);
}

std::unique_ptr<IIndexTabletDatabase> CreateIndexTabletDatabase(
    NKikimr::NTable::TDatabase& database,
    const ITxReschedulerPtr& rescheduler)
{
    std::unique_ptr<IIndexTabletDatabase> db =
        std::make_unique<TIndexTabletDatabase>(database);
    if (rescheduler) {
        db = std::make_unique<TIndexTabletDatabaseFailureInjection>(
            std::move(db),
            rescheduler);
    }
    return db;
}

std::unique_ptr<IIndexTabletDatabase> CreateIndexTabletDatabaseProxy(
    NKikimr::NTable::TDatabase& database,
    TVector<IInMemoryIndexState::TIndexStateRequest>& nodeUpdates,
    const ITxReschedulerPtr& rescheduler)
{
    std::unique_ptr<IIndexTabletDatabase> db =
        std::make_unique<TIndexTabletDatabaseProxy>(database, nodeUpdates);
    if (rescheduler) {
        db = std::make_unique<TIndexTabletDatabaseFailureInjection>(
            std::move(db),
            rescheduler);
    }
    return db;
}

}   // namespace NCloud::NFileStore::NStorage
