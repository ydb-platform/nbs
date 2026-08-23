#include "tablet_database_failure_injection.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletDatabaseWithFailureInjection::InitSchema()
{
    Real->InitSchema();
}

void TIndexTabletDatabaseWithFailureInjection::WriteFileSystem(
    const NProto::TFileSystem& fileSystem)
{
    Real->WriteFileSystem(fileSystem);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadFileSystem(
    NProto::TFileSystem& fileSystem)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadFileSystem(fileSystem);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadFileSystemStats(
    NProto::TFileSystemStats& stats)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadFileSystemStats(stats);
}

#define FILESTORE_IMPLEMENT_STATS(name, ...)                                   \
    void TIndexTabletDatabaseWithFailureInjection::Write##name(ui64 value)     \
    {                                                                          \
        Real->Write##name(value);                                              \
    }                                                                          \
// FILESTORE_IMPLEMENT_STATS

FILESTORE_FILESYSTEM_STATS(FILESTORE_IMPLEMENT_STATS)

#undef FILESTORE_IMPLEMENT_STATS

void TIndexTabletDatabaseWithFailureInjection::WriteStorageConfig(
    const NProto::TStorageConfig& storageConfig)
{
    Real->WriteStorageConfig(storageConfig);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadStorageConfig(
    TMaybe<NProto::TStorageConfig>& storageConfig)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadStorageConfig(storageConfig);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadTabletStorageInfo(
    NCloud::NProto::TTabletStorageInfo& tabletStorageInfo)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadTabletStorageInfo(tabletStorageInfo);
}

void TIndexTabletDatabaseWithFailureInjection::WriteTabletStorageInfo(
    const NCloud::NProto::TTabletStorageInfo& tabletStorageInfo)
{
    Real->WriteTabletStorageInfo(tabletStorageInfo);
}

void TIndexTabletDatabaseWithFailureInjection::WriteNode(
    ui64 nodeId,
    ui64 commitId,
    const NProto::TNode& attrs)
{
    Real->WriteNode(nodeId, commitId, attrs);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteNode(ui64 nodeId)
{
    Real->DeleteNode(nodeId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadNode(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNode(nodeId, commitId, node);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadNodes(
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

void TIndexTabletDatabaseWithFailureInjection::WriteNodeVer(
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const NProto::TNode& attrs)
{
    Real->WriteNodeVer(nodeId, minCommitId, maxCommitId, attrs);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteNodeVer(
    ui64 nodeId,
    ui64 commitId)
{
    Real->DeleteNodeVer(nodeId, commitId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadNodeVer(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeVer(nodeId, commitId, node);
}

void TIndexTabletDatabaseWithFailureInjection::WriteNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    const TString& value,
    ui64 version)
{
    Real->WriteNodeAttr(nodeId, commitId, name, value, version);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteNodeAttr(
    ui64 nodeId,
    const TString& name)
{
    Real->DeleteNodeAttr(nodeId, name);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadNodeAttr(
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

bool TIndexTabletDatabaseWithFailureInjection::ReadNodeAttrs(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeAttr>& attrs)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeAttrs(nodeId, commitId, attrs);
}

void TIndexTabletDatabaseWithFailureInjection::WriteNodeAttrVer(
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

void TIndexTabletDatabaseWithFailureInjection::DeleteNodeAttrVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name)
{
    Real->DeleteNodeAttrVer(nodeId, commitId, name);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadNodeAttrVer(
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

bool TIndexTabletDatabaseWithFailureInjection::ReadNodeAttrVers(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeAttr>& attrs)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeAttrVers(nodeId, commitId, attrs);
}

void TIndexTabletDatabaseWithFailureInjection::WriteNodeRef(
    const TNodeRef& nodeRef,
    bool markExhaustive)
{
    Real->WriteNodeRef(nodeRef, markExhaustive);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteNodeRef(
    ui64 nodeId,
    const TString& name)
{
    Real->DeleteNodeRef(nodeId, name);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadNodeRef(
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

bool TIndexTabletDatabaseWithFailureInjection::ReadNodeRefs(
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

bool TIndexTabletDatabaseWithFailureInjection::ReadNodeRefs(
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

bool TIndexTabletDatabaseWithFailureInjection::PrechargeNodeRefs(
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

void TIndexTabletDatabaseWithFailureInjection::WriteNodeRefVer(
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

void TIndexTabletDatabaseWithFailureInjection::DeleteNodeRefVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name)
{
    Real->DeleteNodeRefVer(nodeId, commitId, name);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadNodeRefVer(
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

bool TIndexTabletDatabaseWithFailureInjection::ReadNodeRefVers(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeRef>& refs)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNodeRefVers(nodeId, commitId, refs);
}

void TIndexTabletDatabaseWithFailureInjection::WriteTruncateQueueEntry(
    ui64 nodeId,
    TByteRange range)
{
    Real->WriteTruncateQueueEntry(nodeId, range);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteTruncateQueueEntry(ui64 id)
{
    Real->DeleteTruncateQueueEntry(id);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadTruncateQueue(
    TVector<NProto::TTruncateEntry>& entries)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadTruncateQueue(entries);
}

void TIndexTabletDatabaseWithFailureInjection::WriteSession(
    const NProto::TSession& session)
{
    Real->WriteSession(session);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteSession(
    const TString& sessionId)
{
    Real->DeleteSession(sessionId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadSessions(
    TVector<NProto::TSession>& sessions)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessions(sessions);
}

void TIndexTabletDatabaseWithFailureInjection::WriteSessionHandle(
    const NProto::TSessionHandle& handle)
{
    Real->WriteSessionHandle(handle);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteSessionHandle(
    const TString& sessionId,
    ui64 handle)
{
    Real->DeleteSessionHandle(sessionId, handle);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadSessionHandles(
    TVector<NProto::TSessionHandle>& handles)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionHandles(handles);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadSessionHandles(
    const TString& sessionId,
    TVector<NProto::TSessionHandle>& handles)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionHandles(sessionId, handles);
}

void TIndexTabletDatabaseWithFailureInjection::WriteSessionLock(
    const NProto::TSessionLock& lock)
{
    Real->WriteSessionLock(lock);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteSessionLock(
    const TString& sessionId,
    ui64 lockId)
{
    Real->DeleteSessionLock(sessionId, lockId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadSessionLocks(
    TVector<NProto::TSessionLock>& locks)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionLocks(locks);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadSessionLocks(
    const TString& sessionId,
    TVector<NProto::TSessionLock>& locks)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionLocks(sessionId, locks);
}

void TIndexTabletDatabaseWithFailureInjection::WriteSessionDupCacheEntry(
    const NProto::TDupCacheEntry& entry)
{
    Real->WriteSessionDupCacheEntry(entry);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteSessionDupCacheEntry(
    const TString& sessionId,
    ui64 entryId)
{
    Real->DeleteSessionDupCacheEntry(sessionId, entryId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadSessionDupCacheEntries(
    TVector<NProto::TDupCacheEntry>& entries)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionDupCacheEntries(entries);
}

void TIndexTabletDatabaseWithFailureInjection::WriteSessionHistoryEntry(
    const NProto::TSessionHistoryEntry& entry)
{
    Real->WriteSessionHistoryEntry(entry);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteSessionHistoryEntry(
    ui64 entryId)
{
    Real->DeleteSessionHistoryEntry(entryId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadSessionHistoryEntries(
    TVector<NProto::TSessionHistoryEntry>& entries)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadSessionHistoryEntries(entries);
}

void TIndexTabletDatabaseWithFailureInjection::WriteFreshBytes(
    ui64 nodeId,
    ui64 commitId,
    ui64 offset,
    TStringBuf data)
{
    Real->WriteFreshBytes(nodeId, commitId, offset, data);
}

void TIndexTabletDatabaseWithFailureInjection::WriteFreshBytesDeletionMarker(
    ui64 nodeId,
    ui64 commitId,
    ui64 offset,
    ui64 len)
{
    Real->WriteFreshBytesDeletionMarker(nodeId, commitId, offset, len);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteFreshBytes(
    ui64 nodeId,
    ui64 commitId,
    ui64 offset)
{
    Real->DeleteFreshBytes(nodeId, commitId, offset);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadFreshBytes(
    TVector<TFreshBytesEntry>& bytes)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadFreshBytes(bytes);
}

void TIndexTabletDatabaseWithFailureInjection::WriteFreshBlock(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    TStringBuf blockData)
{
    Real->WriteFreshBlock(nodeId, commitId, blockIndex, blockData);
}

void TIndexTabletDatabaseWithFailureInjection::MarkFreshBlockDeleted(
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

void TIndexTabletDatabaseWithFailureInjection::DeleteFreshBlock(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex)
{
    Real->DeleteFreshBlock(nodeId, commitId, blockIndex);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadFreshBlocks(TVector<TFreshBlock>& blocks)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadFreshBlocks(blocks);
}

void TIndexTabletDatabaseWithFailureInjection::WriteMixedBlocks(
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

void TIndexTabletDatabaseWithFailureInjection::DeleteMixedBlocks(
    ui32 rangeId,
    const TPartialBlobId& blobId)
{
    Real->DeleteMixedBlocks(rangeId, blobId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadMixedBlocks(
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

bool TIndexTabletDatabaseWithFailureInjection::ReadMixedBlocks(
    ui32 rangeId,
    TVector<TMixedBlob>& blobs,
    IAllocator* alloc)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadMixedBlocks(rangeId, blobs, alloc);
}

void TIndexTabletDatabaseWithFailureInjection::WriteDeletionMarkers(
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

void TIndexTabletDatabaseWithFailureInjection::DeleteDeletionMarker(
    ui32 rangeId,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex)
{
    Real->DeleteDeletionMarker(rangeId, nodeId, commitId, blockIndex);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadDeletionMarkers(
    ui32 rangeId,
    TVector<TDeletionMarker>& deletionMarkers)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadDeletionMarkers(rangeId, deletionMarkers);
}

void TIndexTabletDatabaseWithFailureInjection::WriteLargeDeletionMarkers(
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

void TIndexTabletDatabaseWithFailureInjection::DeleteLargeDeletionMarker(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex)
{
    Real->DeleteLargeDeletionMarker(nodeId, commitId, blockIndex);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadLargeDeletionMarkers(
    TVector<TDeletionMarker>& deletionMarkers)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadLargeDeletionMarkers(deletionMarkers);
}

void TIndexTabletDatabaseWithFailureInjection::WriteOrphanNode(ui64 nodeId)
{
    Real->WriteOrphanNode(nodeId);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteOrphanNode(ui64 nodeId)
{
    Real->DeleteOrphanNode(nodeId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadOrphanNodes(TVector<ui64>& nodeIds)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadOrphanNodes(nodeIds);
}

void TIndexTabletDatabaseWithFailureInjection::WriteDeferredNodeDestruction(
    ui64 nodeId)
{
    Real->WriteDeferredNodeDestruction(nodeId);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteDeferredNodeDestruction(
    ui64 nodeId)
{
    Real->DeleteDeferredNodeDestruction(nodeId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadDeferredNodeDestructions(
    TVector<ui64>& nodeIds)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadDeferredNodeDestructions(nodeIds);
}

void TIndexTabletDatabaseWithFailureInjection::WriteNewBlob(
    const TPartialBlobId& blobId)
{
    Real->WriteNewBlob(blobId);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteNewBlob(
    const TPartialBlobId& blobId)
{
    Real->DeleteNewBlob(blobId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadNewBlobs(TVector<TPartialBlobId>& blobIds)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadNewBlobs(blobIds);
}

void TIndexTabletDatabaseWithFailureInjection::WriteGarbageBlob(
    const TPartialBlobId& blobId)
{
    Real->WriteGarbageBlob(blobId);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteGarbageBlob(
    const TPartialBlobId& blobId)
{
    Real->DeleteGarbageBlob(blobId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadGarbageBlobs(
    TVector<TPartialBlobId>& blobIds)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadGarbageBlobs(blobIds);
}

void TIndexTabletDatabaseWithFailureInjection::WriteCheckpoint(
    const NProto::TCheckpoint& checkpoint)
{
    Real->WriteCheckpoint(checkpoint);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteCheckpoint(
    const TString& checkpointId)
{
    Real->DeleteCheckpoint(checkpointId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadCheckpoints(
    TVector<NProto::TCheckpoint>& checkpoints)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadCheckpoints(checkpoints);
}

void TIndexTabletDatabaseWithFailureInjection::WriteQuota(
    const NProto::TQuota& quota)
{
    Real->WriteQuota(quota);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteQuota(ui32 quotaId)
{
    Real->DeleteQuota(quotaId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadQuotas(
    TVector<NProto::TQuota>& quotas)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadQuotas(quotas);
}

void TIndexTabletDatabaseWithFailureInjection::WriteQuotaUsage(
    ui32 quotaId,
    ui64 usedBytes,
    ui64 usedNodes)
{
    Real->WriteQuotaUsage(quotaId, usedBytes, usedNodes);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteQuotaUsage(ui32 quotaId)
{
    Real->DeleteQuotaUsage(quotaId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadQuotaUsages(
    TVector<TQuotaUsage>& usages)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadQuotaUsages(usages);
}

void TIndexTabletDatabaseWithFailureInjection::WriteCheckpointNode(
    ui64 checkpointId,
    ui64 nodeId)
{
    Real->WriteCheckpointNode(checkpointId, nodeId);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteCheckpointNode(
    ui64 checkpointId,
    ui64 nodeId)
{
    Real->DeleteCheckpointNode(checkpointId, nodeId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadCheckpointNodes(
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

void TIndexTabletDatabaseWithFailureInjection::WriteCheckpointBlob(
    ui64 checkpointId,
    ui32 rangeId,
    const TPartialBlobId& blobId)
{
    Real->WriteCheckpointBlob(checkpointId, rangeId, blobId);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteCheckpointBlob(
    ui64 checkpointId,
    ui32 rangeId,
    const TPartialBlobId& blobId)
{
    Real->DeleteCheckpointBlob(checkpointId, rangeId, blobId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadCheckpointBlobs(
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

void TIndexTabletDatabaseWithFailureInjection::ForceWriteCompactionMap(
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

void TIndexTabletDatabaseWithFailureInjection::WriteCompactionMap(
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

bool TIndexTabletDatabaseWithFailureInjection::ReadCompactionMap(
    TVector<TCompactionRangeInfo>& compactionMap)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadCompactionMap(compactionMap);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadCompactionMap(
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

void TIndexTabletDatabaseWithFailureInjection::WriteOpLogEntry(
    const NProto::TOpLogEntry& entry)
{
    Real->WriteOpLogEntry(entry);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteOpLogEntry(ui64 entryId)
{
    Real->DeleteOpLogEntry(entryId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadOpLogEntry(
    ui64 entryId,
    TMaybe<NProto::TOpLogEntry>& entry)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadOpLogEntry(entryId, entry);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadOpLog(TVector<NProto::TOpLogEntry>& opLog)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadOpLog(opLog);
}

void TIndexTabletDatabaseWithFailureInjection::WriteResponseLogEntry(
    const NProtoPrivate::TResponseLogEntry& entry)
{
    Real->WriteResponseLogEntry(entry);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteResponseLogEntry(
    ui64 clientTabletId,
    ui64 requestId)
{
    Real->DeleteResponseLogEntry(clientTabletId, requestId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadResponseLogEntry(
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

bool TIndexTabletDatabaseWithFailureInjection::ReadResponseLog(
    TVector<NProtoPrivate::TResponseLogEntry>& responseLog)
{
    if (Y_UNLIKELY(ShouldFailReadInTest())) {
        return false;
    }

    return Real->ReadResponseLog(responseLog);
}

void TIndexTabletDatabaseWithFailureInjection::WriteUnconfirmedData(
    ui64 commitId,
    const NProto::TUnconfirmedData& data)
{
    Real->WriteUnconfirmedData(commitId, data);
}

void TIndexTabletDatabaseWithFailureInjection::DeleteUnconfirmedData(ui64 commitId)
{
    Real->DeleteUnconfirmedData(commitId);
}

bool TIndexTabletDatabaseWithFailureInjection::ReadUnconfirmedData(
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
        db = std::make_unique<TIndexTabletDatabaseWithFailureInjection>(
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
        db = std::make_unique<TIndexTabletDatabaseWithFailureInjection>(
            std::move(db),
            rescheduler);
    }
    return db;
}

}   // namespace NCloud::NFileStore::NStorage
