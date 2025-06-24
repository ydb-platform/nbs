#include "tablet_state_impl.h"

#include "helpers.h"

#include <cloud/filestore/libs/storage/model/utils.h>

namespace NCloud::NFileStore::NStorage {

namespace
{

ui64 SizeSum(const TString& v1, const TString& v2)
{
    return v1.size() + v2.size();
}

ui64 SizeDiff(const TString& v1, const TString& v2)
{
    return v1.size() - v2.size();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////
// Nodes

bool TIndexTabletState::HasSpaceLeft(const NProto::TNode& attrs, ui64 newSize) const
{
    i64 delta = GetBlocksDifference(attrs.GetSize(), newSize, GetBlockSize());
    if (delta > 0 && GetUsedBlocksCount() + delta > GetBlocksCount()) {
        return false;
    }

    return true;
}

bool TIndexTabletState::HasBlocksLeft(ui32 blocks) const
{
    if (blocks && GetUsedBlocksCount() + blocks > GetBlocksCount()) {
        return false;
    }

    return true;
}

void TIndexTabletState::UpdateUsedBlocksCount(
    TIndexTabletDatabase& db,
    ui64 currentSize,
    ui64 prevSize)
{
    i64 delta = GetBlocksDifference(prevSize, currentSize, GetBlockSize());
    if (delta > 0) {
        IncrementUsedBlocksCount(db, delta);
    } else if (delta < 0) {
        DecrementUsedBlocksCount(db, -delta);
    }
}

ui64 TIndexTabletState::CreateNode(
    TIndexTabletDatabase& db,
    ui64 commitId,
    const NProto::TNode& attrs)
{
    const ui64 nodeId =
        ShardedId(IncrementLastNodeId(db), GetFileSystem().GetShardNo());

    db.WriteNode(nodeId, commitId, attrs);
    IncrementUsedNodesCount(db);

    // so far symlink node has size
    UpdateUsedBlocksCount(db, attrs.GetSize(), 0);

    return nodeId;
}

void TIndexTabletState::UpdateNode(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const NProto::TNode& attrs,
    const NProto::TNode& prevAttrs)
{
    UpdateUsedBlocksCount(db, attrs.GetSize(), prevAttrs.GetSize());

    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, minCommitId);
    if (checkpointId == InvalidCommitId) {
        // simple in-place update
        db.WriteNode(nodeId, minCommitId, attrs);
    } else {
        // copy-on-write update
        db.WriteNode(nodeId, maxCommitId, attrs);
        db.WriteNodeVer(nodeId, checkpointId, maxCommitId, prevAttrs);

        AddCheckpointNode(db, checkpointId, nodeId);
    }

    InvalidateNodeIndexCache(nodeId);
}

NProto::TError TIndexTabletState::RemoveNode(
    TIndexTabletDatabase& db,
    const IIndexTabletDatabase::TNode& node,
    ui64 minCommitId,
    ui64 maxCommitId)
{
    // SymLinks have size (equal to TargetPath) but store no real data so there
    // is no need to write deletion markers upon SymLink removal
    if (!node.Attrs.GetSymLink()) {
        auto e = Truncate(
            db,
            node.NodeId,
            maxCommitId,
            node.Attrs.GetSize(),
            0);

        if (HasError(e)) {
            return e;
        }
    }

    db.DeleteNode(node.NodeId);
    DecrementUsedNodesCount(db);

    UpdateUsedBlocksCount(db, 0, node.Attrs.GetSize());

    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(node.NodeId, minCommitId);
    if (checkpointId != InvalidCommitId) {
        // keep history version
        db.WriteNodeVer(node.NodeId, checkpointId, maxCommitId, node.Attrs);
        AddCheckpointNode(db, checkpointId, node.NodeId);
    }

    InvalidateNodeIndexCache(node.NodeId);

    return {};
}

NProto::TError TIndexTabletState::UnlinkNode(
    TIndexTabletDatabase& db,
    ui64 parentNodeId,
    const TString& name,
    const IIndexTabletDatabase::TNode& node,
    ui64 minCommitId,
    ui64 maxCommitId,
    bool removeNodeRef)
{
    if (node.Attrs.GetLinks() > 1 || HasOpenHandles(node.NodeId)) {
        auto attrs = CopyAttrs(node.Attrs, E_CM_CMTIME | E_CM_UNREF);
        UpdateNode(
            db,
            node.NodeId,
            minCommitId,
            maxCommitId,
            attrs,
            node.Attrs);
    } else {
        auto e = RemoveNode(
            db,
            node,
            minCommitId,
            maxCommitId);

        if (HasError(e)) {
            return e;
        }
    }
    if (!removeNodeRef) {
        // do not remove node ref
        return {};
    }

    RemoveNodeRef(
        db,
        parentNodeId,
        minCommitId,
        maxCommitId,
        name,
        node.NodeId,
        "", // shardId
        "" // shardNodeName
    );

    return {};
}

void TIndexTabletState::UnlinkExternalNode(
    TIndexTabletDatabase& db,
    ui64 parentNodeId,
    const TString& name,
    const TString& shardId,
    const TString& shardNodeName,
    ui64 minCommitId,
    ui64 maxCommitId)
{
    RemoveNodeRef(
        db,
        parentNodeId,
        minCommitId,
        maxCommitId,
        name,
        InvalidNodeId, // prevChildNodeId
        shardId,
        shardNodeName);
}

bool TIndexTabletState::ReadNode(
    IIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    TMaybe<IIndexTabletDatabase::TNode>& node)
{
    bool ready = db.ReadNode(nodeId, commitId, node);

    if (ready && node) {
        // fast path
        return true;
    }

    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, commitId);
    if (checkpointId != InvalidCommitId) {
        // there could be history versions
        if (!db.ReadNodeVer(nodeId, commitId, node)) {
            ready = false;
        }
    }

    return ready;
}

void TIndexTabletState::RewriteNode(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const NProto::TNode& attrs)
{
    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, minCommitId);
    if (checkpointId != InvalidCommitId) {
        // keep history version
        db.WriteNodeVer(nodeId, checkpointId, maxCommitId, attrs);

        AddCheckpointNode(db, checkpointId, nodeId);
    } else {
        // no need this version any more
        db.DeleteNodeVer(nodeId, minCommitId);
    }

    InvalidateNodeIndexCache(nodeId);
}

void TIndexTabletState::WriteOrphanNode(
    TIndexTabletDatabase& db,
    const TString& message,
    ui64 nodeId)
{
    ReportGeneratedOrphanNode(message);
    db.WriteOrphanNode(nodeId);
    Impl->OrphanNodeIds.insert(nodeId);
}

bool TIndexTabletState::HasPendingNodeCreateInShard(const TString& nodeName) const
{
    return Impl->PendingNodeCreateInShardNames.contains(nodeName);
}

void TIndexTabletState::StartNodeCreateInShard(const TString& nodeName)
{
    Impl->PendingNodeCreateInShardNames.insert(nodeName);
}

void TIndexTabletState::EndNodeCreateInShard(const TString& nodeName)
{
    Impl->PendingNodeCreateInShardNames.erase(nodeName);
}


////////////////////////////////////////////////////////////////////////////////
// NodeAttrs

ui64 TIndexTabletState::CreateNodeAttr(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    const TString& value)
{
    ui64 version = IncrementLastXAttr(db);
    db.WriteNodeAttr(nodeId, commitId, name, value, version);

    IncrementAttrsUsedBytesCount(db, SizeSum(name, value));

    return version;
}

ui64 TIndexTabletState::UpdateNodeAttr(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const TIndexTabletDatabase::TNodeAttr& attr,
    const TString& newValue)
{
    ui64 version = IncrementLastXAttr(db);
    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, minCommitId);
    if (checkpointId == InvalidCommitId) {
        // simple in-place update
        db.WriteNodeAttr(nodeId, minCommitId, attr.Name, newValue, version);
    } else {
        // copy-on-write update
        db.WriteNodeAttr(nodeId, maxCommitId, attr.Name, newValue, version);
        db.WriteNodeAttrVer(
            nodeId,
            checkpointId,
            maxCommitId,
            attr.Name,
            attr.Value,
            attr.Version);

        AddCheckpointNode(db, checkpointId, nodeId);
    }

    if (newValue.size() > attr.Value.size()) {
        IncrementAttrsUsedBytesCount(db, SizeDiff(newValue, attr.Value));
    } else {
        DecrementAttrsUsedBytesCount(db, SizeDiff(attr.Value, newValue));
    }

    InvalidateNodeIndexCache(nodeId);

    return version;
}

void TIndexTabletState::RemoveNodeAttr(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const TIndexTabletDatabase::TNodeAttr& attr)
{
    db.DeleteNodeAttr(nodeId, attr.Name);

    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, minCommitId);
    if (checkpointId != InvalidCommitId) {
        // keep history version
        db.WriteNodeAttrVer(
            nodeId,
            checkpointId,
            maxCommitId,
            attr.Name,
            attr.Value,
            attr.Version);

        AddCheckpointNode(db, checkpointId, nodeId);
    }

    DecrementAttrsUsedBytesCount(db, SizeSum(attr.Name, attr.Value));
}

bool TIndexTabletState::ReadNodeAttr(
    IIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TIndexTabletDatabase::TNodeAttr>& attr)
{
    bool ready = db.ReadNodeAttr(nodeId, commitId, name, attr);

    if (ready && attr) {
        // fast path
        return true;
    }

    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, commitId);
    if (checkpointId != InvalidCommitId) {
        // there could be history versions
        if (!db.ReadNodeAttrVer(nodeId, commitId, name, attr)) {
            ready = false;
        }
    }

    return ready;
}

bool TIndexTabletState::ReadNodeAttrs(
    IIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    TVector<TIndexTabletDatabase::TNodeAttr>& attrs)
{
    bool ready = db.ReadNodeAttrs(nodeId, commitId, attrs);

    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, commitId);
    if (checkpointId != InvalidCommitId) {
        // there could be history versions
        if (!db.ReadNodeAttrVers(nodeId, commitId, attrs)) {
            ready = false;
        }
    }

    return ready;
}

void TIndexTabletState::RewriteNodeAttr(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const TIndexTabletDatabase::TNodeAttr& attr)
{
    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, minCommitId);
    if (checkpointId != InvalidCommitId) {
        // keep history version
        db.WriteNodeAttrVer(
            nodeId,
            checkpointId,
            maxCommitId,
            attr.Name,
            attr.Value,
            attr.Version);

        AddCheckpointNode(db, checkpointId, nodeId);
    } else {
        // no need this version any more
        db.DeleteNodeAttrVer(nodeId, minCommitId, attr.Name);
    }

    InvalidateNodeIndexCache(nodeId);
}

////////////////////////////////////////////////////////////////////////////////
// NodeRefs

void TIndexTabletState::CreateNodeRef(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    const TString& childName,
    ui64 childNodeId,
    const TString& shardId,
    const TString& shardNodeName)
{
    db.WriteNodeRef(
        nodeId,
        commitId,
        childName,
        childNodeId,
        shardId,
        shardNodeName);
}

void TIndexTabletState::RemoveNodeRef(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const TString& childName,
    ui64 prevChildNodeId,
    const TString& shardId,
    const TString& shardNodeName)
{
    db.DeleteNodeRef(nodeId, childName);

    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, minCommitId);
    if (checkpointId != InvalidCommitId) {
        // keep history version
        db.WriteNodeRefVer(
            nodeId,
            checkpointId,
            maxCommitId,
            childName,
            prevChildNodeId,
            shardId,
            shardNodeName);

        AddCheckpointNode(db, checkpointId, nodeId);
    }

    InvalidateNodeIndexCache(nodeId, childName);
}

bool TIndexTabletState::ReadNodeRef(
    IIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<IIndexTabletDatabase::TNodeRef>& ref)
{
    bool ready = db.ReadNodeRef(nodeId, commitId, name, ref);

    if (ready && ref) {
        // fast path
        return true;
    }

    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, commitId);
    if (checkpointId != InvalidCommitId) {
        // there could be history versions
        if (!db.ReadNodeRefVer(nodeId, commitId, name, ref)) {
            ready = false;
        }
    }

    return ready;
}

bool TIndexTabletState::ReadNodeRefs(
    IIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 commitId,
    const TString& cookie,
    TVector<IIndexTabletDatabase::TNodeRef>& refs,
    ui32 maxBytes,
    TString* next)
{
    bool ready = db.ReadNodeRefs(nodeId, commitId, cookie, refs, maxBytes, next);

    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, commitId);
    if (checkpointId != InvalidCommitId) {
        // there could be history versions
        if (!db.ReadNodeRefVers(nodeId, commitId, refs)) {
            ready = false;
        }
    }

    return ready;
}

bool TIndexTabletState::ReadNodeRefs(
    IIndexTabletDatabase& db,
    ui64 startNodeId,
    const TString& startCookie,
    ui64 maxCount,
    TVector<IIndexTabletDatabase::TNodeRef>& refs,
    ui64& nextNodeId,
    TString& nextCookie)
{
    return db.ReadNodeRefs(
        startNodeId,
        startCookie,
        maxCount,
        refs,
        nextNodeId,
        nextCookie);
}

bool TIndexTabletState::PrechargeNodeRefs(
    IIndexTabletDatabase& db,
    ui64 nodeId,
    const TString& cookie,
    ui32 bytesToPrecharge)
{
    return db.PrechargeNodeRefs(nodeId, cookie, bytesToPrecharge);
}

void TIndexTabletState::RewriteNodeRef(
    TIndexTabletDatabase& db,
    ui64 nodeId,
    ui64 minCommitId,
    ui64 maxCommitId,
    const TString& childName,
    ui64 childNodeId,
    const TString& shardId,
    const TString& shardNodeName)
{
    ui64 checkpointId = Impl->Checkpoints.FindCheckpoint(nodeId, minCommitId);
    if (checkpointId != InvalidCommitId) {
        // keep history version
        db.WriteNodeRefVer(
            nodeId,
            checkpointId,
            maxCommitId,
            childName,
            childNodeId,
            shardId,
            shardNodeName);

        AddCheckpointNode(db, checkpointId, nodeId);
    } else {
        // no need this version any more
        db.DeleteNodeRefVer(nodeId, minCommitId, childName);
    }

    InvalidateNodeIndexCache(nodeId, childName);
}

bool TIndexTabletState::TryLockNodeRef(TNodeRefKey key)
{
    return Impl->LockedNodeRefs.insert(std::move(key)).second;
}

void TIndexTabletState::UnlockNodeRef(TNodeRefKey key)
{
    Impl->LockedNodeRefs.erase(std::move(key));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletState::TryFillGetNodeAttrResult(
    ui64 parentNodeId,
    const TString& name,
    NProto::TNodeAttr* response)
{
    return Impl->NodeIndexCache.TryFillGetNodeAttrResult(
        parentNodeId,
        name,
        response);
}

void TIndexTabletState::InvalidateNodeIndexCache(
    ui64 parentNodeId,
    const TString& name)
{
    Impl->NodeIndexCache.InvalidateCache(parentNodeId, name);
}

void TIndexTabletState::InvalidateNodeIndexCache(ui64 nodeId)
{
    Impl->NodeIndexCache.InvalidateCache(nodeId);
}

void TIndexTabletState::RegisterGetNodeAttrResult(
    ui64 parentNodeId,
    const TString& name,
    const NProto::TNodeAttr& result)
{
    Impl->NodeIndexCache.RegisterGetNodeAttrResult(parentNodeId, name, result);
}

TNodeIndexCacheStats TIndexTabletState::CalculateNodeIndexCacheStats() const
{
    return Impl->NodeIndexCache.GetStats();
}

////////////////////////////////////////////////////////////////////////////////

IIndexTabletDatabase& TIndexTabletState::AccessInMemoryIndexState()
{
    return Impl->InMemoryIndexState;
}

void TIndexTabletState::UpdateInMemoryIndexState(
    TVector<TInMemoryIndexState::TIndexStateRequest> nodeUpdates)
{
    Impl->InMemoryIndexState.UpdateState(nodeUpdates);
}

void TIndexTabletState::MarkNodeRefsLoadComplete()
{
    Impl->InMemoryIndexState.MarkNodeRefsLoadComplete();
}

TInMemoryIndexStateStats TIndexTabletState::GetInMemoryIndexStateStats() const
{
    return Impl->InMemoryIndexState.GetStats();
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletState::InvalidateNodeCaches(ui64 nodeId)
{
    InvalidateReadAheadCache(nodeId);
    InvalidateNodeIndexCache(nodeId);
}

}   // namespace NCloud::NFileStore::NStorage
