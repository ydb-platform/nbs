#include "tablet_state_cache.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TInMemoryIndexState::TInMemoryIndexState(IAllocator* allocator)
    : Nodes(0), NodeAttrs(0), NodeRefs(allocator)
{
}

void TInMemoryIndexState::Reset(
    ui64 nodesCapacity,
    ui64 nodeAttrsCapacity,
    ui64 nodeRefsCapacity)
{
    Nodes.SetMaxSize(nodesCapacity);
    NodeAttrs.SetMaxSize(nodeAttrsCapacity);
    if(NodeRefs.size() == NodeRefs.capacity() && nodeRefsCapacity < NodeRefs.capacity()) {
        IsNodeRefsExhaustive = false;
        IsNodeRefsEvictionObserved = true;
    }
    NodeRefs.SetCapacity(nodeRefsCapacity);
}

void TInMemoryIndexState::LoadNodeRefs(const TVector<TNodeRef>& nodeRefs)
{
    for (const auto& nodeRef: nodeRefs) {
        WriteNodeRef(
            nodeRef.NodeId,
            nodeRef.MinCommitId,
            nodeRef.Name,
            nodeRef.ChildNodeId,
            nodeRef.ShardId,
            nodeRef.ShardNodeName);
    }
}

void TInMemoryIndexState::MarkNodeRefsLoadComplete()
{
    // If during the startup there were no evictions, then the cache should be
    // complete upon the load completion.
    IsNodeRefsExhaustive = !IsNodeRefsEvictionObserved;
}

TInMemoryIndexStateStats TInMemoryIndexState::GetStats() const
{
    return TInMemoryIndexStateStats{
        .NodesCount = Nodes.Size(),
        .NodesCapacity = Nodes.GetMaxSize(),
        .NodeRefsCount = NodeRefs.size(),
        .NodeRefsCapacity = NodeRefs.capacity(),
        .NodeAttrsCount = NodeAttrs.Size(),
        .NodeAttrsCapacity = NodeAttrs.GetMaxSize(),
        .IsNodeRefsExhaustive = IsNodeRefsExhaustive,
    };
}

//
// Nodes
//

bool TInMemoryIndexState::ReadNode(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    auto it = Nodes.Find(nodeId);
    if (it == Nodes.End()) {
        return false;
    }

    ui64 minCommitId = it.Value().CommitId;
    ui64 maxCommitId = InvalidCommitId;

    if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
        node = TNode{nodeId, it.Value().Node, minCommitId, maxCommitId};
    }

    // We found the entry in table. There is at most one entry matching the key,
    // meaning that cache lookup was successful, independent of whether the
    // entry is visible or not to the given commitId.
    return true;
}

bool TInMemoryIndexState::ReadNodes(
    ui64 startNodeId,
    ui64 maxNodes,
    ui64& nextNodeId,
    TVector<TNode>& nodes)
{
    Y_UNUSED(startNodeId, maxNodes, nextNodeId, nodes);
    // TInMemoryIndexState is a preemptive cache, thus it is impossible to
    // determine, whether the set of stored nodes is complete.
    return false;
}

void TInMemoryIndexState::WriteNode(
    ui64 nodeId,
    ui64 commitId,
    const NProto::TNode& attrs)
{
    Nodes.Update(nodeId, TNodeRow{.CommitId = commitId, .Node = attrs});
}

void TInMemoryIndexState::DeleteNode(ui64 nodeId)
{
    auto it = Nodes.Find(nodeId);
    if (it != Nodes.End()) {
        Nodes.Erase(it);
    }
}

//
// Nodes_Ver
//

bool TInMemoryIndexState::ReadNodeVer(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    Y_UNUSED(nodeId, commitId, node);
    // TODO(#1146): _Ver tables not supported yet
    return false;
}

//
// NodeAttrs
//

bool TInMemoryIndexState::ReadNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    auto it = NodeAttrs.Find(TNodeAttrsKey(nodeId, name));
    if (it == NodeAttrs.End()) {
        return false;
    }

    ui64 minCommitId = it.Value().CommitId;
    ui64 maxCommitId = InvalidCommitId;

    if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
        attr = TNodeAttr{
            nodeId,
            name,
            it.Value().Value,
            minCommitId,
            maxCommitId,
            it.Value().Version};
    }
    // We found the entry in table. There is at most one entry matching the key,
    // meaning that cache lookup was successful, independent of whether the
    // entry is visible or not to the given commitId.
    return true;
}

bool TInMemoryIndexState::ReadNodeAttrs(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeAttr>& attrs)
{
    // TInMemoryIndexState is a preemptive cache, thus it is impossible to
    // determine, whether the set of stored attributes is complete.
    Y_UNUSED(nodeId, commitId, attrs);
    return false;
}

void TInMemoryIndexState::WriteNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    const TString& value,
    ui64 version)
{
    const auto key = TNodeAttrsKey(nodeId, name);
    NodeAttrs.Update(key, TNodeAttrsRow{.CommitId = commitId, .Value = value, .Version = version});
}

void TInMemoryIndexState::DeleteNodeAttr(ui64 nodeId, const TString& name)
{
    auto it = NodeAttrs.Find(TNodeAttrsKey(nodeId, name));
    if (it != NodeAttrs.End()) {
        NodeAttrs.Erase(it);
    }
}

//
// NodeAttrs_Ver
//

bool TInMemoryIndexState::ReadNodeAttrVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    Y_UNUSED(nodeId, commitId, name, attr);
    // TODO(#1146): _Ver tables not supported yet
    return false;
}

bool TInMemoryIndexState::ReadNodeAttrVers(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeAttr>& attrs)
{
    // TInMemoryIndexState is a preemptive cache, thus it is impossible to
    // determine, whether the set of stored attributes is complete.
    Y_UNUSED(nodeId, commitId, attrs);
    return false;
}

//
// NodeRefs
//

bool TInMemoryIndexState::ReadNodeRef(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    auto it = NodeRefs.find(TNodeRefsKey(nodeId, name));
    if (it == NodeRefs.end()) {
        return false;
    }

    ui64 minCommitId = it->second.CommitId;
    ui64 maxCommitId = InvalidCommitId;

    if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
        ref = TNodeRef{
            nodeId,
            name,
            it->second.ChildId,
            it->second.ShardId,
            it->second.ShardNodeName,
            minCommitId,
            maxCommitId};
    }

    // We found the entry in table. There is at most one entry matching the key,
    // meaning that cache lookup was successful, independent of whether the
    // entry is visible or not to the given commitId.
    return true;
}

bool TInMemoryIndexState::ReadNodeRefs(
    ui64 nodeId,
    ui64 commitId,
    const TString& cookie,
    TVector<TNodeRef>& refs,
    ui32 maxBytes,
    TString* next)
{
    if (!IsNodeRefsExhaustive) {
        // TInMemoryIndexState is a preemptive cache, thus it is impossible to
        // determine, whether the set of stored references is complete.
        return false;
    }

    auto it = NodeRefs.lower_bound(TNodeRefsKey(nodeId, cookie));

    ui32 bytes = 0;
    while (it != NodeRefs.end() && it->first.NodeId == nodeId) {
        NodeRefs.UpdateOrder(it->first);

        ui64 minCommitId = it->second.CommitId;
        ui64 maxCommitId = InvalidCommitId;

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            refs.emplace_back(TNodeRef{
                nodeId,
                it->first.Name,
                it->second.ChildId,
                it->second.ShardId,
                it->second.ShardNodeName,
                minCommitId,
                maxCommitId});

            bytes += sizeof(refs.back().NodeId)
                + refs.back().Name.size()
                + sizeof(refs.back().ChildNodeId)
                + refs.back().ShardId.size()
                + refs.back().ShardNodeName.size()
                + sizeof(refs.back().MinCommitId)
                + sizeof(refs.back().MaxCommitId);
        }

        ++it;

        if (maxBytes && bytes >= maxBytes) {
            break;
        }
    }

    if (next && it != NodeRefs.end() && it->first.NodeId == nodeId) {
        *next = it->first.Name;
    }

    return true;
}

bool TInMemoryIndexState::ReadNodeRefs(
    ui64 startNodeId,
    const TString& startCookie,
    ui64 maxCount,
    TVector<IIndexTabletDatabase::TNodeRef>& refs,
    ui64& nextNodeId,
    TString& nextCookie)
{
    Y_UNUSED(startNodeId, startCookie, maxCount, refs, nextNodeId, nextCookie);
    // This method is supposed to be called only upon tablet load in order to
    // populate the cache with data from localDb. Thus implementing in via
    // in-memory cache is unnecessary.
    return false;
}

bool TInMemoryIndexState::PrechargeNodeRefs(
    ui64 nodeId,
    const TString& cookie,
    ui32 bytesToPrecharge)
{
    Y_UNUSED(nodeId, cookie, bytesToPrecharge);
    return true;
}

void TInMemoryIndexState::WriteNodeRef(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    ui64 childNode,
    const TString& shardId,
    const TString& shardNodeName)
{
    const auto key = TNodeRefsKey(nodeId, name);
    auto it = NodeRefs.find(key);
    TNodeRefsRow value{
            .CommitId = commitId,
            .ChildId = childNode,
            .ShardId = shardId,
            .ShardNodeName = shardNodeName};
    
    if(it == NodeRefs.end()) {
        if(NodeRefs.size() == NodeRefs.capacity()) {
            IsNodeRefsEvictionObserved = true;
            IsNodeRefsExhaustive = false;           
        }
        NodeRefs.emplace(key, value);
    } else {
        it->second = value;
    }
}

void TInMemoryIndexState::DeleteNodeRef(ui64 nodeId, const TString& name)
{
    NodeRefs.erase(TNodeRefsKey(nodeId, name));
}

//
// NodeRefs_Ver
//

bool TInMemoryIndexState::ReadNodeRefVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    Y_UNUSED(nodeId, commitId, name, ref);
    // TODO(#1146): _Ver tables not supported yet
    return false;
}

bool TInMemoryIndexState::ReadNodeRefVers(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeRef>& refs)
{
    // TInMemoryIndexState is a preemptive cache, thus it is impossible to
    // determine, whether the set of stored references is complete.
    Y_UNUSED(nodeId, commitId, refs);
    return false;
}

//
// CheckpointNodes
//

bool TInMemoryIndexState::ReadCheckpointNodes(
    ui64 checkpointId,
    TVector<ui64>& nodes,
    size_t maxCount)
{
    // TInMemoryIndexState is a preemptive cache, thus it is impossible to
    // determine, whether the set of stored nodes is complete.
    Y_UNUSED(checkpointId, nodes, maxCount);
    return false;
}

//
// MixedIndex
//

bool TInMemoryIndexState::ReadMixedBlocks(
    ui32 rangeId,
    TVector<IIndexTabletDatabase::TMixedBlob>& blobs,
    IAllocator* alloc)
{
    Y_UNUSED(rangeId, blobs, alloc);
    return false;
}

bool TInMemoryIndexState::ReadDeletionMarkers(
    ui32 rangeId,
    TVector<TDeletionMarker>& deletionMarkers)
{
    Y_UNUSED(rangeId, deletionMarkers);
    return false;
}

////////////////////////////////////////////////////////////////////////////////

void TInMemoryIndexState::UpdateState(
    const TVector<TIndexStateRequest>& nodeUpdates)
{
    for (const auto& update: nodeUpdates) {
        if (const auto* request = std::get_if<TWriteNodeRequest>(&update)) {
            WriteNode(
                request->NodeId,
                request->Row.CommitId,
                request->Row.Node);
        } else if (
            const auto* request = std::get_if<TDeleteNodeRequest>(&update))
        {
            DeleteNode(request->NodeId);
        } else if (
            const auto* request = std::get_if<TWriteNodeAttrsRequest>(&update))
        {
            WriteNodeAttr(
                request->NodeAttrsKey.NodeId,
                request->NodeAttrsRow.CommitId,
                request->NodeAttrsKey.Name,
                request->NodeAttrsRow.Value,
                request->NodeAttrsRow.Version);
        } else if (
            const auto* request = std::get_if<TDeleteNodeAttrsRequest>(&update))
        {
            DeleteNodeAttr(request->NodeId, request->Name);
        } else if (
            const auto* request = std::get_if<TWriteNodeRefsRequest>(&update))
        {
            WriteNodeRef(
                request->NodeRefsKey.NodeId,
                request->NodeRefsRow.CommitId,
                request->NodeRefsKey.Name,
                request->NodeRefsRow.ChildId,
                request->NodeRefsRow.ShardId,
                request->NodeRefsRow.ShardNodeName);
        } else if (
            const auto* request = std::get_if<TDeleteNodeRefsRequest>(&update))
        {
            DeleteNodeRef(request->NodeId, request->Name);
        } else {
            Y_UNREACHABLE();
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
