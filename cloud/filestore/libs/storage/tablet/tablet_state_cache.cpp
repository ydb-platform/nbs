#include "tablet_state_cache.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TInMemoryIndexState::TInMemoryIndexState(IAllocator* allocator)
{
    Y_UNUSED(allocator);
}

void TInMemoryIndexState::Reset(
    ui64 nodesCapacity,
    ui64 nodeAttrsCapacity,
    ui64 nodeRefsCapacity)
{
    NodesCapacity = nodesCapacity;
    NodeAttrsCapacity = nodeAttrsCapacity;
    NodeRefsCapacity = nodeRefsCapacity;
}

//
// Nodes
//

bool TInMemoryIndexState::ReadNode(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    auto it = Nodes.find(nodeId);
    if (it == Nodes.end()) {
        return false;
    }

    ui64 minCommitId = it->second.CommitId;
    ui64 maxCommitId = InvalidCommitId;

    if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
        node = TNode{nodeId, it->second.Node, minCommitId, maxCommitId};
    }

    // We found the entry in table. There is at most one entry matching the key,
    // meaning that cache lookup was successful, independent of whether the
    // entry is visible or not to the given commitId.
    return true;
}

void TInMemoryIndexState::WriteNode(
    ui64 nodeId,
    ui64 commitId,
    const NProto::TNode& attrs)
{
    if (Nodes.size() == NodesCapacity && !Nodes.contains(nodeId)) {
        Nodes.clear();
    }
    Nodes[nodeId] = TNodeRow{.CommitId = commitId, .Node = attrs};
}

void TInMemoryIndexState::DeleteNode(ui64 nodeId)
{
    Nodes.erase(nodeId);
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
    auto it = NodeAttrs.find(TNodeAttrsKey(nodeId, name));
    if (it == NodeAttrs.end()) {
        return false;
    }

    ui64 minCommitId = it->second.CommitId;
    ui64 maxCommitId = InvalidCommitId;

    if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
        attr = TNodeAttr{
            nodeId,
            name,
            it->second.Value,
            minCommitId,
            maxCommitId,
            it->second.Version};
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
    if (NodeAttrs.size() == NodeAttrsCapacity && !NodeAttrs.contains(key)) {
        NodeAttrs.clear();
    }
    NodeAttrs[key] =
        TNodeAttrsRow{.CommitId = commitId, .Value = value, .Version = version};
}

void TInMemoryIndexState::DeleteNodeAttr(ui64 nodeId, const TString& name)
{
    NodeAttrs.erase(TNodeAttrsKey(nodeId, name));
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
            it->second.FollowerId,
            it->second.FollowerName,
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
    // TInMemoryIndexState is a preemptive cache, thus it is impossible to
    // determine, whether the set of stored references is complete.
    Y_UNUSED(nodeId, commitId, cookie, refs, maxBytes, next);
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
    const TString& followerId,
    const TString& followerName)
{
    const auto key = TNodeRefsKey(nodeId, name);
    if (NodeRefs.size() == NodeRefsCapacity && !NodeRefs.contains(key)) {
        NodeRefs.clear();
    }
    NodeRefs[key] = TNodeRefsRow{
        .CommitId = commitId,
        .ChildId = childNode,
        .FollowerId = followerId,
        .FollowerName = followerName};
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
                request->NodeRefsRow.FollowerId,
                request->NodeRefsRow.FollowerName);
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
