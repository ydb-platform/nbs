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
    ui64 nodesVerCapacity,
    ui64 nodeAttrsCapacity,
    ui64 nodeAttrsVerCapacity,
    ui64 nodeRefsCapacity,
    ui64 nodeRefsVerCapacity)
{
    NodesCapacity = nodesCapacity;
    NodesVerCapacity = nodesVerCapacity;
    NodeAttrsCapacity = nodeAttrsCapacity;
    NodeAttrsVerCapacity = nodeAttrsVerCapacity;
    NodeRefsCapacity = nodeRefsCapacity;
    NodeRefsVerCapacity = nodeRefsVerCapacity;
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
    return true;
}

//
// Nodes_Ver
//

bool TInMemoryIndexState::ReadNodeVer(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    auto it =
        NodesVer.lower_bound(TNodesVerKey(nodeId, ReverseCommitId(commitId)));
    if (it == NodesVer.end()) {
        return false;
    }

    while (it != NodesVer.end()) {
        if (it->first.NodeId != nodeId) {
            break;
        }

        ui64 minCommitId = ReverseCommitId(it->first.MinCommitId);
        ui64 maxCommitId = it->second.MaxCommitId;

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            node = TNode{nodeId, it->second.Node, minCommitId, maxCommitId};
            return true;
        }

        ++it;
    }
    // Unlike TIndexTabletDatabase, if the node has not been explicitly set,
    // there is no way to determine, whether the entry is present in the
    // original table.
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
        return true;
    }

    return false;
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

//
// NodeAttrs_Ver
//

bool TInMemoryIndexState::ReadNodeAttrVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    auto it = NodeAttrsVer.lower_bound(
        TNodeAttrsVerKey(nodeId, name, ReverseCommitId(commitId)));
    if (it == NodeAttrsVer.end()) {
        return false;
    }

    while (it != NodeAttrsVer.end()) {
        if (it->first.NodeId != nodeId || it->first.Name != name) {
            break;
        }

        ui64 minCommitId = ReverseCommitId(it->first.MinCommitId);
        ui64 maxCommitId = it->second.MaxCommitId;

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            attr = TNodeAttr{
                nodeId,
                name,
                it->second.Value,
                minCommitId,
                maxCommitId,
                it->second.Version};
            return true;
        }

        ++it;
    }

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
        return true;
    }

    return false;
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

//
// NodeRefs_Ver
//

bool TInMemoryIndexState::ReadNodeRefVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    auto it = NodeRefsVer.lower_bound(
        TNodeRefsVerKey(nodeId, name, ReverseCommitId(commitId)));
    if (it == NodeRefsVer.end()) {
        return false;
    }

    while (it != NodeRefsVer.end()) {
        if (it->first.NodeId != nodeId || it->first.Name != name) {
            break;
        }

        ui64 minCommitId = ReverseCommitId(it->first.MinCommitId);
        ui64 maxCommitId = it->second.MaxCommitId;

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            ref = TNodeRef{
                nodeId,
                name,
                it->second.ChildId,
                it->second.FollowerId,
                it->second.FollowerName,
                minCommitId,
                maxCommitId};
            return true;
        }

        ++it;
    }

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

}   // namespace NCloud::NFileStore::NStorage
