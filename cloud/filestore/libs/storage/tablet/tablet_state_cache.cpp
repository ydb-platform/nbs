#include "tablet_state_cache.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename TNodeRefsImpl>
TInMemoryIndexState<TNodeRefsImpl>::TInMemoryIndexState(
    IAllocator* allocator,
    const TCacheReadBypass& cacheReadBypass,
    ui64 nodesCapacity,
    ui64 nodeAttrsCapacity,
    ui64 nodeRefsCapacity,
    ui64 nodeRefsExhaustivenessCapacity)
    : CacheReadBypass(cacheReadBypass)
    , Nodes(nodesCapacity)
    , NodeAttrs(nodeAttrsCapacity)
    , NodeRefs(allocator, nodeRefsCapacity)
    , NodeRefsExhaustivenessInfo(nodeRefsExhaustivenessCapacity)
{}

template <typename TNodeRefsImpl>
void TInMemoryIndexState<TNodeRefsImpl>::LoadNodeRefs(
    const TVector<TNodeRef>& nodeRefs)
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

template <typename TNodeRefsImpl>
void TInMemoryIndexState<TNodeRefsImpl>::MarkNodeRefsLoadComplete()
{
    NodeRefsExhaustivenessInfo.MarkNodeRefsLoadComplete();
}

template <typename TNodeRefsImpl>
void TInMemoryIndexState<TNodeRefsImpl>::MarkNodeRefsExhaustive(ui64 nodeId)
{
    NodeRefsExhaustivenessInfo.MarkNodeRefsExhaustive(nodeId);
}

template <typename TNodeRefsImpl>
void TInMemoryIndexState<TNodeRefsImpl>::UpdateLogTag(TString logTag)
{
    LogTag = std::move(logTag);
}

template <typename TNodeRefsImpl>
TInMemoryIndexStateStats TInMemoryIndexState<TNodeRefsImpl>::GetStats() const
{
    return TInMemoryIndexStateStats{
        .NodesCount = Nodes.Size(),
        .NodesCapacity = Nodes.GetMaxSize(),
        .NodeRefsCount = NodeRefs.Size(),
        .NodeRefsCapacity = NodeRefs.GetMaxSize(),
        .NodeAttrsCount = NodeAttrs.Size(),
        .NodeAttrsCapacity = NodeAttrs.GetMaxSize(),
        .NodeRefsExhaustivenessCapacity =
            NodeRefsExhaustivenessInfo.GetMaxSize(),
        .NodeRefsExhaustivenessCount = NodeRefsExhaustivenessInfo.GetSize(),
        .IsNodeRefsExhaustive = NodeRefsExhaustivenessInfo.IsExhaustive()};
}

//
// Nodes
//

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNode(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    // TODO (#5912) use waiting queue instead of going full TX flow
    if (CacheReadBypass.ShouldBypassRead(nodeId, commitId)) {
        return false;
    }

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

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNodes(
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

template <typename TNodeRefsImpl>
void TInMemoryIndexState<TNodeRefsImpl>::WriteNode(
    ui64 nodeId,
    ui64 commitId,
    const NProto::TNode& attrs)
{
    Nodes.Update(nodeId, TNodeRow{.CommitId = commitId, .Node = attrs});
}

template <typename TNodeRefsImpl>
void TInMemoryIndexState<TNodeRefsImpl>::DeleteNode(ui64 nodeId)
{
    auto it = Nodes.Find(nodeId);
    if (it != Nodes.End()) {
        Nodes.Erase(it);
    }
}

//
// Nodes_Ver
//

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNodeVer(
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

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    // TODO (#5912) use waiting queue instead of going full TX flow
    if (CacheReadBypass.ShouldBypassRead(nodeId, commitId)) {
        return false;
    }

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

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNodeAttrs(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeAttr>& attrs)
{
    // TInMemoryIndexState is a preemptive cache, thus it is impossible to
    // determine, whether the set of stored attributes is complete.
    Y_UNUSED(nodeId, commitId, attrs);
    return false;
}

template <typename TNodeRefsImpl>
void TInMemoryIndexState<TNodeRefsImpl>::WriteNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    const TString& value,
    ui64 version)
{
    const auto key = TNodeAttrsKey(nodeId, name);
    NodeAttrs.Update(key, TNodeAttrsRow{.CommitId = commitId, .Value = value, .Version = version});
}

template <typename TNodeRefsImpl>
void TInMemoryIndexState<TNodeRefsImpl>::DeleteNodeAttr(
    ui64 nodeId,
    const TString& name)
{
    auto it = NodeAttrs.Find(TNodeAttrsKey(nodeId, name));
    if (it != NodeAttrs.End()) {
        NodeAttrs.Erase(it);
    }
}

//
// NodeAttrs_Ver
//

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNodeAttrVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    Y_UNUSED(nodeId, commitId, name, attr);
    // TODO(#1146): _Ver tables not supported yet
    return false;
}

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNodeAttrVers(
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

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNodeRef(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    auto* v = NodeRefs.Find(TNodeRefsKey(nodeId, name));
    if (!v) {
        // If the cache is exhaustive for the node and we did not find the
        // entry, then we are sure that the entry does not exist and we can
        // return true, meaning that cache lookup was successful. But we do not
        // set the ref, meaning that the entry does not exist.
        return NodeRefsExhaustivenessInfo.IsExhaustiveForNode(nodeId);
    }

    ui64 minCommitId = v->CommitId;
    ui64 maxCommitId = InvalidCommitId;

    if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
        ref = TNodeRef{
            nodeId,
            name,
            v->ChildId,
            v->ShardId,
            v->ShardNodeName,
            minCommitId,
            maxCommitId};
    }

    // We found the entry in table. There is at most one entry matching the key,
    // meaning that cache lookup was successful, independent of whether the
    // entry is visible or not to the given commitId.
    return true;
}

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNodeRefs(
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
    Y_UNUSED(noAutoPrecharge);  // Not applicable to in-memory cache
    if (!NodeRefsExhaustivenessInfo.IsExhaustiveForNode(nodeId)) {
        return false;
    }

    auto it = NodeRefs.LowerBound(TNodeRefsKey(nodeId, cookie));

    ui32 bytes = 0;
    ui32 skipped = 0;
    const TNodeRefsKey* key = nullptr;
    const TNodeRefsRow* value = nullptr;
    while (it.Get(&key, &value) && key->NodeId == nodeId) {
        NodeRefs.TouchKey(*key);

        ui64 minCommitId = value->CommitId;
        ui64 maxCommitId = InvalidCommitId;

        if (VisibleCommitId(commitId, minCommitId, maxCommitId)) {
            refs.emplace_back(TNodeRef{
                nodeId,
                key->Name,
                value->ChildId,
                value->ShardId,
                value->ShardNodeName,
                minCommitId,
                maxCommitId});

            const auto& ref = refs.back();
            // TODO(#5148): consider other size calculation modes
            if (sizeMode == NProto::LNSM_FULL_ROW) {
                bytes += ref.CalculateByteSize();
            } else {
                bytes += ref.Name.size();
            }
        } else {
            ++skipped;
        }

        ++it;

        if (maxBytes && bytes >= maxBytes) {
            break;
        }
    }

    if (next && it.Get(&key, &value) && key->NodeId == nodeId) {
        *next = key->Name;
    }

    if (skippedRefs) {
        *skippedRefs = skipped;
    }

    return true;
}

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNodeRefs(
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

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::PrechargeNodeRefs(
    ui64 nodeId,
    const TString& cookie,
    ui64 rowsToPrecharge,
    ui64 bytesToPrecharge)
{
    Y_UNUSED(nodeId, cookie, rowsToPrecharge, bytesToPrecharge);
    return true;
}

template <typename TNodeRefsImpl>
void TInMemoryIndexState<TNodeRefsImpl>::WriteNodeRef(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    ui64 childNode,
    const TString& shardId,
    const TString& shardNodeName)
{
    const auto key = TNodeRefsKey(nodeId, name);
    auto* v = NodeRefs.Find(key);
    TNodeRefsRow value{
        .CommitId = commitId,
        .ChildId = childNode,
        .ShardId = shardId,
        .ShardNodeName = shardNodeName};

    if (!v) {
        const auto evicted = NodeRefs.Put(key, std::move(value));
        if (evicted) {
            NodeRefsExhaustivenessInfo.NodeRefsEvictionObserved(
                evicted->NodeId);
        }
    } else {
        *v = std::move(value);
    }
}

template <typename TNodeRefsImpl>
void TInMemoryIndexState<TNodeRefsImpl>::DeleteNodeRef(
    ui64 nodeId,
    const TString& name)
{
    NodeRefs.Erase(TNodeRefsKey(nodeId, name));
}

//
// NodeRefs_Ver
//

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNodeRefVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    Y_UNUSED(nodeId, commitId, name, ref);
    // TODO(#1146): _Ver tables not supported yet
    return false;
}

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadNodeRefVers(
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

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadCheckpointNodes(
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

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadMixedBlocks(
    ui32 rangeId,
    TVector<IIndexTabletDatabase::TMixedBlob>& blobs,
    IAllocator* alloc)
{
    Y_UNUSED(rangeId, blobs, alloc);
    return false;
}

template <typename TNodeRefsImpl>
bool TInMemoryIndexState<TNodeRefsImpl>::ReadDeletionMarkers(
    ui32 rangeId,
    TVector<TDeletionMarker>& deletionMarkers)
{
    Y_UNUSED(rangeId, deletionMarkers);
    return false;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TNodeRefsImpl>
void TInMemoryIndexState<TNodeRefsImpl>::UpdateState(
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
        } else if (
            const auto* request =
                std::get_if<TMarkNodeRefsAsCachedRequest>(&update))
        {
            if (NodeRefs.Size() >= request->RefsSize) {
                NodeRefsExhaustivenessInfo.MarkNodeRefsExhaustive(
                    request->NodeId);
            }
        } else {
            Y_UNREACHABLE();
        }
    }
}

template class TInMemoryIndexState<TStandardNodeRefsCache>;
template class TInMemoryIndexState<TUnlimitedBTreeNodeRefsCache>;

}   // namespace NCloud::NFileStore::NStorage
