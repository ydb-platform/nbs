#include "node_state_holder.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

TNodeStateHolder::TNodeStateHolder(IWriteBackCacheStatsPtr stats)
    : Stats(std::move(stats))
{}

TNodeState* TNodeStateHolder::GetNodeState(ui64 nodeId)
{
    TItem* nodeState = NodeStates.FindPtr(nodeId);
    return nodeState && nodeState->ErasureSequenceId == 0 ? nodeState : nullptr;
}

TNodeState& TNodeStateHolder::GetOrCreateNodeState(ui64 nodeId)
{
    auto [it, inserted] = NodeStates.try_emplace(nodeId, TNodeState(nodeId));
    if (inserted) {
        Stats->IncrementNodeCount();
    }

    TItem& nodeState = it->second;
    if (nodeState.ErasureSequenceId != 0) {
        Y_ABORT_UNLESS(
            ErasedNodeStates.erase(nodeState.ErasureSequenceId),
            "Node %lu with ErasureSequenceId %lu is not found in ErasedNodes",
            nodeId,
            nodeState.ErasureSequenceId);

        nodeState.ErasureSequenceId = 0;
    }
    return nodeState;
}

void TNodeStateHolder::EraseNodeState(ui64 nodeId)
{
    auto it = NodeStates.find(nodeId);
    Y_ABORT_UNLESS(it != NodeStates.end(), "Node %lu is not found in Nodes", nodeId);
    Y_ABORT_UNLESS(
        it->second.ErasureSequenceId == 0,
        "Node %lu is already erased with ErasureSequenceId %lu",
        nodeId,
        it->second.ErasureSequenceId);

    if (References.empty()) {
        NodeStates.erase(it);
        Stats->DecrementNodeCount();
        return;
    }

    const ui64 erasureSequenceId = ++SequenceId;
    it->second.ErasureSequenceId = erasureSequenceId;
    ErasedNodeStates[erasureSequenceId] = nodeId;
}

const TNodeState* TNodeStateHolder::GetPinnedNodeState(ui64 nodeId) const
{
    return NodeStates.FindPtr(nodeId);
}

TNodeState* TNodeStateHolder::GetPinnedNodeState(ui64 nodeId)
{
    return NodeStates.FindPtr(nodeId);
}

ui64 TNodeStateHolder::Ref()
{
    const ui64 sequenceId = ++SequenceId;
    References.insert(sequenceId);
    return sequenceId;
}

void TNodeStateHolder::Unref(ui64 refId)
{
    Y_ABORT_UNLESS(
        References.erase(refId),
        "Reference %lu is not found",
        refId);

    const ui64 minRefId =
        References.empty() ? Max<ui64>() : *References.begin();

    auto it = ErasedNodeStates.begin();
    while (it != ErasedNodeStates.end() && it->first < minRefId) {
        Y_ABORT_UNLESS(
            NodeStates.erase(it->second),
            "Node %lu with ErasureSequenceId %lu is not found in Nodes",
            it->second,
            it->first);

        it = ErasedNodeStates.erase(it);
        Stats->DecrementNodeCount();
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
