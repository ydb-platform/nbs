#include "node_state_holder.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

TNodeStateHolder::TNodeStateHolder(IWriteBackCacheStatsPtr stats)
    : Stats(std::move(stats))
{}

TNodeState& TNodeStateHolder::GetOrCreateNodeState(ui64 nodeId)
{
    auto [it, inserted] = NodeStates.try_emplace(nodeId, TNodeState{});
    if (inserted) {
        Stats->IncrementNodeCount();
    }

    TItem& nodeState = it->second;
    if (nodeState.DeletionSequenceId != 0) {
        Y_ABORT_UNLESS(
            Deletions.erase(nodeState.DeletionSequenceId),
            "Node %lu with DeletionSequenceId %lu is not found in Deletions",
            nodeId,
            nodeState.DeletionSequenceId);

        nodeState.DeletionSequenceId = 0;
    }
    return nodeState;
}

TNodeState* TNodeStateHolder::GetNodeState(ui64 nodeId, bool includeDeleted)
{
    TItem* nodeState = NodeStates.FindPtr(nodeId);
    return nodeState && (nodeState->DeletionSequenceId == 0 || includeDeleted)
               ? nodeState
               : nullptr;
}

const TNodeState* TNodeStateHolder::GetNodeState(
    ui64 nodeId,
    bool includeDeleted) const
{
    const TItem* nodeState = NodeStates.FindPtr(nodeId);
    return nodeState && (nodeState->DeletionSequenceId == 0 || includeDeleted)
               ? nodeState
               : nullptr;
}

void TNodeStateHolder::DeleteNodeState(ui64 nodeId)
{
    auto it = NodeStates.find(nodeId);

    Y_ABORT_UNLESS(
        it != NodeStates.end(),
        "Node %lu is not found in NodeStates",
        nodeId);

    Y_ABORT_UNLESS(
        it->second.DeletionSequenceId == 0,
        "Node %lu is already deleted with DeletionSequenceId %lu",
        nodeId,
        it->second.DeletionSequenceId);

    if (Pins.empty()) {
        NodeStates.erase(it);
        Stats->DecrementNodeCount();
        return;
    }

    const ui64 deletionSequenceId = ++SequenceId;
    it->second.DeletionSequenceId = deletionSequenceId;
    Deletions[deletionSequenceId] = nodeId;
}

ui64 TNodeStateHolder::Pin()
{
    const ui64 pinId = ++SequenceId;
    Pins.insert(pinId);
    return pinId;
}

void TNodeStateHolder::Unpin(ui64 pinId)
{
    Y_ABORT_UNLESS(Pins.erase(pinId), "Pin %lu is not found", pinId);

    const ui64 minPinId = Pins.empty() ? Max<ui64>() : *Pins.begin();

    auto it = Deletions.begin();
    while (it != Deletions.end() && it->first < minPinId) {
        Y_ABORT_UNLESS(
            NodeStates.erase(it->second),
            "Node %lu with DeletionSequenceId %lu is not found in NodeStates",
            it->second,
            it->first);

        it = Deletions.erase(it);
        Stats->DecrementNodeCount();
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
