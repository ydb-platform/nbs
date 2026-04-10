#include "node_state_holder.h"

#include <cloud/storage/core/libs/common/timer.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

TNodeStateHolder::TNodeStateHolder(
    ITimerPtr timer,
    INodeStateHolderStatsPtr stats)
    : Timer(std::move(timer))
    , Stats(std::move(stats))
{}

TNodeState& TNodeStateHolder::GetOrCreateNodeState(ui64 nodeId)
{
    auto [it, inserted] = NodeStates.try_emplace(nodeId);
    if (inserted) {
        Stats->IncrementNodeCount();
    }

    TItem& nodeState = it->second;
    if (nodeState.DeletionSequenceId != 0) {
        // Resurrect previously deleted node state
        auto erased = Deletions.erase(nodeState.DeletionSequenceId);
        Y_ABORT_UNLESS(
            erased,
            "Node %lu with DeletionSequenceId %lu is not found in Deletions",
            nodeId,
            nodeState.DeletionSequenceId);

        nodeState.DeletionSequenceId = 0;
        Stats->DecrementDeletedNodeCount();
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
    Stats->IncrementDeletedNodeCount();
}

ui64 TNodeStateHolder::Pin()
{
    const ui64 pinId = ++SequenceId;
    Pins[pinId] = Timer->Now();
    Stats->Pinned();
    return pinId;
}

void TNodeStateHolder::Unpin(ui64 pinId)
{
    auto pinIt = Pins.find(pinId);
    Y_ABORT_UNLESS(pinIt != Pins.end(), "Pin %lu is not found", pinId);
    Stats->Unpinned(Timer->Now() - pinIt->second);
    Pins.erase(pinIt);

    // Erase deleted nodes that are no more pinned
    const ui64 minPinId = Pins.empty() ? Max<ui64>() : Pins.begin()->first;

    auto it = Deletions.begin();
    while (it != Deletions.end() && it->first < minPinId) {
        auto erased = NodeStates.erase(it->second);
        Y_ABORT_UNLESS(
            erased,
            "Node %lu with DeletionSequenceId %lu is not found in NodeStates",
            it->second,
            it->first);

        it = Deletions.erase(it);
        Stats->DecrementNodeCount();
        Stats->DecrementDeletedNodeCount();
    }
}

void TNodeStateHolder::UpdateStats() const
{
    const auto maxActivePinDuration =
        Pins.empty() ? TDuration::Zero() : Timer->Now() - Pins.begin()->second;

    Stats->UpdateStats(maxActivePinDuration);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
