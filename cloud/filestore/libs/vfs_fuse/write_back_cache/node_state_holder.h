#pragma once

#include "node_state.h"
#include "write_back_cache_stats.h"

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

// TNodeStateHolder owns in-memory node state objects and supports a simple
// "logical delete" mechanism combined with pinning.
// Not thread-safe: the caller is responsible for synchronization.
class TNodeStateHolder
{
private:
    struct TItem: TNodeState
    {
        // Non-zero when the node state has been logically deleted
        ui64 DeletionSequenceId = 0;
    };

    const IWriteBackCacheStatsPtr Stats;

    // Monotonic sequence counter used to assign unique ids for:
    //  - deletion events (DeletionSequenceId)
    //  - pin tokens returned from Pin()
    // This counter is unrelated to ISequenceIdGenerator
    ui64 SequenceId = 0;

    // Holds both active and logically deleted node states
    THashMap<ui64, TItem> NodeStates;

    // DeletionSequenceId -> NodeId
    TMap<ui64, ui64> Deletions;

    // Set of active pin ids (the tokens returned by Pin()).
    // A pin with lower id prevents deletion of logically deleted states
    // with higher id.
    TSet<ui64> Pins;

public:
    explicit TNodeStateHolder(IWriteBackCacheStatsPtr stats);

    // Retrieve an existing node state or create a new empty one if it
    // does not exist. The returned reference is stored inside the holder
    // and remains valid until the state is erased from the holder.
    TNodeState& GetOrCreateNodeState(ui64 nodeId);

    // Return a pointer to the node state for nodeId or nullptr if it does not
    // exist.
    //
    // If includeDeleted is false (default), logically deleted states are
    // treated as absent and nullptr may be returned. If includeDeleted is
    // true, the function will return states that were deleted but are still
    // retained due to active pins.
    TNodeState* GetNodeState(ui64 nodeId, bool includeDeleted = false);

    const TNodeState* GetNodeState(
        ui64 nodeId,
        bool includeDeleted = false) const;

    // Logically delete the node state for nodeId.
    // The state is only physically removed when there are no pins with id lower
    // than DeletionSequenceId.
    void DeleteNodeState(ui64 nodeId);

    [[nodiscard]] ui64 Pin();
    void Unpin(ui64 pinId);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
