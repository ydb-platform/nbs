#pragma once

#include "node_state.h"
#include "write_back_cache_stats.h"

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

// The class is not thread safe
class TNodeStateHolder
{
private:
    struct TItem: TNodeState
    {
        // Non-zero value indicates that the node is erased
        ui64 ErasureSequenceId = 0;
    };

    const IWriteBackCacheStatsPtr Stats;

    // This counter is independent from SequenceId associated with requests
    ui64 SequenceId = 0;
    THashMap<ui64, TItem> NodeStates;

    // Map ErasureSequenceId -> NodeId
    TMap<ui64, ui64> ErasedNodeStates;
    TSet<ui64> References;

public:
    explicit TNodeStateHolder(IWriteBackCacheStatsPtr stats);

    TNodeState* GetNodeState(ui64 nodeId);
    TNodeState& GetOrCreateNodeState(ui64 nodeId);
    void EraseNodeState(ui64 nodeId);

    // Access the erased node state if it is referenced
    TNodeState* GetPinnedNodeState(ui64 nodeId);
    const TNodeState* GetPinnedNodeState(ui64 nodeId) const;

    // Prevent states that was erase after Ref() from being completely removed
    [[nodiscard]] ui64 Ref();
    void Unref(ui64 refId);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
