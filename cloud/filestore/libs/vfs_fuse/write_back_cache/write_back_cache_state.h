#pragma once

#include "node_cache.h"
#include "node_state.h"
#include "node_state_holder.h"
#include "persistent_storage.h"
#include "queued_operations.h"
#include "sequence_id_generator.h"
#include "write_back_cache_stats.h"
#include "write_data_request.h"
#include "write_data_request_manager.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/deque.h>
#include <util/generic/function_ref.h>
#include <util/generic/hash_set.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

// The class is thread safe
class TWriteBackCacheState
{
private:
    const ISequenceIdGeneratorPtr SequenceIdGenerator;
    const ITimerPtr Timer;
    const IWriteBackCacheStatsPtr Stats;

    TNodeStateHolder Nodes;
    TDeque<TFlushRequest> FlushAllRequestQueue;

    // FlushAll request that isn't associated with a promise
    ui64 FlushAllSequenceId = 0;

    // Nodes that has unflushed data but are not scheduled for flush
    // Used for optimization: avoid checking flush condition for all nodes when
    // FlushAll is requested
    THashSet<ui64> NodesReadyToFlush;

    TWriteDataRequestManager RequestManager;
    TQueuedOperations QueuedOperations;

public:
    using TEntryVisitor = TFunctionRef<bool(const TCachedWriteDataRequest*)>;
    using TPin = ui64;

    TWriteBackCacheState(
        IQueuedOperationsProcessor& processor,
        ITimerPtr timer,
        IWriteBackCacheStatsPtr stats);

    // Read state from the persistent storage
    bool Init(IPersistentStoragePtr persistentStorage);

    bool HasUnflushedRequests() const;

    // Add a WriteData request to the pending queue and completes the future
    // when the request is stored in the persistent storage and becomes cached
    NThreading::TFuture<NProto::TWriteDataResponse> AddWriteDataRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    NThreading::TFuture<void> AddFlushRequest(ui64 nodeId);

    NThreading::TFuture<void> AddFlushAllRequest();

    void TriggerPeriodicFlushAll();

    // Includes both flushed and unflushed data
    TCachedData GetCachedData(ui64 nodeId, ui64 offset, ui64 byteCount) const;

    ui64 GetCachedNodeSize(ui64 nodeId) const;
    void SetCachedNodeSize(ui64 nodeId, ui64 size);

    // Prevent WriteData requests from being evicted from cache after flush
    TPin PinCachedData(ui64 nodeId);
    void UnpinCachedData(ui64 nodeId, TPin pinId);

    // Keep NodeStates alive
    // Used to prevent data race and return correct node size
    TPin PinNodeStates();
    void UnpinNodeStates(TPin pinId);

    // Visit unflushed cached requests in the increasing order of SequenceId
    void VisitUnflushedRequests(
        ui64 nodeId,
        const TEntryVisitor& visitor) const;

    // Inform that the first |requestCount| unflushed changes requests have
    // been flushed
    void FlushSucceeded(ui64 nodeId, size_t requestCount);

private:
    NThreading::TFuture<NProto::TWriteDataResponse> AddRequest(
        std::unique_ptr<TPendingWriteDataRequest> request);

    NThreading::TFuture<NProto::TWriteDataResponse> AddRequest(
        std::unique_ptr<TCachedWriteDataRequest> request);

    void TriggerFlushAll(bool includePendingRequests);

    ENodeFlushStatus GetFlushStatus(const TNodeState& nodeState) const;
    void UpdateFlushStatus(ui64 nodeId, TNodeState& nodeState);

    void EvictUnpinnedFlushedEntries(ui64 nodeId, TNodeState& nodeState);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
