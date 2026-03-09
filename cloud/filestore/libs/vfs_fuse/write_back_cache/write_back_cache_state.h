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

enum class EFlushRetryStatus
{
    // Flusher should stop trying to flush data and wait for the next
    // IQueuedOperationProcessor::ScheduleFlushNode call.
    // This may happen when a client has requested to release all handles with
    // active requests and flush fails.
    ShouldNotRetry,

    // TNodeState::FlushStatus remains in ENodeFlushStatus::FlushRequested
    // state, the flusher should retry attempts to flush data
    ShouldRetry
};

////////////////////////////////////////////////////////////////////////////////

// The class is thread safe
class TWriteBackCacheState
{
private:
    const ISequenceIdGeneratorPtr SequenceIdGenerator;
    const ITimerPtr Timer;
    const IWriteBackCacheStatsPtr Stats;
    const TString LogTag;

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
        IWriteBackCacheStatsPtr stats,
        TString logTag);

    // Read state from the persistent storage
    bool Init(IPersistentStoragePtr persistentStorage);

    bool HasUnflushedRequests() const;

    // Add a WriteData request to the pending queue and completes the future
    // when the request is stored in the persistent storage and becomes cached
    NThreading::TFuture<NProto::TWriteDataResponse> AddWriteDataRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    NThreading::TFuture<NCloud::NProto::TError> AddFlushRequest(ui64 nodeId);

    NThreading::TFuture<NCloud::NProto::TError> AddFlushAllRequest();

    NThreading::TFuture<NCloud::NProto::TError> AddReleaseHandleRequest(
        ui64 nodeId,
        ui64 handle);

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

    // Inform that the flush has failed - the error should be propagated to
    // Flush, FlushAll and ReleaseHandle requests
    EFlushRetryStatus FlushFailed(
        ui64 nodeId,
        const NCloud::NProto::TError& error);

private:
    // Combines acquiring mutex and executing queued operations on mutex release
    TGuard<TQueuedOperations> LockStateAndPostponeQueuedOperations() const;

    NThreading::TFuture<NProto::TWriteDataResponse> AddRequest(
        std::unique_ptr<TPendingWriteDataRequest> request);

    NThreading::TFuture<NProto::TWriteDataResponse> AddRequest(
        std::unique_ptr<TCachedWriteDataRequest> request);

    void TriggerFlushAll(bool includePendingRequests);

    ENodeFlushStatus GetFlushStatus(const TNodeState& nodeState) const;
    void UpdateFlushStatus(ui64 nodeId, TNodeState& nodeState);

    void EvictUnpinnedFlushedEntries(ui64 nodeId, TNodeState& nodeState);

    void AddActiveRequestToHandleState(TNodeState& nodeState, ui64 handle);
    void RemoveActiveRequestFromHandleState(TNodeState& nodeState, ui64 handle);

    void DropCachedData(
        ui64 nodeId,
        TNodeState& nodeState,
        const NCloud::NProto::TError& error);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
