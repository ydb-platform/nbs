#include "write_back_cache_state.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NCloud::NFileStore::NProto;
using namespace NCloud::NProto;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TWriteBackCacheState::TWriteBackCacheState(
    IQueuedOperationsProcessor& processor,
    ITimerPtr timer,
    IWriteBackCacheStatsPtr stats,
    TString logTag)
    : SequenceIdGenerator(std::make_shared<TSequenceIdGenerator>())
    , Timer(std::move(timer))
    , Stats(std::move(stats))
    , LogTag(std::move(logTag))
    , Nodes(Stats)
    , QueuedOperations(processor)
{}

bool TWriteBackCacheState::Init(IPersistentStoragePtr persistentStorage)
{
    RequestManager = TWriteDataRequestManager(
        SequenceIdGenerator,
        std::move(persistentStorage),
        Timer,
        Stats);

    return RequestManager.Init(
        [this](std::unique_ptr<TCachedWriteDataRequest> request)
        { AddRequest(std::move(request)); });
}

bool TWriteBackCacheState::HasUnflushedRequests() const
{
    auto guard = LockStateAndPostponeQueuedOperations();

    return RequestManager.HasPendingOrUnflushedRequests();
}

TFuture<TWriteDataResponse> TWriteBackCacheState::AddWriteDataRequest(
    std::shared_ptr<TWriteDataRequest> request)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto variant = RequestManager.AddRequest(std::move(request));

    return std::visit(
        [this](auto res) { return AddRequest(std::move(res)); },
        std::move(variant));
}

TFuture<TError> TWriteBackCacheState::AddFlushRequest(ui64 nodeId)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto* nodeState = Nodes.GetNodeState(nodeId, /* includeDeleted = */ false);
    if (nodeState == nullptr ||
        !nodeState->Cache.HasPendingOrUnflushedRequests())
    {
        return MakeFuture<TError>();
    }

    auto future =
        nodeState->FlushRequests.emplace_back(SequenceIdGenerator->GenerateId())
            .Promise.GetFuture();

    UpdateFlushStatus(nodeId, *nodeState);

    return future;
}

TFuture<TError> TWriteBackCacheState::AddFlushAllRequest()
{
    auto guard = LockStateAndPostponeQueuedOperations();

    if (!RequestManager.HasPendingOrUnflushedRequests()) {
        return MakeFuture<TError>();
    }

    TriggerFlushAll(true);

    return FlushAllRequestQueue.emplace_back(FlushAllSequenceId)
        .Promise.GetFuture();
}

TFuture<TError> TWriteBackCacheState::AddReleaseHandleRequest(
    ui64 nodeId,
    ui64 handle)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto* nodeState = Nodes.GetNodeState(nodeId, /* includeDeleted = */ false);
    if (nodeState == nullptr) {
        return MakeFuture<TError>();
    }

    auto* handleState = nodeState->Handles.FindPtr(handle);
    if (handleState == nullptr) {
        return MakeFuture<TError>();
    }

    if (handleState->ReadyToReleasePromise.Initialized()) {
        return handleState->ReadyToReleasePromise.GetFuture();
    }

    handleState->ReadyToReleasePromise = NewPromise<TError>();
    nodeState->HandleToReleaseCount++;

    UpdateFlushStatus(nodeId, *nodeState);

    return handleState->ReadyToReleasePromise.GetFuture();
}

void TWriteBackCacheState::TriggerPeriodicFlushAll()
{
    auto guard = LockStateAndPostponeQueuedOperations();

    TriggerFlushAll(false);
}

TCachedData TWriteBackCacheState::GetCachedData(
    ui64 nodeId,
    ui64 offset,
    ui64 byteCount) const
{
    auto guard = LockStateAndPostponeQueuedOperations();

    const auto* nodeState = Nodes.GetNodeState(nodeId);
    if (nodeState == nullptr) {
        return {};
    }

    return nodeState->Cache.GetCachedData(offset, byteCount);
}

ui64 TWriteBackCacheState::GetCachedNodeSize(ui64 nodeId) const
{
    auto guard = LockStateAndPostponeQueuedOperations();

    const auto* nodeState =
        Nodes.GetNodeState(nodeId, /* includeDeleted = */ true);

    return nodeState ? nodeState->CachedNodeSize : 0;
}

void TWriteBackCacheState::SetCachedNodeSize(ui64 nodeId, ui64 size)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto* nodeState = Nodes.GetNodeState(nodeId, true);
    if (nodeState) {
        nodeState->CachedNodeSize = size;
    }
}

TWriteBackCacheState::TPin TWriteBackCacheState::PinCachedData(ui64 nodeId)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);

    // Setting a pin allows evicting from cache only those WriteData requests
    // that are flushed by the moment. Other requests will not be evicted
    // until the pin is removed.
    const ui64 allowedToEvictMaxSequenceId =
        nodeState.Cache.HasFlushedRequests()
            ? nodeState.Cache.GetMaxFlushedSequenceId()
            : 0;

    nodeState.CachedDataPins.insert(allowedToEvictMaxSequenceId);

    return allowedToEvictMaxSequenceId;
}

void TWriteBackCacheState::UnpinCachedData(ui64 nodeId, TPin pinId)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto* nodeState = Nodes.GetNodeState(nodeId, /* includeDeleted= */ false);
    Y_ABORT_UNLESS(nodeState, "Node %lu not found", nodeId);

    auto it = nodeState->CachedDataPins.find(pinId);
    Y_ABORT_UNLESS(
        it != nodeState->CachedDataPins.end(),
        "Pin %lu not found for node %lu",
        pinId,
        nodeId);

    nodeState->CachedDataPins.erase(it);

    EvictUnpinnedFlushedEntries(nodeId, *nodeState);
}

TWriteBackCacheState::TPin TWriteBackCacheState::PinNodeStates()
{
    auto guard = LockStateAndPostponeQueuedOperations();

    return Nodes.Pin();
}

void TWriteBackCacheState::UnpinNodeStates(TPin pinId)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    Nodes.Unpin(pinId);
}

void TWriteBackCacheState::VisitUnflushedRequests(
    ui64 nodeId,
    const TEntryVisitor& visitor) const
{
    auto guard = LockStateAndPostponeQueuedOperations();

    const auto* nodeState = Nodes.GetNodeState(nodeId);
    if (nodeState == nullptr) {
        return;
    }

    nodeState->Cache.VisitUnflushedRequests(visitor);
}

void TWriteBackCacheState::FlushSucceeded(ui64 nodeId, size_t requestCount)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);

    Y_ABORT_UNLESS(
        nodeState.FlushStatus == ENodeFlushStatus::FlushRequested,
        "Flush wasn't requested for a node %lu",
        nodeId);

    // We will recalculate the flush status later
    nodeState.FlushStatus = ENodeFlushStatus::NothingToFlush;

    for (size_t i = 0; i < requestCount; i++) {
        Y_ABORT_UNLESS(nodeState.Cache.HasUnflushedRequests());
        auto* cachedRequest =
            nodeState.Cache.MoveFrontUnflushedRequestToFlushed();
        RequestManager.SetFlushed(cachedRequest);
        RemoveActiveRequestFromHandleState(
            nodeState,
            cachedRequest->GetHandle());
    }

    // Trigger Flush completions
    const ui64 sequenceId =
        nodeState.Cache.HasPendingOrUnflushedRequests()
            ? nodeState.Cache.GetMinPendingOrUnflushedSequenceId()
            : Max<ui64>();

    while (!nodeState.FlushRequests.empty() &&
           nodeState.FlushRequests.front().SequenceId < sequenceId)
    {
        QueuedOperations.CompleteFlushOrReleasePromise(
            std::move(nodeState.FlushRequests.front().Promise));
        nodeState.FlushRequests.pop_front();
    }

    // Trigger FlushAll completions
    const ui64 globalSequenceId =
        RequestManager.GetMinPendingOrUnflushedSequenceId();

    while (!FlushAllRequestQueue.empty() &&
           FlushAllRequestQueue.front().SequenceId < globalSequenceId)
    {
        QueuedOperations.CompleteFlushOrReleasePromise(
            std::move(FlushAllRequestQueue.front().Promise));
        FlushAllRequestQueue.pop_front();
    }

    UpdateFlushStatus(nodeId, nodeState);
    EvictUnpinnedFlushedEntries(nodeId, nodeState);
}

EFlushRetryStatus TWriteBackCacheState::FlushFailed(
    ui64 nodeId,
    const TError& error)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);

    Y_ABORT_UNLESS(
        nodeState.FlushStatus == ENodeFlushStatus::FlushRequested,
        "Flush wasn't requested for node %lu",
        nodeId);

    // Fail Flush and FlushAll requests
    for (auto& it: nodeState.FlushRequests) {
        QueuedOperations.FailFlushOrReleasePromise(
            std::move(it.Promise),
            error);
    }

    for (auto& it: FlushAllRequestQueue) {
        QueuedOperations.FailFlushOrReleasePromise(
            std::move(it.Promise),
            error);
    }

    nodeState.FlushRequests.clear();
    FlushAllRequestQueue.clear();

    if (nodeState.Handles.size() == nodeState.HandleToReleaseCount) {
        // All handles with active WriteData requests are to be released
        // Drop node data on flush failure
        nodeState.FlushStatus = ENodeFlushStatus::NothingToFlush;
        DropCachedData(nodeId, nodeState, error);
        return EFlushRetryStatus::ShouldNotRetry;
    }

    // Keep status ENodeFlushStatus::FlushRequested if flush is retried
    return EFlushRetryStatus::ShouldRetry;
}

TGuard<TQueuedOperations>
TWriteBackCacheState::LockStateAndPostponeQueuedOperations() const
{
    return Guard(QueuedOperations);
}

TFuture<TWriteDataResponse> TWriteBackCacheState::AddRequest(
    std::unique_ptr<TPendingWriteDataRequest> request)
{
    auto future = request->AccessPromise().GetFuture();
    TriggerFlushAll(false);

    auto& nodeState =
        Nodes.GetOrCreateNodeState(request->GetRequest().GetNodeId());

    AddActiveRequestToHandleState(nodeState, request->GetRequest().GetHandle());
    nodeState.Cache.EnqueuePendingRequest(std::move(request));

    return future;
}

TFuture<TWriteDataResponse> TWriteBackCacheState::AddRequest(
    std::unique_ptr<TCachedWriteDataRequest> request)
{
    const ui64 nodeId = request->GetNodeId();

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);
    AddActiveRequestToHandleState(nodeState, request->GetHandle());
    nodeState.CachedNodeSize = Max(nodeState.CachedNodeSize, request->GetEnd());
    nodeState.Cache.EnqueueUnflushedRequest(std::move(request));

    UpdateFlushStatus(nodeId, nodeState);

    return MakeFuture<TWriteDataResponse>();
}

void TWriteBackCacheState::TriggerFlushAll(bool includePendingRequests)
{
    if (includePendingRequests) {
        FlushAllSequenceId = SequenceIdGenerator->GenerateId();
    } else {
        FlushAllSequenceId =
            Max(FlushAllSequenceId, RequestManager.GetMaxUnflushedSequenceId());
    }

    for (auto nodeId: NodesReadyToFlush) {
        auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);
        nodeState.FlushStatus = ENodeFlushStatus::FlushRequested;
        QueuedOperations.ScheduleFlushNode(nodeId);
    }

    NodesReadyToFlush.clear();
}

void TWriteBackCacheState::UpdateFlushStatus(ui64 nodeId, TNodeState& nodeState)
{
    auto newFlushStatus = nodeState.GetExpectedFlushStatus(FlushAllSequenceId);
    if (nodeState.FlushStatus == newFlushStatus) {
        return;
    }

    // Process previous state
    switch (nodeState.FlushStatus) {
        case ENodeFlushStatus::NothingToFlush:
        case ENodeFlushStatus::FlushRequested:
            break;

        case ENodeFlushStatus::ReadyToFlush:
            auto erased = NodesReadyToFlush.erase(nodeId);
            Y_ABORT_UNLESS(erased);
            break;
    }

    nodeState.FlushStatus = newFlushStatus;

    // Process new state
    switch (nodeState.FlushStatus) {
        case ENodeFlushStatus::NothingToFlush:
            break;

        case ENodeFlushStatus::FlushRequested:
            QueuedOperations.ScheduleFlushNode(nodeId);
            break;

        case ENodeFlushStatus::ReadyToFlush:
            auto inserted = NodesReadyToFlush.insert(nodeId).second;
            Y_ABORT_UNLESS(inserted);
            break;
    }
}

// nodeState becomes unusable after this call
void TWriteBackCacheState::EvictUnpinnedFlushedEntries(
    ui64 nodeId,
    TNodeState& nodeState)
{
    bool entriesDeleted = false;

    const ui64 allowedToEvictMaxSequenceId =
        nodeState.CachedDataPins.empty() ? Max<ui64>()
                                         : *nodeState.CachedDataPins.begin();

    while (nodeState.Cache.HasFlushedRequests()) {
        const ui64 sequenceId = nodeState.Cache.GetMinFlushedSequenceId();
        if (sequenceId > allowedToEvictMaxSequenceId) {
            break;
        }
        auto cachedRequest = nodeState.Cache.DequeueFlushedRequest();
        RequestManager.Evict(std::move(cachedRequest));
        entriesDeleted = true;
    }

    if (nodeState.CanBeDeleted()) {
        Nodes.DeleteNodeState(nodeId);
    }

    if (!entriesDeleted) {
        return;
    }

    while (RequestManager.HasPendingRequests()) {
        auto request = RequestManager.TryProcessPendingRequest();
        if (!request) {
            TriggerFlushAll(false);
            break;
        }

        const ui64 nodeId = request->GetNodeId();
        auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);

        Y_ABORT_UNLESS(nodeState.Cache.HasPendingRequests());
        auto pendingRequest = nodeState.Cache.DequeuePendingRequest();

        Y_ABORT_UNLESS(
            pendingRequest->GetSequenceId() == request->GetSequenceId());

        QueuedOperations.CompleteWriteDataPromise(
            std::move(pendingRequest->AccessPromise()));

        nodeState.Cache.EnqueueUnflushedRequest(std::move(request));

        UpdateFlushStatus(nodeId, nodeState);
    }
}

void TWriteBackCacheState::AddActiveRequestToHandleState(
    TNodeState& nodeState,
    ui64 handle)
{
    auto& handleState = nodeState.Handles[handle];
    handleState.ActiveWriteDataRequestCount++;
}

void TWriteBackCacheState::RemoveActiveRequestFromHandleState(
    TNodeState& nodeState,
    ui64 handle)
{
    auto& handleState = nodeState.Handles[handle];
    Y_ABORT_UNLESS(handleState.ActiveWriteDataRequestCount > 0);

    if (--handleState.ActiveWriteDataRequestCount == 0) {
        if (handleState.ReadyToReleasePromise.Initialized()) {
            // The promise is initialized when ReleaseHandle was requested
            Y_ABORT_UNLESS(nodeState.HandleToReleaseCount > 0);
            nodeState.HandleToReleaseCount--;
            QueuedOperations.CompleteFlushOrReleasePromise(
                std::move(handleState.ReadyToReleasePromise));
        }
        nodeState.Handles.erase(handle);
    }
}

void TWriteBackCacheState::DropCachedData(
    ui64 nodeId,
    TNodeState& nodeState,
    const NCloud::NProto::TError& error)
{
    while (nodeState.Cache.HasPendingRequests()) {
        auto request = nodeState.Cache.DequeuePendingRequest();
        QueuedOperations.FailWriteDataPromise(
            std::move(request->AccessPromise()),
            error);
    }

    if (nodeState.Cache.HasUnflushedRequests()) {
        // TODO(#1751): Implement logging for dropped unflushed requests
        // similar to STORAGE_WARN macro
    }

    while (nodeState.Cache.HasUnflushedRequests()) {
        auto* request = nodeState.Cache.MoveFrontUnflushedRequestToFlushed();
        RequestManager.SetFlushed(request);
        Stats->WriteDataRequestDropped();
    }

    for (auto& it: nodeState.Handles) {
        QueuedOperations.FailFlushOrReleasePromise(
            std::move(it.second.ReadyToReleasePromise),
            error);
    }

    nodeState.Handles.clear();
    nodeState.HandleToReleaseCount = 0;

    EvictUnpinnedFlushedEntries(nodeId, nodeState);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
