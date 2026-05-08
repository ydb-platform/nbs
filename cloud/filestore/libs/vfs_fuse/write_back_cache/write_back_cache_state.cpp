#include "write_back_cache_state.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NCloud::NFileStore::NProto;
using namespace NCloud::NProto;
using namespace NThreading;

using ERequestType = IWriteBackCacheStateStats::ERequestType;

////////////////////////////////////////////////////////////////////////////////

TWriteBackCacheState::TWriteBackCacheState(
    IQueuedOperationsProcessor& processor,
    ITimerPtr timer,
    IWriteBackCacheStateStatsPtr writeBackCacheStateStats,
    IWriteDataRequestManagerStatsPtr writeDataRequestManagerStats,
    INodeStateHolderStatsPtr nodeStateHolderStats,
    TString logTag)
    : SequenceIdGenerator(std::make_shared<TSequenceIdGenerator>())
    , Timer(std::move(timer))
    , Stats(std::move(writeBackCacheStateStats))
    , RequestManagerStats(std::move(writeDataRequestManagerStats))
    , LogTag(std::move(logTag))
    , Nodes(Timer, std::move(nodeStateHolderStats))
    , QueuedOperations(processor)
{}

bool TWriteBackCacheState::Init(IPersistentStoragePtr persistentStorage)
{
    RequestManager = TWriteDataRequestManager(
        SequenceIdGenerator,
        std::move(persistentStorage),
        Timer,
        RequestManagerStats);

    return RequestManager.Init(
        [this](std::unique_ptr<TCachedWriteDataRequest> request)
        { AddRequest(std::move(request)); });
}

void TWriteBackCacheState::SetDrainingMode()
{
    auto guard = LockStateAndPostponeQueuedOperations();

    DrainingMode = true;
}

bool TWriteBackCacheState::IsDrained() const
{
    auto guard = LockStateAndPostponeQueuedOperations();

    return DrainingMode && !RequestManager.HasPendingOrUnflushedRequests();
}

TFuture<TWriteDataResponse> TWriteBackCacheState::AddWriteDataRequest(
    std::shared_ptr<TWriteDataRequest> request)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    if (DrainingMode) {
        // This may happen due to race condition that is valid scenario:
        // - TWriteBackCache::Drain is called in one thread;
        // - WriteData request is being processed in another thread.
        //
        // But currently, Drain is called only at session creation
        // or destruction where WriteData requests are not expected.
        // Therefore, we report an error.
        ReportWriteBackCacheWritingNotAllowedInDrainingMode(
            LogTag +
            " Cached WriteData request received while WriteBackCache is "
            "in draining mode");

        return MakeFuture(
            ErrorResponse<TWriteDataResponse>(
                E_REJECTED,
                "WriteBackCache doesn't accept new WriteData requests"));
    }

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
        Stats->RequestCompletedImmediately(ERequestType::Flush);
        return MakeFuture<TError>();
    }

    auto flushRequest = std::make_unique<TFlushRequest>(
        SequenceIdGenerator->GenerateId(),
        Timer->Now());

    auto future = flushRequest->Promise.GetFuture();

    FlushRequests.PushBack(flushRequest.get());
    nodeState->FlushRequests.push_back(std::move(flushRequest));
    Stats->RequestAdded(ERequestType::Flush);

    UpdateFlushStatus(nodeId, *nodeState);

    return future;
}

TFuture<TError> TWriteBackCacheState::AddFlushAllRequest()
{
    auto guard = LockStateAndPostponeQueuedOperations();

    if (!RequestManager.HasPendingOrUnflushedRequests()) {
        Stats->RequestCompletedImmediately(ERequestType::FlushAll);
        return MakeFuture<TError>();
    }

    TriggerFlushAll(true);

    Stats->RequestAdded(ERequestType::FlushAll);

    return FlushAllRequestQueue.emplace_back(FlushAllSequenceId, Timer->Now())
        .Promise.GetFuture();
}

TFuture<TError> TWriteBackCacheState::AddReleaseHandleRequest(
    ui64 nodeId,
    ui64 handle)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto* nodeState = Nodes.GetNodeState(nodeId, /* includeDeleted = */ false);
    if (nodeState == nullptr) {
        Stats->RequestCompletedImmediately(ERequestType::ReleaseHandle);
        return MakeFuture<TError>();
    }

    auto* handleState = nodeState->Handles.FindPtr(handle);
    if (handleState == nullptr) {
        Stats->RequestCompletedImmediately(ERequestType::ReleaseHandle);
        return MakeFuture<TError>();
    }

    if (!handleState->ReleaseHandleRequest) {
        handleState->ReleaseHandleRequest =
            std::make_unique<TReleaseHandleRequest>(Timer->Now());

        ReleaseHandleRequests.PushBack(handleState->ReleaseHandleRequest.get());
        Stats->RequestAdded(ERequestType::ReleaseHandle);

        nodeState->HandleToReleaseCount++;

        UpdateFlushStatus(nodeId, *nodeState);
    }

    return handleState->ReleaseHandleRequest->ReadyToReleasePromise.GetFuture();
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

ui64 TWriteBackCacheState::GetMaxWrittenOffset(ui64 nodeId) const
{
    auto guard = LockStateAndPostponeQueuedOperations();

    const auto* nodeState =
        Nodes.GetNodeState(nodeId, /* includeDeleted = */ true);

    return nodeState ? nodeState->Cache.GetMaxWrittenOffset() : 0;
}

void TWriteBackCacheState::ResetMaxWrittenOffset(ui64 nodeId)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);

    Y_ABORT_UNLESS(
        !nodeState.Barriers.empty() &&
            nodeState.Barriers.cbegin()->second->IsAcquired,
        "MaxWrittenOffset can only be reset if the barrier is acquired");

    nodeState.Cache.ResetMaxWrittenOffset();
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

    const ui64 maxSequenceId = nodeState->Barriers.empty()
                                   ? Max<ui64>()
                                   : nodeState->Barriers.cbegin()->first;

    nodeState->Cache.VisitUnflushedRequests(visitor, maxSequenceId);
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
    Stats->FlushCompleted();

    for (size_t i = 0; i < requestCount; i++) {
        Y_ABORT_UNLESS(nodeState.Cache.HasUnflushedRequests());
        auto* cachedRequest =
            nodeState.Cache.MoveFrontUnflushedRequestToFlushed();
        RequestManager.SetFlushed(cachedRequest);
        RemoveActiveRequestFromHandleState(
            nodeState,
            cachedRequest->GetHandle());
    }

    TriggerFlushCompletions(nodeState);
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

    Y_ABORT_UNLESS(
        nodeState.Cache.HasUnflushedRequests(),
        "Node %lu has no unflushed requests",
        nodeId);

    Stats->FlushFailed();

    const auto now = Timer->Now();

    // Fail all Flush requests
    for (auto& it: nodeState.FlushRequests) {
        Stats->RequestFailed(ERequestType::Flush, now - it->RequestStartTime);
        QueuedOperations.FailFlushOrReleasePromise(
            std::move(it->Promise),
            error);
    }
    nodeState.FlushRequests.clear();

    // Fail FlushAll requests that were requested after the failed request
    auto failedRequestSequenceId = nodeState.Cache.GetMinUnflushedSequenceId();
    while (!FlushAllRequestQueue.empty()) {
        auto& rq = FlushAllRequestQueue.back();
        if (rq.SequenceId < failedRequestSequenceId) {
            // The items in FlushAllRequestQueue are strictly ordered - no need
            // to check any further requests once we find one that was requested
            // before the failed request
            break;
        }
        Stats->RequestFailed(ERequestType::FlushAll, now - rq.RequestStartTime);
        QueuedOperations.FailFlushOrReleasePromise(
            std::move(rq.Promise),
            error);
        FlushAllRequestQueue.pop_back();
    }

    // Fail barrier acquisitions
    while (!nodeState.Barriers.empty()) {
        auto it = std::prev(nodeState.Barriers.end());
        if (it->second->IsAcquired) {
            break;
        }
        Stats->RequestFailed(
            ERequestType::AcquireBarrier,
            now - it->second->RequestStartTime);

        QueuedOperations.FailAcquireBarrierPromise(
            std::move(it->second->Promise),
            error);

        nodeState.Barriers.erase(it);
    }

    if (nodeState.Handles.size() == nodeState.HandleToReleaseCount) {
        // All handles with active WriteData requests are to be released
        // Drop node data on flush failure
        nodeState.FlushStatus = ENodeFlushStatus::NothingToFlush;
        Stats->FlushCompleted();
        DropCachedData(nodeId, nodeState, error);
        return EFlushRetryStatus::ShouldNotRetry;
    }

    // Keep status ENodeFlushStatus::FlushRequested if flush is retried
    return EFlushRetryStatus::ShouldRetry;
}

NThreading::TFuture<TResultOrError<ui64>> TWriteBackCacheState::AcquireBarrier(
    ui64 nodeId)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);

    const ui64 barrierId = SequenceIdGenerator->GenerateId();
    auto& barrier = nodeState.Barriers[barrierId];
    barrier = std::make_unique<TBarrier>();

    if (nodeState.Cache.Empty()) {
        barrier->BarrierAcquisitionTime = Timer->Now();
        barrier->IsAcquired = true;
        ActiveBarriers.PushBack(barrier.get());
        Stats->RequestCompletedImmediately(ERequestType::AcquireBarrier);
        Stats->BarrierAcquired();
        return MakeFuture(TResultOrError<ui64>(barrierId));
    }

    barrier->RequestStartTime = Timer->Now();
    barrier->Promise = NThreading::NewPromise<TResultOrError<ui64>>();
    PendingBarriers.PushBack(barrier.get());
    Stats->RequestAdded(ERequestType::AcquireBarrier);

    UpdateFlushStatus(nodeId, nodeState);

    return barrier->Promise.GetFuture();
}

void TWriteBackCacheState::ReleaseBarrier(ui64 nodeId, ui64 barrierId)
{
    auto guard = LockStateAndPostponeQueuedOperations();

    auto* nodeState = Nodes.GetNodeState(nodeId, /* includeDeleted= */ false);
    Y_ABORT_UNLESS(nodeState, "Node %lu not found", nodeId);

    auto it = nodeState->Barriers.find(barrierId);
    Y_ABORT_UNLESS(
        it != nodeState->Barriers.end(),
        "Barrier %lu not found for node %lu",
        barrierId,
        nodeId);

    Y_ABORT_UNLESS(
        it->second->IsAcquired,
        "Barrier %lu for node %lu has not been acquired",
        barrierId,
        nodeId);

    Stats->BarrierReleased(Timer->Now() - it->second->BarrierAcquisitionTime);
    nodeState->Barriers.erase(it);

    if (nodeState->CanBeDeleted()) {
        Nodes.DeleteNodeState(nodeId);
    } else {
        UpdateFlushStatus(nodeId, *nodeState);
    }
}

void TWriteBackCacheState::UpdateStats() const
{
    auto now = Timer->Now();

    auto guard = LockStateAndPostponeQueuedOperations();

    Stats->UpdateStats({
        .ActiveBarrier =
            ActiveBarriers.Empty()
                ? TDuration::Zero()
                : now - ActiveBarriers.Front()->BarrierAcquisitionTime,
        .FlushRequest = FlushRequests.Empty()
                            ? TDuration::Zero()
                            : now - FlushRequests.Front()->RequestStartTime,
        .FlushAllRequest =
            FlushAllRequestQueue.empty()
                ? TDuration::Zero()
                : now - FlushAllRequestQueue.front().RequestStartTime,
        .ReleaseHandleRequest =
            ReleaseHandleRequests.Empty()
                ? TDuration::Zero()
                : now - ReleaseHandleRequests.Front()->RequestStartTime,
        .AcquireBarrierRequest =
            PendingBarriers.Empty()
                ? TDuration::Zero()
                : now - PendingBarriers.Front()->RequestStartTime,
    });

    Nodes.UpdateStats();
    RequestManager.UpdateStats();
}

// Private methods

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
        Stats->FlushStarted();
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
            Stats->FlushStarted();
            QueuedOperations.ScheduleFlushNode(nodeId);
            break;

        case ENodeFlushStatus::ReadyToFlush:
            auto inserted = NodesReadyToFlush.insert(nodeId).second;
            Y_ABORT_UNLESS(inserted);
            break;
    }
}

void TWriteBackCacheState::TriggerFlushCompletions(TNodeState& nodeState)
{
    // Trigger Flush completions
    const ui64 sequenceId =
        nodeState.Cache.HasPendingOrUnflushedRequests()
            ? nodeState.Cache.GetMinPendingOrUnflushedSequenceId()
            : Max<ui64>();

    while (!nodeState.FlushRequests.empty() &&
           nodeState.FlushRequests.front()->SequenceId < sequenceId)
    {
        auto& front = nodeState.FlushRequests.front();
        Stats->RequestCompleted(
            ERequestType::Flush,
            Timer->Now() - front->RequestStartTime);
        QueuedOperations.CompleteFlushOrReleasePromise(
            std::move(front->Promise));
        nodeState.FlushRequests.pop_front();
    }

    // Trigger FlushAll completions
    const ui64 globalSequenceId =
        RequestManager.GetMinPendingOrUnflushedSequenceId();

    while (!FlushAllRequestQueue.empty() &&
           FlushAllRequestQueue.front().SequenceId < globalSequenceId)
    {
        auto& front = FlushAllRequestQueue.front();
        Stats->RequestCompleted(
            ERequestType::FlushAll,
            Timer->Now() - front.RequestStartTime);
        QueuedOperations.CompleteFlushOrReleasePromise(
            std::move(front.Promise));
        FlushAllRequestQueue.pop_front();
    }
}

// nodeState becomes unusable after this call
void TWriteBackCacheState::EvictUnpinnedFlushedEntries(
    ui64 nodeId,
    TNodeState& nodeState)
{
    bool entriesEvicted = false;

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
        entriesEvicted = true;
    }

    if (nodeState.CanBeDeleted()) {
        Nodes.DeleteNodeState(nodeId);
    } else {
        CheckAndAcquireBarriers(nodeState);
    }

    if (entriesEvicted) {
        ProcessPendingRequests();
    }
}

void TWriteBackCacheState::CheckAndAcquireBarriers(TNodeState& nodeState)
{
    // Barrier acquisition condition:
    // all requests with SequenceId <= BarrierId are flushed and evicted

    // (BarrierId1) (BarrierId2) (UnflushedSequenceId1) (BarrierId3) ...
    //  ^ acquired   ^ acquired                          ^ not acquired

    if (nodeState.Barriers.empty() || nodeState.Cache.HasFlushedRequests()) {
        return;
    }

    auto it = nodeState.Barriers.begin();
    if (it->second->IsAcquired) {
        // Once the front barrier is acquired, it is not possible to flush
        // any requests - so the acquisition condition for newly added barriers
        // will not change. Therefore, no need to check it again.
        return;
    }

    const ui64 minSequenceId =
        nodeState.Cache.HasPendingOrUnflushedRequests()
            ? nodeState.Cache.GetMinPendingOrUnflushedSequenceId()
            : Max<ui64>();

    for (; it != nodeState.Barriers.end(); ++it) {
        auto* barrier = it->second.get();

        Y_ABORT_UNLESS(
            !barrier->IsAcquired,
            "Newer barriers cannot be acquired before older");

        if (it->first > minSequenceId) {
            break;
        }

        auto now = Timer->Now();

        PendingBarriers.Remove(barrier);
        Stats->RequestCompleted(
            ERequestType::AcquireBarrier,
            now - barrier->RequestStartTime);

        barrier->BarrierAcquisitionTime = now;
        barrier->IsAcquired = true;
        ActiveBarriers.PushBack(barrier);
        Stats->BarrierAcquired();

        QueuedOperations.CompleteAcquireBarrierPromise(
            std::move(it->second->Promise),
            it->first);
    }
}

void TWriteBackCacheState::ProcessPendingRequests()
{
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
        if (handleState.ReleaseHandleRequest) {
            Y_ABORT_UNLESS(nodeState.HandleToReleaseCount > 0);
            nodeState.HandleToReleaseCount--;

            Stats->RequestCompleted(
                ERequestType::ReleaseHandle,
                Timer->Now() -
                    handleState.ReleaseHandleRequest->RequestStartTime);

            QueuedOperations.CompleteFlushOrReleasePromise(
                std::move(
                    handleState.ReleaseHandleRequest->ReadyToReleasePromise));
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
        if (it.second.ReleaseHandleRequest) {
            Stats->RequestFailed(
                ERequestType::ReleaseHandle,
                Timer->Now() -
                    it.second.ReleaseHandleRequest->RequestStartTime);

            QueuedOperations.FailFlushOrReleasePromise(
                std::move(
                    it.second.ReleaseHandleRequest->ReadyToReleasePromise),
                error);
        }
    }

    nodeState.Handles.clear();
    nodeState.HandleToReleaseCount = 0;

    EvictUnpinnedFlushedEntries(nodeId, nodeState);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
