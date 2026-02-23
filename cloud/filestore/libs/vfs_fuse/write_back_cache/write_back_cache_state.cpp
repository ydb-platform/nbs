#include "write_back_cache_state.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NCloud::NFileStore::NProto;
using namespace NCloud::NProto;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TWriteBackCacheState::TWriteBackCacheState(
    IQueuedOperationsProcessor& processor,
    ITimerPtr timer,
    IWriteBackCacheStatsPtr stats)
    : SequenceIdGenerator(std::make_shared<TSequenceIdGenerator>())
    , Timer(std::move(timer))
    , Stats(std::move(stats))
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
    auto guard = Guard(QueuedOperations);

    return RequestManager.HasPendingOrUnflushedRequests();
}

TFuture<TWriteDataResponse> TWriteBackCacheState::AddWriteDataRequest(
    std::shared_ptr<TWriteDataRequest> request)
{
    auto guard = Guard(QueuedOperations);

    auto variant = RequestManager.AddRequest(std::move(request));

    return std::visit(
        [this](auto res) { return AddRequest(std::move(res)); },
        std::move(variant));
}

TFuture<void> TWriteBackCacheState::AddFlushRequest(ui64 nodeId)
{
    auto guard = Guard(QueuedOperations);

    auto* nodeState = Nodes.GetNodeState(nodeId);
    if (nodeState == nullptr ||
        !nodeState->Cache.HasPendingOrUnflushedRequests())
    {
        return MakeFuture();
    }

    auto future =
        nodeState->FlushRequests.emplace_back(SequenceIdGenerator->GenerateId())
            .Promise.GetFuture();

    UpdateFlushStatus(nodeId, *nodeState);

    return future;
}

TFuture<void> TWriteBackCacheState::AddFlushAllRequest()
{
    auto guard = Guard(QueuedOperations);

    if (!RequestManager.HasPendingOrUnflushedRequests()) {
        return MakeFuture();
    }

    TriggerFlushAll(true);

    return FlushAllRequestQueue.emplace_back(FlushAllSequenceId)
        .Promise.GetFuture();
}

void TWriteBackCacheState::TriggerPeriodicFlushAll()
{
    auto guard = Guard(QueuedOperations);

    TriggerFlushAll(false);
}

TCachedData TWriteBackCacheState::GetCachedData(
    ui64 nodeId,
    ui64 offset,
    ui64 byteCount) const
{
    auto guard = Guard(QueuedOperations);

    const auto* nodeState = Nodes.GetNodeState(nodeId);
    if (nodeState == nullptr) {
        return {};
    }

    return nodeState->Cache.GetCachedData(offset, byteCount);
}

ui64 TWriteBackCacheState::GetCachedNodeSize(ui64 nodeId) const
{
    auto guard = Guard(QueuedOperations);

    const auto* nodeState = Nodes.GetNodeState(nodeId, true);
    return nodeState ? nodeState->CachedNodeSize : 0;
}

void TWriteBackCacheState::SetCachedNodeSize(ui64 nodeId, ui64 size)
{
    auto guard = Guard(QueuedOperations);

    auto* nodeState = Nodes.GetNodeState(nodeId, true);
    if (nodeState) {
        nodeState->CachedNodeSize = size;
    }
}

TWriteBackCacheState::TPin TWriteBackCacheState::PinNodeStates()
{
    auto guard = Guard(QueuedOperations);

    return Nodes.Pin();
}

void TWriteBackCacheState::UnpinNodeStates(TPin pinId)
{
    auto guard = Guard(QueuedOperations);

    Nodes.Unpin(pinId);
}

void TWriteBackCacheState::VisitUnflushedRequests(
    ui64 nodeId,
    const TEntryVisitor& visitor) const
{
    auto guard = Guard(QueuedOperations);

    const auto* nodeState = Nodes.GetNodeState(nodeId);
    if (nodeState == nullptr) {
        return;
    }

    nodeState->Cache.VisitUnflushedRequests(visitor);
}

void TWriteBackCacheState::FlushSucceeded(ui64 nodeId, size_t requestCount)
{
    auto guard = Guard(QueuedOperations);

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);

    if (nodeState.FlushStatus == ENodeFlushStatus::FlushScheduled) {
        nodeState.FlushStatus = ENodeFlushStatus::NothingToFlush;
    }

    for (size_t i = 0; i < requestCount; i++) {
        Y_ABORT_UNLESS(nodeState.Cache.HasUnflushedRequests());
        auto* cachedRequest =
            nodeState.Cache.MoveFrontUnflushedRequestToFlushed();
        RequestManager.SetFlushed(cachedRequest);
    }

    // Trigger Flush completions
    const ui64 sequenceId =
        nodeState.Cache.GetMinPendingOrUnflushedSequenceId(Max<ui64>());

    while (!nodeState.FlushRequests.empty() &&
           nodeState.FlushRequests.front().SequenceId < sequenceId)
    {
        QueuedOperations.Complete(
            std::move(nodeState.FlushRequests.front().Promise));
        nodeState.FlushRequests.pop_front();
    }

    // Trigger FlushAll completions
    const ui64 globalSequenceId =
        RequestManager.GetMinPendingOrUnflushedSequenceId();

    while (!FlushAllRequestQueue.empty() &&
           FlushAllRequestQueue.front().SequenceId < globalSequenceId)
    {
        QueuedOperations.Complete(
            std::move(FlushAllRequestQueue.front().Promise));
        FlushAllRequestQueue.pop_front();
    }

    UpdateFlushStatus(nodeId, nodeState);
    EvictUnpinnedFlushedEntries(nodeId, nodeState);
}

NThreading::TFuture<void>
TWriteBackCacheState::LockRead(ui64 nodeId, ui64 begin, ui64 end)
{
    auto guard = Guard(QueuedOperations);

    auto promise = NThreading::NewPromise<void>();

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);
    nodeState.RangeLock.LockRead(
        begin,
        end,
        [this, promise]() mutable
        { QueuedOperations.Complete(std::move(promise)); });

    return promise.GetFuture();
}

NThreading::TFuture<void>
TWriteBackCacheState::LockWrite(ui64 nodeId, ui64 begin, ui64 end)
{
    auto guard = Guard(QueuedOperations);

    auto promise = NThreading::NewPromise<void>();

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);
    nodeState.RangeLock.LockWrite(
        begin,
        end,
        [this, promise]() mutable
        { QueuedOperations.Complete(std::move(promise)); });

    return promise.GetFuture();
}

void TWriteBackCacheState::UnlockRead(ui64 nodeId, ui64 begin, ui64 end)
{
    auto guard = Guard(QueuedOperations);

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);
    nodeState.RangeLock.UnlockRead(begin, end);

    if (nodeState.CanBeDeleted()) {
        Nodes.DeleteNodeState(nodeId);
    }
}

void TWriteBackCacheState::UnlockWrite(ui64 nodeId, ui64 begin, ui64 end)
{
    auto guard = Guard(QueuedOperations);

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);
    nodeState.RangeLock.UnlockWrite(begin, end);

    if (nodeState.CanBeDeleted()) {
        Nodes.DeleteNodeState(nodeId);
    }
}

TFuture<TWriteDataResponse> TWriteBackCacheState::AddRequest(
    std::unique_ptr<TPendingWriteDataRequest> request)
{
    auto future = request->AccessPromise().GetFuture();
    TriggerFlushAll(false);

    auto& nodeState =
        Nodes.GetOrCreateNodeState(request->GetRequest().GetNodeId());

    nodeState.Cache.EnqueuePendingRequest(std::move(request));

    return future;
}

TFuture<TWriteDataResponse> TWriteBackCacheState::AddRequest(
    std::unique_ptr<TCachedWriteDataRequest> request)
{
    const ui64 nodeId = request->GetNodeId();

    auto& nodeState = Nodes.GetOrCreateNodeState(nodeId);
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
        nodeState.FlushStatus = ENodeFlushStatus::FlushScheduled;
        QueuedOperations.ScheduleFlushNode(nodeId);
    }

    NodesReadyToFlush.clear();
}

void TWriteBackCacheState::UpdateFlushStatus(ui64 nodeId, TNodeState& nodeState)
{
    if (nodeState.FlushStatus == ENodeFlushStatus::FlushScheduled) {
        // Once Flush has been scheduled, the status can be changed only in
        // FlushSucceeded and FlushFailed calls
        return;
    }

    auto newFlushStatus = nodeState.GetFlushStatus(FlushAllSequenceId);
    if (nodeState.FlushStatus == newFlushStatus) {
        return;
    }

    if (nodeState.FlushStatus == ENodeFlushStatus::ReadyToFlush) {
        NodesReadyToFlush.erase(nodeId);
    }
    if (newFlushStatus == ENodeFlushStatus::ReadyToFlush) {
        NodesReadyToFlush.insert(nodeId);
    }

    nodeState.FlushStatus = newFlushStatus;
    if (nodeState.FlushStatus == ENodeFlushStatus::ShouldFlush) {
        nodeState.FlushStatus = ENodeFlushStatus::FlushScheduled;
        QueuedOperations.ScheduleFlushNode(nodeId);
    }
}

// nodeState becomes unusable after this call
void TWriteBackCacheState::EvictUnpinnedFlushedEntries(
    ui64 nodeId,
    TNodeState& nodeState)
{
    bool entriesDeleted = false;

    while (nodeState.Cache.HasFlushedRequests()) {
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

        QueuedOperations.Complete(std::move(pendingRequest->AccessPromise()));

        nodeState.Cache.EnqueueUnflushedRequest(std::move(request));

        UpdateFlushStatus(nodeId, nodeState);
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
