#include "write_back_cache_state.h"

#include "node_cache.h"
#include "node_state.h"
#include "node_state_holder.h"
#include "sequence_id_generator.h"
#include "write_data_request_manager.h"

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/overloaded.h>
#include <util/generic/set.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NCloud::NFileStore::NProto;
using namespace NCloud::NProto;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TState
{
    TNodeStateHolder Nodes;
    TDeque<TFlushRequest> FlushAllRequestQueue;

    // FlushAll request that isn't associated with a promise
    ui64 FlushAllSequenceId = 0;

    // Nodes that has unflushed data but are not scheduled for flush
    // Used for optimization: avoid checking flush condition for all nodes when
    // FlushAll is requested
    THashSet<ui64> NodesReadyToFlush;

    explicit TState(IWriteBackCacheStatsPtr stats)
        : Nodes(std::move(stats))
     {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheState::TImpl
{
private:
    ISequenceIdGeneratorPtr SequenceIdGenerator;
    TWriteDataRequestManager RequestManager;
    TState State;
    TQueuedOperations QueuedOperations;
    const IWriteBackCacheStatsPtr Stats;

public:
    TImpl(
        IPersistentStoragePtr persistentStorage,
        IQueuedOperationsProcessor& processor,
        ITimerPtr timer,
        IWriteBackCacheStatsPtr stats)
        : SequenceIdGenerator(std::make_shared<TSequenceIdGenerator>())
        , RequestManager(
              SequenceIdGenerator,
              std::move(persistentStorage),
              std::move(timer),
              stats)
        , State(stats)
        , QueuedOperations(processor)
        , Stats(std::move(stats))
    {}

    bool Init()
    {
        return RequestManager.Init(
            [this](std::unique_ptr<TCachedWriteDataRequest> request)
            { AddRequest(std::move(request)); });
    }

    bool HasPendingOrUnflushedRequests() const
    {
        auto guard = Guard(QueuedOperations);

        return RequestManager.HasPendingOrUnflushedRequests();
    }

    TFuture<TWriteDataResponse> AddWriteDataRequest(
        std::shared_ptr<TWriteDataRequest> request)
    {
        auto guard = Guard(QueuedOperations);

        auto variant = RequestManager.AddRequest(std::move(request));

        return std::visit(
            [this](auto res) { return AddRequest(std::move(res)); },
            std::move(variant));
    }

    TFuture<void> AddFlushRequest(ui64 nodeId)
    {
        auto guard = Guard(QueuedOperations);

        auto* nodeState = State.Nodes.GetNodeState(nodeId);
        if (nodeState == nullptr ||
            !nodeState->Cache.HasPendingOrUnflushedRequests())
        {
            return MakeFuture();
        }

        auto future = nodeState->FlushRequests
                          .emplace_back(SequenceIdGenerator->GenerateId())
                          .Promise.GetFuture();

        UpdateFlushStatus(nodeId, *nodeState);

        return future;
    }

    TFuture<void> AddFlushAllRequest()
    {
        auto guard = Guard(QueuedOperations);

        if (!RequestManager.HasPendingOrUnflushedRequests()) {
            return MakeFuture();
        }

        TriggerFlushAll(true);

        return State.FlushAllRequestQueue.emplace_back(State.FlushAllSequenceId)
            .Promise.GetFuture();
    }

    void TriggerPeriodicFlushAll()
    {
        auto guard = Guard(QueuedOperations);

        TriggerFlushAll(false);
    }

    TCachedData GetCachedData(ui64 nodeId, ui64 offset, ui64 byteCount) const
    {
        auto guard = Guard(QueuedOperations);

        const auto* nodeState = State.Nodes.GetNodeState(nodeId);
        if (nodeState == nullptr) {
            return {};
        }

        return nodeState->Cache.GetCachedData(offset, byteCount);
    }

    ui64 GetCachedNodeSize(ui64 nodeId) const
    {
        auto guard = Guard(QueuedOperations);

        const auto* nodeState = State.Nodes.GetNodeState(nodeId, true);
        return nodeState ? nodeState->CachedNodeSize : 0;
    }

    void SetCachedNodeSize(ui64 nodeId, ui64 size)
    {
        auto guard = Guard(QueuedOperations);

        auto* nodeState = State.Nodes.GetNodeState(nodeId, true);
        nodeState->CachedNodeSize = size;
    }

    TPin PinCachedData(ui64 nodeId)
    {
        auto guard = Guard(QueuedOperations);

        auto& nodeState = State.Nodes.GetOrCreateNodeState(nodeId);

        // Prevent unflushed requests from being evicted after flush
        const ui64 sequenceId =
            nodeState.Cache.GetMinPendingOrUnflushedSequenceId(0);

        nodeState.CachedDataPins.insert(sequenceId);

        return sequenceId;
    }

    void UnpinCachedData(ui64 nodeId, TPin pinId)
    {
        auto guard = Guard(QueuedOperations);

        auto& nodeState = State.Nodes.GetOrCreateNodeState(nodeId);

        auto it = nodeState.CachedDataPins.find(pinId);
        Y_ENSURE(
            it != nodeState.CachedDataPins.end(),
            "Pin " << pinId << " not found for node " << nodeId);

        nodeState.CachedDataPins.erase(it);

        EvictUnpinnedFlushedEntries(nodeId, nodeState);
    }

    TPin PinMetadata()
    {
        auto guard = Guard(QueuedOperations);

        return State.Nodes.Pin();
    }

    void UnpinMetadata(TPin pinId)
    {
        auto guard = Guard(QueuedOperations);

        State.Nodes.Unpin(pinId);
    }

    void VisitUnflushedCachedRequests(
        ui64 nodeId,
        const TEntryVisitor& visitor) const
    {
        auto guard = Guard(QueuedOperations);

        const auto* nodeState = State.Nodes.GetNodeState(nodeId);
        if (nodeState == nullptr) {
            return;
        }

        nodeState->Cache.VisitUnflushedRequests(visitor);
    }

    void FlushSucceeded(ui64 nodeId, size_t requestCount)
    {
        auto guard = Guard(QueuedOperations);

        auto& nodeState = State.Nodes.GetOrCreateNodeState(nodeId);

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

        while (!State.FlushAllRequestQueue.empty() &&
               State.FlushAllRequestQueue.front().SequenceId < globalSequenceId)
        {
            QueuedOperations.Complete(
                std::move(State.FlushAllRequestQueue.front().Promise));
            State.FlushAllRequestQueue.pop_front();
        }

        UpdateFlushStatus(nodeId, nodeState);
        EvictUnpinnedFlushedEntries(nodeId, nodeState);
    }

    void FlushFailed(ui64 nodeId, const TError& error)
    {
        Y_UNUSED(error);

        auto guard = Guard(QueuedOperations);

        auto& nodeState = State.Nodes.GetOrCreateNodeState(nodeId);

        // Will retry on the next periodic flush
        if (nodeState.FlushStatus == ENodeFlushStatus::FlushScheduled) {
            nodeState.FlushStatus = ENodeFlushStatus::ReadyToFlush;
            State.NodesReadyToFlush.insert(nodeId);
        }
    }

private:
    void TriggerFlushAll(bool includePendingRequests)
    {
        if (includePendingRequests) {
            State.FlushAllSequenceId = SequenceIdGenerator->GenerateId();
        } else {
            State.FlushAllSequenceId =
                Max(State.FlushAllSequenceId,
                    RequestManager.GetMaxUnflushedSequenceId());
        }

        for (auto nodeId: State.NodesReadyToFlush) {
            auto& nodeState =State.Nodes.GetOrCreateNodeState(nodeId);
            nodeState.FlushStatus = ENodeFlushStatus::FlushScheduled;
            QueuedOperations.ScheduleFlushNode(nodeId);
        }

        State.NodesReadyToFlush.clear();
    }

    ENodeFlushStatus GetFlushStatus(const TNodeState& nodeState) const
    {
        if (!nodeState.Cache.HasUnflushedRequests()) {
            return ENodeFlushStatus::NothingToFlush;
        }
        if (!nodeState.FlushRequests.empty() ||
            nodeState.Cache.GetMinUnflushedSequenceId() <=
                State.FlushAllSequenceId)
        {
            return ENodeFlushStatus::FlushScheduled;
        }
        return ENodeFlushStatus::ReadyToFlush;
    }

    void UpdateFlushStatus(ui64 nodeId, TNodeState& nodeState)
    {
        auto newFlushStatus = GetFlushStatus(nodeState);
        if (nodeState.FlushStatus == newFlushStatus) {
            return;
        }
        if (nodeState.FlushStatus == ENodeFlushStatus::ReadyToFlush) {
            State.NodesReadyToFlush.erase(nodeId);
        }
        if (newFlushStatus == ENodeFlushStatus::ReadyToFlush) {
            State.NodesReadyToFlush.insert(nodeId);
        } else if (newFlushStatus == ENodeFlushStatus::FlushScheduled) {
            QueuedOperations.ScheduleFlushNode(nodeId);
        }
        nodeState.FlushStatus = newFlushStatus;
    }

    TFuture<TWriteDataResponse> AddRequest(
        std::unique_ptr<TPendingWriteDataRequest> request)
    {
        auto future = request->AccessPromise().GetFuture();
        TriggerFlushAll(false);

        auto& nodeState =
            State.Nodes.GetOrCreateNodeState(request->GetRequest().GetNodeId());

        nodeState.Cache.EnqueuePendingRequest(std::move(request));

        return future;
    }

    TFuture<TWriteDataResponse> AddRequest(
        std::unique_ptr<TCachedWriteDataRequest> request)
    {
        const ui64 nodeId = request->GetNodeId();

        auto& nodeState = State.Nodes.GetOrCreateNodeState(nodeId);
        nodeState.CachedNodeSize =
            Max(nodeState.CachedNodeSize, request->GetEnd());
        nodeState.Cache.EnqueueUnflushedRequest(std::move(request));

        UpdateFlushStatus(nodeId, nodeState);

        return MakeFuture<TWriteDataResponse>();
    }

    // nodeState becomes unusable after this call
    void EvictUnpinnedFlushedEntries(ui64 nodeId, TNodeState& nodeState)
    {
        bool entriesDeleted = false;

        const ui64 nodePinSequenceId = nodeState.CachedDataPins.empty()
                                           ? Max<ui64>()
                                           : *nodeState.CachedDataPins.begin();

        while (nodeState.Cache.HasFlushedRequests()) {
            const ui64 sequenceId = nodeState.Cache.GetMinFlushedSequenceId();
            if (sequenceId >= nodePinSequenceId) {
                break;
            }
            auto cachedRequest = nodeState.Cache.DequeueFlushedRequest();
            RequestManager.Evict(std::move(cachedRequest));
            entriesDeleted = true;
        }

        if (nodeState.CanBeDeleted()) {
            State.Nodes.DeleteNodeState(nodeId);
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
            auto& nodeState = State.Nodes.GetOrCreateNodeState(nodeId);

            Y_ABORT_UNLESS(nodeState.Cache.HasPendingRequests());
            auto pendingRequest = nodeState.Cache.DequeuePendingRequest();

            Y_ABORT_UNLESS(
                pendingRequest->GetSequenceId() ==
                request->GetSequenceId());

            QueuedOperations.Complete(
                std::move(pendingRequest->AccessPromise()));

            nodeState.Cache.EnqueueUnflushedRequest(std::move(request));

            UpdateFlushStatus(nodeId, nodeState);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TWriteBackCacheState::TWriteBackCacheState() = default;

TWriteBackCacheState::TWriteBackCacheState(
    TWriteBackCacheState&&) noexcept = default;

TWriteBackCacheState& TWriteBackCacheState::operator=(
    TWriteBackCacheState&&) noexcept = default;

TWriteBackCacheState::~TWriteBackCacheState() = default;

TWriteBackCacheState::TWriteBackCacheState(
    IPersistentStoragePtr persistentStorage,
    IQueuedOperationsProcessor& processor,
    ITimerPtr timer,
    IWriteBackCacheStatsPtr stats)
    : Impl(
          std::make_unique<TImpl>(
              std::move(persistentStorage),
              processor,
              std::move(timer),
              std::move(stats)))
{}

bool TWriteBackCacheState::Init()
{
    return Impl->Init();
}

bool TWriteBackCacheState::HasUnflushedRequests() const
{
    return Impl->HasPendingOrUnflushedRequests();
}

TFuture<TWriteDataResponse> TWriteBackCacheState::AddWriteDataRequest(
    std::shared_ptr<TWriteDataRequest> request)
{
    return Impl->AddWriteDataRequest(std::move(request));
}

TFuture<void> TWriteBackCacheState::AddFlushRequest(ui64 nodeId)
{
    return Impl->AddFlushRequest(nodeId);
}

TFuture<void> TWriteBackCacheState::AddFlushAllRequest()
{
    return Impl->AddFlushAllRequest();
}

void TWriteBackCacheState::TriggerPeriodicFlushAll()
{
    Impl->TriggerPeriodicFlushAll();
}

TCachedData TWriteBackCacheState::GetCachedData(
    ui64 nodeId,
    ui64 offset,
    ui64 byteCount) const
{
    return Impl->GetCachedData(nodeId, offset, byteCount);
}

ui64 TWriteBackCacheState::GetCachedNodeSize(ui64 nodeId) const
{
    return Impl->GetCachedNodeSize(nodeId);
}

void TWriteBackCacheState::SetCachedNodeSize(ui64 nodeId, ui64 size)
{
    Impl->SetCachedNodeSize(nodeId, size);
}

TWriteBackCacheState::TPin TWriteBackCacheState::PinCachedData(ui64 nodeId)
{
    return Impl->PinCachedData(nodeId);
}

void TWriteBackCacheState::UnpinCachedData(ui64 nodeId, TPin pinId)
{
    Impl->UnpinCachedData(nodeId, pinId);
}

TWriteBackCacheState::TPin TWriteBackCacheState::PinMetadata()
{
    return Impl->PinMetadata();
}

void TWriteBackCacheState::UnpinMetadata(TPin pinId)
{
    Impl->UnpinMetadata(pinId);
}

void TWriteBackCacheState::VisitUnflushedCachedRequests(
    ui64 nodeId,
    const TEntryVisitor& visitor) const
{
    Impl->VisitUnflushedCachedRequests(nodeId, visitor);
}

void TWriteBackCacheState::FlushSucceeded(ui64 nodeId, size_t requestCount)
{
    Impl->FlushSucceeded(nodeId, requestCount);
}

void TWriteBackCacheState::FlushFailed(ui64 nodeId, const TError& error)
{
    Impl->FlushFailed(nodeId, error);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
