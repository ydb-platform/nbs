#include "write_back_cache_state.h"

#include "node_cache.h"
#include "persistent_request_storage.h"
#include "sequence_id_generator_impl.h"

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

struct THandleState
{
    ui64 Count = 0;
    TPromise<TError> ReleasePromise;
};

////////////////////////////////////////////////////////////////////////////////

struct TFlushRequest
{
    ui64 SequenceId = 0;
    TPromise<TError> FlushPromise;
};

////////////////////////////////////////////////////////////////////////////////

enum class ENodeFlushStatus
{
    NothingToFlush,
    ReadyToFlush,
    FlushScheduled
};

////////////////////////////////////////////////////////////////////////////////

struct TNodeState
{
    TDeque<std::unique_ptr<TPendingWriteDataRequest>> PendingRequestQueue;
    TNodeCache Cache;
    TDeque<TFlushRequest> FlushRequestQueue;
    THashMap<ui64, THandleState> Handles;
    TMultiSet<ui64> Pins;
    size_t HandleReleaseCount = 0;
    ENodeFlushStatus FlushStatus = ENodeFlushStatus::NothingToFlush;

    bool Empty() const
    {
        return PendingRequestQueue.empty() && Cache.Empty() &&
               FlushRequestQueue.empty() && Handles.empty() && Pins.empty() &&
               HandleReleaseCount == 0;
    }

    bool HasUnflushedRequests() const
    {
        return Cache.HasUnflushedRequests() || !PendingRequestQueue.empty();
    }

    ui64 GetMinimalUnflushedSequenceId() const
    {
        if (Cache.HasUnflushedRequests()) {
            return Cache.GetMinUnflushedSequenceId();
        }
        if (!PendingRequestQueue.empty()) {
            return PendingRequestQueue.front()->SequenceId;
        }
        return Max<ui64>();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TState
{
    THashMap<ui64, TNodeState> Nodes;
    TDeque<TFlushRequest> FlushAllRequestQueue;
    THashSet<ui64> NodesReadyToFlush;
    ui64 FlushAllSequenceId = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataPromiseCompletedEvent
{
    NThreading::TPromise<TWriteDataResponse> Promise;

    void Invoke()
    {
        Promise.SetValue({});
    }
};

struct TWriteDataPromiseFailedEvent
{
    NThreading::TPromise<TWriteDataResponse> Promise;
    const TError& Error;

    void Invoke()
    {
        TWriteDataResponse response;
        *response.MutableError() = Error;
        Promise.SetValue(std::move(response));
    }
};

struct TFlushOrReleasePromiseCompletedEvent
{
    NThreading::TPromise<TError> Promise;

    void Invoke()
    {
        Promise.SetValue({});
    }
};

struct TFlushOrReleasePromiseFailedEvent
{
    NThreading::TPromise<TError> Promise;
    const TError& Error;

    void Invoke()
    {
        Promise.SetValue(Error);
    }
};

struct TScheduleFlushEvent
{
    IWriteBackCacheStateListener& Listener;
    ui64 NodeId;

    void Invoke()
    {
        Listener.ShouldFlushNode(NodeId);
    }
};

using TEvent = std::variant<
    TWriteDataPromiseCompletedEvent,
    TWriteDataPromiseFailedEvent,
    TFlushOrReleasePromiseCompletedEvent,
    TFlushOrReleasePromiseFailedEvent,
    TScheduleFlushEvent>;

////////////////////////////////////////////////////////////////////////////////

class TListener: IWriteBackCacheStateListener
{
private:
    IWriteBackCacheStateListener& Listener;
    TAdaptiveLock Lock;
    TVector<TEvent> Events;

public:
    explicit TListener(IWriteBackCacheStateListener& listener)
        : Listener(listener)
    {}

    void ShouldFlushNode(ui64 nodeId) override
    {
        Events.push_back(TScheduleFlushEvent{Listener, nodeId});
    }

    void Complete(NThreading::TPromise<TWriteDataResponse> promise)
    {
        Events.push_back(TWriteDataPromiseCompletedEvent{std::move(promise)});
    }

    void Fail(
        NThreading::TPromise<TWriteDataResponse> promise,
        const TError& error)
    {
        Events.push_back(
            TWriteDataPromiseFailedEvent{std::move(promise), error});
    }

    void Complete(NThreading::TPromise<TError> promise)
    {
        Events.push_back(
            TFlushOrReleasePromiseCompletedEvent{std::move(promise)});
    }

    void Fail(NThreading::TPromise<TError> promise, const TError& error)
    {
        Events.push_back(
            TFlushOrReleasePromiseFailedEvent{std::move(promise), error});
    }

    void Acquire()
    {
        Lock.Acquire();
    }

    void Release()
    {
        auto events = std::exchange(Events, {});
        Lock.Release();
        for (auto event: events) {
            std::visit([](auto& ev) { ev.Invoke(); }, event);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheState::TImpl
{
private:
    TSequenceIdGenerator SequenceIdGenerator;
    TPersistentRequestStorage Storage;
    TState State;
    TListener Listener;

public:
    TImpl(IPersistentStorage& persistentStorage, IWriteBackCacheStateListener& listener)
        : Storage(SequenceIdGenerator, persistentStorage)
        , Listener(listener)
    {}

    bool Init()
    {
        auto success = Storage.Init(
            [this](auto request)
            {
                auto& nodeState = State.Nodes[request->GetNodeId()];
                nodeState.Cache.PushUnflushed(std::move(request));
            });

        for (auto& [nodeId, nodeState]: State.Nodes) {
            State.NodesReadyToFlush.insert(nodeId);
        }

        return success;
    }

    TFuture<TWriteDataResponse> AddWriteDataRequest(
        std::shared_ptr<TWriteDataRequest> request)
    {
        auto guard = Guard(Listener);

        return std::visit(
            [this](auto res) { return AddRequest(std::move(res)); },
            Storage.AddRequest(std::move(request)));
    }

    TFuture<TError> AddFlushRequest(ui64 nodeId)
    {
        auto guard = Guard(Listener);

        auto* nodeState = State.Nodes.FindPtr(nodeId);
        if (nodeState == nullptr || !nodeState->HasUnflushedRequests()) {
            return MakeFuture<TError>();
        }

        auto future = nodeState->FlushRequestQueue
            .emplace_back(SequenceIdGenerator.Generate())
            .FlushPromise.GetFuture();

        UpdateFlushStatus(nodeId, *nodeState);

        return future;
    }

    TFuture<TError> AddFlushAllRequest()
    {
        auto guard = Guard(Listener);

        if (!Storage.HasUnflushedRequests()) {
            return MakeFuture<TError>();
        }

        TriggerFlushAll();

        return State.FlushAllRequestQueue
            .emplace_back(State.FlushAllSequenceId)
            .FlushPromise.GetFuture();
    }

    TFuture<TError> AddReleaseHandleRequest(ui64 nodeId, ui64 handle)
    {
        auto guard = Guard(Listener);

        auto* nodeState = State.Nodes.FindPtr(nodeId);
        if (nodeState == nullptr) {
            return MakeFuture<TError>();
        }

        auto* handleState = nodeState->Handles.FindPtr(handle);
        if (handleState == nullptr) {
            return MakeFuture<TError>();
        }

        if (handleState->ReleasePromise.Initialized()) {
            return handleState->ReleasePromise.GetFuture();
        }

        handleState->ReleasePromise = TPromise<TError>();
        nodeState->HandleReleaseCount++;

        UpdateFlushStatus(nodeId, *nodeState);

        return handleState->ReleasePromise.GetFuture();
    }

    void VisitCachedData(
        ui64 nodeId,
        ui64 offset,
        ui64 byteCount,
        const TCachedDataVisitor& visitor) const
    {
        auto guard = Guard(Listener);

        const auto* nodeState = State.Nodes.FindPtr(nodeId);
        if (nodeState == nullptr) {
            return;
        }

        nodeState->Cache.VisitCachedData(offset, byteCount, visitor);
    }

    TPin PinCachedData(ui64 nodeId)
    {
        auto guard = Guard(Listener);

        auto& nodeState = State.Nodes[nodeId];

        // Prevent unflushed requests from being evicted after flush
        const ui64 sequenceId =
            nodeState.Cache.GetMinUnflushedSequenceId();

        nodeState.Pins.insert(sequenceId);

        return sequenceId;
    }

    void UnpinCachedData(ui64 nodeId, TPin pinId)
    {
        auto guard = Guard(Listener);

        auto& nodeState = State.Nodes[nodeId];

        auto it = nodeState.Pins.find(pinId);
        Y_ENSURE(
            it != nodeState.Pins.end(),
            "Pin " << pinId << " not found for node " << nodeId);

        nodeState.Pins.erase(it);

        EvictUnpinnedFlushedEntries(nodeId, nodeState);
    }

    void VisitUnflushedCachedRequests(
        ui64 nodeId,
        const TEntryVisitor& visitor) const
    {
        auto guard = Guard(Listener);

        const auto* nodeState = State.Nodes.FindPtr(nodeId);
        if (nodeState == nullptr) {
            return;
        }

        nodeState->Cache.VisitUnflushedRequests(visitor);
    }

    void FlushSucceeded(ui64 nodeId, size_t requestCount)
    {
        auto guard = Guard(Listener);

        auto& nodeState = State.Nodes[nodeId];

        if (nodeState.FlushStatus == ENodeFlushStatus::FlushScheduled) {
            nodeState.FlushStatus = ENodeFlushStatus::NothingToFlush;
        }

        for (size_t i = 0; i < requestCount; i++) {
            Y_ABORT_UNLESS(nodeState.Cache.HasUnflushedRequests());
            auto* cachedRequest = nodeState.Cache.SetFrontFlushed();
            Storage.SetFlushed(cachedRequest);
            DecrementHandleCount(cachedRequest->GetHandle(), nodeState);
        }

        // Trigger Flush completions
        const ui64 sequenceId = nodeState.GetMinimalUnflushedSequenceId();
        while (!nodeState.FlushRequestQueue.empty() &&
               nodeState.FlushRequestQueue.front().SequenceId < sequenceId)
        {
            Listener.Complete(
                std::move(nodeState.FlushRequestQueue.front().FlushPromise));
            nodeState.FlushRequestQueue.pop_front();
        }

        // Trigger FlushAll completions
        const ui64 globalSequenceId = Storage.GetMinUnflushedSequenceId();
        while (!State.FlushAllRequestQueue.empty() &&
               State.FlushAllRequestQueue.front().SequenceId < globalSequenceId)
        {
            Listener.Complete(
                std::move(State.FlushAllRequestQueue.front().FlushPromise));
            State.FlushAllRequestQueue.pop_front();
        }

        UpdateFlushStatus(nodeId, nodeState);
        EvictUnpinnedFlushedEntries(nodeId, nodeState);
    }

    void FlushFailed(ui64 nodeId, const TError& error)
    {
        auto guard = Guard(Listener);

        auto& nodeState = State.Nodes[nodeId];

        for (auto& it: nodeState.FlushRequestQueue) {
            Listener.Fail(std::move(it.FlushPromise), error);
        }

        for (auto& it: State.FlushAllRequestQueue) {
            Listener.Fail(std::move(it.FlushPromise), error);
        }

        nodeState.FlushRequestQueue.clear();
        State.FlushAllRequestQueue.clear();

        if (nodeState.Handles.size() == nodeState.HandleReleaseCount) {
            // Drop node data
            for (auto& it: nodeState.Handles) {
                Listener.Fail(std::move(it.second.ReleasePromise), error);
            }

            for (auto& it: nodeState.PendingRequestQueue) {
                Listener.Fail(std::move(it->Promise), error);
                Storage.Remove(std::move(it));
            }

            while (nodeState.Cache.HasUnflushedRequests()) {
                auto* request = nodeState.Cache.SetFrontFlushed();
                Storage.SetFlushed(request);
            }

            nodeState.Handles.clear();
            nodeState.HandleReleaseCount = 0;
            nodeState.PendingRequestQueue.clear();

            EvictUnpinnedFlushedEntries(nodeId, nodeState);
        }
    }

private:
    void TriggerFlushAll()
    {
        State.FlushAllSequenceId = SequenceIdGenerator.Generate();

        for (auto nodeId: State.NodesReadyToFlush) {
            State.Nodes[nodeId].FlushStatus = ENodeFlushStatus::FlushScheduled;
            Listener.ShouldFlushNode(nodeId);
        }

        State.NodesReadyToFlush.clear();
    }

    ENodeFlushStatus GetFlushStatus(const TNodeState& nodeState) const
    {
        if (!nodeState.Cache.HasUnflushedRequests()) {
            return ENodeFlushStatus::NothingToFlush;
        }
        if (!nodeState.FlushRequestQueue.empty() ||
            nodeState.HandleReleaseCount > 0 ||
            nodeState.Cache.GetMinUnflushedSequenceId() <
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
            Listener.ShouldFlushNode(nodeId);
        }
        nodeState.FlushStatus = newFlushStatus;
    }

    void DecrementHandleCount(ui64 handle, TNodeState& nodeState)
    {
        auto& handleState = nodeState.Handles[handle];
        Y_ABORT_UNLESS(handleState.Count > 0);
        if (--handleState.Count == 0) {
            if (handleState.ReleasePromise.Initialized()) {
                Y_ABORT_UNLESS(nodeState.HandleReleaseCount > 0);
                nodeState.HandleReleaseCount--;
                Listener.Complete(std::move(handleState.ReleasePromise));
            }
            nodeState.Handles.erase(handle);
        }
    }

    TFuture<TWriteDataResponse> AddRequest(
        std::unique_ptr<TPendingWriteDataRequest> request)
    {
        auto future = request->Promise.GetFuture();
        TriggerFlushAll();

        auto& nodeState = State.Nodes[request->Request->GetNodeId()];
        nodeState.Handles[request->Request->GetHandle()].Count++;
        nodeState.PendingRequestQueue.push_back(std::move(request));

        return future;
    }

    TFuture<TWriteDataResponse> AddRequest(
        std::unique_ptr<TCachedWriteDataRequest> request)
    {
        const ui64 nodeId = request->GetNodeId();

        auto& nodeState = State.Nodes[nodeId];
        nodeState.Handles[request->GetHandle()].Count++;
        nodeState.Cache.PushUnflushed(std::move(request));

        UpdateFlushStatus(nodeId, nodeState);

        return MakeFuture<TWriteDataResponse>();
    }

    // nodeState becomes unusable after this call
    void EvictUnpinnedFlushedEntries(ui64 nodeId, TNodeState& nodeState)
    {
        bool entriesDeleted = false;

        const ui64 sequenceId =
            nodeState.Pins.empty() ? Max<ui64>() : *nodeState.Pins.begin();

        while (nodeState.Cache.HasFlushedRequests() &&
               nodeState.Cache.GetMinFlushedSequenceId() < sequenceId)
        {
            auto cachedRequest = nodeState.Cache.PopFlushed();
            Storage.Evict(std::move(cachedRequest));
            entriesDeleted = true;
        }

        if (!entriesDeleted) {
            return;
        }

        if (nodeState.Empty()) {
            State.Nodes.erase(nodeId);
        }

        while (Storage.HasPendingRequests()) {
            auto request = Storage.TryProcessPendingRequest();
            if (!request) {
                TriggerFlushAll();
                break;
            }

            const ui64 nodeId = request->GetNodeId();
            auto& nodeState = State.Nodes[nodeId];

            Y_ABORT_UNLESS(!nodeState.PendingRequestQueue.empty());
            Y_ABORT_UNLESS(
                nodeState.PendingRequestQueue.front()->SequenceId ==
                request->GetSequenceId());

            Listener.Complete(
                std::move(nodeState.PendingRequestQueue.front()->Promise));

            nodeState.PendingRequestQueue.pop_front();
            nodeState.Cache.PushUnflushed(std::move(request));

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
    IPersistentStorage& persistentStorage,
    IWriteBackCacheStateListener& listener)
    : Impl(std::make_unique<TImpl>(persistentStorage, listener))
{}

bool TWriteBackCacheState::Init()
{
    return Impl->Init();
}

TFuture<TWriteDataResponse> TWriteBackCacheState::AddWriteDataRequest(
    std::shared_ptr<TWriteDataRequest> request)
{
    return Impl->AddWriteDataRequest(std::move(request));
}

TFuture<TError> TWriteBackCacheState::AddFlushRequest(ui64 nodeId)
{
    return Impl->AddFlushRequest(nodeId);
}

TFuture<TError> TWriteBackCacheState::AddFlushAllRequest()
{
    return Impl->AddFlushAllRequest();
}

TFuture<TError> TWriteBackCacheState::AddReleaseHandleRequest(
    ui64 nodeId,
    ui64 handle)
{
    return Impl->AddReleaseHandleRequest(nodeId, handle);
}

void TWriteBackCacheState::VisitCachedData(
    ui64 nodeId,
    ui64 offset,
    ui64 byteCount,
    const TCachedDataVisitor& visitor) const
{
    Impl->VisitCachedData(nodeId, offset, byteCount, visitor);
}

TWriteBackCacheState::TPin TWriteBackCacheState::PinCachedData(ui64 nodeId)
{
    return Impl->PinCachedData(nodeId);
}

void TWriteBackCacheState::UnpinCachedData(ui64 nodeId, TPin pinId)
{
    Impl->UnpinCachedData(nodeId, pinId);
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
