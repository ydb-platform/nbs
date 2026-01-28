#include "write_back_cache_state.h"

#include "node_cache.h"
#include "persistent_request_storage.h"
#include "sequence_id_generator.h"

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
    TPromise<TError> FlushPromise = NewPromise<TError>();
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

    bool HasPendingOrUnflushedRequests() const
    {
        return !PendingRequestQueue.empty() || Cache.HasUnflushedRequests();
    }

    ui64 GetMinPendingOrUnflushedSequenceId(ui64 defValue) const
    {
        if (Cache.HasUnflushedRequests()) {
            return Cache.GetMinUnflushedSequenceId();
        }
        if (!PendingRequestQueue.empty()) {
            return PendingRequestQueue.front()->SequenceId;
        }
        return defValue;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TState
{
    THashMap<ui64, TNodeState> Nodes;
    TDeque<TFlushRequest> FlushAllRequestQueue;
    THashSet<ui64> NodesReadyToFlush;
    ui64 FlushAllSequenceId = 0;
    THashSet<ui64> NodesWithGlobalPinnedFlushedData;
    TMultiSet<ui64> GlobalPins;

    ui64 GetFrontGlobalPin() const
    {
        return GlobalPins.empty() ? Max<ui64>() : *GlobalPins.begin();
    }
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
        Listener.ScheduleFlushNode(NodeId);
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

    void ScheduleFlushNode(ui64 nodeId) override
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
    IWriteBackCacheStats& Stats;
    TWriteBackCacheStateConfig Config;

public:
    TImpl(
        IPersistentStorage& persistentStorage,
        IWriteBackCacheStateListener& listener,
        ITimer& timer,
        IWriteBackCacheStats& stats,
        TWriteBackCacheStateConfig config)
        : Storage(SequenceIdGenerator, persistentStorage, timer, stats)
        , Listener(listener)
        , Stats(stats)
        , Config(config)
    {}

    bool Init()
    {
        return Storage.Init(
            [this](std::unique_ptr<TCachedWriteDataRequest> request)
            { AddRequest(std::move(request)); });
    }

    bool HasPendingOrUnflushedRequests() const
    {
        auto guard = Guard(Listener);

        return Storage.HasPendingOrUnflushedRequests();
    }

    TFuture<TWriteDataResponse> AddWriteDataRequest(
        std::shared_ptr<TWriteDataRequest> request)
    {
        auto guard = Guard(Listener);

        auto variant = Storage.AddRequest(std::move(request));

        return std::visit(
            [this](auto res) { return AddRequest(std::move(res)); },
            std::move(variant));
    }

    TFuture<TError> AddFlushRequest(ui64 nodeId)
    {
        auto guard = Guard(Listener);

        auto* nodeState = State.Nodes.FindPtr(nodeId);
        if (nodeState == nullptr || !nodeState->HasPendingOrUnflushedRequests())
        {
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

        if (!Storage.HasPendingOrUnflushedRequests()) {
            return MakeFuture<TError>();
        }

        TriggerFlushAll(true);

        return State.FlushAllRequestQueue.emplace_back(State.FlushAllSequenceId)
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

        handleState->ReleasePromise = NewPromise<TError>();
        nodeState->HandleReleaseCount++;

        UpdateFlushStatus(nodeId, *nodeState);

        return handleState->ReleasePromise.GetFuture();
    }

    void TriggerPeriodicFlushAll()
    {
        auto guard = Guard(Listener);

        TriggerFlushAll(false);
    }

    TCachedData GetCachedData(ui64 nodeId, ui64 offset, ui64 byteCount) const
    {
        auto guard = Guard(Listener);

        const auto* nodeState = State.Nodes.FindPtr(nodeId);
        if (nodeState == nullptr) {
            return {};
        }

        return nodeState->Cache.GetCachedData(offset, byteCount);
    }

    ui64 GetCachedDataEndOffset(ui64 nodeId) const
    {
        auto guard = Guard(Listener);

        const auto* nodeState = State.Nodes.FindPtr(nodeId);
        if (nodeState == nullptr) {
            return 0;
        }

        return nodeState->Cache.GetCachedDataEndOffset();
    }

    TPin PinCachedData(ui64 nodeId)
    {
        auto guard = Guard(Listener);

        auto& nodeState = GetOrCreateNodeState(nodeId);

        // Prevent unflushed requests from being evicted after flush
        const ui64 sequenceId = nodeState.GetMinPendingOrUnflushedSequenceId(0);
        nodeState.Pins.insert(sequenceId);

        return sequenceId;
    }

    void UnpinCachedData(ui64 nodeId, TPin pinId)
    {
        auto guard = Guard(Listener);

        auto& nodeState = GetOrCreateNodeState(nodeId);

        auto it = nodeState.Pins.find(pinId);
        Y_ENSURE(
            it != nodeState.Pins.end(),
            "Pin " << pinId << " not found for node " << nodeId);

        nodeState.Pins.erase(it);

        EvictUnpinnedFlushedEntries(nodeId, nodeState);
    }

    TPin PinAllCachedData()
    {
        auto guard = Guard(Listener);

        const ui64 sequenceId = Storage.GetMinPendingOrUnflushedSequenceId();
        State.GlobalPins.insert(sequenceId);

        return sequenceId;
    }

    void UnpinAllCachedData(TPin pinId)
    {
        auto guard = Guard(Listener);

        const ui64 prevSequenceId = State.GetFrontGlobalPin();

        auto it = State.GlobalPins.find(pinId);
        Y_ENSURE(
            it != State.GlobalPins.end(),
            "Global pin " << pinId << " not found");

        State.GlobalPins.erase(it);

        const ui64 sequenceId = State.GetFrontGlobalPin();

        if (prevSequenceId != sequenceId) {
            auto nodes =
                std::exchange(State.NodesWithGlobalPinnedFlushedData, {});

            for (auto nodeId: nodes) {
                auto& nodeState = GetOrCreateNodeState(nodeId);
                EvictUnpinnedFlushedEntries(nodeId, nodeState);
            }
        }
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

        auto& nodeState = GetOrCreateNodeState(nodeId);

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
        const ui64 sequenceId =
            nodeState.GetMinPendingOrUnflushedSequenceId(Max<ui64>());

        while (!nodeState.FlushRequestQueue.empty() &&
               nodeState.FlushRequestQueue.front().SequenceId < sequenceId)
        {
            Listener.Complete(
                std::move(nodeState.FlushRequestQueue.front().FlushPromise));
            nodeState.FlushRequestQueue.pop_front();
        }

        // Trigger FlushAll completions
        const ui64 globalSequenceId =
            Storage.GetMinPendingOrUnflushedSequenceId();
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

        auto& nodeState = GetOrCreateNodeState(nodeId);

        // Will retry on the next periodic flush
        if (nodeState.FlushStatus == ENodeFlushStatus::FlushScheduled) {
            nodeState.FlushStatus = ENodeFlushStatus::ReadyToFlush;
            State.NodesReadyToFlush.insert(nodeId);
        }

        if (Config.EnableFlushFailure) {
            for (auto& it: nodeState.FlushRequestQueue) {
                Listener.Fail(std::move(it.FlushPromise), error);
            }

            for (auto& it: State.FlushAllRequestQueue) {
                Listener.Fail(std::move(it.FlushPromise), error);
            }

            nodeState.FlushRequestQueue.clear();
            State.FlushAllRequestQueue.clear();
        }

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
    void TriggerFlushAll(bool includePendingRequests)
    {
        if (includePendingRequests) {
            State.FlushAllSequenceId = SequenceIdGenerator.Generate();
        } else {
            State.FlushAllSequenceId =
                Max(State.FlushAllSequenceId,
                    Storage.GetMaxUnflushedSequenceId());
        }

        for (auto nodeId: State.NodesReadyToFlush) {
            auto& nodeState = GetOrCreateNodeState(nodeId);
            nodeState.FlushStatus = ENodeFlushStatus::FlushScheduled;
            Listener.ScheduleFlushNode(nodeId);
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
            Listener.ScheduleFlushNode(nodeId);
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
        TriggerFlushAll(false);

        auto& nodeState = GetOrCreateNodeState(request->Request->GetNodeId());
        nodeState.Handles[request->Request->GetHandle()].Count++;
        nodeState.PendingRequestQueue.push_back(std::move(request));

        return future;
    }

    TFuture<TWriteDataResponse> AddRequest(
        std::unique_ptr<TCachedWriteDataRequest> request)
    {
        const ui64 nodeId = request->GetNodeId();

        auto& nodeState = GetOrCreateNodeState(nodeId);
        nodeState.Handles[request->GetHandle()].Count++;
        nodeState.Cache.PushUnflushed(std::move(request));

        UpdateFlushStatus(nodeId, nodeState);

        return MakeFuture<TWriteDataResponse>();
    }

    // nodeState becomes unusable after this call
    void EvictUnpinnedFlushedEntries(ui64 nodeId, TNodeState& nodeState)
    {
        bool entriesDeleted = false;

        const ui64 nodePinSequenceId =
            nodeState.Pins.empty() ? Max<ui64>() : *nodeState.Pins.begin();

        const ui64 globalPinSequenceId = State.GetFrontGlobalPin();

        while (nodeState.Cache.HasFlushedRequests()) {
            const ui64 sequenceId = nodeState.Cache.GetMinFlushedSequenceId();
            if (sequenceId >= nodePinSequenceId) {
                break;
            }
            if (sequenceId >= globalPinSequenceId) {
                State.NodesWithGlobalPinnedFlushedData.insert(nodeId);
                break;
            }
            auto cachedRequest = nodeState.Cache.PopFlushed();
            Storage.Evict(std::move(cachedRequest));
            entriesDeleted = true;
        }

        if (nodeState.Empty()) {
            Stats.DecrementNodeCount();
            State.Nodes.erase(nodeId);
        }

        if (!entriesDeleted) {
            return;
        }

        while (Storage.HasPendingRequests()) {
            auto request = Storage.TryProcessPendingRequest();
            if (!request) {
                TriggerFlushAll(false);
                break;
            }

            const ui64 nodeId = request->GetNodeId();
            auto& nodeState = GetOrCreateNodeState(nodeId);

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

    TNodeState& GetOrCreateNodeState(ui64 nodeId)
    {
        auto* res = State.Nodes.FindPtr(nodeId);
        if (res) {
            return *res;
        }

        Stats.IncrementNodeCount();
        return State.Nodes[nodeId];
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
    IWriteBackCacheStateListener& listener,
    ITimer& timer,
    IWriteBackCacheStats& stats,
    TWriteBackCacheStateConfig config)
    : Impl(
          std::make_unique<TImpl>(
              persistentStorage,
              listener,
              timer,
              stats,
              config))
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

ui64 TWriteBackCacheState::GetCachedDataEndOffset(ui64 nodeId) const
{
    return Impl->GetCachedDataEndOffset(nodeId);
}

TWriteBackCacheState::TPin TWriteBackCacheState::PinCachedData(ui64 nodeId)
{
    return Impl->PinCachedData(nodeId);
}

void TWriteBackCacheState::UnpinCachedData(ui64 nodeId, TPin pinId)
{
    Impl->UnpinCachedData(nodeId, pinId);
}

TWriteBackCacheState::TPin TWriteBackCacheState::PinAllCachedData()
{
    return Impl->PinAllCachedData();
}

void TWriteBackCacheState::UnpinAllCachedData(TPin pinId)
{
    Impl->UnpinAllCachedData(pinId);
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
