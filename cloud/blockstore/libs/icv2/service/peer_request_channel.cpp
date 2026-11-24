#include "peer_request_channel.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <util/generic/map.h>
#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/system/spinlock.h>

#include <algorithm>
#include <atomic>
#include <optional>

namespace NCloud::NBlockStore::NIcV2 {

using namespace NThreading;

using NCloud::NIcV2::IPeerPtr;
using NCloud::NIcV2::IRequestChannel;
using NCloud::NIcV2::TChannelFuture;
using NCloud::NIcV2::TChannelResult;
using NCloud::NIcV2::TResponseFuture;
using NCloud::NIcV2::TResponseResult;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError RejectTransportError(NProto::TError error)
{
    error.SetCode(E_REJECTED);
    return error;
}

TDataResponseResult ClassifyChannelResult(
    TResponseResult result,
    std::weak_ptr<IRequestChannel> source)
{
    const auto& error = result.GetError();
    if (HasError(error)) {
        const auto code = error.GetCode();
        if (code == E_REJECTED || code == E_ARGUMENT || code == E_TIMEOUT) {
            return error;
        }
        return RejectTransportError(error);
    }
    return TDataResponse{
        .Event = result.ExtractResult(),
        .SourceChannel = std::move(source),
    };
}

////////////////////////////////////////////////////////////////////////////////

struct TClientRequest
{
    std::unique_ptr<NActors::IEventBase> Event;
    TInstant Deadline;
    TPromise<TDataResponseResult> Promise;
};

struct TWaitingRequest
{
    TFutureStateId Channel;
    TClientRequest Request;
};

using TWaitingRequests = TMultiMap<TInstant, TWaitingRequest>;

////////////////////////////////////////////////////////////////////////////////

class TPeerRequestChannel final
    : public IDataRequestChannel
    , public std::enable_shared_from_this<TPeerRequestChannel>
{
    enum class EPhase : ui32
    {
        Running,
        Stopping,
        Stopped,
    };

    struct TState
    {
        ui32 Operations = 0;
        EPhase Phase = EPhase::Running;
    };

    static_assert(std::atomic<TState>::is_always_lock_free);
    const IPeerPtr Peer;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ITaskQueuePtr Completions;

    std::atomic<TState> State{TState{}};
    TPromise<void> Closed = NewPromise<void>();

    TAdaptiveLock WaitingLock;
    TWaitingRequests WaitingRequests;
    TVector<TChannelFuture> SubscribedChannels;
    std::optional<TInstant> ScheduledDeadline;
    ui64 TimerGeneration = 0;

public:
    TPeerRequestChannel(
        IPeerPtr peer,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ITaskQueuePtr completions)
        : Peer(std::move(peer))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Completions(std::move(completions))
    {}

    TDataResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) override
    {
        Y_DEBUG_ABORT_UNLESS(event);

        const auto keepAlive = shared_from_this();
        const auto deadline = Timer->Now() + timeout;
        // One operation for this call and one until result delivery finishes.
        if (!TryReserveOperations(2)) {
            return MakeFuture<TDataResponseResult>(MakeClosedError());
        }
        Y_DEFER
        {
            ReleaseOperations(1);
        };

        TClientRequest request{
            .Event = std::move(event),
            .Deadline = deadline,
            .Promise = NewPromise<TDataResponseResult>(),
        };
        auto future = request.Promise.GetFuture();
        const auto channel = Peer->GetChannel();
        if (channel.HasValue()) {
            DispatchRequest(std::move(request), channel.GetValue());
        } else {
            EnqueueForChannel(std::move(request), channel);
        }
        return future;
    }

    TFuture<void> Close() override
    {
        auto closed = Closed.GetFuture();
        if (!TryBeginStop()) {
            return closed;
        }

        const auto keepAlive = shared_from_this();
        TWaitingRequests waiting;
        TVector<TChannelFuture> subscribed;
        {
            TGuard<TAdaptiveLock> guard(WaitingLock);
            waiting.swap(WaitingRequests);
            subscribed.swap(SubscribedChannels);
            ScheduledDeadline.reset();
        }
        subscribed.clear();
        if (!waiting.empty()) {
            Completions->ExecuteSimple(
                [self = shared_from_this(),
                 waiting = std::move(waiting)]() mutable
                {
                    const auto error = MakeClosedError();
                    for (auto& [deadline, waiter]: waiting) {
                        waiter.Request.Promise.SetValue(error);
                    }
                    const auto count = static_cast<ui32>(waiting.size());
                    waiting.clear();
                    self->ReleaseOperations(count);
                });
        }
        Peer->Close();
        ReleaseOperations(1);
        return closed;
    }

private:
    static NProto::TError MakeClosedError()
    {
        return MakeError(E_REJECTED, "IC V2 peer request channel is closed");
    }

    void EnqueueForChannel(
        TClientRequest request,
        const TChannelFuture& channel)
    {
        const auto channelId = channel.StateId();
        bool subscribe = false;
        {
            TGuard<TAdaptiveLock> guard(WaitingLock);
            if (!IsRunning()) {
                guard.Release();
                EnqueueError(std::move(request.Promise), MakeClosedError());
                return;
            }
            WaitingRequests.emplace(
                request.Deadline,
                TWaitingRequest{
                    .Channel = *channelId,
                    .Request = std::move(request),
                });
            if (std::ranges::find(
                    SubscribedChannels,
                    channelId,
                    &TChannelFuture::StateId) == SubscribedChannels.end())
            {
                SubscribedChannels.push_back(channel);
                subscribe = true;
            }
        }
        if (subscribe) {
            channel.Subscribe(
                [weak = weak_from_this()](const TChannelFuture& future)
                {
                    if (auto self = weak.lock()) {
                        self->HandleChannelResult(future);
                    }
                });
        }
        ArmTimer();
    }

    void HandleChannelResult(const TChannelFuture& future)
    {
        if (!TryReserveOperations(1)) {
            return;
        }

        const auto channelId = future.StateId();
        TVector<TClientRequest> waiting;
        {
            TGuard<TAdaptiveLock> guard(WaitingLock);
            std::erase_if(
                SubscribedChannels,
                [channelId](const TChannelFuture& channel)
                { return channel.StateId() == channelId; });
            for (auto it = WaitingRequests.begin();
                 it != WaitingRequests.end();)
            {
                if (it->second.Channel == *channelId) {
                    waiting.push_back(std::move(it->second.Request));
                    it = WaitingRequests.erase(it);
                } else {
                    ++it;
                }
            }
        }
        const auto& result = future.GetValue();
        for (auto& request: waiting) {
            DispatchRequest(std::move(request), result);
        }
        ReleaseOperations(1);
    }

    void DispatchRequest(TClientRequest request, const TChannelResult& result)
    {
        if (!IsRunning()) {
            EnqueueError(std::move(request.Promise), MakeClosedError());
            return;
        }
        if (const auto& error = result.GetError(); HasError(error)) {
            EnqueueError(
                std::move(request.Promise),
                IsConnectionError(error) ? error : RejectTransportError(error));
            return;
        }

        const auto& channel = result.GetResult();
        channel
            ->Request(std::move(request.Event), request.Deadline - Timer->Now())
            .Subscribe(
                [self = shared_from_this(),
                 promise = std::move(request.Promise),
                 source = std::weak_ptr<IRequestChannel>(channel)]   //
                (TResponseFuture future) mutable
                {
                    self->CompleteRequest(
                        std::move(promise),
                        ClassifyChannelResult(
                            future.ExtractValue(),
                            std::move(source)));
                });
    }

    void EnqueueError(
        TPromise<TDataResponseResult> promise,
        NProto::TError error)
    {
        Completions->ExecuteSimple(
            [self = shared_from_this(),
             promise = std::move(promise),
             error = std::move(error)]() mutable
            { self->CompleteRequest(std::move(promise), std::move(error)); });
    }

    void CompleteRequest(
        TPromise<TDataResponseResult> promise,
        TDataResponseResult result)
    {
        promise.SetValue(std::move(result));
        ReleaseOperations(1);
    }

    void ArmTimer()
    {
        TInstant deadline;
        ui64 generation = 0;
        {
            TGuard<TAdaptiveLock> guard(WaitingLock);
            if (!IsRunning() || WaitingRequests.empty()) {
                return;
            }

            deadline = WaitingRequests.begin()->first;
            if (ScheduledDeadline && *ScheduledDeadline <= deadline) {
                return;
            }

            ScheduledDeadline = deadline;
            generation = ++TimerGeneration;
        }
        Scheduler->Schedule(
            nullptr,
            deadline,
            [weak = weak_from_this(), generation]
            {
                if (auto self = weak.lock()) {
                    self->OnTimer(generation);
                }
            });
    }

    void OnTimer(ui64 generation)
    {
        if (!TryReserveOperations(1)) {
            return;
        }
        Y_DEFER
        {
            ReleaseOperations(1);
        };

        TVector<TClientRequest> expired;
        {
            TGuard<TAdaptiveLock> guard(WaitingLock);
            if (!ScheduledDeadline || generation != TimerGeneration) {
                return;
            }

            ScheduledDeadline.reset();
            const auto now = Timer->Now();
            while (!WaitingRequests.empty() &&
                   WaitingRequests.begin()->first <= now)
            {
                const auto request = WaitingRequests.begin();
                expired.push_back(std::move(request->second.Request));
                WaitingRequests.erase(request);
            }
        }
        for (auto& request: expired) {
            EnqueueError(
                std::move(request.Promise),
                MakeError(E_TIMEOUT, "IC V2 request timed out"));
        }
        ArmTimer();
    }

    bool TryReserveOperations(ui32 count)
    {
        auto state = State.load();
        while (state.Phase == EPhase::Running) {
            auto next = state;
            next.Operations += count;
            if (State.compare_exchange_weak(state, next)) {
                return true;
            }
        }
        return false;
    }

    bool TryBeginStop()
    {
        auto state = State.load();
        while (state.Phase == EPhase::Running) {
            auto next = state;
            next.Phase = EPhase::Stopping;
            ++next.Operations;
            if (State.compare_exchange_weak(state, next)) {
                return true;
            }
        }
        return false;
    }

    bool IsRunning() const
    {
        return State.load().Phase == EPhase::Running;
    }

    void ReleaseOperations(ui32 count)
    {
        auto state = State.load();
        for (;;) {
            Y_ABORT_UNLESS(state.Operations >= count);

            auto next = state;
            next.Operations -= count;
            if (next.Phase == EPhase::Stopping && !next.Operations) {
                next.Phase = EPhase::Stopped;
            }
            if (State.compare_exchange_weak(state, next)) {
                if (next.Phase == EPhase::Stopped) {
                    Closed.SetValue();
                }
                return;
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDataRequestChannelPtr CreatePeerRequestChannel(
    IPeerPtr peer,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions)
{
    Y_DEBUG_ABORT_UNLESS(peer && timer && scheduler && completions);

    return std::make_shared<TPeerRequestChannel>(
        std::move(peer),
        std::move(timer),
        std::move(scheduler),
        std::move(completions));
}

}   // namespace NCloud::NBlockStore::NIcV2
