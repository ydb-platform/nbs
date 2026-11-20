#include "request_client.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <util/generic/hash.h>
#include <util/generic/scope.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>

namespace NCloud::NBlockStore::NIcV2 {

using namespace NThreading;

using NCloud::NIcV2::IPeerPtr;
using NCloud::NIcV2::TChannelFuture;
using NCloud::NIcV2::TChannelResult;
using NCloud::NIcV2::TResponseFuture;
using NCloud::NIcV2::TResponseResult;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeTimeoutError()
{
    return MakeError(E_TIMEOUT, "IC V2 request timed out");
}

////////////////////////////////////////////////////////////////////////////////

struct TClientRequest
{
    std::unique_ptr<NActors::IEventBase> Event;
    TInstant Deadline;
    TPromise<TResponseResult> Promise;
};

////////////////////////////////////////////////////////////////////////////////

class TPeerRequestClient final
    : public IPeerRequestClient
    , public std::enable_shared_from_this<TPeerRequestClient>
{
    enum class EPhase: ui32
    {
        Created,
        Running,
        Stopping,
        Stopped,
    };

    struct TState
    {
        ui32 Operations = 0;
        EPhase Phase = EPhase::Created;
    };

    static_assert(std::atomic<TState>::is_always_lock_free);
    static constexpr ui32 RequestOperationCount = 3;

    const IPeerPtr Peer;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ITaskQueuePtr Completions;

    std::atomic<TState> State{TState{}};
    TPromise<void> Closed = NewPromise<void>();

    TMutex WaitingMutex;
    // Keep the subscription even if every waiter expires.
    std::atomic<bool> WaitingForChannel = false;
    THashMap<ui64, TClientRequest> WaitingRequests;
    TSet<std::pair<TInstant, ui64>> ConnectionDeadlines;
    std::optional<TInstant> ScheduledDeadline;
    ui64 TimerGeneration = 0;
    ui64 NextRequestId = 1;

public:
    TPeerRequestClient(
        IPeerPtr peer,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ITaskQueuePtr completions)
        : Peer(std::move(peer))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Completions(std::move(completions))
    {}

    ~TPeerRequestClient() override
    {
        Stop();
    }

    // IPeerRequestClient implementation

    void Start() override
    {
        TState expected;
        Y_ABORT_UNLESS(State.compare_exchange_strong(
            expected,
            TState{.Operations = 0, .Phase = EPhase::Running}));
    }

    TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TInstant deadline) override
    {
        const auto keepAlive = shared_from_this();
        // The request call, completion task and Enqueue may finish separately.
        if (!TryReserveOperations(RequestOperationCount)) {
            return MakeFuture<TResponseResult>(MakeCancelledError());
        }
        Y_DEFER
        {
            ReleaseOperations(1);
        };

        if (!event) {
            ReleaseOperations(RequestOperationCount - 1);
            return MakeFuture<TResponseResult>(
                MakeError(E_ARGUMENT, "empty IC V2 request event"));
        }
        if (Timer->Now() >= deadline) {
            ReleaseOperations(RequestOperationCount - 1);
            return MakeFuture<TResponseResult>(MakeTimeoutError());
        }

        TClientRequest request{
            .Event = std::move(event),
            .Deadline = deadline,
            .Promise = NewPromise<TResponseResult>(),
        };
        auto future = request.Promise.GetFuture();
        TChannelFuture channel;
        if (!WaitingForChannel.load()) {
            channel = Peer->GetChannel();
            if (channel.HasValue()) {
                DispatchRequest(std::move(request), channel.GetValue());
                return future;
            }
        }
        EnqueueForChannel(std::move(request), std::move(channel));
        return future;
    }

    TFuture<void> WhenClosed() const override
    {
        return Closed.GetFuture();
    }

    void RequestStop() override
    {
        if (!TryBeginStop()) {
            return;
        }

        THashMap<ui64, TClientRequest> waiting;
        {
            TGuard<TMutex> guard(WaitingMutex);
            waiting.swap(WaitingRequests);
            WaitingForChannel.store(false);
            ConnectionDeadlines.clear();
            ScheduledDeadline.reset();
        }
        if (!waiting.empty()) {
            const auto count = static_cast<ui32>(waiting.size());
            Completions->ExecuteSimple(
                [self = shared_from_this(),
                 waiting = std::move(waiting),
                 count]() mutable
                {
                    const auto error = MakeCancelledError();
                    for (auto& [id, request]: waiting) {
                        request.Promise.SetValue(error);
                    }
                    self->ReleaseOperations(count);
                });
            ReleaseOperations(count);
        }
        Peer->Close();
        ReleaseOperations(1);
    }

    void Stop() override
    {
        RequestStop();
        Closed.GetFuture().GetValueSync();
    }

private:
    static NProto::TError MakeCancelledError()
    {
        return MakeError(E_CANCELLED, "IC V2 request client is stopping");
    }

    void EnqueueForChannel(TClientRequest request, TChannelFuture channel)
    {
        bool subscribe = false;
        {
            TGuard<TMutex> guard(WaitingMutex);
            if (!IsRunning()) {
                guard.Release();
                EnqueueCompletion(
                    std::move(request.Promise),
                    MakeCancelledError());
                return;
            }
            const ui64 id = NextRequestId++;
            ConnectionDeadlines.emplace(request.Deadline, id);
            WaitingRequests.emplace(id, std::move(request));
            if (!WaitingForChannel.load()) {
                WaitingForChannel.store(true);
                subscribe = true;
            }
        }
        if (subscribe) {
            if (!channel.Initialized()) {
                channel = Peer->GetChannel();
            }
            channel.Subscribe(
                [weak = weak_from_this()](const TChannelFuture& future)
                {
                    if (auto self = weak.lock()) {
                        self->HandleChannelResult(future.GetValue());
                    }
                });
        }
        ArmTimer();
    }

    void HandleChannelResult(const TChannelResult& result)
    {
        if (!TryReserveOperations(1)) {
            return;
        }

        THashMap<ui64, TClientRequest> waiting;
        {
            TGuard<TMutex> guard(WaitingMutex);
            waiting.swap(WaitingRequests);
            ConnectionDeadlines.clear();
            ScheduledDeadline.reset();
            WaitingForChannel.store(false);
        }
        for (auto& [id, request]: waiting) {
            DispatchRequest(std::move(request), result);
        }
        ReleaseOperations(1);
    }

    void DispatchRequest(TClientRequest request, const TChannelResult& result)
    {
        if (!IsRunning()) {
            EnqueueCompletion(std::move(request.Promise), MakeCancelledError());
            return;
        }
        const auto now = Timer->Now();
        if (now >= request.Deadline) {
            EnqueueCompletion(std::move(request.Promise), MakeTimeoutError());
            return;
        }
        if (HasError(result.GetError())) {
            EnqueueCompletion(std::move(request.Promise), result.GetError());
            return;
        }

        result.GetResult()
            ->Request(std::move(request.Event), request.Deadline - now)
            .Subscribe(
                [self = shared_from_this(),
                 promise = std::move(request.Promise)]   //
                (TResponseFuture future) mutable
                {
                    self->EnqueueCompletion(
                        std::move(promise),
                        future.ExtractValue());
                });
    }

    void EnqueueCompletion(
        TPromise<TResponseResult> promise,
        TResponseResult result)
    {
        Completions->ExecuteSimple(
            [self = shared_from_this(),
             promise = std::move(promise),
             result = std::move(result)]() mutable
            {
                if (!self->IsRunning()) {
                    result = MakeCancelledError();
                }
                promise.SetValue(std::move(result));
                self->ReleaseOperations(1);
            });
        ReleaseOperations(1);
    }

    void ArmTimer()
    {
        TInstant deadline;
        ui64 generation = 0;
        {
            TGuard<TMutex> guard(WaitingMutex);
            if (!IsRunning() || ConnectionDeadlines.empty()) {
                return;
            }

            deadline = ConnectionDeadlines.begin()->first;
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
            TGuard<TMutex> guard(WaitingMutex);
            if (!ScheduledDeadline || generation != TimerGeneration) {
                return;
            }

            ScheduledDeadline.reset();
            const auto now = Timer->Now();
            while (!ConnectionDeadlines.empty() &&
                   ConnectionDeadlines.begin()->first <= now)
            {
                const auto deadline = ConnectionDeadlines.begin();
                const auto request = WaitingRequests.find(deadline->second);
                Y_ABORT_UNLESS(request != WaitingRequests.end());

                expired.push_back(std::move(request->second));
                WaitingRequests.erase(request);
                ConnectionDeadlines.erase(deadline);
            }
        }
        for (auto& request: expired) {
            EnqueueCompletion(std::move(request.Promise), MakeTimeoutError());
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
        while (state.Phase == EPhase::Created || state.Phase == EPhase::Running)
        {
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

TResultOrError<IPeerRequestClientPtr> CreatePeerRequestClient(
    IPeerPtr peer,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions)
{
    if (!peer || !timer || !scheduler || !completions) {
        return MakeError(
            E_ARGUMENT,
            "invalid IC V2 request client configuration");
    }
    return IPeerRequestClientPtr(
        std::make_shared<TPeerRequestClient>(
            std::move(peer),
            std::move(timer),
            std::move(scheduler),
            std::move(completions)));
}

}   // namespace NCloud::NBlockStore::NIcV2
