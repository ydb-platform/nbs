#include "request_channel.h"

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>
#include <tuple>

namespace NCloud::NIcV2 {

namespace {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

// Requests map to their response type; responses map to zero.
using TEventTypes = TMap<ui32, ui32>;

ui32 GetResponseType(const TEventTypes& eventTypes, ui32 requestType)
{
    const auto* responseType = eventTypes.FindPtr(requestType);
    return responseType ? *responseType : 0;
}

NProto::TError MakeSendError(ESendResult result)
{
    if (result == ESendResult::Accepted) {
        return {};
    }
    if (result == ESendResult::QueueFull) {
        return MakeError(E_REJECTED, "IC V2 output queue limit reached");
    }

    return MakeError(E_INVALID_STATE, "IC V2 connection is not active");
}

NProto::TError MakeRequestClosureError(const NProto::TError& error)
{
    if (error.GetCode() == E_BADMSG) {
        return MakeError(
            E_ABORTED,
            "IC V2 channel closed after a protocol error");
    }
    if (HasError(error)) {
        return error;
    }

    return MakeError(E_REJECTED, "IC V2 request channel closed");
}

TResultOrError<TEventTypes> BuildEventTypes(const TRequestChannelConfig& config)
{
    if (config.EventTypes.empty()) {
        return MakeError(E_ARGUMENT, "IC V2 event types must not be empty");
    }

    TEventTypes eventTypes;
    for (const auto& types: config.EventTypes) {
        if (!types.RequestType || !types.ResponseType) {
            return MakeError(E_ARGUMENT, "IC V2 event types must be nonzero");
        }
        if (!eventTypes.emplace(types.RequestType, types.ResponseType).second ||
            !eventTypes.emplace(types.ResponseType, 0).second)
        {
            return MakeError(E_ARGUMENT, "duplicate IC V2 event type");
        }
    }
    return eventTypes;
}

////////////////////////////////////////////////////////////////////////////////

struct TPendingRequest
{
    TInstant Deadline;
    ui32 ResponseType;
    TPromise<TResponseResult> Promise;
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestDeadline
{
    TInstant Deadline;
    ui64 Cookie;

    bool operator<(const TRequestDeadline& other) const;
};

bool TRequestDeadline::operator<(const TRequestDeadline& other) const
{
    auto makeTie = [](const TRequestDeadline& t)
    {
        return std::tie(t.Deadline, t.Cookie);
    };
    return makeTie(*this) < makeTie(other);
}

////////////////////////////////////////////////////////////////////////////////

struct IRequestTimerClient
{
    virtual ~IRequestTimerClient() = default;

    virtual void HandleTimeout(ui64 generation) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Protect timer state with the owner's lock. Call Schedule outside that lock.
class TRequestTimer final
{
public:
    struct TTask
    {
        TInstant Deadline;
        ui64 Generation;
    };

private:
    const ISchedulerPtr Scheduler;
    TInstant Deadline = TInstant::Max();
    ui64 Generation = 0;

public:
    explicit TRequestTimer(ISchedulerPtr scheduler)
        : Scheduler(std::move(scheduler))
    {}

    std::optional<TTask> Prepare(TInstant deadline)
    {
        // Reuse the earlier wakeup even if its request has already completed.
        if (Deadline <= deadline) {
            return {};
        }

        Deadline = deadline;
        return TTask{
            .Deadline = deadline,
            .Generation = ++Generation,
        };
    }

    [[nodiscard]] bool IsCurrent(ui64 generation) const
    {
        return generation == Generation;
    }

    void Finish(ui64 generation)
    {
        if (generation == Generation) {
            Deadline = TInstant::Max();
        }
    }

    void Schedule(
        std::optional<TTask> task,
        std::weak_ptr<IRequestTimerClient> client) const
    {
        if (!task) {
            return;
        }

        Scheduler->Schedule(
            nullptr,
            task->Deadline,
            [client = std::move(client), generation = task->Generation]
            {
                if (auto owner = client.lock()) {
                    owner->HandleTimeout(generation);
                }
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestState final
    : public IRequestTimerClient
    , public std::enable_shared_from_this<TRequestState>
{
    enum class EPhase
    {
        Open,
        Closing,
        Draining,
        Closed,
    };

    const ITimerPtr Timer;
    const size_t MaxPendingRequests;

    ITaskQueuePtr Completions;
    std::weak_ptr<IConnection> Connection;
    TMutex Mutex;
    THashMap<ui64, TPendingRequest> Pending;
    TSet<TRequestDeadline> Deadlines;
    std::atomic<size_t> Outstanding = 0;
    TRequestTimer RequestTimer;
    ui64 NextCookie = 1;
    std::atomic<size_t> CompletionOperations = 0;
    std::atomic<EPhase> Phase = EPhase::Open;
    NProto::TError CloseError;
    TPromise<NProto::TError> Closed = NewPromise<NProto::TError>();

public:
    TRequestState(
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ITaskQueuePtr completions,
        size_t maxPendingRequests)
        : Timer(std::move(timer))
        , MaxPendingRequests(maxPendingRequests)
        , Completions(std::move(completions))
        , RequestTimer(std::move(scheduler))
    {}

    void SetConnection(const IConnectionPtr& connection)
    {
        Connection = connection;
    }

    TResultOrError<ui64> Admit(
        TInstant deadline,
        ui32 responseType,
        TPromise<TResponseResult> promise)
    {
        ui64 cookie = 0;
        std::optional<TRequestTimer::TTask> timer;
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase != EPhase::Open) {
                return MakeError(
                    E_INVALID_STATE,
                    "IC V2 request channel closed");
            }
            if (Outstanding.load(std::memory_order_relaxed) >=
                MaxPendingRequests)
            {
                return MakeError(E_REJECTED, "IC V2 request limit reached");
            }
            if (Timer->Now() >= deadline) {
                return MakeError(E_TIMEOUT, "IC V2 request timed out");
            }

            cookie = NextCookie++;
            Pending.emplace(
                cookie,
                TPendingRequest{
                    .Deadline = deadline,
                    .ResponseType = responseType,
                    .Promise = std::move(promise),
                });
            Deadlines.insert(
                TRequestDeadline{.Deadline = deadline, .Cookie = cookie});
            Outstanding.fetch_add(1, std::memory_order_relaxed);
            timer = PrepareTimerLocked();
        }
        RequestTimer.Schedule(timer, weak_from_this());
        return cookie;
    }

    bool IsOpen() const
    {
        return Phase.load(std::memory_order_acquire) == EPhase::Open;
    }

    void FinishCompletionOperation()
    {
        // Keep the counter and phase check sequentially consistent so closure
        // and the last completion operation cannot miss each other.
        const auto previous = CompletionOperations.fetch_sub(1);
        Y_ABORT_UNLESS(previous);

        if (previous != 1 || Phase == EPhase::Open) {
            return;
        }

        TryPublishClosed();
    }

    void Complete(ui64 cookie, TResponseResult result)
    {
        TPromise<TResponseResult> promise;
        bool timedOut = false;
        bool badResponse = false;
        {
            TGuard<TMutex> guard(Mutex);
            const auto it = Pending.find(cookie);
            if (it != Pending.end()) {
                timedOut = !HasError(result.GetError()) &&
                           Timer->Now() >= it->second.Deadline;
                badResponse =
                    !HasError(result.GetError()) && !timedOut &&
                    result.GetResult()->Type != it->second.ResponseType;
                promise = std::move(it->second.Promise);
                Deadlines.erase(
                    TRequestDeadline{
                        .Deadline = it->second.Deadline,
                        .Cookie = cookie,
                    });
                Pending.erase(it);
            } else if (!result.GetResult()) {
                return;
            }
            ++CompletionOperations;
        }
        NProto::TError error;
        if (timedOut) {
            error = MakeError(E_TIMEOUT, "IC V2 request timed out");
        } else if (badResponse) {
            error = MakeError(
                E_BADMSG,
                "unexpected IC V2 response type for cookie");
            Close(error);
        }
        PublishCompletion(
            std::move(promise),
            std::move(result),
            std::move(error));
    }

    // Connection callbacks keep the executor available until this is queued.
    void DiscardEvent(std::unique_ptr<NActors::IEventHandle> event)
    {
        ++CompletionOperations;
        PublishCompletion({}, std::move(event), {});
    }

    void Close(const NProto::TError& error)
    {
        const auto keepAlive = shared_from_this();
        THashMap<ui64, TPendingRequest> pending;
        bool first = false;
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase == EPhase::Open) {
                Phase = EPhase::Closing;
                CloseError = error;
                pending.swap(Pending);
                Deadlines.clear();
                CompletionOperations += pending.size() + 1;
                first = true;
            }
        }
        // Every caller closes admission before returning, including callers
        // that race with a previous Close before it reaches the connection.
        if (auto connection = Connection.lock()) {
            connection->Close();
        }
        if (!first) {
            return;
        }
        const auto requestError = MakeRequestClosureError(error);
        for (auto& [cookie, request]: pending) {
            PublishCompletion(std::move(request.Promise), requestError, {});
        }
        FinishCompletionOperation();
    }

    void OnConnectionClosed()
    {
        // WhenClosed() already waits for transport callbacks to return.
        Close(MakeError(E_REJECTED, "IC V2 connection closed"));
        Phase = EPhase::Draining;
        TryPublishClosed();
    }

    TFuture<NProto::TError> WhenClosed() const
    {
        return Closed.GetFuture();
    }

    void HandleTimeout(ui64 generation) override
    {
        TVector<ui64> expired;
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase != EPhase::Open || !RequestTimer.IsCurrent(generation)) {
                return;
            }

            const auto now = Timer->Now();
            for (const auto& request: Deadlines) {
                if (request.Deadline > now) {
                    break;
                }
                expired.push_back(request.Cookie);
            }
        }
        for (ui64 cookie: expired) {
            Complete(cookie, MakeError(E_TIMEOUT, "IC V2 request timed out"));
        }
        std::optional<TRequestTimer::TTask> timer;
        {
            TGuard<TMutex> guard(Mutex);
            RequestTimer.Finish(generation);
            timer = PrepareTimerLocked();
        }
        RequestTimer.Schedule(timer, weak_from_this());
    }

private:
    void PublishCompletion(
        TPromise<TResponseResult> promise,
        TResponseResult result,
        NProto::TError error)
    {
        // Enqueue may return after the task finishes; wait for both.
        ++CompletionOperations;
        Completions->ExecuteSimple(
            [self = shared_from_this(),
             promise = std::move(promise),
             result = std::move(result),
             error = std::move(error)]() mutable
            {
                if (HasError(error)) {
                    result = std::move(error);
                }
                if (promise.Initialized()) {
                    self->ReleaseRequest();
                    promise.SetValue(std::move(result));
                }
                // Drop unsolicited payloads before publishing WhenClosed().
                result.ExtractResult().reset();
                self->FinishCompletionOperation();
            });
        FinishCompletionOperation();
    }

    std::optional<TRequestTimer::TTask> PrepareTimerLocked()
    {
        if (Phase != EPhase::Open || Deadlines.empty()) {
            return {};
        }

        return RequestTimer.Prepare(Deadlines.begin()->Deadline);
    }

    void ReleaseRequest()
    {
        const auto previous =
            Outstanding.fetch_sub(1, std::memory_order_relaxed);
        Y_ABORT_UNLESS(previous);
    }

    void TryPublishClosed()
    {
        ITaskQueuePtr completions;
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase != EPhase::Draining || CompletionOperations) {
                return;
            }

            Phase = EPhase::Closed;
            completions = std::move(Completions);
        }
        // The caller may destroy the executor as soon as Closed becomes ready.
        completions.reset();
        Closed.SetValue(CloseError);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestChannel;

class TIncomingRequest final: public IIncomingRequest
{
    const std::weak_ptr<TRequestChannel> Channel;
    const std::unique_ptr<NActors::IEventHandle> Event;
    const ui32 ResponseType;
    std::atomic<bool> ReplyAttempted = false;

public:
    TIncomingRequest(
        std::weak_ptr<TRequestChannel> channel,
        std::unique_ptr<NActors::IEventHandle> event,
        ui32 responseType);

    NActors::IEventHandle& GetEvent() override;
    NProto::TError Reply(
        std::unique_ptr<NActors::IEventBase> response) override;
    void CloseChannel() override;
};

////////////////////////////////////////////////////////////////////////////////

class TRequestChannel final
    : public IRequestChannel
    , public std::enable_shared_from_this<TRequestChannel>
{
    const TEventTypes EventTypes;
    const ITimerPtr Timer;
    const IRequestChannelHandlerPtr Handler;
    const std::shared_ptr<TRequestState> State;
    IConnectionPtr Connection;

public:
    TRequestChannel(
        TEventTypes eventTypes,
        ITimerPtr timer,
        IRequestChannelHandlerPtr handler,
        std::shared_ptr<TRequestState> state)
        : EventTypes(std::move(eventTypes))
        , Timer(std::move(timer))
        , Handler(std::move(handler))
        , State(std::move(state))
    {}

    ~TRequestChannel() override
    {
        Close();
    }

    void SetConnection(IConnectionPtr connection)
    {
        Connection = std::move(connection);
        State->SetConnection(Connection);
        // Keep completion state alive after destruction, without retaining the
        // channel or connection through their own completion future.
        Connection->WhenClosed().Subscribe([state = State](const auto&)
                                           { state->OnConnectionClosed(); });
    }

    bool Activate() override
    {
        const auto keepAlive = shared_from_this();
        return Connection->Activate();
    }

    TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) override
    {
        const auto keepAlive = shared_from_this();
        auto promise = NewPromise<TResponseResult>();
        auto future = promise.GetFuture();
        const ui32 responseType =
            event ? GetResponseType(EventTypes, event->Type()) : 0;
        if (!responseType) {
            promise.SetValue(
                MakeError(E_ARGUMENT, "invalid IC V2 request event type"));
            return future;
        }
        if (!timeout) {
            promise.SetValue(MakeError(E_TIMEOUT, "IC V2 request timed out"));
            return future;
        }
        const auto deadline = Timer->Now() + timeout;
        auto [cookie, error] = State->Admit(deadline, responseType, promise);
        if (HasError(error)) {
            promise.SetValue(std::move(error));
            return future;
        }
        auto sendError = Send(cookie, std::move(event));
        if (HasError(sendError)) {
            State->Complete(cookie, std::move(sendError));
        }
        return future;
    }

    void Close() override
    {
        const auto keepAlive = weak_from_this().lock();
        State->Close({});
    }

    TFuture<NProto::TError> WhenClosed() const override
    {
        return State->WhenClosed();
    }

    void HandleEvent(std::unique_ptr<NActors::IEventHandle> event) noexcept
    {
        if (!State->IsOpen()) {
            State->DiscardEvent(std::move(event));
            return;
        }
        try {
            const auto* responseType = EventTypes.FindPtr(event->Type);
            if (responseType && !*responseType) {
                const ui64 cookie = event->Cookie;
                State->Complete(cookie, std::move(event));
            } else if (responseType && event->Cookie) {
                Handler->HandleRequest(
                    std::make_unique<TIncomingRequest>(
                        weak_from_this(),
                        std::move(event),
                        *responseType));
            } else {
                State->DiscardEvent(std::move(event));
                State->Close(MakeError(
                    E_BADMSG,
                    "unexpected IC V2 event type or cookie"));
            }
        } catch (...) {
            State->Close(MakeError(E_FAIL, "IC V2 request handler failed"));
        }
    }

    void HandleDisconnect(const TString& reason) noexcept
    {
        State->Close(MakeError(E_TRANSPORT_ERROR, reason));
    }

    NProto::TError Send(
        ui64 cookie,
        std::unique_ptr<NActors::IEventBase> event) noexcept
    {
        try {
            return MakeSendError(Connection->Send(
                std::make_unique<NActors::IEventHandle>(
                    NActors::TActorId(),
                    NActors::TActorId(),
                    event.release(),
                    0,
                    cookie)));
        } catch (...) {
            auto error = MakeError(
                E_TRANSPORT_ERROR,
                "IC V2 send failed: " + CurrentExceptionMessage());
            State->Close(error);
            return error;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TIncomingRequest::TIncomingRequest(
    std::weak_ptr<TRequestChannel> channel,
    std::unique_ptr<NActors::IEventHandle> event,
    ui32 responseType)
    : Channel(std::move(channel))
    , Event(std::move(event))
    , ResponseType(responseType)
{}

NActors::IEventHandle& TIncomingRequest::GetEvent()
{
    return *Event;
}

NProto::TError TIncomingRequest::Reply(
    std::unique_ptr<NActors::IEventBase> response)
{
    if (ReplyAttempted.exchange(true)) {
        return MakeError(E_INVALID_STATE, "IC V2 reply already attempted");
    }

    if (!response || response->Type() != ResponseType) {
        return MakeError(E_ARGUMENT, "invalid IC V2 response type");
    }

    auto channel = Channel.lock();
    if (!channel) {
        return MakeError(E_INVALID_STATE, "IC V2 request channel destroyed");
    }
    return channel->Send(Event->Cookie, std::move(response));
}

void TIncomingRequest::CloseChannel()
{
    if (auto channel = Channel.lock()) {
        channel->Close();
    }
}

////////////////////////////////////////////////////////////////////////////////

// The connection owns its handler. The adapter breaks the ownership cycle so
// dropping the last channel reference also closes it and fails its requests.
class TWeakHandler final: public IConnectionHandler
{
    const std::weak_ptr<TRequestChannel> Channel;
    const std::shared_ptr<TRequestState> State;

public:
    TWeakHandler(
        std::weak_ptr<TRequestChannel> channel,
        std::shared_ptr<TRequestState> state)
        : Channel(std::move(channel))
        , State(std::move(state))
    {}

    void HandleEvent(
        std::unique_ptr<NActors::IEventHandle> event) noexcept override
    {
        if (auto channel = Channel.lock()) {
            channel->HandleEvent(std::move(event));
        } else {
            State->DiscardEvent(std::move(event));
        }
    }

    void HandleDisconnect(const TString& reason) noexcept override
    {
        if (auto channel = Channel.lock()) {
            channel->HandleDisconnect(reason);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IIncomingRequest::~IIncomingRequest() = default;
IRequestChannelHandler::~IRequestChannelHandler() = default;
IRequestChannel::~IRequestChannel() = default;

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IRequestChannelPtr> CreateRequestChannel(
    const ITransportPtr& transport,
    TSocketPtr socket,
    TSocketPtr xdcSocket,
    const TRequestChannelConfig& config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions,
    IRequestChannelHandlerPtr handler)
{
    if (!transport || !socket || !timer || !scheduler || !completions ||
        !handler || !config.MaxPendingRequests)
    {
        return MakeError(
            E_ARGUMENT,
            "invalid IC V2 request channel configuration");
    }
    auto [eventTypes, error] = BuildEventTypes(config);
    if (HasError(error)) {
        return error;
    }

    auto state = std::make_shared<TRequestState>(
        timer,
        std::move(scheduler),
        std::move(completions),
        config.MaxPendingRequests);
    auto channel = std::make_shared<TRequestChannel>(
        std::move(eventTypes),
        std::move(timer),
        std::move(handler),
        state);
    auto connection = transport->CreateConnection(
        std::move(socket),
        std::move(xdcSocket),
        std::make_shared<TWeakHandler>(channel, std::move(state)),
        config.SendPings);
    if (!connection) {
        return MakeError(
            E_REJECTED,
            "IC V2 transport refused the request channel");
    }
    channel->SetConnection(std::move(connection));
    return IRequestChannelPtr(std::move(channel));
}

}   // namespace NCloud::NIcV2
