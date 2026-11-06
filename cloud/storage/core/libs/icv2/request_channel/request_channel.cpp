#include "request_channel.h"

#include <contrib/ydb/library/actors/core/scheduler_cookie.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <atomic>
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

    return MakeError(E_REJECTED, "IC V2 connection is not active");
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
        ui64 Generation = 0;
        std::shared_ptr<NActors::TSchedulerCookieHolder> Cookie;
    };

private:
    const ISchedulerPtr Scheduler;
    NActors::TSchedulerCookieHolder Cookie;
    TInstant Deadline;
    ui64 Generation = 0;

public:
    explicit TRequestTimer(ISchedulerPtr scheduler)
        : Scheduler(std::move(scheduler))
    {}

    // TODO: Remove cancelled tasks from the scheduler before their deadlines.
    TTask Prepare(TInstant deadline)
    {
        if (Cookie.Get() && Deadline <= deadline) {
            return {};
        }

        auto* cookie = NActors::ISchedulerCookie::Make2Way();
        auto scheduledCookie =
            std::make_shared<NActors::TSchedulerCookieHolder>(cookie);
        Cookie.Reset(cookie);
        Deadline = deadline;
        return {
            .Deadline = deadline,
            .Generation = ++Generation,
            .Cookie = std::move(scheduledCookie),
        };
    }

    void Cancel()
    {
        Cookie.Detach();
    }

    [[nodiscard]] bool IsCurrent(ui64 generation) const
    {
        return generation == Generation && Cookie.Get() != nullptr;
    }

    void Finish(ui64 generation)
    {
        if (generation == Generation) {
            Cancel();
        }
    }

    void Schedule(TTask task, std::weak_ptr<IRequestTimerClient> client) const
    {
        if (!task.Cookie) {
            return;
        }

        Scheduler->Schedule(
            nullptr,
            task.Deadline,
            [client = std::move(client),
             generation = task.Generation,
             cookie = std::move(task.Cookie)]
            {
                if (cookie->Detach()) {
                    if (auto owner = client.lock()) {
                        owner->HandleTimeout(generation);
                    }
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
    const ITaskQueuePtr Completions;
    const size_t MaxPendingRequests;

    std::weak_ptr<IConnection> Connection;
    TMutex Mutex;
    THashMap<ui64, TPendingRequest> Pending;
    TSet<TRequestDeadline> Deadlines;
    size_t Outstanding = 0;
    TRequestTimer RequestTimer;
    ui64 NextCookie = 1;
    size_t Callbacks = 0;
    EPhase Phase = EPhase::Open;
    NProto::TError CloseError;
    TPromise<NProto::TError> Closed = NewPromise<NProto::TError>();

public:
    TRequestState(
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ITaskQueuePtr completions,
        size_t maxPendingRequests)
        : Timer(std::move(timer))
        , Completions(std::move(completions))
        , MaxPendingRequests(maxPendingRequests)
        , RequestTimer(std::move(scheduler))
    {}

    void SetConnection(const IConnectionPtr& connection)
    {
        Connection = connection;
    }

    ui64
    Add(TInstant deadline, ui32 responseType, TPromise<TResponseResult> promise)
    {
        ui64 cookie = 0;
        TRequestTimer::TTask timer;
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase != EPhase::Open || Outstanding >= MaxPendingRequests) {
                return 0;
            }
            Y_ABORT_UNLESS(NextCookie, "IC V2 request cookies exhausted");
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
            ++Outstanding;
            timer = PrepareTimerLocked();
        }
        RequestTimer.Schedule(std::move(timer), weak_from_this());
        return cookie;
    }

    bool AdmitSend(ui64 cookie)
    {
        bool expired = false;
        {
            TGuard<TMutex> guard(Mutex);
            const auto it = Pending.find(cookie);
            if (it == Pending.end()) {
                return false;
            }

            expired = Timer->Now() >= it->second.Deadline;
        }
        if (expired) {
            Complete(cookie, MakeError(E_TIMEOUT, "IC V2 request timed out"));
            return false;
        }

        return true;
    }

    bool BeginCallback()
    {
        TGuard<TMutex> guard(Mutex);
        if (Phase != EPhase::Open) {
            return false;
        }
        ++Callbacks;
        return true;
    }

    void FinishCallback()
    {
        bool publish = false;
        {
            TGuard<TMutex> guard(Mutex);
            Y_ABORT_UNLESS(Callbacks);

            --Callbacks;
            publish = PrepareClosedLocked();
        }
        if (publish) {
            Closed.SetValue(CloseError);
        }
    }

    void Complete(ui64 cookie, TResponseResult result)
    {
        TPromise<TResponseResult> promise;
        bool timedOut = false;
        bool badResponse = false;
        TRequestTimer::TTask timer;
        {
            TGuard<TMutex> guard(Mutex);
            const auto it = Pending.find(cookie);
            if (it == Pending.end()) {
                return;
            }
            timedOut = !HasError(result.GetError()) &&
                       Timer->Now() >= it->second.Deadline;
            badResponse = !HasError(result.GetError()) && !timedOut &&
                          result.GetResult()->Type != it->second.ResponseType;
            promise = std::move(it->second.Promise);
            Deadlines.erase(
                TRequestDeadline{
                    .Deadline = it->second.Deadline,
                    .Cookie = cookie,
                });
            Pending.erase(it);
            ++Callbacks;
            timer = PrepareTimerLocked();
        }
        RequestTimer.Schedule(std::move(timer), weak_from_this());
        if (timedOut) {
            // Releasing a response may run its event destructor. Keep this
            // outside the state lock, just like promise continuations.
            result = MakeError(E_TIMEOUT, "IC V2 request timed out");
        } else if (badResponse) {
            result = MakeError(
                E_BADMSG,
                "unexpected IC V2 response type for cookie");
            Close(result.GetError());
        }
        PublishResult(std::move(promise), std::move(result));
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
                RequestTimer.Cancel();
                Callbacks += pending.size() + 1;
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
        const auto requestError =
            HasError(error)
                ? error
                : MakeError(E_REJECTED, "IC V2 request channel closed");
        for (auto& [cookie, request]: pending) {
            PublishResult(std::move(request.Promise), requestError);
        }
        FinishCallback();
    }

    void OnConnectionClosed()
    {
        Close(MakeError(E_REJECTED, "IC V2 connection closed"));
        bool publish = false;
        {
            TGuard<TMutex> guard(Mutex);
            Phase = EPhase::Draining;
            publish = PrepareClosedLocked();
        }
        if (publish) {
            Closed.SetValue(CloseError);
        }
    }

    TFuture<NProto::TError> WhenClosed() const
    {
        return Closed.GetFuture();
    }

private:
    void PublishResult(
        TPromise<TResponseResult> promise,
        TResponseResult result)
    {
        Completions->ExecuteSimple(
            [self = shared_from_this(),
             promise = std::move(promise),
             result = std::move(result)]() mutable
            {
                self->ReleaseRequests(1);
                promise.SetValue(std::move(result));
                self->FinishCallback();
            });
    }

    TRequestTimer::TTask PrepareTimerLocked()
    {
        if (Phase != EPhase::Open || Deadlines.empty()) {
            RequestTimer.Cancel();
            return {};
        }

        return RequestTimer.Prepare(Deadlines.begin()->Deadline);
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
        TRequestTimer::TTask timer;
        {
            TGuard<TMutex> guard(Mutex);
            RequestTimer.Finish(generation);
            timer = PrepareTimerLocked();
        }
        RequestTimer.Schedule(std::move(timer), weak_from_this());
    }

    void ReleaseRequests(size_t count)
    {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(Outstanding >= count);

        Outstanding -= count;
    }

    bool PrepareClosedLocked()
    {
        if (Phase != EPhase::Draining || Callbacks) {
            return false;
        }

        Phase = EPhase::Closed;
        return true;
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
    , public IConnectionHandler
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
        ui32 maxPendingRequests,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ITaskQueuePtr completions,
        IRequestChannelHandlerPtr handler)
        : EventTypes(std::move(eventTypes))
        , Timer(std::move(timer))
        , Handler(std::move(handler))
        , State(
              std::make_shared<TRequestState>(
                  Timer,
                  std::move(scheduler),
                  std::move(completions),
                  maxPendingRequests))
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
        const ui64 cookie = State->Add(deadline, responseType, promise);
        if (!cookie) {
            promise.SetValue(MakeError(
                E_REJECTED,
                "IC V2 channel closed or request limit reached"));
            return future;
        }
        if (State->AdmitSend(cookie)) {
            const auto result = Send(cookie, std::move(event));
            if (result != ESendResult::Accepted) {
                State->Complete(cookie, MakeSendError(result));
            }
        }
        return future;
    }

    ESendResult SendResponse(
        ui64 cookie,
        std::unique_ptr<NActors::IEventBase> event)
    {
        return Send(cookie, std::move(event));
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

    void HandleEvent(
        std::unique_ptr<NActors::IEventHandle> event) noexcept override
    {
        if (!State->BeginCallback()) {
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
                State->Close(MakeError(
                    E_BADMSG,
                    "unexpected IC V2 event type or cookie"));
            }
        } catch (...) {
            State->Close(MakeError(E_FAIL, "IC V2 request handler failed"));
        }
        State->FinishCallback();
    }

    void HandleDisconnect(const TString& reason) noexcept override
    {
        State->Close(MakeError(E_TRANSPORT_ERROR, reason));
    }

private:
    ESendResult Send(ui64 cookie, std::unique_ptr<NActors::IEventBase> event)
    {
        return Connection->Send(
            std::make_unique<NActors::IEventHandle>(
                NActors::TActorId(),
                NActors::TActorId(),
                event.release(),
                0,
                cookie));
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
        return MakeError(E_REJECTED, "IC V2 request channel destroyed");
    }
    return MakeSendError(
        channel->SendResponse(Event->Cookie, std::move(response)));
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

public:
    explicit TWeakHandler(std::weak_ptr<TRequestChannel> channel)
        : Channel(std::move(channel))
    {}

    void HandleEvent(
        std::unique_ptr<NActors::IEventHandle> event) noexcept override
    {
        if (auto channel = Channel.lock()) {
            channel->HandleEvent(std::move(event));
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

    auto channel = std::make_shared<TRequestChannel>(
        std::move(eventTypes),
        config.MaxPendingRequests,
        std::move(timer),
        std::move(scheduler),
        std::move(completions),
        std::move(handler));
    auto connection = transport->CreateConnection(
        std::move(socket),
        std::move(xdcSocket),
        std::make_shared<TWeakHandler>(channel),
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
