#include "request_channel.h"

#include <util/generic/hash.h>

#include <mutex>
#include <set>
#include <vector>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 GetResponseType(const TRequestChannelConfig& config, ui32 requestType)
{
    for (const auto& types: config.EventTypes) {
        if (types.RequestType == requestType) {
            return types.ResponseType;
        }
    }
    return 0;
}

bool IsResponseType(const TRequestChannelConfig& config, ui32 responseType)
{
    for (const auto& types: config.EventTypes) {
        if (types.ResponseType == responseType) {
            return true;
        }
    }
    return false;
}

bool ValidateEventTypes(const TRequestChannelConfig& config)
{
    if (config.EventTypes.empty()) {
        return false;
    }
    for (size_t i = 0; i < config.EventTypes.size(); ++i) {
        const auto& types = config.EventTypes[i];
        if (!types.RequestType || !types.ResponseType ||
            types.RequestType == types.ResponseType)
        {
            return false;
        }
        for (size_t j = 0; j < i; ++j) {
            const auto& previous = config.EventTypes[j];
            if (types.RequestType == previous.RequestType ||
                types.RequestType == previous.ResponseType ||
                types.ResponseType == previous.RequestType ||
                types.ResponseType == previous.ResponseType)
            {
                return false;
            }
        }
    }
    return true;
}

struct TPendingRequest
{
    TInstant Deadline;
    ui32 ResponseType;
    NThreading::TPromise<TResponseResult> Promise;
};

class TRequestState: public std::enable_shared_from_this<TRequestState>
{
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ITaskQueuePtr Completions;
    const size_t MaxPendingRequests;
    std::weak_ptr<IConnection> Connection;
    std::mutex Mutex;
    THashMap<ui64, TPendingRequest> Pending;
    std::set<std::pair<TInstant, ui64>> Deadlines;
    size_t Outstanding = 0;
    bool TimerArmed = false;
    ui64 NextCookie = 1;
    size_t Callbacks = 0;
    bool Closing = false;
    bool ConnectionClosed = false;
    bool Published = false;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

public:
    TRequestState(
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ITaskQueuePtr completions,
        size_t maxPendingRequests)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Completions(std::move(completions))
        , MaxPendingRequests(maxPendingRequests)
    {}

    void SetConnection(const IConnectionPtr& connection)
    {
        Connection = connection;
    }

    ui64 Add(
        TInstant deadline,
        ui32 responseType,
        NThreading::TPromise<TResponseResult> promise)
    {
        ui64 cookie;
        {
            std::lock_guard guard(Mutex);
            if (Closing || Outstanding >= MaxPendingRequests) {
                return 0;
            }
            Y_ABORT_UNLESS(NextCookie, "IC V2 request cookies exhausted");
            cookie = NextCookie++;
            Pending.emplace(
                cookie,
                TPendingRequest{deadline, responseType, std::move(promise)});
            Deadlines.emplace(deadline, cookie);
            ++Outstanding;
        }
        ArmTimer();
        return cookie;
    }

    bool BeginCallback()
    {
        std::lock_guard guard(Mutex);
        if (Closing) {
            return false;
        }
        ++Callbacks;
        return true;
    }

    void FinishCallback()
    {
        {
            std::lock_guard guard(Mutex);
            Y_ABORT_UNLESS(Callbacks);
            --Callbacks;
        }
        PublishClosed();
    }

    void Complete(ui64 cookie, TResponseResult result)
    {
        NThreading::TPromise<TResponseResult> promise;
        bool timedOut = false;
        bool badResponse = false;
        {
            std::lock_guard guard(Mutex);
            const auto it = Pending.find(cookie);
            if (it == Pending.end()) {
                return;
            }
            timedOut = !HasError(result.GetError()) &&
                       Timer->Now() >= it->second.Deadline;
            badResponse = !HasError(result.GetError()) && !timedOut &&
                          result.GetResult()->Type != it->second.ResponseType;
            promise = std::move(it->second.Promise);
            Deadlines.erase({it->second.Deadline, cookie});
            Pending.erase(it);
            ++Callbacks;
        }
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
        Completions->ExecuteSimple(
            [self = shared_from_this(),
             promise,
             result = std::move(result)]() mutable
            {
                promise.SetValue(std::move(result));
                self->FinishResults(1);
            });
    }

    void Close(const NProto::TError& error)
    {
        const auto keepAlive = shared_from_this();
        THashMap<ui64, TPendingRequest> pending;
        bool first = false;
        {
            std::lock_guard guard(Mutex);
            if (!Closing) {
                Closing = true;
                pending.swap(Pending);
                Deadlines.clear();
                ++Callbacks;
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
        const size_t count = pending.size();
        if (!count) {
            FinishResults(0);
            return;
        }
        Completions->ExecuteSimple(
            [self = shared_from_this(),
             pending = std::move(pending),
             error,
             count]() mutable
            {
                for (auto& [cookie, request]: pending) {
                    request.Promise.SetValue(error);
                }
                self->FinishResults(count);
            });
    }

    void OnConnectionClosed()
    {
        Close(MakeError(E_REJECTED, "IC V2 connection closed"));
        {
            std::lock_guard guard(Mutex);
            ConnectionClosed = true;
        }
        PublishClosed();
    }

    NThreading::TFuture<void> WhenClosed() const
    {
        return Closed.GetFuture();
    }

private:
    void ArmTimer()
    {
        TInstant deadline;
        {
            std::lock_guard guard(Mutex);
            if (Closing || TimerArmed || Deadlines.empty()) {
                return;
            }
            // A short tick bounds delay for a newly inserted earlier deadline
            // without leaving an obsolete scheduler task for each request.
            deadline =
                Min(Deadlines.begin()->first,
                    Timer->Now() + TDuration::MilliSeconds(10));
            TimerArmed = true;
        }
        const std::weak_ptr<TRequestState> weak = shared_from_this();
        Scheduler->Schedule(
            nullptr,
            deadline,
            [weak]
            {
                if (auto state = weak.lock()) {
                    state->OnTimer();
                }
            });
    }

    void OnTimer()
    {
        std::vector<ui64> expired;
        {
            std::lock_guard guard(Mutex);
            // Keep the timer armed until this callback finishes scanning.
            if (Closing) {
                TimerArmed = false;
                return;
            }
            const auto now = Timer->Now();
            for (const auto& [deadline, cookie]: Deadlines) {
                if (deadline > now) {
                    break;
                }
                expired.push_back(cookie);
            }
        }
        for (ui64 cookie: expired) {
            Complete(cookie, MakeError(E_TIMEOUT, "IC V2 request timed out"));
        }
        {
            std::lock_guard guard(Mutex);
            TimerArmed = false;
        }
        ArmTimer();
    }

    void FinishResults(size_t count)
    {
        {
            std::lock_guard guard(Mutex);
            Y_ABORT_UNLESS(Outstanding >= count);
            Outstanding -= count;
        }
        FinishCallback();
    }

    void PublishClosed()
    {
        {
            std::lock_guard guard(Mutex);
            if (!Closing || !ConnectionClosed || Callbacks || Published) {
                return;
            }
            Published = true;
        }
        Closed.SetValue();
    }
};

class TRequestChannel final
    : public IRequestChannel
    , public IConnectionHandler
    , public std::enable_shared_from_this<TRequestChannel>
{
    const TRequestChannelConfig Config;
    const ITimerPtr Timer;
    const TRequestHandler OnRequest;
    const std::shared_ptr<TRequestState> State;
    IConnectionPtr Connection;

public:
    TRequestChannel(
        TRequestChannelConfig config,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ITaskQueuePtr completions,
        TRequestHandler onRequest)
        : Config(config)
        , Timer(std::move(timer))
        , OnRequest(std::move(onRequest))
        , State(std::make_shared<TRequestState>(
              Timer,
              std::move(scheduler),
              std::move(completions),
              config.MaxPendingRequests))
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
        auto promise = NThreading::NewPromise<TResponseResult>();
        auto future = promise.GetFuture();
        const ui32 responseType =
            event ? GetResponseType(Config, event->Type()) : 0;
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
        if (!Send(cookie, std::move(event))) {
            State->Complete(
                cookie,
                MakeError(E_REJECTED, "IC V2 send rejected"));
        }
        return future;
    }

    bool Reply(ui64 cookie, std::unique_ptr<NActors::IEventBase> event) override
    {
        const auto keepAlive = shared_from_this();
        return cookie && event && IsResponseType(Config, event->Type()) &&
               Send(cookie, std::move(event));
    }

    void Close() override
    {
        const auto keepAlive = weak_from_this().lock();
        State->Close(MakeError(E_REJECTED, "IC V2 request channel closed"));
    }

    NThreading::TFuture<void> WhenClosed() const override
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
            if (IsResponseType(Config, event->Type)) {
                const ui64 cookie = event->Cookie;
                State->Complete(
                    cookie,
                    std::shared_ptr<NActors::IEventHandle>(std::move(event)));
            } else if (GetResponseType(Config, event->Type) && event->Cookie) {
                OnRequest(std::move(event));
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
    bool Send(ui64 cookie, std::unique_ptr<NActors::IEventBase> event)
    {
        return Connection->Send(std::make_unique<NActors::IEventHandle>(
            NActors::TActorId(),
            NActors::TActorId(),
            event.release(),
            0,
            cookie));
    }
};

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

TResultOrError<IRequestChannelPtr> CreateRequestChannel(
    ITransportPtr transport,
    TSocketPtr socket,
    TSocketPtr xdcSocket,
    TRequestChannelConfig config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions,
    TRequestHandler onRequest)
{
    if (!transport || !socket || !timer || !scheduler || !completions ||
        !onRequest || !config.MaxPendingRequests ||
        !ValidateEventTypes(config))
    {
        return MakeError(
            E_ARGUMENT,
            "invalid IC V2 request channel configuration");
    }
    auto channel = std::make_shared<TRequestChannel>(
        config,
        std::move(timer),
        std::move(scheduler),
        std::move(completions),
        std::move(onRequest));
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

}   // namespace NCloud::NBlockStore::NIcV2
