#include "peer_impl.h"

#include "peer_manager_impl.h"

#include <cloud/storage/core/libs/common/scheduler.h>

#include <util/generic/vector.h>
#include <util/network/address.h>

#include <exception>
#include <new>

namespace NCloud::NIcV2 {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

enum class EConnectState
{
    Idle,
    Running,
    RetryPending,
};

constexpr TDuration ReconnectDelay = TDuration::Seconds(1);

void TerminateOnOutOfMemory(const std::exception& error)
{
    if (dynamic_cast<const std::bad_alloc*>(&error)) {
        std::terminate();
    }
}

NProto::TError MakeExceptionError(const std::exception& error)
{
    TerminateOnOutOfMemory(error);
    return MakeError(E_FAIL, error.what());
}

void CloseChannel(IRequestChannelPtr channel) noexcept
{
    try {
        channel->Close();
    } catch (const std::exception& error) {
        TerminateOnOutOfMemory(error);
        // Shutdown still waits for WhenClosed(), even if Close() throws.
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

enum class TPeer::EPeerEvent
{
    Connected,
    Disconnected,
    Unavailable,
};

////////////////////////////////////////////////////////////////////////////////

// Kept by callbacks and completion tasks without retaining the peer itself.
// The last reference removes the peer from the manager's shutdown wait.
struct TPeer::TActivity
{
    const std::shared_ptr<TPeerManager> Manager;
    const ui64 Id;

    TActivity(std::shared_ptr<TPeerManager> manager, ui64 id)
        : Manager(std::move(manager))
        , Id(id)
    {}

    ~TActivity()
    {
        Manager->UnregisterPeer(Id);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPeer::TNotificationContext
{
    std::shared_ptr<TActivity> Activity;
    IClientEndpointHandlerPtr Handler;
};

////////////////////////////////////////////////////////////////////////////////

struct TPeer::TConnectionAttempt
{
    ui64 Generation;
    TInstant Deadline;
    TVector<NInterconnect::TAddress> Addresses;
    size_t NextAddress = 0;
    TPromise<TChannelResult> Promise = NewPromise<TChannelResult>();
    EConnectState ConnectState = EConnectState::Idle;
    NProto::TError RetryError;
};

////////////////////////////////////////////////////////////////////////////////

struct TPeer::TReadyChannel: public TAtomicRefCount<TReadyChannel>
{
    const IRequestChannelPtr Channel;
    const TFuture<NProto::TError> Closed;

    explicit TReadyChannel(IRequestChannelPtr channel)
        : Channel(std::move(channel))
        , Closed(Channel->WhenClosed())
    {}
};

////////////////////////////////////////////////////////////////////////////////

TPeer::TPeer(
    std::shared_ptr<TPeerManager> manager,
    ui64 id,
    TPeerDependencies dependencies,
    TString host,
    ui16 port,
    IClientEndpointHandlerPtr handler)
    : Dependencies(std::move(dependencies))
    , Host(std::move(host))
    , Port(port)
    , Activity(std::make_shared<TActivity>(std::move(manager), id))
    , Handler(std::move(handler))
{}

TPeer::~TPeer()
{
    Close();
}

TChannelFuture TPeer::GetChannel()
{
    if (auto channel = GetReadyChannel()) {
        return MakeFuture<TChannelResult>(std::move(channel));
    }
    const auto keepAlive = shared_from_this();
    std::shared_ptr<TConnectionAttempt> attempt;
    IRequestChannelPtr retired;
    TNotificationContext disconnected;
    {
        TGuard<TMutex> guard(Mutex);
        if (HasError(CloseError)) {
            return MakeFuture<TChannelResult>(CloseError);
        }
        if (Attempt) {
            return Attempt->Promise.GetFuture();
        }
        if (auto channel = GetReadyChannel()) {
            return MakeFuture<TChannelResult>(std::move(channel));
        }
        if (RetryAt > Dependencies.Timer->Now()) {
            return MakeFuture<TChannelResult>(
                MakeError(E_UNAVAILABLE, "IC V2 host is waiting to reconnect"));
        }
        if (Channel) {
            retired = TakeChannel();
            disconnected = QueueNotification(EPeerEvent::Disconnected);
        }
        ++Generation;
        RetryAt = {};
        attempt = std::make_shared<TConnectionAttempt>(
            Generation,
            Dependencies.Timer->Now() + Dependencies.ConnectTimeout);
        Attempt = attempt;
    }
    DispatchNotifications(std::move(disconnected));
    const auto future = attempt->Promise.GetFuture();
    ResolveHost(std::move(attempt));
    return future;
}

TPeerStatus TPeer::GetStatus()
{
    TGuard<TMutex> guard(Mutex);
    if (HasError(CloseError)) {
        return {.State = EPeerState::Disconnected};
    }
    EPeerState state = EPeerState::Idle;
    if (Attempt) {
        state = EPeerState::Connecting;
    } else if (GetReadyChannel()) {
        state = EPeerState::Connected;
    } else if (Generation) {
        state = EPeerState::Disconnected;
    }
    return {
        .State = state,
        .ConnectionAttempts = Generation,
        .ConnectionFailures = ConnectionFailures,
    };
}

void TPeer::Close()
{
    Close(MakeError(E_CANCELLED, "IC V2 peer is closed"));
}

void TPeer::Close(NProto::TError error)
{
    const auto keepAlive = weak_from_this().lock();
    std::shared_ptr<TActivity> activity;
    IRequestChannelPtr channel;
    std::shared_ptr<TConnectionAttempt> attempt;
    IClientEndpointHandlerPtr handler;
    {
        TGuard<TMutex> guard(Mutex);
        if (HasError(CloseError)) {
            return;
        }
        CloseError = std::move(error);
        activity = std::move(Activity);
        channel = TakeChannel();
        attempt = std::move(Attempt);
        handler = std::move(Handler);
    }
    if (channel) {
        CloseChannel(std::move(channel));
    }
    if (attempt) {
        DeliverAttemptResult(
            std::move(attempt),
            MakeError(E_REJECTED, "IC V2 host binding closed"),
            activity);
    }
}

IRequestChannelPtr TPeer::GetReadyChannel() const
{
    const auto ready = ReadyChannel.AtomicLoad();
    return ready && !ready->Closed.HasValue() ? ready->Channel : nullptr;
}

IRequestChannelPtr TPeer::TakeChannel()
{
    if (Channel) {
        ReadyChannel.AtomicStore(nullptr);
    }
    return std::move(Channel);
}

TPeer::TNotificationContext TPeer::QueueNotification(EPeerEvent event)
{
    if (!Handler) {
        return {};
    }
    Notifications.push_back(event);
    if (DispatchingNotifications) {
        return {};
    }
    DispatchingNotifications = true;
    return {.Activity = Activity, .Handler = Handler};
}

void TPeer::DispatchNotifications(TNotificationContext context)
{
    if (!context.Handler) {
        return;
    }
    while (true) {
        EPeerEvent event = EPeerEvent::Connected;
        {
            TGuard<TMutex> guard(Mutex);
            if (Notifications.empty()) {
                DispatchingNotifications = false;
                break;
            }
            event = Notifications.front();
            Notifications.pop_front();
        }
        switch (event) {
            case EPeerEvent::Connected:
                context.Handler->HandleConnected();
                break;
            case EPeerEvent::Disconnected:
                context.Handler->HandleDisconnected();
                break;
            case EPeerEvent::Unavailable:
                context.Handler->HandleUnavailable();
                break;
        }
    }
}

void TPeer::StartConnectionAttempt(ui64 generation)
{
    {
        TGuard<TMutex> guard(Mutex);
        if (Generation != generation) {
            return;
        }
    }
    GetChannel();
}

void TPeer::ScheduleRetry(ui64 generation, TInstant deadline)
{
    try {
        Dependencies.Scheduler->Schedule(
            deadline,
            [weak = weak_from_this(), generation]
            {
                if (auto self = weak.lock()) {
                    self->StartConnectionAttempt(generation);
                }
            });
    } catch (const std::exception& error) {
        Close(MakeExceptionError(error));
    }
}

template <typename TResult>
void TPeer::HandleConnectionResult(
    std::shared_ptr<TConnectionAttempt> attempt,
    const TFuture<TResult>& future)
{
    try {
        HandleConnectionResult(attempt, future.GetValue());
    } catch (const std::exception& error) {
        FinishAttempt(std::move(attempt), MakeExceptionError(error));
    }
}

void TPeer::ResolveHost(std::shared_ptr<TConnectionAttempt> attempt)
{
    try {
        std::shared_ptr<TActivity> activity;
        {
            TGuard<TMutex> guard(Mutex);
            if (Attempt != attempt) {
                return;
            }
            activity = Activity;
        }
        const auto deadline =
            TMonotonic::Now() + (attempt->Deadline - Dependencies.Timer->Now());
        Dependencies.Resolver->Resolve(Host, Port, deadline)
            .Subscribe(
                [activity = std::move(activity),
                 weak = weak_from_this(),
                 attempt]   //
                (const TResolveFuture& future) mutable
                {
                    if (auto self = weak.lock()) {
                        self->HandleConnectionResult(
                            std::move(attempt),
                            future);
                    }
                });
    } catch (const std::exception& error) {
        FinishAttempt(std::move(attempt), MakeExceptionError(error));
    }
}

void TPeer::HandleConnectionResult(
    std::shared_ptr<TConnectionAttempt> attempt,
    const TResolveResult& result)
{
    if (HasError(result.GetError())) {
        FinishAttempt(std::move(attempt), result.GetError());
    } else {
        attempt->Addresses = result.GetResult();
        ConnectNext(std::move(attempt), {});
    }
}

void TPeer::ConnectNext(
    std::shared_ptr<TConnectionAttempt> attempt,
    NProto::TError error)
{
    {
        TGuard<TMutex> guard(Mutex);
        if (Attempt != attempt) {
            return;
        }
        if (attempt->ConnectState != EConnectState::Idle) {
            // Inline completions request an iteration instead of recursing.
            attempt->ConnectState = EConnectState::RetryPending;
            attempt->RetryError = std::move(error);
            return;
        }
        attempt->ConnectState = EConnectState::Running;
    }
    while (true) {
        ConnectNextAddress(attempt, std::move(error));

        TGuard<TMutex> guard(Mutex);
        if (attempt->ConnectState != EConnectState::RetryPending) {
            attempt->ConnectState = EConnectState::Idle;
            return;
        }
        error = std::move(attempt->RetryError);
        attempt->ConnectState = EConnectState::Running;
    }
}

void TPeer::ConnectNextAddress(
    std::shared_ptr<TConnectionAttempt> attempt,
    NProto::TError error)
{
    NInterconnect::TAddress address;
    TDuration remaining;
    {
        TGuard<TMutex> guard(Mutex);
        if (Attempt != attempt) {
            return;
        }
        const auto now = Dependencies.Timer->Now();
        if (now >= attempt->Deadline) {
            if (!HasError(error)) {
                error =
                    MakeError(E_TIMEOUT, "IC V2 connect and hello timed out");
            }
        } else if (attempt->NextAddress < attempt->Addresses.size()) {
            address = attempt->Addresses[attempt->NextAddress++];
            remaining = attempt->Deadline - now;
        }
    }
    if (!remaining) {
        if (!HasError(error)) {
            error = MakeError(E_UNAVAILABLE, "IC V2 host has no addresses");
        }
        FinishAttempt(std::move(attempt), std::move(error));
        return;
    }

    Dependencies.Connector->Connect(address, remaining)
        .Subscribe(
            [weak = weak_from_this(),
             attempt = std::move(attempt)]   //
            (const TSocketFuture& future) mutable
            {
                if (auto self = weak.lock()) {
                    self->HandleConnectionResult(std::move(attempt), future);
                }
            });
}

void TPeer::HandleConnectionResult(
    std::shared_ptr<TConnectionAttempt> attempt,
    const TSocketResult& result)
{
    TDuration remaining;
    {
        TGuard<TMutex> guard(Mutex);
        if (Attempt != attempt) {
            return;
        }
        remaining = attempt->Deadline - Dependencies.Timer->Now();
    }
    if (!remaining || HasError(result.GetError())) {
        ConnectNext(std::move(attempt), result.GetError());
        return;
    }
    Dependencies.Handshake->Connect(result.GetResult(), remaining)
        .Subscribe(
            [weak = weak_from_this(),
             attempt = std::move(attempt)]   //
            (const TConnectHandshakeFuture& future) mutable
            {
                if (auto self = weak.lock()) {
                    self->HandleConnectionResult(std::move(attempt), future);
                }
            });
}

void TPeer::HandleConnectionResult(
    std::shared_ptr<TConnectionAttempt> attempt,
    const TConnectHandshakeResult& result)
{
    {
        TGuard<TMutex> guard(Mutex);
        if (Attempt != attempt) {
            return;
        }
    }
    if (HasError(result.GetError())) {
        ConnectNext(std::move(attempt), result.GetError());
        return;
    }
    auto [channel, error] =
        Dependencies.ChannelFactory->CreateChannel(result.GetResult());
    if (HasError(error)) {
        FinishAttempt(std::move(attempt), std::move(error));
        return;
    }
    std::shared_ptr<TActivity> activity;
    {
        TGuard<TMutex> guard(Mutex);
        if (Attempt == attempt) {
            Channel = channel;
            activity = Activity;
        }
    }
    if (!activity) {
        CloseChannel(std::move(channel));
        return;
    }
    channel->WhenClosed().Subscribe(
        [activity = std::move(activity),
         weak = weak_from_this(),
         generation = attempt->Generation]   //
        (const auto&)
        {
            if (auto self = weak.lock()) {
                self->OnClosed(generation);
            }
        });
    if (!channel->Activate()) {
        FinishAttempt(
            std::move(attempt),
            MakeError(E_TRANSPORT_ERROR, "IC V2 channel activation failed"));
        return;
    }
    FinishAttempt(std::move(attempt), std::move(channel));
}

void TPeer::FinishAttempt(
    std::shared_ptr<TConnectionAttempt> attempt,
    TChannelResult result)
{
    const auto ready = HasError(result.GetError())
                           ? nullptr
                           : MakeIntrusive<TReadyChannel>(result.GetResult());
    std::shared_ptr<TActivity> activity;
    IRequestChannelPtr channel;
    TInstant retryAt;
    TNotificationContext notifications;
    {
        TGuard<TMutex> guard(Mutex);
        if (Attempt != attempt) {
            return;
        }
        activity = Activity;
        Attempt.reset();
        if (ready) {
            ReadyChannel.AtomicStore(ready);
        } else {
            ++ConnectionFailures;
            channel = TakeChannel();
            retryAt = Dependencies.Timer->Now() + ReconnectDelay;
            RetryAt = retryAt;
        }
        notifications = QueueNotification(
            HasError(result.GetError()) ? EPeerEvent::Unavailable
                                        : EPeerEvent::Connected);
    }
    if (channel) {
        CloseChannel(std::move(channel));
    }
    DispatchNotifications(std::move(notifications));
    if (retryAt) {
        ScheduleRetry(attempt->Generation, retryAt);
    }
    DeliverAttemptResult(
        std::move(attempt),
        std::move(result),
        std::move(activity));
}

void TPeer::OnClosed(ui64 generation)
{
    std::shared_ptr<TActivity> activity;
    std::shared_ptr<TConnectionAttempt> attempt;
    IRequestChannelPtr retired;
    TInstant retryAt;
    TNotificationContext disconnected;
    {
        TGuard<TMutex> guard(Mutex);
        if (Generation == generation && Channel) {
            retired = TakeChannel();
            attempt = std::move(Attempt);
            if (attempt) {
                ++ConnectionFailures;
                activity = Activity;
            }
            if (!HasError(CloseError)) {
                retryAt = Dependencies.Timer->Now() + ReconnectDelay;
                RetryAt = retryAt;
                disconnected = QueueNotification(
                    attempt ? EPeerEvent::Unavailable
                            : EPeerEvent::Disconnected);
            }
        }
    }
    if (attempt) {
        DeliverAttemptResult(
            std::move(attempt),
            MakeError(
                E_TRANSPORT_ERROR,
                "IC V2 channel closed during activation"),
            std::move(activity));
    }
    DispatchNotifications(std::move(disconnected));
    if (retryAt) {
        ScheduleRetry(generation, retryAt);
    }
}

void TPeer::DeliverAttemptResult(
    std::shared_ptr<TConnectionAttempt> attempt,
    TChannelResult result,
    std::shared_ptr<TActivity> activity)
{
    auto complete = [activity = std::move(activity),
                     attempt = std::move(attempt),
                     result = std::move(result)]() mutable
    {
        attempt->Promise.TrySetValue(std::move(result));
    };
    try {
        Dependencies.Completions->ExecuteSimple(complete);
    } catch (const std::exception& error) {
        TerminateOnOutOfMemory(error);
        complete();
    }
}

////////////////////////////////////////////////////////////////////////////////

IPeer::~IPeer() = default;

}   // namespace NCloud::NIcV2
