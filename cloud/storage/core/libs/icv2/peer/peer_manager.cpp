#include "peer_manager.h"

#include <cloud/storage/core/libs/common/scheduler.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/network/address.h>

#include <mutex>

namespace NCloud::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration HostRetryDelay = TDuration::Seconds(1);

enum class EHostEvent
{
    Connected,
    Disconnected,
    Unavailable,
};

////////////////////////////////////////////////////////////////////////////////

struct THostNotifications
{
    const IClientEndpointHandlerPtr Handler;
    TDeque<EHostEvent> Events;
    bool Dispatching = false;

    explicit THostNotifications(IClientEndpointHandlerPtr handler)
        : Handler(std::move(handler))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TAttempt
{
    ui64 Generation;
    TInstant Deadline;
    size_t NextAddress = 0;
    NThreading::TPromise<TChannelResult> Promise =
        NThreading::NewPromise<TChannelResult>();
};

////////////////////////////////////////////////////////////////////////////////

struct TPeerState
{
    TVector<NInterconnect::TAddress> Addresses;
    TString Host;
    ui16 Port = 0;
    TInstant RetryAt;
    ui64 Generation = 0;
    ui64 ConnectionFailures = 0;
    std::shared_ptr<TAttempt> Attempt;
    IRequestChannelPtr Channel;
    std::shared_ptr<THostNotifications> Notifications;
};

using TPeerId = ui64;

class TPeerConnectionManager;

////////////////////////////////////////////////////////////////////////////////

class TPeer final: public IPeer
{
    const std::weak_ptr<TPeerConnectionManager> Manager;
    const TPeerId Id;

public:
    TPeer(std::weak_ptr<TPeerConnectionManager> manager, TPeerId id)
        : Manager(std::move(manager))
        , Id(id)
    {}

    ~TPeer() override;
    TChannelFuture GetChannel() override;
    TPeerStatus GetStatus() const override;
    void Close() override;
};

////////////////////////////////////////////////////////////////////////////////

class TPeerConnectionManager final
    : public IPeerConnectionManager
    , public std::enable_shared_from_this<TPeerConnectionManager>
{
    friend class TPeer;

    const IConnectorPtr Connector;
    const IHandshakeProviderPtr Handshake;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const NCloud::NIcV2::IAddressResolverPtr Resolver;
    const ITaskQueuePtr Completions;
    const IChannelFactoryPtr ChannelFactory;
    const TDuration ConnectTimeout;
    const size_t MaxConnections;

    std::mutex Mutex;
    THashMap<TPeerId, TPeerState> Peers;
    size_t Connections = 0;
    TPeerId NextPeerId = 1;
    size_t ChannelCallbacks = 0;
    size_t ResultCallbacks = 0;
    enum class EState
    {
        Created,
        Running,
        StopRequested,
        Joining,
        Stopped,
    };

    EState State = EState::Created;
    NThreading::TPromise<void> Joined = NThreading::NewPromise<void>();
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

public:
    TPeerConnectionManager(
        IConnectorPtr connector,
        IHandshakeProviderPtr handshake,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        NCloud::NIcV2::IAddressResolverPtr resolver,
        ITaskQueuePtr completions,
        IChannelFactoryPtr channelFactory,
        TDuration connectTimeout,
        size_t maxConnections)
        : Connector(std::move(connector))
        , Handshake(std::move(handshake))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Resolver(std::move(resolver))
        , Completions(std::move(completions))
        , ChannelFactory(std::move(channelFactory))
        , ConnectTimeout(connectTimeout)
        , MaxConnections(maxConnections)

    {}

    ~TPeerConnectionManager() override
    {
        Stop();
    }

    void Start() override
    {
        std::lock_guard guard(Mutex);
        Y_ABORT_UNLESS(State == EState::Created);
        // No peers can be registered before the workers have started.
        Connector->Start();
        Handshake->Start();
        State = EState::Running;
    }

    TResultOrError<IPeerPtr> RegisterPeer(
        TString host,
        ui16 port,
        IClientEndpointHandlerPtr handler) override
    {
        TPeerId peerId = 0;
        {
            std::lock_guard guard(Mutex);
            if (State != EState::Running) {
                return MakeError(
                    E_REJECTED,
                    "IC V2 peer connections are not running");
            }
            if (host.empty() || !port) {
                return MakeError(E_ARGUMENT, "invalid IC V2 host binding");
            }
            if (Peers.size() >= MaxConnections) {
                return MakeError(
                    E_REJECTED,
                    "IC V2 host binding limit reached");
            }
            TPeerState peer;
            peer.Host = std::move(host);
            peer.Port = port;
            if (handler) {
                peer.Notifications =
                    std::make_shared<THostNotifications>(std::move(handler));
            }
            peerId = NextPeerId++;
            Peers.emplace(peerId, std::move(peer));
        }
        KickHost(peerId, 0);
        return IPeerPtr(std::make_shared<TPeer>(weak_from_this(), peerId));
    }

private:
    TPeerStatus GetPeerStatus(TPeerId peerId)
    {
        std::lock_guard guard(Mutex);
        const auto it = Peers.find(peerId);
        if (it == Peers.end()) {
            return {.State = EPeerState::Disconnected};
        }
        const auto& peer = it->second;
        EPeerState state = EPeerState::Idle;
        if (peer.Attempt) {
            state = EPeerState::Connecting;
        } else if (peer.Channel && !peer.Channel->WhenClosed().HasValue()) {
            state = EPeerState::Connected;
        } else if (peer.Generation) {
            state = EPeerState::Disconnected;
        }
        return {
            .State = state,
            .ConnectionAttempts = peer.Generation,
            .ConnectionFailures = peer.ConnectionFailures,
        };
    }

    void UnregisterPeer(TPeerId peerId)
    {
        Y_ABORT_UNLESS(peerId);
        IRequestChannelPtr channel;
        std::shared_ptr<TAttempt> attempt;
        {
            std::lock_guard guard(Mutex);
            const auto it = Peers.find(peerId);
            if (it == Peers.end()) {
                return;
            }
            channel = std::move(it->second.Channel);
            attempt = std::move(it->second.Attempt);
            if (State < EState::StopRequested && (channel || attempt)) {
                --Connections;
            }
            if (attempt) {
                ++ResultCallbacks;
            }
            Peers.erase(it);
        }
        if (channel) {
            channel->Close();
        }
        if (attempt) {
            Complete(
                attempt,
                MakeError(E_REJECTED, "IC V2 host binding closed"));
        }
    }

    TChannelFuture Get(TPeerId peerId)
    {
        const auto keepAlive = shared_from_this();
        std::shared_ptr<TAttempt> attempt;
        IRequestChannelPtr retired;
        std::shared_ptr<THostNotifications> disconnected;
        {
            std::lock_guard guard(Mutex);
            if (State != EState::Running) {
                return NThreading::MakeFuture<TChannelResult>(MakeError(
                    E_REJECTED,
                    "IC V2 peer connections are not running"));
            }
            auto it = Peers.find(peerId);
            if (it == Peers.end()) {
                return NThreading::MakeFuture<TChannelResult>(
                    MakeError(E_CANCELLED, "IC V2 peer is closed"));
            }
            auto& peer = it->second;
            if (peer.Attempt) {
                return peer.Attempt->Promise.GetFuture();
            }
            if (peer.Channel && !peer.Channel->WhenClosed().HasValue()) {
                return NThreading::MakeFuture<TChannelResult>(peer.Channel);
            }
            if (peer.RetryAt > Timer->Now()) {
                return NThreading::MakeFuture<TChannelResult>(MakeError(
                    E_UNAVAILABLE,
                    "IC V2 host is waiting to reconnect"));
            }
            if (Connections + ResultCallbacks - (peer.Channel ? 1 : 0) >=
                MaxConnections)
            {
                return NThreading::MakeFuture<TChannelResult>(MakeError(
                    E_REJECTED,
                    "IC V2 outgoing connection limit reached"));
            }
            if (peer.Channel) {
                retired = std::move(peer.Channel);
                disconnected =
                    QueueNotification(peer, EHostEvent::Disconnected);
                --Connections;
            }
            Y_ABORT_UNLESS(
                ++peer.Generation,
                "IC V2 peer generations exhausted");
            peer.RetryAt = {};
            attempt = std::make_shared<TAttempt>(
                peer.Generation,
                Timer->Now() + ConnectTimeout);
            peer.Attempt = attempt;
            ++Connections;
        }
        DispatchNotifications(disconnected);
        ResolveHost(peerId, attempt);
        return attempt->Promise.GetFuture();
    }

public:
    void RequestStop() override
    {
        TVector<IRequestChannelPtr> channels;
        TVector<std::shared_ptr<TAttempt>> attempts;
        {
            std::lock_guard guard(Mutex);
            if (State >= EState::StopRequested) {
                return;
            }
            State = EState::StopRequested;
            for (auto& [peerId, peer]: Peers) {
                if (peer.Attempt) {
                    attempts.push_back(std::move(peer.Attempt));
                    ++ResultCallbacks;
                }
                if (peer.Channel) {
                    channels.push_back(std::move(peer.Channel));
                }
            }
            Connections = 0;
        }
        Connector->RequestStop();
        Handshake->RequestStop();
        for (const auto& channel: channels) {
            channel->Close();
        }
        for (const auto& attempt: attempts) {
            Complete(
                attempt,
                MakeError(E_REJECTED, "IC V2 peer connections are stopping"));
        }
        PublishClosed();
    }

    void Stop() override
    {
        RequestStop();
        {
            std::unique_lock guard(Mutex);
            if (State == EState::Stopped) {
                return;
            }
            if (State == EState::Joining) {
                guard.unlock();
                Joined.GetFuture().GetValueSync();
                return;
            }
            State = EState::Joining;
        }
        Connector->Stop();
        Handshake->Stop();
        Closed.GetFuture().GetValueSync();
        {
            std::lock_guard guard(Mutex);
            State = EState::Stopped;
        }
        Joined.SetValue();
    }

private:
    std::shared_ptr<THostNotifications> QueueNotification(
        TPeerState& peer,
        EHostEvent event)
    {
        if (peer.Notifications) {
            peer.Notifications->Events.push_back(event);
            ++ResultCallbacks;
        }
        return peer.Notifications;
    }

    void DispatchNotifications(
        const std::shared_ptr<THostNotifications>& notifications)
    {
        if (!notifications) {
            return;
        }
        {
            std::lock_guard guard(Mutex);
            if (notifications->Dispatching) {
                return;
            }
            notifications->Dispatching = true;
        }
        size_t count = 0;
        while (true) {
            EHostEvent event = EHostEvent::Connected;
            {
                std::lock_guard guard(Mutex);
                if (notifications->Events.empty()) {
                    notifications->Dispatching = false;
                    Y_DEBUG_ABORT_UNLESS(ResultCallbacks >= count);
                    ResultCallbacks -= count;
                    break;
                }
                event = notifications->Events.front();
                notifications->Events.pop_front();
            }
            switch (event) {
                case EHostEvent::Connected:
                    notifications->Handler->HandleConnected();
                    break;
                case EHostEvent::Disconnected:
                    notifications->Handler->HandleDisconnected();
                    break;
                case EHostEvent::Unavailable:
                    notifications->Handler->HandleUnavailable();
                    break;
            }
            ++count;
        }
        PublishClosed();
    }

    void KickHost(TPeerId key, ui64 generation)
    {
        {
            std::lock_guard guard(Mutex);
            auto* peer = Peers.FindPtr(key);
            if (State >= EState::StopRequested || !peer ||
                peer->Generation != generation || peer->Attempt ||
                peer->Channel)
            {
                return;
            }
            peer->RetryAt = {};
        }
        auto future = Get(key);
        if (future.HasValue() && HasError(future.GetValue().GetError())) {
            // Admission can fail before an attempt is created. Keep probing
            // when the global connection budget becomes available again.
            ScheduleRetry(key, generation);
        }
    }

    void ScheduleRetry(TPeerId key, ui64 generation)
    {
        TInstant deadline;
        {
            std::lock_guard guard(Mutex);
            auto* peer = Peers.FindPtr(key);
            if (State >= EState::StopRequested || !peer ||
                peer->Generation != generation || peer->Attempt ||
                peer->Channel || peer->RetryAt)
            {
                return;
            }
            deadline = Timer->Now() + HostRetryDelay;
            peer->RetryAt = deadline;
        }
        Scheduler->Schedule(
            deadline,
            [weak = weak_from_this(), key, generation]
            {
                if (auto self = weak.lock()) {
                    self->KickHost(key, generation);
                }
            });
    }

    void ResolveHost(TPeerId key, const std::shared_ptr<TAttempt>& attempt)
    {
        TString host;
        ui16 port = 0;
        {
            std::lock_guard guard(Mutex);
            auto* peer = Peers.FindPtr(key);
            if (!peer || !IsCurrent(*peer, attempt)) {
                return;
            }
            host = peer->Host;
            port = peer->Port;
            ++ResultCallbacks;
        }
        const auto deadline =
            TMonotonic::Now() + (attempt->Deadline - Timer->Now());
        Resolver->Resolve(host, port, deadline)
            .Subscribe(
                [self = shared_from_this(), key, attempt](
                    const NCloud::NIcV2::TResolveFuture& future)
                {
                    self->OnResolved(key, attempt, future.GetValue());
                    {
                        std::lock_guard guard(self->Mutex);
                        Y_DEBUG_ABORT_UNLESS(self->ResultCallbacks);

                        --self->ResultCallbacks;
                    }
                    self->PublishClosed();
                });
    }

    void OnResolved(
        TPeerId key,
        const std::shared_ptr<TAttempt>& attempt,
        const NCloud::NIcV2::TResolveResult& result)
    {
        {
            std::lock_guard guard(Mutex);
            auto* peer = Peers.FindPtr(key);
            if (!peer || !IsCurrent(*peer, attempt)) {
                return;
            }
            if (!HasError(result.GetError())) {
                peer->Addresses = result.GetResult();
            }
        }
        if (HasError(result.GetError())) {
            Finish(key, attempt, result.GetError());
        } else {
            ConnectNext(key, attempt, {});
        }
    }

    bool IsCurrent(
        const TPeerState& peer,
        const std::shared_ptr<TAttempt>& attempt) const
    {
        return State < EState::StopRequested && peer.Attempt == attempt;
    }

    void ConnectNext(
        TPeerId peerId,
        const std::shared_ptr<TAttempt>& attempt,
        NProto::TError error)
    {
        NInterconnect::TAddress address;
        TDuration remaining;
        {
            std::lock_guard guard(Mutex);
            const auto it = Peers.find(peerId);
            if (it == Peers.end()) {
                return;
            }
            auto& peer = it->second;
            if (!IsCurrent(peer, attempt)) {
                return;
            }
            const auto now = Timer->Now();
            if (now >= attempt->Deadline) {
                error =
                    MakeError(E_TIMEOUT, "IC V2 connect and hello timed out");
            } else if (attempt->NextAddress < peer.Addresses.size()) {
                address = peer.Addresses[attempt->NextAddress++];
                remaining = attempt->Deadline - now;
            }
        }
        if (!remaining) {
            Finish(peerId, attempt, std::move(error));
            return;
        }
        const std::weak_ptr<TPeerConnectionManager> weak = shared_from_this();
        Connector->Connect(address, remaining)
            .Subscribe(
                [weak, peerId, attempt](const TSocketFuture& future)
                {
                    if (auto self = weak.lock()) {
                        self->OnConnect(peerId, attempt, future.GetValue());
                    }
                });
    }

    void OnConnect(
        TPeerId peerId,
        const std::shared_ptr<TAttempt>& attempt,
        TSocketResult result)
    {
        TDuration remaining;
        {
            std::lock_guard guard(Mutex);
            const auto it = Peers.find(peerId);
            if (it == Peers.end() || !IsCurrent(it->second, attempt)) {
                return;
            }
            const auto now = Timer->Now();
            if (now < attempt->Deadline) {
                remaining = attempt->Deadline - now;
            }
        }
        if (!remaining || HasError(result.GetError())) {
            ConnectNext(peerId, attempt, result.GetError());
            return;
        }
        const std::weak_ptr<TPeerConnectionManager> weak = shared_from_this();
        Handshake->Connect(result.ExtractResult(), remaining)
            .Subscribe(
                [weak, peerId, attempt](const TConnectHandshakeFuture& future)
                {
                    if (auto self = weak.lock()) {
                        self->OnHello(peerId, attempt, future.GetValue());
                    }
                });
    }

    void OnHello(
        TPeerId peerId,
        const std::shared_ptr<TAttempt>& attempt,
        const TConnectHandshakeResult& result)
    {
        {
            std::lock_guard guard(Mutex);
            const auto it = Peers.find(peerId);
            if (it == Peers.end() || !IsCurrent(it->second, attempt)) {
                return;
            }
        }
        if (HasError(result.GetError()) || Timer->Now() >= attempt->Deadline) {
            ConnectNext(peerId, attempt, result.GetError());
            return;
        }
        auto [channel, error] =
            ChannelFactory->CreateChannel(result.GetResult());
        if (HasError(error)) {
            Finish(peerId, attempt, std::move(error));
            return;
        }
        bool admitted = false;
        {
            std::lock_guard guard(Mutex);
            const auto it = Peers.find(peerId);
            if (it != Peers.end() && IsCurrent(it->second, attempt)) {
                it->second.Channel = channel;
                ++ChannelCallbacks;
                admitted = true;
            }
        }
        if (!admitted) {
            channel->Close();
            return;
        }
        const std::weak_ptr<TPeerConnectionManager> weak = shared_from_this();
        channel->WhenClosed().Subscribe(
            [weak, peerId, generation = attempt->Generation](const auto&)
            {
                if (auto self = weak.lock()) {
                    self->OnClosed(peerId, generation);
                }
            });
        if (!channel->Activate()) {
            Finish(
                peerId,
                attempt,
                MakeError(
                    E_TRANSPORT_ERROR,
                    "IC V2 channel activation failed"));
            return;
        }
        Finish(peerId, attempt, std::move(channel));
    }

    void Finish(
        TPeerId peerId,
        const std::shared_ptr<TAttempt>& attempt,
        TChannelResult result)
    {
        if (!HasError(result.GetError()) && Timer->Now() >= attempt->Deadline) {
            result = MakeError(E_TIMEOUT, "IC V2 channel activation timed out");
        }
        IRequestChannelPtr channel;
        std::shared_ptr<THostNotifications> notifications;
        {
            std::lock_guard guard(Mutex);
            const auto it = Peers.find(peerId);
            if (it == Peers.end()) {
                return;
            }
            auto& peer = it->second;
            if (!IsCurrent(peer, attempt)) {
                return;
            }
            peer.Attempt.reset();
            if (HasError(result.GetError())) {
                ++peer.ConnectionFailures;
                channel = std::move(peer.Channel);
                --Connections;
            }
            notifications = QueueNotification(
                peer,
                HasError(result.GetError()) ? EHostEvent::Unavailable
                                            : EHostEvent::Connected);
            ++ResultCallbacks;
        }
        if (channel) {
            channel->Close();
        }
        DispatchNotifications(notifications);
        if (HasError(result.GetError())) {
            ScheduleRetry(peerId, attempt->Generation);
        }
        Complete(attempt, std::move(result));
    }

    void OnClosed(TPeerId peerId, ui64 generation)
    {
        std::shared_ptr<TAttempt> attempt;
        IRequestChannelPtr retired;
        std::shared_ptr<THostNotifications> disconnected;
        {
            std::lock_guard guard(Mutex);
            Y_ABORT_UNLESS(ChannelCallbacks);
            const auto it = Peers.find(peerId);
            if (it != Peers.end() && it->second.Generation == generation &&
                it->second.Channel)
            {
                retired = std::move(it->second.Channel);
                --Connections;
                if (State < EState::StopRequested && !it->second.Attempt) {
                    disconnected =
                        QueueNotification(it->second, EHostEvent::Disconnected);
                }
                attempt = std::move(it->second.Attempt);
                if (attempt) {
                    ++it->second.ConnectionFailures;
                    if (State < EState::StopRequested) {
                        disconnected = QueueNotification(
                            it->second,
                            EHostEvent::Unavailable);
                    }
                    ++ResultCallbacks;
                }
            }
        }
        if (attempt) {
            Complete(
                attempt,
                MakeError(
                    E_TRANSPORT_ERROR,
                    "IC V2 channel closed during activation"));
        }
        DispatchNotifications(disconnected);
        ScheduleRetry(peerId, generation);
        {
            std::lock_guard guard(Mutex);
            --ChannelCallbacks;
        }
        PublishClosed();
    }

    void Complete(
        const std::shared_ptr<TAttempt>& attempt,
        TChannelResult result)
    {
        Completions->ExecuteSimple(
            [self = shared_from_this(),
             attempt,
             result = std::move(result)]() mutable
            {
                attempt->Promise.SetValue(std::move(result));
                {
                    std::lock_guard guard(self->Mutex);
                    Y_ABORT_UNLESS(self->ResultCallbacks);
                    --self->ResultCallbacks;
                }
                self->PublishClosed();
            });
    }

    void PublishClosed()
    {
        {
            std::lock_guard guard(Mutex);
            if (State < EState::StopRequested || ChannelCallbacks ||
                ResultCallbacks)
            {
                return;
            }
        }
        Closed.TrySetValue();
    }
};

////////////////////////////////////////////////////////////////////////////////

TPeer::~TPeer()
{
    Close();
}

TChannelFuture TPeer::GetChannel()
{
    if (const auto manager = Manager.lock()) {
        return manager->Get(Id);
    }
    return NThreading::MakeFuture<TChannelResult>(
        MakeError(E_CANCELLED, "IC V2 peer manager is gone"));
}

TPeerStatus TPeer::GetStatus() const
{
    if (const auto manager = Manager.lock()) {
        return manager->GetPeerStatus(Id);
    }
    return {.State = EPeerState::Disconnected};
}

void TPeer::Close()
{
    if (const auto manager = Manager.lock()) {
        manager->UnregisterPeer(Id);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IChannelFactory::~IChannelFactory() = default;

IPeer::~IPeer() = default;

TResultOrError<IPeerConnectionManagerPtr> CreatePeerConnectionManager(
    TPeerConnectionManagerConfig config,
    IConnectorPtr connector,
    IHandshakeProviderPtr handshake,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    NCloud::NIcV2::IAddressResolverPtr resolver,
    ITaskQueuePtr completions,
    IChannelFactoryPtr channelFactory)
{
    if (!connector || !handshake || !timer || !scheduler || !resolver ||
        !completions || !channelFactory)
    {
        return MakeError(
            E_ARGUMENT,
            "invalid IC V2 peer connections configuration");
    }
    if (!config.ConnectTimeout || !config.MaxConnections) {
        return MakeError(E_ARGUMENT, "invalid IC V2 connection limits");
    }
    return IPeerConnectionManagerPtr(
        std::make_shared<TPeerConnectionManager>(
            std::move(connector),
            std::move(handshake),
            std::move(timer),
            std::move(scheduler),
            std::move(resolver),
            std::move(completions),
            std::move(channelFactory),
            config.ConnectTimeout,
            config.MaxConnections));
}

}   // namespace NCloud::NIcV2
