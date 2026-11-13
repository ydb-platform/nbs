#include "peer_connections.h"

#include <cloud/blockstore/libs/cells/iface/config.h>

#include <cloud/storage/core/libs/common/scheduler.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/network/address.h>
#include <util/system/mutex.h>

#include <mutex>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IRequestChannelPtr;

using NCloud::NIcV2::IHandshakeProviderPtr;
using NCloud::NIcV2::TConnectHandshakeFuture;
using NCloud::NIcV2::TConnectHandshakeResult;
using NCloud::NIcV2::THandshakeReady;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TPeerKey = std::pair<TString, ui64>;
constexpr TDuration HostRetryDelay = TDuration::Seconds(1);

enum class EHostEvent
{
    Connected,
    Disconnected,
    Unavailable,
};

////////////////////////////////////////////////////////////////////////////////

class THostNotifications
{
    const IClientEndpointHandlerPtr Handler;
    TMutex Mutex;
    TDeque<EHostEvent> Events;
    bool Dispatching = false;

public:
    explicit THostNotifications(IClientEndpointHandlerPtr handler)
        : Handler(std::move(handler))
    {}

    void Push(EHostEvent event)
    {
        TGuard<TMutex> guard(Mutex);
        Events.push_back(event);
    }

    size_t Dispatch()
    {
        {
            TGuard<TMutex> guard(Mutex);
            if (Dispatching) {
                return 0;
            }
            Dispatching = true;
        }
        size_t count = 0;
        while (true) {
            EHostEvent event;
            {
                TGuard<TMutex> guard(Mutex);
                if (Events.empty()) {
                    Dispatching = false;
                    return count;
                }
                event = Events.front();
                Events.pop_front();
            }
            switch (event) {
                case EHostEvent::Connected:
                    Handler->HandleConnected();
                    break;
                case EHostEvent::Disconnected:
                    Handler->HandleDisconnected();
                    break;
                case EHostEvent::Unavailable:
                    Handler->HandleUnavailable();
                    break;
            }
            ++count;
        }
    }
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

struct TPeer
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

class TPeerConnections final
    : public IPeerConnections
    , public std::enable_shared_from_this<TPeerConnections>
{
    const IConnectorPtr Connector;
    const IHandshakeProviderPtr Handshake;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const NCloud::NIcV2::IAddressResolverPtr Resolver;
    const ITaskQueuePtr Completions;
    const TChannelFactory CreateChannel;
    const TDuration ConnectTimeout;
    const size_t MaxConnections;

    std::mutex LifecycleMutex;
    std::mutex Mutex;
    THashMap<TPeerKey, TPeer> Peers;
    size_t Connections = 0;
    size_t HostBindings = 0;
    ui64 LastBindingId = 0;
    size_t ChannelCallbacks = 0;
    size_t ResultCallbacks = 0;
    bool Started = false;
    bool Stopping = false;
    bool Stopped = false;
    bool Published = false;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

public:
    TPeerConnections(
        THashMap<TPeerKey, TPeer> peers,
        IConnectorPtr connector,
        IHandshakeProviderPtr handshake,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        NCloud::NIcV2::IAddressResolverPtr resolver,
        ITaskQueuePtr completions,
        TChannelFactory createChannel,
        TDuration connectTimeout,
        size_t maxConnections)
        : Connector(std::move(connector))
        , Handshake(std::move(handshake))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Resolver(std::move(resolver))
        , Completions(std::move(completions))
        , CreateChannel(std::move(createChannel))
        , ConnectTimeout(connectTimeout)
        , MaxConnections(maxConnections)
        , Peers(std::move(peers))
    {}

    ~TPeerConnections() override
    {
        Stop();
    }

    void Start() override
    {
        std::lock_guard lifecycle(LifecycleMutex);
        {
            std::lock_guard guard(Mutex);
            Y_ABORT_UNLESS(!Started && !Stopping);
        }
        Connector->Start();
        Handshake->Start();
        std::lock_guard guard(Mutex);
        Started = true;
    }

    TChannelFuture GetChannel(const TString& cellId) override
    {
        return Get({cellId, 0});
    }

    NProto::TError RegisterHost(
        const TString& cellId,
        ui64 bindingId,
        TString host,
        ui16 port,
        IClientEndpointHandlerPtr handler) override
    {
        {
            std::lock_guard guard(Mutex);
            if (!Started || Stopping) {
                return MakeError(
                    E_REJECTED,
                    "IC V2 peer connections are not running");
            }
            if (bindingId <= LastBindingId || host.empty() || !port ||
                !Peers.contains({cellId, 0}))
            {
                return MakeError(E_ARGUMENT, "invalid IC V2 host binding");
            }
            if (HostBindings >= MaxConnections) {
                return MakeError(
                    E_REJECTED,
                    "IC V2 host binding limit reached");
            }
            TPeer peer;
            peer.Host = std::move(host);
            peer.Port = port;
            if (handler) {
                peer.Notifications =
                    std::make_shared<THostNotifications>(std::move(handler));
            }
            if (!Peers.emplace(TPeerKey{cellId, bindingId}, std::move(peer))
                     .second)
            {
                return MakeError(E_ARGUMENT, "duplicate IC V2 host binding");
            }
            LastBindingId = bindingId;
            ++HostBindings;
        }
        KickHost({cellId, bindingId}, 0);
        return {};
    }

    TChannelFuture GetHostChannel(
        const TString& cellId,
        ui64 bindingId) override
    {
        Y_ABORT_UNLESS(bindingId);
        return Get({cellId, bindingId});
    }

    THostChannelStatus GetHostStatus(
        const TString& cellId,
        ui64 bindingId) override
    {
        std::lock_guard guard(Mutex);
        const auto it = Peers.find(TPeerKey{cellId, bindingId});
        if (it == Peers.end()) {
            return {.State = EHostChannelState::Disconnected};
        }
        const auto& peer = it->second;
        EHostChannelState state = EHostChannelState::Idle;
        if (peer.Attempt) {
            state = EHostChannelState::Connecting;
        } else if (peer.Channel && !peer.Channel->WhenClosed().HasValue()) {
            state = EHostChannelState::Connected;
        } else if (peer.Generation) {
            state = EHostChannelState::Disconnected;
        }
        return {
            .State = state,
            .ConnectionAttempts = peer.Generation,
            .ConnectionFailures = peer.ConnectionFailures,
        };
    }

    void UnregisterHost(const TString& cellId, ui64 bindingId) override
    {
        Y_ABORT_UNLESS(bindingId);
        IRequestChannelPtr channel;
        std::shared_ptr<TAttempt> attempt;
        {
            std::lock_guard guard(Mutex);
            const auto it = Peers.find(TPeerKey{cellId, bindingId});
            if (it == Peers.end()) {
                return;
            }
            channel = std::move(it->second.Channel);
            attempt = std::move(it->second.Attempt);
            if (!Stopping && (channel || attempt)) {
                --Connections;
            }
            if (attempt) {
                ++ResultCallbacks;
            }
            Peers.erase(it);
            --HostBindings;
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

private:
    TChannelFuture Get(const TPeerKey& cellId)
    {
        const auto keepAlive = shared_from_this();
        std::shared_ptr<TAttempt> attempt;
        IRequestChannelPtr retired;
        std::shared_ptr<THostNotifications> disconnected;
        {
            std::lock_guard guard(Mutex);
            if (!Started || Stopping) {
                return NThreading::MakeFuture<TChannelResult>(MakeError(
                    E_REJECTED,
                    "IC V2 peer connections are not running"));
            }
            auto it = Peers.find(cellId);
            if (it == Peers.end()) {
                return NThreading::MakeFuture<TChannelResult>(MakeError(
                    E_ARGUMENT,
                    "IC V2 peer endpoint is not configured"));
            }
            auto& peer = it->second;
            if (peer.Attempt) {
                return peer.Attempt->Promise.GetFuture();
            }
            if (peer.Channel && !peer.Channel->WhenClosed().HasValue()) {
                return NThreading::MakeFuture<TChannelResult>(peer.Channel);
            }
            if (cellId.second && peer.RetryAt > Timer->Now()) {
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
        if (cellId.second) {
            ResolveHost(cellId, attempt);
        } else {
            ConnectNext(cellId, attempt, {});
        }
        return attempt->Promise.GetFuture();
    }

    void RequestStop() override
    {
        TVector<IRequestChannelPtr> channels;
        TVector<std::shared_ptr<TAttempt>> attempts;
        {
            std::lock_guard guard(Mutex);
            if (Stopping) {
                return;
            }
            Stopping = true;
            for (auto& [cellId, peer]: Peers) {
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
        std::lock_guard lifecycle(LifecycleMutex);
        if (Stopped) {
            return;
        }
        RequestStop();
        Connector->Stop();
        Handshake->Stop();
        Closed.GetFuture().GetValueSync();
        Stopped = true;
    }

private:
    std::shared_ptr<THostNotifications> QueueNotification(
        TPeer& peer,
        EHostEvent event)
    {
        if (peer.Notifications) {
            peer.Notifications->Push(event);
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
        const auto count = notifications->Dispatch();
        {
            std::lock_guard guard(Mutex);
            Y_DEBUG_ABORT_UNLESS(ResultCallbacks >= count);

            ResultCallbacks -= count;
        }
        PublishClosed();
    }

    void KickHost(const TPeerKey& key, ui64 generation)
    {
        {
            std::lock_guard guard(Mutex);
            auto* peer = Peers.FindPtr(key);
            if (Stopping || !peer || peer->Generation != generation ||
                peer->Attempt || peer->Channel)
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

    void ScheduleRetry(const TPeerKey& key, ui64 generation)
    {
        TInstant deadline;
        {
            std::lock_guard guard(Mutex);
            auto* peer = Peers.FindPtr(key);
            if (Stopping || !peer || peer->Generation != generation ||
                peer->Attempt || peer->Channel || peer->RetryAt)
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

    void ResolveHost(
        const TPeerKey& key,
        const std::shared_ptr<TAttempt>& attempt)
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
        Resolver->Resolve(host, port)
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
        const TPeerKey& key,
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
        const TPeer& peer,
        const std::shared_ptr<TAttempt>& attempt) const
    {
        return !Stopping && peer.Attempt == attempt;
    }

    void ConnectNext(
        const TPeerKey& cellId,
        const std::shared_ptr<TAttempt>& attempt,
        NProto::TError error)
    {
        NInterconnect::TAddress address;
        TDuration remaining;
        {
            std::lock_guard guard(Mutex);
            const auto it = Peers.find(cellId);
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
            Finish(cellId, attempt, std::move(error));
            return;
        }
        const std::weak_ptr<TPeerConnections> weak = shared_from_this();
        Connector->Connect(address, remaining)
            .Subscribe(
                [weak, cellId, attempt](const TSocketFuture& future)
                {
                    if (auto self = weak.lock()) {
                        self->OnConnect(cellId, attempt, future.GetValue());
                    }
                });
    }

    void OnConnect(
        const TPeerKey& cellId,
        const std::shared_ptr<TAttempt>& attempt,
        TSocketResult result)
    {
        TDuration remaining;
        {
            std::lock_guard guard(Mutex);
            const auto it = Peers.find(cellId);
            if (it == Peers.end() || !IsCurrent(it->second, attempt)) {
                return;
            }
            const auto now = Timer->Now();
            if (now < attempt->Deadline) {
                remaining = attempt->Deadline - now;
            }
        }
        if (!remaining || HasError(result.GetError())) {
            ConnectNext(cellId, attempt, result.GetError());
            return;
        }
        const std::weak_ptr<TPeerConnections> weak = shared_from_this();
        Handshake->Connect(result.ExtractResult(), remaining)
            .Subscribe(
                [weak, cellId, attempt](const TConnectHandshakeFuture& future)
                {
                    if (auto self = weak.lock()) {
                        self->OnHello(cellId, attempt, future.GetValue());
                    }
                });
    }

    void OnHello(
        const TPeerKey& cellId,
        const std::shared_ptr<TAttempt>& attempt,
        const TConnectHandshakeResult& result)
    {
        {
            std::lock_guard guard(Mutex);
            const auto it = Peers.find(cellId);
            if (it == Peers.end() || !IsCurrent(it->second, attempt)) {
                return;
            }
        }
        if (HasError(result.GetError()) || Timer->Now() >= attempt->Deadline) {
            ConnectNext(cellId, attempt, result.GetError());
            return;
        }
        auto [channel, error] = CreateChannel(result.GetResult());
        if (HasError(error)) {
            Finish(cellId, attempt, std::move(error));
            return;
        }
        bool admitted = false;
        {
            std::lock_guard guard(Mutex);
            const auto it = Peers.find(cellId);
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
        const std::weak_ptr<TPeerConnections> weak = shared_from_this();
        channel->WhenClosed().Subscribe(
            [weak, cellId, generation = attempt->Generation](const auto&)
            {
                if (auto self = weak.lock()) {
                    self->OnClosed(cellId, generation);
                }
            });
        if (!channel->Activate()) {
            Finish(
                cellId,
                attempt,
                MakeError(
                    E_TRANSPORT_ERROR,
                    "IC V2 channel activation failed"));
            return;
        }
        Finish(cellId, attempt, std::move(channel));
    }

    void Finish(
        const TPeerKey& cellId,
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
            const auto it = Peers.find(cellId);
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
            if (!peer.Host.empty()) {
                notifications = QueueNotification(
                    peer,
                    HasError(result.GetError()) ? EHostEvent::Unavailable
                                                : EHostEvent::Connected);
            }
            ++ResultCallbacks;
        }
        if (channel) {
            channel->Close();
        }
        DispatchNotifications(notifications);
        if (cellId.second && HasError(result.GetError())) {
            ScheduleRetry(cellId, attempt->Generation);
        }
        Complete(attempt, std::move(result));
    }

    void OnClosed(const TPeerKey& cellId, ui64 generation)
    {
        std::shared_ptr<TAttempt> attempt;
        IRequestChannelPtr retired;
        std::shared_ptr<THostNotifications> disconnected;
        {
            std::lock_guard guard(Mutex);
            Y_ABORT_UNLESS(ChannelCallbacks);
            const auto it = Peers.find(cellId);
            if (it != Peers.end() && it->second.Generation == generation &&
                it->second.Channel)
            {
                retired = std::move(it->second.Channel);
                --Connections;
                if (!Stopping && !it->second.Attempt) {
                    disconnected =
                        QueueNotification(it->second, EHostEvent::Disconnected);
                }
                attempt = std::move(it->second.Attempt);
                if (attempt) {
                    ++it->second.ConnectionFailures;
                    if (!Stopping) {
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
        if (cellId.second) {
            ScheduleRetry(cellId, generation);
        }
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
            if (!Stopping || ChannelCallbacks || ResultCallbacks || Published) {
                return;
            }
            Published = true;
        }
        Closed.SetValue();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IPeerConnectionsPtr> CreatePeerConnections(
    NCells::TCellsConfigConstPtr config,
    IConnectorPtr connector,
    IHandshakeProviderPtr handshake,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    NCloud::NIcV2::IAddressResolverPtr resolver,
    ITaskQueuePtr completions,
    TChannelFactory createChannel)
{
    if (!config || !connector || !handshake || !timer || !scheduler ||
        !resolver || !completions || !createChannel)
    {
        return MakeError(
            E_ARGUMENT,
            "invalid IC V2 peer connections configuration");
    }
    const auto& options = config->GetCellsConfig().GetIcV2();
    if (!options.GetHandshakeTimeoutMs() ||
        !options.GetMaxOutgoingConnections())
    {
        return MakeError(
            E_ARGUMENT,
            "invalid IC V2 outgoing connection limits");
    }
    THashMap<TPeerKey, TPeer> peers;
    for (const auto& [cellId, cell]: config->GetCells()) {
        if (cellId == config->GetCellId() || !cell->GetIcV2Port()) {
            continue;
        }
        if (cell->GetIcV2Port() > 65535 || cell->GetHosts().empty()) {
            return MakeError(
                E_ARGUMENT,
                "invalid IC V2 peer endpoint: " + cellId);
        }
        auto& peer = peers[TPeerKey{cellId, 0}];
        try {
            for (const auto& host: cell->GetCellConfig().GetHosts()) {
                if (host.GetFqdn().empty()) {
                    return MakeError(
                        E_ARGUMENT,
                        "empty IC V2 peer hostname: " + cellId);
                }
                const TNetworkAddress resolved(
                    host.GetFqdn(),
                    cell->GetIcV2Port());
                for (auto it = resolved.Begin(); it != resolved.End(); ++it) {
                    if (it->ai_family == AF_INET || it->ai_family == AF_INET6) {
                        NAddr::TAddrInfo remote(&*it);
                        peer.Addresses.emplace_back(remote);
                    }
                }
            }
        } catch (const yexception& e) {
            return MakeError(
                E_ARGUMENT,
                "cannot resolve IC V2 peer " + cellId + ": " + e.what());
        }
        if (peer.Addresses.empty()) {
            return MakeError(
                E_ARGUMENT,
                "no IP addresses for IC V2 peer: " + cellId);
        }
    }
    return IPeerConnectionsPtr(
        std::make_shared<TPeerConnections>(
            std::move(peers),
            std::move(connector),
            std::move(handshake),
            std::move(timer),
            std::move(scheduler),
            std::move(resolver),
            std::move(completions),
            std::move(createChannel),
            TDuration::MilliSeconds(options.GetHandshakeTimeoutMs()),
            options.GetMaxOutgoingConnections()));
}

}   // namespace NCloud::NBlockStore::NIcV2
