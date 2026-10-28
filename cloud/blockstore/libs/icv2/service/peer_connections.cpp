#include "peer_connections.h"

#include <cloud/blockstore/libs/cells/iface/config.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/network/address.h>

#include <mutex>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TPeerKey = std::pair<TString, ui64>;

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
    ui64 Generation = 0;
    ui64 ConnectionFailures = 0;
    std::shared_ptr<TAttempt> Attempt;
    IRequestChannelPtr Channel;
};

class TPeerConnections final
    : public IPeerConnections
    , public std::enable_shared_from_this<TPeerConnections>
{
    const IConnectorPtr Connector;
    const IHandshakeProviderPtr Handshake;
    const ITimerPtr Timer;
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
        ITaskQueuePtr completions,
        TChannelFactory createChannel,
        TDuration connectTimeout,
        size_t maxConnections)
        : Connector(std::move(connector))
        , Handshake(std::move(handshake))
        , Timer(std::move(timer))
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
        TVector<NInterconnect::TAddress> addresses) override
    {
        std::lock_guard guard(Mutex);
        if (Stopping) {
            return MakeError(E_REJECTED, "IC V2 peer connections are stopping");
        }
        if (bindingId <= LastBindingId || addresses.empty() ||
            !Peers.contains({cellId, 0}))
        {
            return MakeError(E_ARGUMENT, "invalid IC V2 host binding");
        }
        if (HostBindings >= MaxConnections) {
            return MakeError(E_REJECTED, "IC V2 host binding limit reached");
        }
        TPeer peer;
        peer.Addresses = std::move(addresses);
        if (!Peers.emplace(TPeerKey{cellId, bindingId}, std::move(peer)).second)
        {
            return MakeError(E_ARGUMENT, "duplicate IC V2 host binding");
        }
        LastBindingId = bindingId;
        ++HostBindings;
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
            if (peer.Channel) {
                retired = std::move(peer.Channel);
                --Connections;
            }
            if (Connections + ResultCallbacks >= MaxConnections) {
                return NThreading::MakeFuture<TChannelResult>(MakeError(
                    E_REJECTED,
                    "IC V2 outgoing connection limit reached"));
            }
            Y_ABORT_UNLESS(
                ++peer.Generation,
                "IC V2 peer generations exhausted");
            attempt = std::make_shared<TAttempt>(
                peer.Generation,
                Timer->Now() + ConnectTimeout);
            peer.Attempt = attempt;
            ++Connections;
        }
        ConnectNext(cellId, attempt, {});
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
                [weak, cellId, attempt](const THandshakeFuture& future)
                {
                    if (auto self = weak.lock()) {
                        self->OnHello(cellId, attempt, future.GetValue());
                    }
                });
    }

    void OnHello(
        const TPeerKey& cellId,
        const std::shared_ptr<TAttempt>& attempt,
        const THandshakeResult& result)
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
            ++ResultCallbacks;
        }
        if (channel) {
            channel->Close();
        }
        Complete(attempt, std::move(result));
    }

    void OnClosed(const TPeerKey& cellId, ui64 generation)
    {
        std::shared_ptr<TAttempt> attempt;
        IRequestChannelPtr retired;
        {
            std::lock_guard guard(Mutex);
            Y_ABORT_UNLESS(ChannelCallbacks);
            --ChannelCallbacks;
            const auto it = Peers.find(cellId);
            if (it != Peers.end() && it->second.Generation == generation &&
                it->second.Channel)
            {
                retired = std::move(it->second.Channel);
                --Connections;
                attempt = std::move(it->second.Attempt);
                if (attempt) {
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
    ITaskQueuePtr completions,
    TChannelFactory createChannel)
{
    if (!config || !connector || !handshake || !timer || !completions ||
        !createChannel)
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
            std::move(completions),
            std::move(createChannel),
            TDuration::MilliSeconds(options.GetHandshakeTimeoutMs()),
            options.GetMaxOutgoingConnections()));
}

}   // namespace NCloud::NBlockStore::NIcV2
