#include "peer_manager_impl.h"

#include "peer_impl.h"

#include <util/generic/vector.h>

namespace NCloud::NIcV2 {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TPeerManager::TPeerManager(TPeerDependencies dependencies)
    : Dependencies(std::move(dependencies))
{}

TPeerManager::~TPeerManager()
{
    Stop();
}

void TPeerManager::Start()
{
    TGuard<TMutex> guard(Mutex);
    Y_ABORT_UNLESS(State == EState::Created);
    // No peers can be created before the workers have started.
    Dependencies.Connector->Start();
    Dependencies.Handshake->Start();
    State = EState::Running;
}

TResultOrError<IPeerPtr> TPeerManager::CreatePeer(
    TString host,
    ui16 port,
    IClientEndpointHandlerPtr handler)
{
    std::shared_ptr<TPeer> peer;
    {
        TGuard<TMutex> guard(Mutex);
        if (State != EState::Running) {
            return MakeError(
                E_REJECTED,
                "IC V2 peer connections are not running");
        }
        if (host.empty() || !port) {
            return MakeError(E_ARGUMENT, "invalid IC V2 peer address");
        }
        const auto id = NextPeerId++;
        peer = std::make_shared<TPeer>(
            shared_from_this(),
            id,
            Dependencies,
            std::move(host),
            port,
            std::move(handler));
        Peers.emplace(id, peer);
    }
    peer->GetChannel();
    return IPeerPtr(std::move(peer));
}

void TPeerManager::RequestStop()
{
    TVector<std::shared_ptr<TPeer>> peers;
    {
        TGuard<TMutex> guard(Mutex);
        if (State >= EState::StopRequested) {
            return;
        }
        State = EState::StopRequested;
        for (auto& [peerId, weakPeer]: Peers) {
            if (auto peer = weakPeer.lock()) {
                peers.push_back(std::move(peer));
            }
        }
    }
    for (const auto& peer: peers) {
        peer->Close(
            MakeError(E_REJECTED, "IC V2 peer connections are stopping"));
    }
    Dependencies.Connector->RequestStop();
    Dependencies.Handshake->RequestStop();
    {
        TGuard<TMutex> guard(Mutex);
        if (!Peers.empty()) {
            return;
        }
    }
    Closed.TrySetValue();
}

void TPeerManager::Stop()
{
    RequestStop();
    {
        TGuard<TMutex> guard(Mutex);
        if (State == EState::Stopped) {
            return;
        }
        if (State == EState::Joining) {
            guard.Release();
            Joined.GetFuture().GetValueSync();
            return;
        }
        State = EState::Joining;
    }
    Dependencies.Connector->Stop();
    Dependencies.Handshake->Stop();
    Closed.GetFuture().GetValueSync();
    {
        TGuard<TMutex> guard(Mutex);
        State = EState::Stopped;
    }
    Joined.SetValue();
}

void TPeerManager::UnregisterPeer(ui64 id)
{
    {
        TGuard<TMutex> guard(Mutex);
        Peers.erase(id);
        if (State < EState::StopRequested || !Peers.empty()) {
            return;
        }
    }
    Closed.TrySetValue();
}

////////////////////////////////////////////////////////////////////////////////

IChannelFactory::~IChannelFactory() = default;

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IPeerManagerPtr> CreatePeerManager(
    TPeerManagerConfig config,
    IConnectorPtr connector,
    IHandshakeProviderPtr handshake,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IAddressResolverPtr resolver,
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
    if (!config.ConnectTimeout) {
        return MakeError(E_ARGUMENT, "invalid IC V2 connect timeout");
    }
    return IPeerManagerPtr(
        std::make_shared<TPeerManager>(TPeerDependencies{
            .Connector = std::move(connector),
            .Handshake = std::move(handshake),
            .Timer = std::move(timer),
            .Scheduler = std::move(scheduler),
            .Resolver = std::move(resolver),
            .Completions = std::move(completions),
            .ChannelFactory = std::move(channelFactory),
            .ConnectTimeout = config.ConnectTimeout,
        }));
}

}   // namespace NCloud::NIcV2
