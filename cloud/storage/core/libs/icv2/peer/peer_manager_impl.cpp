#include "peer_manager_impl.h"

#include "peer_impl.h"

#include <util/generic/vector.h>
#include <util/system/guard.h>

namespace NCloud::NIcV2 {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

void TPeerRegistry::UnregisterPeer(ui64 id)
{
    {
        TGuard<TAdaptiveLock> guard(Lock);
        Peers.erase(id);
        if (State < EState::StopRequested || !Peers.empty()) {
            return;
        }
    }
    Closed.TrySetValue();
}

////////////////////////////////////////////////////////////////////////////////

TPeerManager::TPeerManager(TPeerDependencies dependencies)
    : Dependencies(std::move(dependencies))
    , Registry(std::make_shared<TPeerRegistry>())
{}

TPeerManager::~TPeerManager()
{
    Stop();
}

void TPeerManager::Start()
{
    TGuard<TAdaptiveLock> guard(Registry->Lock);
    Y_ABORT_UNLESS(Registry->State == EState::Created);
    // No peers can be created before the workers have started.
    Dependencies.Connector->Start();
    Dependencies.Handshake->Start();
    Registry->State = EState::Running;
}

TResultOrError<IPeerPtr> TPeerManager::CreatePeer(
    TString host,
    ui16 port,
    IClientConnectionHandlerPtr handler)
{
    std::shared_ptr<TPeer> peer;
    {
        TGuard<TAdaptiveLock> guard(Registry->Lock);
        if (Registry->State != EState::Running) {
            return MakeError(
                E_REJECTED,
                "IC V2 peer connections are not running");
        }
        if (host.empty() || !port) {
            return MakeError(E_ARGUMENT, "invalid IC V2 peer address");
        }
        const auto id = NextPeerId++;
        peer = std::make_shared<TPeer>(
            Registry,
            id,
            Dependencies,
            std::move(host),
            port,
            std::move(handler));
        Registry->Peers.emplace(id, peer);
    }
    peer->GetRequestChannel();
    return IPeerPtr(std::move(peer));
}

void TPeerManager::RequestStop()
{
    TVector<std::shared_ptr<TPeer>> peers;
    {
        TGuard<TAdaptiveLock> guard(Registry->Lock);
        if (Registry->State >= EState::StopRequested) {
            return;
        }
        Registry->State = EState::StopRequested;
        for (auto& [peerId, weakPeer]: Registry->Peers) {
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
        TGuard<TAdaptiveLock> guard(Registry->Lock);
        if (!Registry->Peers.empty()) {
            return;
        }
    }
    Registry->Closed.TrySetValue();
}

void TPeerManager::Stop()
{
    RequestStop();
    {
        TGuard<TAdaptiveLock> guard(Registry->Lock);
        if (Registry->State == EState::Stopped) {
            return;
        }
        if (Registry->State == EState::Joining) {
            guard.Release();
            Joined.GetFuture().GetValueSync();
            return;
        }
        Registry->State = EState::Joining;
    }
    Dependencies.Connector->Stop();
    Dependencies.Handshake->Stop();
    Registry->Closed.GetFuture().GetValueSync();
    {
        TGuard<TAdaptiveLock> guard(Registry->Lock);
        Registry->State = EState::Stopped;
    }
    Joined.SetValue();
}

////////////////////////////////////////////////////////////////////////////////

IRequestChannelFactory::~IRequestChannelFactory() = default;

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IPeerManagerPtr> CreatePeerManager(
    TPeerManagerConfig config,
    IConnectorPtr connector,
    IHandshakeProviderPtr handshake,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IAddressResolverPtr resolver,
    ITaskQueuePtr completions,
    IRequestChannelFactoryPtr channelFactory)
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
