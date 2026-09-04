#include "host_pool.h"

#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

TCellHostPool::TCellHostPool(TCellConfigPtr config, TBootstrap bootstrap)
    : Config(std::move(config))
    , Bootstrap(std::move(bootstrap))
{
    for (const auto& [fqdn, hostConfig]: Config->GetHosts()) {
        Y_UNUSED(hostConfig);

        auto& channel = Channels[fqdn];
        channel.Configured = true;
        channel.Alive = true;
    }
}

TResultOrError<TCellHostConfig> TCellHostPool::PickConfiguredHost() const
{
    TVector<TString> candidates;

    with_lock (Lock) {
        for (const auto& [fqdn, channel]: Channels) {
            if (channel.Configured && channel.Alive) {
                candidates.push_back(fqdn);
            }
        }
    }

    if (candidates.empty()) {
        return MakeError(
            E_REJECTED,
            TStringBuilder()
                << "No live hosts in cell " << Config->GetCellId());
    }

    const auto& fqdn = candidates[RandomNumber<ui32>(candidates.size())];
    return MakeHostConfig(fqdn);
}

TCellHostConfig TCellHostPool::MakeHostConfig(const TString& fqdn) const
{
    // Config is immutable, so no lock is needed here.
    if (const auto* known = Config->GetHosts().FindPtr(fqdn)) {
        return *known;
    }

    NProto::TCellHostConfig proto;
    proto.SetFqdn(fqdn);

    return TCellHostConfig(proto, *Config);
}

void TCellHostPool::SetHostAlive(const TString& fqdn, bool alive)
{
    with_lock (Lock) {
        if (auto* channel = Channels.FindPtr(fqdn);
            channel && channel->Configured)
        {
            channel->Alive = alive;
        }
    }
}

ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture
TCellHostPool::EnsureChannelLocked(const TString& fqdn)
{
    auto& channel = Channels[fqdn];
    if (!channel.Endpoint.Initialized()) {
        // cheap: the setup only wraps an already pooled gRPC client
        channel.Endpoint = Bootstrap.EndpointsSetup->SetupHostGrpcEndpoint(
            Bootstrap,
            MakeHostConfig(fqdn));
    }

    return channel.Endpoint;
}

ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture
TCellHostPool::AcquireControlChannel(const TString& fqdn)
{
    with_lock (Lock) {
        auto future = EnsureChannelLocked(fqdn);
        Channels[fqdn].RefCount++;
        return future;
    }
}

void TCellHostPool::Start()
{
    auto warm = Config->GetMinCellConnections();

    with_lock (Lock) {
        for (const auto& [fqdn, hostConfig]: Config->GetHosts()) {
            Y_UNUSED(hostConfig);

            if (!warm) {
                break;
            }
            --warm;

            EnsureChannelLocked(fqdn);
        }
    }
}

TCellHostEndpoints TCellHostPool::GetDescribeEndpoints(
    const NClient::TClientAppConfigPtr& clientConfig)
{
    auto count = Config->GetDescribeVolumeHostCount();

    TCellHostEndpoints result;

    with_lock (Lock) {
        for (auto& [fqdn, channel]: Channels) {
            if (!count) {
                break;
            }

            if (!channel.Configured || !channel.Alive) {
                continue;
            }

            auto future = EnsureChannelLocked(fqdn);
            if (!future.HasValue() || !future.GetValue()) {
                // still connecting - skip it rather than block a describe
                continue;
            }

            --count;
            result.emplace_back(
                clientConfig,
                fqdn,
                future.GetValue()->CreateClientEndpoint(
                    clientConfig->GetClientId(),
                    clientConfig->GetInstanceId()),
                nullptr);
        }
    }

    return result;
}

void TCellHostPool::ReleaseControlChannel(const TString& fqdn)
{
    with_lock (Lock) {
        auto it = Channels.find(fqdn);
        if (it == Channels.end()) {
            return;
        }

        auto& channel = it->second;
        if (channel.RefCount) {
            --channel.RefCount;
        }

        // configured hosts stay warm, discovered ones live only as long as
        // somebody is mounted through them
        if (!channel.Configured && channel.RefCount == 0) {
            Channels.erase(it);
        }
    }
}


}   // namespace NCloud::NBlockStore::NCells
