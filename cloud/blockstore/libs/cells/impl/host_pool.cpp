#include "host_pool.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NCells {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

TCellCounters MakeCellCounters(
    const IMonitoringServicePtr& monitoring,
    const TString& cellId)
{
    // tests build a pool with a bare TBootstrap
    auto group = monitoring
        ? monitoring->GetCounters()
              ->GetSubgroup("counters", "blockstore")
              ->GetSubgroup("component", "cells")
              ->GetSubgroup("cell", cellId)
        : MakeIntrusive<TDynamicCounters>();

    return {
        .HostsConfigured = group->GetCounter("HostsConfigured"),
        .HostsUnavailable = group->GetCounter("HostsUnavailable"),
        .Connections = group->GetCounter("Connections"),
        .HostBecameUnavailable =
            group->GetCounter("HostBecameUnavailable", true),
        .HostBecameAvailable = group->GetCounter("HostBecameAvailable", true),
        .ChannelWarmupErrors = group->GetCounter("ChannelWarmupErrors", true),
        .DescribeEndpointErrors =
            group->GetCounter("DescribeEndpointErrors", true),
        .PingSweepErrors = group->GetCounter("PingSweepErrors", true),
        .Migrations = group->GetCounter("Migrations", true),
        .MigrationFailures = group->GetCounter("MigrationFailures", true),
        .NoMigrationTarget = group->GetCounter("NoMigrationTarget", true),
    };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCellHostPool::TCellHostPool(TCellConfigPtr config, TBootstrap bootstrap)
    : Config(std::move(config))
    , Bootstrap(std::move(bootstrap))
    , Counters(MakeCellCounters(Bootstrap.Monitoring, Config->GetCellId()))
{
    if (Bootstrap.Logging) {
        Log = Bootstrap.Logging->CreateLog("BLOCKSTORE_CELLS");
    }

    Y_ABORT_UNLESS(Config->GetHostPingPeriod(), "HostPingPeriod must be non-zero");
    Y_ABORT_UNLESS(
        Config->GetHostPingTimeout(),
        "HostPingTimeout must be non-zero");

    for (const auto& [fqdn, hostConfig]: Config->GetHosts()) {
        Y_UNUSED(hostConfig);

        Channels[fqdn].Configured = true;
    }

    *Counters.HostsConfigured = Config->GetHosts().size();
}

TResultOrError<TCellHostConfig> TCellHostPool::PickHost() const
{
    return PickHostImpl({}, false);
}

TResultOrError<TCellHostConfig> TCellHostPool::PickHostExcept(
    const THashSet<TString>& except) const
{
    return PickHostImpl(except, true);
}

TResultOrError<TCellHostConfig> TCellHostPool::PickHostImpl(
    const THashSet<TString>& except,
    bool preferVerified) const
{
    TVector<TString> verifiedConfigured;
    TVector<TString> verifiedDiscovered;
    TVector<TString> unknownConfigured;
    TVector<TString> unknownDiscovered;

    with_lock (Lock) {
        for (const auto& [fqdn, channel]: Channels) {
            if (channel.Liveness == EHostLiveness::Dead ||
                except.contains(fqdn))
            {
                continue;
            }

            const bool verified = channel.Liveness == EHostLiveness::Alive;
            auto& tier = channel.Configured
                ? (verified ? verifiedConfigured : unknownConfigured)
                : (verified ? verifiedDiscovered : unknownDiscovered);
            tier.push_back(fqdn);
        }
    }

    if (!preferVerified) {
        verifiedConfigured.insert(
            verifiedConfigured.end(),
            unknownConfigured.begin(),
            unknownConfigured.end());
        verifiedDiscovered.insert(
            verifiedDiscovered.end(),
            unknownDiscovered.begin(),
            unknownDiscovered.end());
        unknownConfigured.clear();
        unknownDiscovered.clear();
    }

    for (const auto* candidates:
         {&verifiedConfigured,
          &verifiedDiscovered,
          &unknownConfigured,
          &unknownDiscovered})
    {
        if (!candidates->empty()) {
            const auto& fqdn =
                (*candidates)[RandomNumber<ui32>(candidates->size())];
            return MakeHostConfig(fqdn);
        }
    }

    return MakeError(
        E_REJECTED,
        TStringBuilder() << "No live hosts in cell " << Config->GetCellId());
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

bool TCellHostPool::SetHostAlive(const TString& fqdn, bool alive)
{
    return ApplyLiveness(fqdn, {}, alive, {});
}

bool TCellHostPool::SetHostAlive(
    const TString& fqdn,
    bool alive,
    const NProto::TError& error)
{
    return ApplyLiveness(fqdn, {}, alive, error);
}

bool TCellHostPool::ApplyLiveness(
    const TString& fqdn,
    const TMaybe<ui64>& epoch,
    bool alive,
    const NProto::TError& error)
{
    TVector<ICellHostWatcherPtr> watchers;
    bool transition = false;
    bool recovered = false;
    ui64 channelEpoch = 0;

    with_lock (Lock) {
        if (Stopped) {
            return false;
        }

        auto* channel = Channels.FindPtr(fqdn);
        if (!channel) {
            return false;
        }

        if (epoch && *epoch != channel->Epoch) {
            // the channel this came from has been dropped and built again
            // since; it says nothing about the one standing in its place
            return false;
        }

        if (epoch) {
            // answered, so the next sweep may probe this channel again. Done
            // here rather than in the caller so that no sweep can slip in
            // between the release and the liveness it belongs to
            channel->PingInFlight = false;
        }

        channelEpoch = channel->Epoch;

        const auto liveness =
            alive ? EHostLiveness::Alive : EHostLiveness::Dead;
        transition = channel->Liveness != liveness;
        recovered = transition && channel->Liveness == EHostLiveness::Dead;
        AccountChannel(*channel, -1);
        channel->Liveness = liveness;
        AccountChannel(*channel, 1);
        if (recovered) {
            Counters.HostsUnavailable->Dec();
        } else if (transition && !alive) {
            Counters.HostsUnavailable->Inc();
        }

        // pruned whatever the liveness: a connection cannot unwatch itself
        // from its own destructor, so a host that never dies would keep a
        // slot for every connection ever made on it
        EraseIf(
            channel->Watchers,
            [&](const auto& w)
            {
                auto watcher = w.lock();
                if (!watcher) {
                    return true;
                }
                if (!alive) {
                    watchers.push_back(std::move(watcher));
                }
                return false;
            });

        if (!channel->Configured && !alive && channel->RefCount == 0) {
            EraseChannel(Channels.find(fqdn));
        }

        if (recovered) {
            DropUnneededDiscoveredHosts();
        }
    }

    if (transition) {
        if (recovered) {
            Counters.HostBecameAvailable->Inc();
            STORAGE_INFO(
                "[" << fqdn << "] is answering again in cell "
                    << Config->GetCellId());
        } else if (alive) {
            STORAGE_INFO(
                "[" << fqdn << "] is answering in cell " << Config->GetCellId());
        } else {
            Counters.HostBecameUnavailable->Inc();
            STORAGE_WARN(
                "[" << fqdn << "] stopped answering in cell "
                    << Config->GetCellId() << ": " << FormatError(error));
        }
    }

    for (const auto& watcher: watchers) {
        watcher->OnHostUnavailable(fqdn, channelEpoch);
    }

    return transition;
}

bool TCellHostPool::IsHostKnownDead(const TString& fqdn) const
{
    with_lock (Lock) {
        const auto* channel = Channels.FindPtr(fqdn);
        return channel && channel->Liveness == EHostLiveness::Dead;
    }
}

ui64 TCellHostPool::GetChannelEpoch(const TString& fqdn) const
{
    with_lock (Lock) {
        const auto* channel = Channels.FindPtr(fqdn);
        return channel ? channel->Epoch : 0;
    }
}

TString TCellHostPool::GetCellId() const
{
    // Config is immutable, so no lock is needed here.
    return Config->GetCellId();
}

const TCellCounters& TCellHostPool::GetCounters() const
{
    return Counters;
}

TVector<TCellHostPool::THostStatus> TCellHostPool::GetHostStatuses() const
{
    TVector<THostStatus> result;

    with_lock (Lock) {
        result.reserve(Channels.size());
        for (const auto& [fqdn, channel]: Channels) {
            result.push_back(THostStatus{
                .Fqdn = fqdn,
                .Liveness = channel.Liveness,
                .Warm = channel.Endpoint.Initialized(),
                .Connections = channel.RefCount,
            });
        }
    }

    return result;
}

bool TCellHostPool::WatchHost(const TString& fqdn, ICellHostWatcherPtr watcher)
{
    if (!watcher) {
        return false;
    }

    with_lock (Lock) {
        auto* channel = Channels.FindPtr(fqdn);
        if (!channel) {
            return false;
        }

        channel->Watchers.push_back(std::move(watcher));

        return channel->Liveness == EHostLiveness::Dead;
    }
}

void TCellHostPool::UnwatchHost(
    const TString& fqdn,
    const ICellHostWatcherPtr& watcher)
{
    with_lock (Lock) {
        if (auto* channel = Channels.FindPtr(fqdn)) {
            EraseIf(
                channel->Watchers,
                [&](const auto& w) { return w.lock() == watcher; });
        }
    }
}

size_t TCellHostPool::GetWatcherCount(const TString& fqdn) const
{
    with_lock (Lock) {
        const auto* channel = Channels.FindPtr(fqdn);
        if (!channel) {
            return 0;
        }

        return channel->Watchers.size();
    }
}

bool TCellHostPool::IsLive(const TChannel& channel)
{
    return channel.Liveness != EHostLiveness::Dead &&
           channel.Endpoint.Initialized();
}

void TCellHostPool::AccountChannel(const TChannel& channel, int delta)
{
    if (IsLive(channel)) {
        LiveChannels += delta;
    }
}

void TCellHostPool::EraseChannel(
    THashMap<TString, TChannel>::iterator it)
{
    AccountChannel(it->second, -1);
    if (it->second.Liveness == EHostLiveness::Dead) {
        Counters.HostsUnavailable->Dec();
    }
    Channels.erase(it);
}

size_t TCellHostPool::CountLiveChannelsExcept(
    const TChannel& except) const
{
    return LiveChannels - IsLive(except);
}

void TCellHostPool::EnsureMinLiveChannels()
{
    const auto warm = Config->GetMinCellConnections();
    auto live = LiveChannels;

    for (const auto& [fqdn, hostConfig]: Config->GetHosts()) {
        if (live >= warm) {
            return;
        }

        Y_UNUSED(hostConfig);

        const auto* channel = Channels.FindPtr(fqdn);
        if (channel && (channel->Liveness == EHostLiveness::Dead ||
                        channel->Endpoint.Initialized()))
        {
            // known dead, or already one of the live channels counted above
            continue;
        }

        try {
            EnsureChannel(fqdn);
        } catch (...) {
            Counters.ChannelWarmupErrors->Inc();
            STORAGE_WARN(
                "[" << fqdn << "] could not warm a channel in cell "
                    << Config->GetCellId() << ": "
                    << CurrentExceptionMessage());
            continue;
        }
        ++live;
    }
}

void TCellHostPool::DropUnneededDiscoveredHosts()
{
    for (auto it = Channels.begin(); it != Channels.end();) {
        auto current = it++;
        const auto& channel = current->second;
        if (!channel.Configured && channel.RefCount == 0 &&
            CountLiveChannelsExcept(channel) >=
                Config->GetMinCellConnections())
        {
            EraseChannel(current);
        }
    }
}

ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture
TCellHostPool::EnsureChannel(const TString& fqdn)
{
    auto& channel = Channels[fqdn];
    if (!channel.Endpoint.Initialized()) {
        // cheap: a channel and a stub, nothing connects yet
        channel.Endpoint = Bootstrap.EndpointsSetup->SetupHostGrpcEndpoint(
            Bootstrap,
            MakeHostConfig(fqdn));
        channel.Epoch = ++LastEpoch;
        AccountChannel(channel, 1);
    }

    return channel.Endpoint;
}

ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture
TCellHostPool::AcquireControlChannel(const TString& fqdn)
{
    with_lock (Lock) {
        auto future = EnsureChannel(fqdn);
        Channels[fqdn].RefCount++;
        Counters.Connections->Inc();
        return future;
    }
}

void TCellHostPool::Start()
{
    with_lock (Lock) {
        EnsureMinLiveChannels();
    }

    SchedulePingSweep();
}

void TCellHostPool::Stop()
{
    with_lock (Lock) {
        Stopped = true;
    }
}

TCellHostEndpoints TCellHostPool::GetDescribeEndpoints(
    const NClient::TClientAppConfigPtr& clientConfig)
{
    auto count = Config->GetDescribeVolumeHostCount();

    TCellHostEndpoints result;

    with_lock (Lock) {
        // two passes: a describe goes to configured hosts while any of them
        // answers, and only falls back to what we heard about at runtime
        for (bool configured: {true, false}) {
            for (auto& [fqdn, channel]: Channels) {
                if (!count) {
                    break;
                }

                if (channel.Configured != configured ||
                    channel.Liveness == EHostLiveness::Dead)
                {
                    continue;
                }

                try {
                    auto future = EnsureChannel(fqdn);
                    if (!future.HasValue() || !future.GetValue()) {
                        // still connecting - skip it rather than block a
                        // describe
                        continue;
                    }

                    auto endpoint = future.GetValue();

                    --count;
                    result.emplace_back(
                        clientConfig,
                        fqdn,
                        std::move(endpoint),
                        nullptr);
                } catch (...) {
                    Counters.DescribeEndpointErrors->Inc();
                    STORAGE_WARN(
                        "[" << fqdn << "] could not build a describe endpoint "
                            "in cell " << Config->GetCellId() << ": "
                            << CurrentExceptionMessage());
                    continue;
                }
            }

            if (!result.empty()) {
                break;
            }
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
            Counters.Connections->Dec();
        }

        // remove unused discovered(heard from MountResponse) channels
        if (!channel.Configured && channel.RefCount == 0 &&
            CountLiveChannelsExcept(channel) >=
                Config->GetMinCellConnections())
        {
            EraseChannel(it);
        }
    }
}

void TCellHostPool::SchedulePingSweep()
{
    with_lock (Lock) {
        if (Stopped) {
            return;
        }
    }

    Bootstrap.Scheduler->Schedule(
        Bootstrap.Timer->Now() + Config->GetHostPingPeriod(),
        [weakSelf = weak_from_this()]
        {
            if (auto self = weakSelf.lock()) {
                self->PingSweep();
            }
        });
}

void TCellHostPool::PingSweep()
{
    struct TTarget
    {
        TString Fqdn;
        ui64 Epoch;
        IBlockStorePtr Endpoint;
    };

    TVector<TTarget> targets;

    try {
        with_lock (Lock) {
            if (Stopped) {
                return;
            }

            // hosts die between sweeps, so the minimum is restored here
            // rather than only at start
            EnsureMinLiveChannels();

            for (auto& [fqdn, channel]: Channels) {
                if (!channel.Endpoint.Initialized() ||
                    !channel.Endpoint.HasValue() ||
                    !channel.Endpoint.GetValue())
                {
                    continue;
                }

                if (channel.PingInFlight) {
                    // the previous answer is still on its way; sending
                    // another would only make the two race for the last word
                    continue;
                }

                channel.PingInFlight = true;
                targets.push_back(
                    {fqdn, channel.Epoch, channel.Endpoint.GetValue()});
            }
        }

        for (auto& [fqdn, epoch, endpoint]: targets) {
            auto request = std::make_shared<NProto::TPingRequest>();
            auto& headers = *request->MutableHeaders();
            headers.SetRequestTimeout(
                Config->GetHostPingTimeout().MilliSeconds());
            headers.SetClientId(Config->GetCellId());

            endpoint->Ping(MakeIntrusive<TCallContext>(), std::move(request))
                .Subscribe(
                    [weakSelf = weak_from_this(), fqdn, epoch](
                        const auto& future)
                    {
                        auto self = weakSelf.lock();
                        if (!self) {
                            return;
                        }
                        self->HandlePingResult(
                            fqdn,
                            epoch,
                            future.GetValue().GetError());
                    });
        }
    } catch (...) {
        Counters.PingSweepErrors->Inc();
        STORAGE_WARN(
            "[cell " << Config->GetCellId() << "] ping sweep failed: "
                << CurrentExceptionMessage());
    }

    SchedulePingSweep();
}

void TCellHostPool::HandlePingResult(
    const TString& fqdn,
    ui64 epoch,
    const NProto::TError& error)
{
    const bool transition = ApplyLiveness(fqdn, epoch, !HasError(error), error);

    if (transition && HasError(error)) {
        with_lock (Lock) {
            if (!Stopped) {
                EnsureMinLiveChannels();
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NCells
