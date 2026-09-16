#include "host_pool.h"

#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

TCellHostPool::TCellHostPool(TCellConfigPtr config, TBootstrap bootstrap)
    : Config(std::move(config))
    , Bootstrap(std::move(bootstrap))
{
    if (Bootstrap.Logging) {
        // tests build a pool with a bare TBootstrap; a closed TLog swallows
        // everything the STORAGE_* macros hand it
        Log = Bootstrap.Logging->CreateLog("BLOCKSTORE_CELLS");
    }

    if (Config->GetHostMigrationEnabled()) {
        // the pinger reschedules itself HostPingPeriod ahead every sweep, so
        // a zero period would busy-loop the scheduler thread; a zero timeout
        // would fail every ping outright. Neither is a running state to
        // recover from - fail at start rather than limp
        Y_ABORT_UNLESS(
            Config->GetHostPingPeriod(),
            "HostPingPeriod must be non-zero when host migration is enabled");
        Y_ABORT_UNLESS(
            Config->GetHostPingTimeout(),
            "HostPingTimeout must be non-zero when host migration is enabled");
    }

    for (const auto& [fqdn, hostConfig]: Config->GetHosts()) {
        Y_UNUSED(hostConfig);

        auto& channel = Channels[fqdn];
        channel.Configured = true;
        channel.Alive = true;
    }
}

TResultOrError<TCellHostConfig> TCellHostPool::PickHost() const
{
    return PickHostExcept({});
}

TResultOrError<TCellHostConfig> TCellHostPool::PickHostExcept(
    const THashSet<TString>& except) const
{
    TVector<TString> configured;
    TVector<TString> discovered;

    with_lock (Lock) {
        for (const auto& [fqdn, channel]: Channels) {
            if (!channel.Alive || except.contains(fqdn)) {
                continue;
            }

            if (channel.Configured) {
                configured.push_back(fqdn);
            } else {
                discovered.push_back(fqdn);
            }
        }
    }

    // the configured list is the contract; a host we happened to hear about
    // in a mount response only stands in when that contract has nothing live
    // left in it
    const auto& candidates = configured.empty() ? discovered : configured;

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
    ui64 channelEpoch = 0;

    with_lock (Lock) {
        // under the same lock as the write itself: a ping sent before the
        // stop lands after it, and its error must not send every connection
        // looking for a new home on the way out
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

        transition = channel->Alive != alive;
        channel->Alive = alive;

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

        // a discovered host that nobody holds and that stopped answering
        // occupies a slot without earning it
        if (!channel->Configured && !alive && channel->RefCount == 0) {
            Channels.erase(fqdn);
        }

        if (transition && alive) {
            // a configured host is back; a discovered one that was kept only
            // to cover the warm minimum has no reason to linger now
            PruneRetainedDiscoveredLocked();
        }
    }

    // before the watchers run: notifying one starts a migration that logs
    // its own line, and the cause must not read as the consequence
    if (transition) {
        if (alive) {
            STORAGE_INFO(
                "[" << fqdn << "] is answering again in cell "
                    << Config->GetCellId());
        } else {
            STORAGE_WARN(
                "[" << fqdn << "] stopped answering in cell "
                    << Config->GetCellId() << ": " << FormatError(error));
        }
    }

    // outside the lock: a watcher asks the pool for another host, and this
    // lock is not recursive
    for (const auto& watcher: watchers) {
        watcher->OnHostUnavailable(fqdn, channelEpoch);
    }

    return transition;
}

bool TCellHostPool::IsHostKnownDead(const TString& fqdn) const
{
    with_lock (Lock) {
        const auto* channel = Channels.FindPtr(fqdn);
        return channel && !channel->Alive;
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

bool TCellHostPool::WatchHost(const TString& fqdn, ICellHostWatcherPtr watcher)
{
    if (!watcher) {
        return false;
    }

    with_lock (Lock) {
        // only for a channel that exists: watching a host nobody talks to
        // would leak the subscription
        auto* channel = Channels.FindPtr(fqdn);
        if (!channel) {
            return false;
        }

        channel->Watchers.push_back(std::move(watcher));

        // read under the same lock as the subscription: a death that lands
        // after this returns reaches the new watcher, and one that already
        // landed is reported here - so there is no gap in which a verdict
        // goes to nobody
        return !channel->Alive;
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

size_t TCellHostPool::CountLiveChannelsLocked(const TString& except) const
{
    size_t count = 0;
    for (const auto& [fqdn, channel]: Channels) {
        // an entry exists for every configured host from the moment the pool
        // is built; what makes it a connection is having been set up
        if (channel.Alive && channel.Endpoint.Initialized() && fqdn != except)
        {
            ++count;
        }
    }
    return count;
}

void TCellHostPool::TopUpWarmChannelsLocked()
{
    const auto warm = Config->GetMinCellConnections();
    auto live = CountLiveChannelsLocked({});

    for (const auto& [fqdn, hostConfig]: Config->GetHosts()) {
        if (live >= warm) {
            return;
        }

        Y_UNUSED(hostConfig);

        const auto* channel = Channels.FindPtr(fqdn);
        if (channel && (!channel->Alive || channel->Endpoint.Initialized())) {
            // known dead, or already one of the live channels counted above
            continue;
        }

        try {
            EnsureChannelLocked(fqdn);
        } catch (...) {
            // building the gRPC wrapper can throw (a bad address, the client
            // shutting down). One host failing must not abort warming the
            // rest - and, since this runs on the scheduler thread whose task
            // is noexcept, must not escape at all
            STORAGE_WARN(
                "[" << fqdn << "] could not warm a channel in cell "
                    << Config->GetCellId() << ": "
                    << CurrentExceptionMessage());
            continue;
        }
        ++live;
    }
}

void TCellHostPool::PruneRetainedDiscoveredLocked()
{
    // repeated to a fixed point: dropping one retained discovered host can
    // still leave another needed, so each drop is judged against what is
    // left, not against the original set
    bool dropped = true;
    while (dropped) {
        dropped = false;
        for (const auto& [fqdn, channel]: Channels) {
            if (!channel.Configured && channel.RefCount == 0 &&
                CountLiveChannelsLocked(fqdn) >=
                    Config->GetMinCellConnections())
            {
                Channels.erase(fqdn);
                dropped = true;
                break;
            }
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
        channel.Epoch = ++LastEpoch;
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
    with_lock (Lock) {
        TopUpWarmChannelsLocked();
    }

    if (Config->GetHostMigrationEnabled()) {
        SchedulePingSweep();
    }
}

void TCellHostPool::Stop()
{
    // the sweep talks to a gRPC client that is about to go down: left
    // running, it would declare every host dead on the way out and send
    // every connection looking for a new home
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

                if (channel.Configured != configured || !channel.Alive) {
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
        }

        // Configured hosts stay warm. A discovered one normally lives only as
        // long as somebody is mounted through it - but if letting it go would
        // leave the pool short of live channels, it is kept: it is then the
        // only way back into a cell whose configured hosts are all down.
        if (!channel.Configured && channel.RefCount == 0 &&
            CountLiveChannelsLocked(fqdn) >= Config->GetMinCellConnections())
        {
            Channels.erase(it);
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
        NClient::IMultiClientEndpointPtr Endpoint;
    };

    TVector<TTarget> targets;

    try {
        with_lock (Lock) {
            if (Stopped) {
                return;
            }

            // hosts die between sweeps, so the minimum is restored here
            // rather than only at start
            TopUpWarmChannelsLocked();

            for (auto& [fqdn, channel]: Channels) {
            // a channel that has not finished connecting says nothing about
            // the host yet
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
        // one request per host: the endpoint of the pool's client writes the
        // request id and the timestamp into the very request it is given, so
        // sharing one between hosts would be concurrent mutation of a single
        // protobuf
            auto request = std::make_shared<NProto::TPingRequest>();
            auto& headers = *request->MutableHeaders();
            headers.SetRequestTimeout(
                Config->GetHostPingTimeout().MilliSeconds());
            // this endpoint does not go through
            // TClientEndpoint::PrepareRequest, which is where every other
            // cells request gets its client id, and the gRPC client aborts
            // on an empty one. The cell id is always there and makes the
            // pings easy to spot in the server log
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
        // nothing here may escape onto the scheduler thread, whose task is
        // noexcept; and whatever went wrong, the pinger must live to try the
        // next sweep - so the reschedule below stays unconditional
        STORAGE_WARN(
            "[cell " << Config->GetCellId() << "] ping sweep failed: "
                << CurrentExceptionMessage());
    }

    // always, so a throw above cannot silently kill the pinger; a no-op once
    // the pool is stopped
    SchedulePingSweep();
}

void TCellHostPool::HandlePingResult(
    const TString& fqdn,
    ui64 epoch,
    const NProto::TError& error)
{
    // a dead host keeps being reported dead every sweep; ApplyLiveness logs
    // the transitions and stays quiet in between
    const bool transition = ApplyLiveness(fqdn, epoch, !HasError(error), error);

    if (transition && HasError(error)) {
        // a warm host just died; build its replacement now rather than
        // waiting for the next sweep to notice the pool is short. Only on the
        // ping path, which always has an endpoint bootstrap to build with
        with_lock (Lock) {
            // the stop may have landed between ApplyLiveness releasing the
            // lock and our retaking it; building a channel now would revive
            // a pool that is on its way down
            if (!Stopped) {
                TopUpWarmChannelsLocked();
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NCells
