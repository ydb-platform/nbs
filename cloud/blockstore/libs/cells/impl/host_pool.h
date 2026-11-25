#pragma once

#include "bootstrap.h"
#include "endpoint_bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/client/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Told when the host it is attached to stops answering.
//
// Called from the pinger's thread, so an implementation must not block: the
// same sweep serves every host of the cell.
//
// The pool holds watchers weakly, so releasing a control channel is enough to
// stop being told - and a watcher that is gone is dropped on the next sweep.
struct ICellHostWatcher
{
    virtual ~ICellHostWatcher() = default;

    // Carries the host it is about and the channel incarnation the verdict
    // was formed on: a watcher can have moved on - even off and back onto the
    // same host, on a fresh channel - between the moment the pool picked it
    // up and the moment it is called, and the epoch lets it tell the two
    // apart.
    virtual void OnHostUnavailable(
        const TString& fqdn,
        ui64 epoch) noexcept = 0;
};

using ICellHostWatcherPtr = std::shared_ptr<ICellHostWatcher>;

////////////////////////////////////////////////////////////////////////////////

// The cell's sensors, under counters=blockstore/component=cells/cell=<id>.
// HostsUnavailable counts the hosts found dead, not the ones never asked: a
// host is only pinged while its channel is open, and only with migration on.
struct TCellCounters
{
    NMonitoring::TDynamicCounters::TCounterPtr HostsConfigured;
    NMonitoring::TDynamicCounters::TCounterPtr HostsUnavailable;
    NMonitoring::TDynamicCounters::TCounterPtr Connections;

    NMonitoring::TDynamicCounters::TCounterPtr HostBecameUnavailable;
    NMonitoring::TDynamicCounters::TCounterPtr HostBecameAvailable;
    NMonitoring::TDynamicCounters::TCounterPtr ChannelWarmupErrors;
    NMonitoring::TDynamicCounters::TCounterPtr DescribeEndpointErrors;
    NMonitoring::TDynamicCounters::TCounterPtr PingSweepErrors;

    NMonitoring::TDynamicCounters::TCounterPtr Migrations;
    NMonitoring::TDynamicCounters::TCounterPtr MigrationFailures;
    NMonitoring::TDynamicCounters::TCounterPtr NoMigrationTarget;
};

////////////////////////////////////////////////////////////////////////////////

// Keeps the control channels to the hosts of a single cell and tracks which
// of them are usable.
//
// Two populations live here. Configured hosts come from TCellConfig and are
// the guaranteed way back into the cell. The rest are discovered at runtime
// from the tablet host reported in mount responses.
//
// The pool keeps MinCellConnections live channels at all times, not only at
// start: when a warm host dies another configured one is warmed in its place,
// and when no configured host is left to try, discovered hosts are kept
// instead of being dropped - a cell is thousands of hosts and the configured
// list is tens of them, so the two populations can die together. Discovered
// hosts only ever fill what the configured ones cannot.
//
// Thread-safe.
class TCellHostPool
    : public std::enable_shared_from_this<TCellHostPool>
{
public:
    struct THostStatus
    {
        TString Fqdn;
        EHostLiveness Liveness = EHostLiveness::Unknown;
        bool Warm = false;
        size_t Connections = 0;
    };

private:
    struct TChannel
    {
        ui64 Epoch = 0;

        bool PingInFlight = false;

        ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture Endpoint;
        ui32 RefCount = 0;
        bool Configured = false;
        EHostLiveness Liveness = EHostLiveness::Unknown;
        TVector<std::weak_ptr<ICellHostWatcher>> Watchers;
    };

    const TCellConfigPtr Config;
    const TBootstrap Bootstrap;
    const TCellCounters Counters;

    TLog Log;

    mutable TAdaptiveLock Lock;
    THashMap<TString, TChannel> Channels;
    ui64 LastEpoch = 0;
    size_t LiveChannels = 0;
    bool Stopped = false;

public:
    TCellHostPool(TCellConfigPtr config, TBootstrap bootstrap);

    void Start();
    void Stop();

    [[nodiscard]] TCellHostEndpoints GetDescribeEndpoints(
        const NClient::TClientAppConfigPtr& clientConfig);

    [[nodiscard]] TResultOrError<TCellHostConfig> PickHost() const;

    [[nodiscard]] TResultOrError<TCellHostConfig> PickHostExcept(
        const THashSet<TString>& except) const;

    [[nodiscard]] TCellHostConfig MakeHostConfig(const TString& fqdn) const;

    bool SetHostAlive(const TString& fqdn, bool alive);
    bool SetHostAlive(
        const TString& fqdn,
        bool alive,
        const NProto::TError& error);

    [[nodiscard]] bool IsHostKnownDead(const TString& fqdn) const;

    [[nodiscard]] ui64 GetChannelEpoch(const TString& fqdn) const;

    [[nodiscard]] TVector<THostStatus> GetHostStatuses() const;

    [[nodiscard]] bool WatchHost(
        const TString& fqdn,
        ICellHostWatcherPtr watcher);
    void UnwatchHost(const TString& fqdn, const ICellHostWatcherPtr& watcher);

    [[nodiscard]] size_t GetWatcherCount(const TString& fqdn) const;

    [[nodiscard]] TString GetCellId() const;

    [[nodiscard]] const TCellCounters& GetCounters() const;

    ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture
        AcquireControlChannel(const TString& fqdn);
    void ReleaseControlChannel(const TString& fqdn);

private:
    [[nodiscard]] TResultOrError<TCellHostConfig> PickHostImpl(
        const THashSet<TString>& except,
        bool preferVerified) const;

    ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture
        EnsureChannel(const TString& fqdn);

    void EnsureMinLiveChannels();
    void DropUnneededDiscoveredHosts();
    [[nodiscard]] static bool IsLive(const TChannel& channel);

    void AccountChannel(const TChannel& channel, int delta);
    void EraseChannel(THashMap<TString, TChannel>::iterator it);
    [[nodiscard]] size_t CountLiveChannelsExcept(
        const TChannel& except) const;

    void SchedulePingSweep();
    void PingSweep();

    bool ApplyLiveness(
        const TString& fqdn,
        const TMaybe<ui64>& epoch,
        bool alive,
        const NProto::TError& error);
    void HandlePingResult(
        const TString& fqdn,
        ui64 epoch,
        const NProto::TError& error);
};

using TCellHostPoolPtr = std::shared_ptr<TCellHostPool>;

}   // namespace NCloud::NBlockStore::NCells
