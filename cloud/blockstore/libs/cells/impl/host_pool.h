#pragma once

#include "bootstrap.h"
#include "endpoint_bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/client/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

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
private:
    struct TChannel
    {
        // Tells one incarnation of a channel from the next: a discovered
        // host can be dropped and taken again while a ping it answered is
        // still on its way back.
        ui64 Epoch = 0;

        // one probe at a time: two answers for the same channel can arrive
        // in the other order, and the older one would have the last word
        bool PingInFlight = false;

        ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture Endpoint;
        ui32 RefCount = 0;
        bool Configured = false;
        bool Alive = true;
        TVector<std::weak_ptr<ICellHostWatcher>> Watchers;
    };

    const TCellConfigPtr Config;
    const TBootstrap Bootstrap;

    TLog Log;

    mutable TAdaptiveLock Lock;
    THashMap<TString, TChannel> Channels;
    ui64 LastEpoch = 0;
    bool Stopped = false;

public:
    TCellHostPool(TCellConfigPtr config, TBootstrap bootstrap);

    void Start();
    void Stop();

    [[nodiscard]] TCellHostEndpoints GetDescribeEndpoints(
        const NClient::TClientAppConfigPtr& clientConfig);

    // A live configured host, or - only when there is none - a live host
    // discovered at runtime.
    [[nodiscard]] TResultOrError<TCellHostConfig> PickHost() const;

    // The same, minus hosts the caller already knows are no good for it -
    // the one it is leaving, and any it has found unusable since.
    [[nodiscard]] TResultOrError<TCellHostConfig> PickHostExcept(
        const THashSet<TString>& except) const;

    [[nodiscard]] TCellHostConfig MakeHostConfig(const TString& fqdn) const;

    // Both return true if this changed the liveness of the host. The error
    // is only there for the log line the transition produces.
    bool SetHostAlive(const TString& fqdn, bool alive);
    bool SetHostAlive(
        const TString& fqdn,
        bool alive,
        const NProto::TError& error);

    // False for a host the pool has never talked to: not knowing whether a
    // host answers is not the same as knowing it does not, and a tablet can
    // name a host this cell has never used.
    [[nodiscard]] bool IsHostKnownDead(const TString& fqdn) const;

    // The current incarnation of the host's channel, or 0 if there is none.
    // A connection records it with its binding, so a late liveness verdict
    // about an older incarnation can be told from one about this one.
    [[nodiscard]] ui64 GetChannelEpoch(const TString& fqdn) const;

    // A point-in-time view of the cell's hosts for monitoring: which are
    // alive, which have a warm channel, and how many connections hold each
    // host's channel (the reference count, which a connection keeps for the
    // whole life of its binding regardless of host migration).
    struct THostStatus
    {
        TString Fqdn;
        bool Alive = false;
        bool Warm = false;
        size_t Connections = 0;
    };

    [[nodiscard]] TVector<THostStatus> GetHostStatuses() const;

    // Returns whether the host is known dead at subscription time, so
    // the caller does not miss a death that landed before it subscribed.
    [[nodiscard]] bool WatchHost(
        const TString& fqdn,
        ICellHostWatcherPtr watcher);
    void UnwatchHost(const TString& fqdn, const ICellHostWatcherPtr& watcher);

    // Registrations held for the host, including those whose watcher is
    // already gone. For tests: nothing in production asks.
    [[nodiscard]] size_t GetWatcherCount(const TString& fqdn) const;

    [[nodiscard]] TString GetCellId() const;

    ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture
        AcquireControlChannel(const TString& fqdn);
    void ReleaseControlChannel(const TString& fqdn);

private:
    ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture
        EnsureChannelLocked(const TString& fqdn);

    // Warms configured hosts until MinCellConnections channels are live.
    void TopUpWarmChannelsLocked();
    void PruneRetainedDiscoveredLocked();
    [[nodiscard]] size_t CountLiveChannelsLocked(
        const TString& except) const;

    void SchedulePingSweep();
    void PingSweep();

    // The heart of both SetHostAlive overloads and of the ping path. An
    // epoch, when given, is checked against the channel: a ping that went
    // out on a channel since dropped says nothing about the one in its
    // place.
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
