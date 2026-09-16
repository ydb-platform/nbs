#include "connection.h"

#include "detachable_target.h"
#include "endpoint_router.h"
#include "transport_switcher.h"
#include "remote_storage.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCellConnection;
using TCellConnectionPtr = std::shared_ptr<TCellConnection>;

////////////////////////////////////////////////////////////////////////////////

// How long a connection stays away from a host it has found no good - one
// whose data transport would not come up, or one the pinger buried under it.
// Long enough that a host with a real problem is not tried every few
// seconds, short enough that one which recovers is picked up again while the
// connection still lives. Not configurable: it only bounds how often a move
// may be retried, and nothing downstream cares about the exact number.
constexpr TDuration AvoidCooldown = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

// Everything that ties a connection to one host.
struct THostBinding
{
    // Tells one binding from the next. Anything that was set in motion while
    // an older binding was current - a request in flight, a handler held by
    // an endpoint being torn down - carries the generation it belongs to, so
    // that its late arrival cannot be mistaken for news about the host we are
    // on now.
    ui64 Generation = 0;

    // The pool channel incarnation this binding was built on. A liveness
    // verdict for our host is ours to act on only if it names this same
    // incarnation - see OnHostUnavailable.
    ui64 ChannelEpoch = 0;

    TCellHostConfig HostConfig;
    IBlockStorePtr ControlService;

    // the same service, wrapped so that whatever it answers is tagged with
    // this binding's generation - see TBoundControlService
    IBlockStorePtr BoundControlService;
    // what arming installs into the data router - and, for the switching
    // transport, what the switcher falls back to
    IBlockStorePtr DataEndpoint;
    ITransportSwitcherPtr Switcher;
    IDetachableTargetPtr Sink;

    // only without a fallback: there is no switcher to hold it
    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr RdmaHandler;
};
using THostBindingPtr = std::shared_ptr<THostBinding>;

TResultOrError<THostBindingPtr> BuildHostBinding(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    const IBlockStorePtr& controlService);

IBlockStorePtr CreateGrpcDataEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    const IBlockStorePtr& controlService);

////////////////////////////////////////////////////////////////////////////////

// Handed out by GetService(). Holds the connection alive - the caller above
// may release its handle while requests are still in flight - and forwards
// everything to the control router, which decides which host serves it.
class TControlService final
    : public TBlockStoreImpl<TControlService, IBlockStore>
{
private:
    const IBlockStorePtr Impl;
    const TCellConnectionPtr Connection;
    const TString CellId;

public:
    TControlService(
            IBlockStorePtr impl,
            TCellConnectionPtr connection,
            TString cellId)
        : Impl(std::move(impl))
        , Connection(std::move(connection))
        , CellId(std::move(cellId))
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Impl->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        if constexpr (
            std::is_same_v<TMethod, TBlockStoreMountVolumeMethod> ||
            std::is_same_v<TMethod, TBlockStoreUnmountVolumeMethod>)
        {
            // marks the request as an inter-cell forward, so the receiving
            // host's forward service can let it past authorization - see the
            // inter-cell-forward design. Describe carries its own cell id
            // through the describe path already
            request->MutableHeaders()->SetCellId(CellId);
        }

        return TMethod::Execute(
            Impl.get(),
            std::move(callContext),
            std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

// A connection serves through one binding at a time and moves between them.
// Everything asynchronous that reaches it - a ping verdict, an rdma callback,
// a mount response - was set in motion under some binding, and arrives under
// whichever is current by then. Three rules keep that from going wrong, and
// every handler below is an instance of them:
//
// 1. Every event carries the identity of the binding it is about: a
//    generation, or a host name resolved to one under the lock. Nothing is
//    read from the connection at the time the event was set in motion; the
//    two are set in different steps, and a move can fall between them.
//
// 2. An event about a binding older than the current one is dropped: that
//    binding's host stopped being ours when we left it. An event about the
//    current binding, or one newer (still being prepared), is acted on only
//    when the connection is whole and idle - and otherwise noted and acted on
//    at the next settle point, by which time it is either about the binding
//    we are on or about one we never kept.
//
// 3. There are exactly two settle points, the end of a move and the end of
//    the initial setup, and both run the same code.
//
// A change that adds an event source, a settle point, or a piece of state
// read by a handler should be checked against all three before anything
// else.
class TCellConnection final
    : public ICellConnection
    , public ICellHostWatcher
    , public std::enable_shared_from_this<TCellConnection>
{
private:
    const TCellHostPoolPtr Pool;
    const TBootstrap Bootstrap;
    const NClient::TClientAppConfigPtr ClientConfig;
    const ICellConnectionObserverPtr Observer;
    const bool MigrationEnabled;

    // outlive any binding: this is what makes a move invisible from outside
    const IEndpointRouterPtr ControlRouter;
    const IEndpointRouterPtr DataRouter;

    TLog Log;

    mutable TAdaptiveLock Lock;
    THostBindingPtr Binding;
    ui64 LastGeneration = 0;

    // Why a host is no good for this connection. Kept apart because each
    // is taken back by its own kind of news: rdma coming up says nothing
    // about a control path the pinger found dead, and the pinger changing
    // its mind says nothing about rdma.
    struct TAvoidance
    {
        TInstant PingUntil;
        TInstant RdmaUntil;

        [[nodiscard]] bool InForce(TInstant now) const
        {
            return PingUntil > now || RdmaUntil > now;
        }
    };

    // Hosts this connection has found no good, and until when. The
    // knowledge lives here rather than in the pool because the pool forgets
    // a discovered host the moment nobody holds its channel, which is right
    // after a move away from it. With a deadline rather than a plain set, a
    // host that recovers is tried again instead of being written off for
    // the life of the connection.
    THashMap<TString, TAvoidance> Avoid;

    // Set from the first hop of a move until a hop ends with nothing queued
    // behind it: a move is a chain, not a single hop, and the flag stays up
    // for the whole of it. That is what keeps the hops in the order the
    // targets arrived, and keeps a move away from cutting in between them.
    bool MigrationInFlight = false;
    TString PendingTarget;

    // The connection is built in steps, and the rdma client is handed its
    // handler before the last one. A callback arriving in between must not
    // act: a move started then would race the rest of the setup and could
    // leave the host, the control path and the data path disagreeing.
    bool BindingInstalled = false;

    // The bindings that have said they cannot serve, awaiting a settle
    // point - see RequestMoveAway. Generations rather than a flag, so that
    // a request can be matched against the binding that is current when
    // it is finally acted on; every one of them rather than the newest,
    // because a newer binding can be abandoned, and the request about the
    // one we are then still on must not have been pushed out by it. Why a
    // binding cannot serve is not kept here: that is what Avoid says about
    // the host, and a request stands exactly as long as Avoid still holds
    // something against it.
    THashSet<ui64> MoveAwayRequests;

public:
    TCellConnection(
            TCellHostPoolPtr pool,
            TBootstrap bootstrap,
            NClient::TClientAppConfigPtr clientConfig,
            ICellConnectionObserverPtr observer,
            IEndpointRouterPtr controlRouter,
            IEndpointRouterPtr dataRouter,
            THostBindingPtr binding)
        : Pool(std::move(pool))
        , Bootstrap(std::move(bootstrap))
        , ClientConfig(std::move(clientConfig))
        , Observer(std::move(observer))
        , MigrationEnabled(binding->HostConfig.GetHostMigrationEnabled())
        , ControlRouter(std::move(controlRouter))
        , DataRouter(std::move(dataRouter))
        , Log(Bootstrap.Logging->CreateLog("BLOCKSTORE_CELLS"))
        , Binding(std::move(binding))
    {}

    ~TCellConnection() override
    {
        // every writer of Binding keeps `self` (shared_from_this()) alive
        // until after the write completes, so by the time nothing
        // references this connection any more, no writer can be in
        // flight - reading Binding here without the lock is safe
        Pool->ReleaseControlChannel(Binding->HostConfig.GetFqdn());
    }

    TString GetHost() const override
    {
        with_lock (Lock) {
            return Binding->HostConfig.GetFqdn();
        }
    }

    IBlockStorePtr GetService() override
    {
        return std::make_shared<TControlService>(
            ControlRouter,
            shared_from_this(),
            Pool->GetCellId());
    }

    IStoragePtr GetStorage() override
    {
        return CreateRemoteStorage(DataRouter, shared_from_this());
    }

    // Builds whatever of the data path can still fail, and touches nothing
    // the connection is currently serving through. A member rather than a
    // free function because without a fallback the rdma client reports "not
    // coming up" to a handler that has to reach this connection.
    NProto::TError PrepareDataPath(const THostBindingPtr& binding);

    // Points both routers at the prepared binding and, for the switching
    // transport, starts the switcher. Runs only once the binding is current.
    void InstallBinding(const THostBindingPtr& binding);

    [[nodiscard]] ui64 ReserveGeneration()
    {
        with_lock (Lock) {
            return ++LastGeneration;
        }
    }

    void OnMountResponse(
        const NProto::TMountVolumeResponse& response,
        ui64 generation) noexcept
    {
        const auto& fqdn = response.GetTabletHost();
        if (!fqdn) {
            // the serving cell is older than this field
            return;
        }

        // the router holds a target until its request completes, so a mount
        // issued before a move still lands on the host we left, and what it
        // reports is news about that host's view of the world, not this one's
        if (generation != CurrentGeneration()) {
            return;
        }

        if (fqdn == GetHost()) {
            return;
        }

        if (Observer) {
            Observer->OnTabletHostChanged(fqdn);
        }

        // checked again inside, under the lock: the observer ran in between,
        // and so may have a move the pinger asked for
        if (MigrationEnabled) {
            MigrateTo(fqdn, "the volume tablet lives there", generation);
        }
    }

    void OnHostUnavailable(const TString& fqdn, ui64 epoch) noexcept override
    {
        ui64 generation = 0;

        with_lock (Lock) {
            const bool aboutCurrentHost =
                fqdn == Binding->HostConfig.GetFqdn();

            // a verdict about the host we are on, but the channel incarnation
            // it names is not the one this binding was built on: a move has
            // taken us off and back since, and this is stale news about the
            // incarnation we left. Acting on it would move us off a host that
            // is fine - drop it whole, cooldown included
            if (aboutCurrentHost && epoch != Binding->ChannelEpoch) {
                return;
            }

            // remembered whatever else happens: a discovered host loses its
            // channel - and with it the pool's verdict - as soon as a move
            // releases it, and this connection may be told to go back there
            Avoid[fqdn].PingUntil = Bootstrap.Timer->Now() + AvoidCooldown;

            // the pool takes a strong reference to us before it drops its
            // lock, so a move that happens in between still gets this call
            // - about a host we have already left
            if (!aboutCurrentHost) {
                return;
            }

            generation = Binding->Generation;
        }

        RequestMoveAway(generation);
    }

    // Without a fallback this host serves nothing at all while its rdma is
    // down, so the reasoning that keeps a connection put when gRPC still
    // carries data does not apply here.
    void OnRdmaUnusable(ui64 generation, const TString& fqdn) noexcept
    {
        if (!MigrationEnabled) {
            return;
        }

        with_lock (Lock) {
            // recorded here, by name, for the current binding and for one
            // still being prepared alike: whether we move because of this
            // is decided later, but what we learned about the host must not
            // depend on that decision ever being reached - another move can
            // carry the connection past it first
            if (generation >= Binding->Generation) {
                Avoid[fqdn].RdmaUntil = Bootstrap.Timer->Now() + AvoidCooldown;
            }
        }

        RequestMoveAway(generation);
    }

    // The one way to say "the binding with this generation cannot serve".
    // Both the pinger and the rdma client come through here, so both get
    // the same three-way treatment: a binding older than the current one is
    // no longer ours and is ignored; anything else is noted and acted on at
    // the next settle point - which is right now, if nothing is in flight.
    void RequestMoveAway(ui64 generation)
    {
        with_lock (Lock) {
            if (generation < Binding->Generation) {
                return;
            }

            MoveAwayRequests.insert(generation);
        }

        RunPendingMoveAway();
    }

    // The settle point. Acts on what RequestMoveAway noted, provided the
    // connection is whole and idle and the request is about the binding it
    // serves through now; called from wherever those become true - the end
    // of a move and the end of the initial setup - and from RequestMoveAway
    // itself, for the case where they already are.
    void RunPendingMoveAway()
    {
        ui64 generation = 0;
        THashSet<TString> except;

        with_lock (Lock) {
            if (!BindingInstalled || MigrationInFlight) {
                return;
            }

            // with nothing in flight, no generation but the current one can
            // ever become current: the others belonged to bindings we never
            // ended up on, and what they said about those hosts was
            // recorded when they said it
            generation = Binding->Generation;
            EraseNodesIf(
                MoveAwayRequests,
                [&](ui64 requested) { return requested != generation; });

            if (MoveAwayRequests.empty()) {
                return;
            }

            // the request stands only as long as something is still held
            // against the host, and the wait for a settle point can outlast
            // that: a verdict that ran out meanwhile is nothing to act on
            const auto* avoidance =
                Avoid.FindPtr(Binding->HostConfig.GetFqdn());
            if (!avoidance || !avoidance->InForce(Bootstrap.Timer->Now())) {
                MoveAwayRequests.clear();
                return;
            }

            // the request itself stays where it is until the move is
            // committed to below: the lock is let go for the pick, and
            // whatever happens meanwhile must still find it there - rdma
            // coming up takes it back, a move the mount response starts
            // leaves it waiting for its settle point
            except = HostsToAvoidLocked();
        }

        // outside the lock: the pool has a lock of its own, and nothing
        // here ever holds both
        auto picked = Pool->PickHostExcept(except);
        if (HasError(picked)) {
            STORAGE_WARN(
                "[" << GetHost() << "] this host cannot serve us, and there "
                    << "is nowhere to move: "
                    << FormatError(picked.GetError()));
            return;
        }

        auto fqdn = picked.GetResult().GetFqdn();

        with_lock (Lock) {
            // the same checks as at the top, repeated because the lock was
            // let go for the pick. A request that is gone was taken back;
            // a move that started meanwhile will either land - and the
            // request is dropped at its settle point - or leave us right
            // here, and the request is acted on then
            if (generation != Binding->Generation || MigrationInFlight ||
                !MoveAwayRequests.contains(generation))
            {
                return;
            }

            const auto* avoidance =
                Avoid.FindPtr(Binding->HostConfig.GetFqdn());
            if (!avoidance || !avoidance->InForce(Bootstrap.Timer->Now())) {
                MoveAwayRequests.clear();
                return;
            }

            MoveAwayRequests.erase(generation);
            MigrationInFlight = true;
        }

        STORAGE_INFO(
            "[" << GetHost() << "] moving to " << fqdn
                << ": this host cannot serve us");

        StartMigration(std::move(fqdn));
    }

    // Data flows here again. That says nothing about the other hosts, so
    // only this one is forgiven.
    void OnRdmaUsable(ui64 generation, const TString& fqdn) noexcept
    {
        with_lock (Lock) {
            if (generation < Binding->Generation) {
                return;
            }

            // by name, so that a host still being prepared is forgiven too
            auto* avoidance = Avoid.FindPtr(fqdn);
            if (!avoidance) {
                return;
            }

            // rdma takes back what rdma said, and nothing else: a request
            // to leave stands as long as anything is still held against
            // the host - the pinger's verdict, say
            avoidance->RdmaUntil = {};
            if (!avoidance->InForce(Bootstrap.Timer->Now())) {
                MoveAwayRequests.erase(generation);
            }
        }
    }

    // Called once the first binding is in place and the connection is
    // watched. Anything an rdma callback asked for while the connection was
    // still being built happens now.
    void CompleteSetup()
    {
        with_lock (Lock) {
            BindingInstalled = true;
        }

        RunPendingMoveAway();
    }

    [[nodiscard]] ui64 CurrentGeneration() const
    {
        with_lock (Lock) {
            return Binding->Generation;
        }
    }

private:
    // The hosts a move away must not land on. Under Lock.
    THashSet<TString> HostsToAvoidLocked()
    {
        THashSet<TString> except;

        // never back onto ourselves: the host we are leaving can be
        // perfectly alive from the pool's point of view and still be
        // unable to serve us
        except.insert(Binding->HostConfig.GetFqdn());

        const auto now = Bootstrap.Timer->Now();
        EraseNodesIf(
            Avoid,
            [&](const auto& kv) { return !kv.second.InForce(now); });

        for (const auto& [fqdn, avoidance]: Avoid) {
            Y_UNUSED(avoidance);
            except.insert(fqdn);
        }

        return except;
    }

    // `generation` is the binding the request is about; the move is only
    // started or queued if that binding is still the current one.
    void MigrateTo(TString fqdn, TString reason, ui64 generation)
    {
        // outside the lock, and before anything else: the pinger may have
        // buried this host already, and a tablet living there is not a
        // reason to undo that
        if (Pool->IsHostKnownDead(fqdn)) {
            return;
        }

        with_lock (Lock) {
            if (generation != Binding->Generation ||
                !IsWorthMovingToLocked(fqdn))
            {
                return;
            }

            if (MigrationInFlight) {
                // remembered rather than dropped: the reason that arrived
                // last is the one that still holds
                PendingTarget = std::move(fqdn);
                return;
            }

            MigrationInFlight = true;
        }

        STORAGE_INFO(
            "[" << GetHost() << "] moving to " << fqdn << ": " << reason);

        StartMigration(std::move(fqdn));
    }

    void StartMigration(TString fqdn)
    {
        // a strong self, deliberately: from here on the migration owns a
        // control channel for the host it is moving to, and only its own
        // tail gives that channel back
        auto self = shared_from_this();

        TFuture<NClient::IMultiClientEndpointPtr> channel;
        try {
            channel = Pool->AcquireControlChannel(fqdn);
        } catch (...) {
            // the channel setup is what threw, so nothing was acquired
            AbortMigration(fqdn, CurrentExceptionMessage(), false);
            return;
        }

        channel.Subscribe(
            [self, fqdn](const auto& future) mutable
            {
                self->OnChannelAcquired(
                    std::move(fqdn),
                    future.GetValue());
            });
    }

    void OnChannelAcquired(
        TString fqdn,
        const NClient::IMultiClientEndpointPtr& endpoint)
    {
        if (!endpoint) {
            AbortMigration(fqdn, "no control channel", true);
            return;
        }

        TResultOrError<THostBindingPtr> built = MakeError(E_FAIL);
        try {
            auto hostConfig = Pool->MakeHostConfig(fqdn);
            auto controlService = endpoint->CreateClientEndpoint(
                ClientConfig->GetClientId(),
                ClientConfig->GetInstanceId());

            built = BuildHostBinding(Bootstrap, hostConfig, controlService);
        } catch (...) {
            AbortMigration(fqdn, CurrentExceptionMessage(), true);
            return;
        }

        if (HasError(built)) {
            AbortMigration(fqdn, FormatError(built.GetError()), true);
            return;
        }

        // everything that can fail happens here, while the connection is
        // still serving through the host it has: a data path that cannot be
        // built must not leave control on one host and data on another
        auto binding = built.GetResult();
        binding->ChannelEpoch = Pool->GetChannelEpoch(fqdn);
        try {
            auto error = PrepareDataPath(binding);
            if (HasError(error)) {
                AbortMigration(fqdn, FormatError(error), true);
                return;
            }
        } catch (...) {
            AbortMigration(fqdn, CurrentExceptionMessage(), true);
            return;
        }

        FinishMigration(binding);
    }

    // The way out of a move that did not happen: the connection stays on the
    // host it already has, gives back whatever it took for the host it never
    // reached, and is free to try again on the next notification.
    void AbortMigration(
        const TString& fqdn,
        const TString& reason,
        bool channelAcquired)
    {
        STORAGE_WARN(
            "[" << GetHost() << "] can't move to " << fqdn << ": " << reason
                << ", staying where we are");

        if (channelAcquired) {
            Pool->ReleaseControlChannel(fqdn);
        }

        EndMigration();
    }

    void FinishMigration(const THostBindingPtr& binding)
    {
        THostBindingPtr old;

        with_lock (Lock) {
            old = std::move(Binding);
            Binding = binding;
        }

        // detached before anything else can reach it: the switcher of
        // the host we just left is still alive and would otherwise
        // point the data back at a host nobody talks to
        if (old->Sink) {
            old->Sink->Detach();
        }

        try {
            // only now that the old sink is cut off: the new switcher can
            // point the router at rdma from inside this call
            InstallBinding(binding);
        } catch (...) {
            // both routers already point at the new host, so the connection
            // is whole; what is missing is transport switching on it, and
            // the next move will build it again
            STORAGE_ERROR(
                "[" << binding->HostConfig.GetFqdn() << "] moved, but "
                    << "transport switching did not start: "
                    << CurrentExceptionMessage());
        }

        auto self = shared_from_this();
        const bool targetDead =
            Pool->WatchHost(binding->HostConfig.GetFqdn(), self);
        Pool->UnwatchHost(old->HostConfig.GetFqdn(), self);
        Pool->ReleaseControlChannel(old->HostConfig.GetFqdn());

        if (targetDead) {
            // the pinger buried the target between our picking it and
            // WatchHost subscribing, so its death notification went to
            // nobody. Replay it now: the settle point in EndMigration then
            // moves us off again, instead of sitting on a dead host until
            // the next sweep notices
            OnHostUnavailable(
                binding->HostConfig.GetFqdn(),
                binding->ChannelEpoch);
        }

        EndMigration();
    }

    // Every path that set MigrationInFlight ends here: a flag left set sends
    // every later notification into the pending branch and pins the
    // connection to the host it failed to leave, for good.
    void EndMigration()
    {
        // cleared only after the tail above has fully run, so that an
        // overlapping move queues behind this one instead of landing half of
        // its outcome on top of half of ours
        TString next;
        with_lock (Lock) {
            next = std::move(PendingTarget);
            PendingTarget.clear();

            // and not cleared at all while a target is queued: the move
            // goes straight on to it under the same flag, so that a target
            // arriving now queues behind it, as the later of the two, rather
            // than starting ahead of it
            if (!next) {
                MigrationInFlight = false;
            }
        }

        if (next) {
            ContinueMigration(std::move(next));
            return;
        }

        // here, and not only at the end of a move that really happened: a
        // deferred request would otherwise hang until the next callback
        // happened to arrive
        RunPendingMoveAway();
    }

    // Runs with MigrationInFlight still set by the move that just ended, and
    // hands it on: to the move it starts, or back to EndMigration if the
    // target is no good any more by now.
    void ContinueMigration(TString fqdn)
    {
        bool worth = !Pool->IsHostKnownDead(fqdn);
        if (worth) {
            with_lock (Lock) {
                worth = IsWorthMovingToLocked(fqdn);
            }
        }

        if (!worth) {
            EndMigration();
            return;
        }

        STORAGE_INFO(
            "[" << GetHost() << "] moving to " << fqdn
                << ": a move was asked for meanwhile");

        StartMigration(std::move(fqdn));
    }

    // Whether the host is somewhere to go from where the connection is
    // now. Under Lock.
    [[nodiscard]] bool IsWorthMovingToLocked(const TString& fqdn) const
    {
        if (Binding->HostConfig.GetFqdn() == fqdn) {
            return false;
        }

        // a tablet living on a host whose transport will not come up is
        // not a reason to go there: following it would undo the move
        // that brought us away
        const auto* avoidance = Avoid.FindPtr(fqdn);
        return !avoidance || !avoidance->InForce(Bootstrap.Timer->Now());
    }
};

////////////////////////////////////////////////////////////////////////////////

// One host's control service, tied to the binding it belongs to. What comes
// back through it is tagged with that binding's generation, so a response is
// identified by the object that actually served the request rather than by
// what the connection pointed at when the request went out: those are two
// different steps, and a move can fall between them.
//
// Holds the connection weakly - the connection owns the binding that owns
// this - and a request in flight keeps this object alive through the router.
class TBoundControlService final
    : public TBlockStoreImpl<TBoundControlService, IBlockStore>
{
private:
    const IBlockStorePtr Impl;
    const std::weak_ptr<TCellConnection> Connection;
    const ui64 Generation;

public:
    TBoundControlService(
            IBlockStorePtr impl,
            std::weak_ptr<TCellConnection> connection,
            ui64 generation)
        : Impl(std::move(impl))
        , Connection(std::move(connection))
        , Generation(generation)
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Impl->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        auto future = TMethod::Execute(
            Impl.get(),
            std::move(callContext),
            std::move(request));

        if constexpr (std::is_same_v<TMethod, TBlockStoreMountVolumeMethod>) {
            return future.Apply(
                [connection = Connection,
                 generation = Generation](const auto& f)
                {
                    const auto& response = f.GetValue();
                    if (!HasError(response)) {
                        if (auto self = connection.lock()) {
                            self->OnMountResponse(response, generation);
                        }
                    }
                    return response;
                });
        } else {
            return future;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// The only listener an rdma endpoint gets when there is no fallback: with no
// second transport to fall back to, the one thing worth reacting to is the
// link never coming up.
class TRdmaOnlyHandler final
    : public NCloud::NStorage::NRdma::IClientEndpointHandler
{
private:
    const std::weak_ptr<TCellConnection> Connection;
    const ui64 Generation;
    const TString Host;

public:
    TRdmaOnlyHandler(
            std::weak_ptr<TCellConnection> connection,
            ui64 generation,
            TString host)
        : Connection(std::move(connection))
        , Generation(generation)
        , Host(std::move(host))
    {}

    void HandleConnected() override
    {
        if (auto connection = Connection.lock()) {
            connection->OnRdmaUsable(Generation, Host);
        }
    }

    void HandleDisconnected() override
    {}

    void HandleUnavailable() override
    {
        // repeats on every reconnect attempt, which is what gives the move
        // its retries for free
        if (auto connection = Connection.lock()) {
            connection->OnRdmaUnusable(Generation, Host);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TError TCellConnection::PrepareDataPath(
    const THostBindingPtr& binding)
{
    binding->Generation = ReserveGeneration();

    // built here, with the generation, so that the router can only ever be
    // pointed at a control service that knows which binding it belongs to
    binding->BoundControlService = std::make_shared<TBoundControlService>(
        binding->ControlService,
        weak_from_this(),
        binding->Generation);

    const auto& hostConfig = binding->HostConfig;
    const bool rdmaOnly =
        hostConfig.GetTransport() == NProto::CELL_DATA_TRANSPORT_RDMA &&
        !hostConfig.GetGrpcDataFallbackEnabled();

    if (!rdmaOnly) {
        // the other transports had their endpoint built along with the
        // binding, and building it cannot fail here
        return {};
    }

    binding->RdmaHandler = std::make_shared<TRdmaOnlyHandler>(
        weak_from_this(),
        binding->Generation,
        hostConfig.GetFqdn());

    auto endpoint = Bootstrap.EndpointsSetup->SetupHostRdmaEndpoint(
        Bootstrap,
        hostConfig,
        binding->RdmaHandler);

    if (HasError(endpoint)) {
        return endpoint.GetError();
    }

    // handed back before it has connected: until it does, data requests fail
    // retriably and the handler above decides when to give up on this host
    binding->DataEndpoint = endpoint.GetResult();
    return {};
}

void TCellConnection::InstallBinding(const THostBindingPtr& binding)
{
    const auto& hostConfig = binding->HostConfig;

    ControlRouter->SetTarget(binding->BoundControlService);

    // the target goes in before the switcher starts: the switcher may point
    // the router at rdma straight away, and installing the fallback
    // afterwards would undo that
    DataRouter->SetTarget(binding->DataEndpoint);

    if (hostConfig.GetTransport() != NProto::CELL_DATA_TRANSPORT_RDMA ||
        !hostConfig.GetGrpcDataFallbackEnabled())
    {
        return;
    }

    binding->Sink = CreateDetachableTarget(DataRouter);
    binding->Switcher = StartTransportSwitching(
        binding->Sink,
        binding->DataEndpoint,
        [bootstrap = Bootstrap, hostConfig](
            NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler)
        {
            return bootstrap.EndpointsSetup->SetupHostRdmaEndpoint(
                bootstrap,
                hostConfig,
                std::move(handler));
        },
        Bootstrap.Timer,
        Bootstrap.Scheduler,
        Bootstrap.Logging,
        hostConfig.GetFqdn(),
        TTransportSwitcherConfig{
            .SettleTime = hostConfig.GetRdmaSettleTime(),
        });
}

////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateGrpcDataEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    const IBlockStorePtr& controlService)
{
    // a channel of its own, so that it dies with the endpoint rather than
    // being shared by everyone talking to this host
    const auto securePort = hostConfig.GetSecureGrpcPort();
    auto endpoint = bootstrap.GrpcClient->CreateDataEndpoint(
        hostConfig.GetFqdn(),
        securePort ? securePort : hostConfig.GetGrpcPort(),
        securePort != 0);

    return endpoint ? std::move(endpoint) : controlService;
}

// Builds everything that ties a connection to one host, EXCEPT the data path
// - that is TCellConnection::PrepareDataPath, which needs a live connection
// to hand the rdma client a handler.
TResultOrError<THostBindingPtr> BuildHostBinding(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    const IBlockStorePtr& controlService)
{
    auto binding = std::make_shared<THostBinding>();
    binding->HostConfig = hostConfig;
    binding->ControlService = controlService;

    switch (hostConfig.GetTransport()) {
        case NProto::CELL_DATA_TRANSPORT_RDMA:
            if (hostConfig.GetGrpcDataFallbackEnabled()) {
                // the switcher starts on this and returns to it whenever rdma
                // is not carrying data
                binding->DataEndpoint = CreateGrpcDataEndpoint(
                    bootstrap,
                    hostConfig,
                    controlService);
            }
            return binding;

        case NProto::CELL_DATA_TRANSPORT_GRPC:
            binding->DataEndpoint = CreateGrpcDataEndpoint(
                bootstrap,
                hostConfig,
                controlService);
            return binding;

        default:
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Unsupported cell data transport "
                    << NProto::ECellDataTransport_Name(
                           hostConfig.GetTransport()));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCellConnectionFuture CreateCellConnection(
    TCellHostPoolPtr pool,
    TCellHostConfig hostConfig,
    TBootstrap bootstrap,
    NClient::TClientAppConfigPtr clientConfig,
    ICellConnectionObserverPtr observer)
{
    auto fqdn = hostConfig.GetFqdn();
    auto controlFuture = pool->AcquireControlChannel(fqdn);

    return controlFuture.Apply(
        [pool = std::move(pool),
         hostConfig = std::move(hostConfig),
         bootstrap = std::move(bootstrap),
         clientConfig = std::move(clientConfig),
         observer = std::move(observer),
         fqdn = std::move(fqdn)](const auto& f) mutable -> TCellConnectionFuture
        {
            auto controlEndpoint = f.GetValue();
            if (!controlEndpoint) {
                pool->ReleaseControlChannel(fqdn);
                return MakeFuture(TResultOrError<ICellConnectionPtr>(MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Can't set up a control channel to " << fqdn)));
            }

            auto controlService = controlEndpoint->CreateClientEndpoint(
                clientConfig->GetClientId(),
                clientConfig->GetInstanceId());

            auto controlRouter = CreateEndpointRouter(controlService);
            auto dataRouter = CreateEndpointRouter(controlService);

            auto built =
                BuildHostBinding(bootstrap, hostConfig, controlService);
            if (HasError(built)) {
                pool->ReleaseControlChannel(fqdn);
                return MakeFuture(
                    TResultOrError<ICellConnectionPtr>(built.GetError()));
            }
            built.GetResult()->ChannelEpoch = pool->GetChannelEpoch(fqdn);

            auto connection = std::make_shared<TCellConnection>(
                pool,
                std::move(bootstrap),
                std::move(clientConfig),
                std::move(observer),
                std::move(controlRouter),
                std::move(dataRouter),
                built.GetResult());

            // built only once the connection exists: without a fallback the
            // rdma endpoint is handed a handler that has to reach it
            auto error = connection->PrepareDataPath(built.GetResult());
            if (HasError(error)) {
                // no explicit release here, unlike the branches above: the
                // connection now exists and owns the channel, and its
                // destructor releases it as it unwinds. Releasing here too
                // would drop the channel a second time, out from under any
                // other connection sharing a discovered host
                return MakeFuture(TResultOrError<ICellConnectionPtr>(error));
            }

            connection->InstallBinding(built.GetResult());

            if (hostConfig.GetHostMigrationEnabled()) {
                if (pool->WatchHost(fqdn, connection)) {
                    connection->OnHostUnavailable(
                        fqdn,
                        pool->GetChannelEpoch(fqdn));
                }
            }

            // last: from here on callbacks may act on their own
            connection->CompleteSetup();

            return MakeFuture(
                TResultOrError<ICellConnectionPtr>(std::move(connection)));
        });
}

}   // namespace NCloud::NBlockStore::NCells
