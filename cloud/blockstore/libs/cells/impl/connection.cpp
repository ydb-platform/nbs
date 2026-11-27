#include "connection.h"

#include "detachable_target.h"
#include "endpoint_router.h"
#include "remote_storage.h"
#include "transport_switcher.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/algorithm.h>
#include <util/generic/guid.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>

#include <optional>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCellConnection;
using TCellConnectionPtr = std::shared_ptr<TCellConnection>;

////////////////////////////////////////////////////////////////////////////////

// How long a connection stays away from a host that could not serve it.
constexpr TDuration AvoidCooldown = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

// Everything that ties a connection to one host.
struct THostBinding
{
    // Late events carry the generation of the binding they are about, so that
    // they cannot be taken for news about the current one.
    ui64 Generation = 0;

    // Unlike Generation, kept across a data transport change on the same host.
    ui64 ControlGeneration = 0;

    // The pool channel incarnation this binding was built on.
    ui64 ChannelEpoch = 0;

    TCellHostConfig HostConfig;
    IBlockStorePtr ControlService;

    // ControlService tagged with ControlGeneration
    IBlockStorePtr BoundControlService;
    // the gRPC endpoint, or the preferred one when there is no fallback
    IBlockStorePtr DataEndpoint;
    ITransportSwitcherPtr Switcher;
    IDetachableTargetPtr Sink;

    // only without a fallback: there is no switcher to hold it
    NCloud::IClientEndpointHandlerPtr DataHandler;
};
using THostBindingPtr = std::shared_ptr<THostBinding>;

IBlockStorePtr CreateGrpcDataEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    const IBlockStorePtr& controlService)
{
    // a separate connection: bulk I/O must not hold back the pings
    const auto securePort = hostConfig.GetSecureGrpcPort();
    auto endpoint = bootstrap.GrpcClient->CreateIOEndpoint(
        hostConfig.GetFqdn(),
        securePort ? securePort : hostConfig.GetGrpcPort(),
        securePort != 0);

    return endpoint ? std::move(endpoint) : controlService;
}

// The part of a binding that does not need the connection; the rest is
// TCellConnection::PrepareBinding.
TResultOrError<THostBindingPtr> BuildHostBinding(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    const IBlockStorePtr& controlService,
    IBlockStorePtr grpcDataEndpoint)
{
    auto binding = std::make_shared<THostBinding>();
    binding->HostConfig = hostConfig;
    binding->ControlService = controlService;

    switch (hostConfig.GetTransport()) {
        case NProto::CELL_DATA_TRANSPORT_RDMA:
        case NProto::CELL_DATA_TRANSPORT_ICV2:
            if (!hostConfig.GetGrpcDataFallbackEnabled()) {
                return binding;
            }
            // the gRPC endpoint is the fallback
            [[fallthrough]];

        case NProto::CELL_DATA_TRANSPORT_GRPC:
            binding->DataEndpoint = grpcDataEndpoint
                                        ? std::move(grpcDataEndpoint)
                                        : CreateGrpcDataEndpoint(
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

TString DescribeDataTransport(
    const THostBinding& binding,
    const ITransportSwitcherPtr& switcher)
{
    const auto& hostConfig = binding.HostConfig;
    switch (hostConfig.GetTransport()) {
        case NProto::CELL_DATA_TRANSPORT_GRPC:
            return "grpc";
        case NProto::CELL_DATA_TRANSPORT_RDMA:
        case NProto::CELL_DATA_TRANSPORT_ICV2:
            // until the switcher is in, the data goes the fallback way
            if (hostConfig.GetGrpcDataFallbackEnabled() &&
                (!switcher || !switcher->IsPreferredActive()))
            {
                return "grpc fallback";
            }
            return hostConfig.GetTransport() == NProto::CELL_DATA_TRANSPORT_RDMA
                       ? "rdma"
                       : "icv2";
        default:
            return "unknown";
    }
}

// Returns before the endpoint has connected.
TResultOrError<IBlockStorePtr> SetupHostDataEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    NCloud::IClientEndpointHandlerPtr handler)
{
    if (hostConfig.GetTransport() == NProto::CELL_DATA_TRANSPORT_ICV2) {
        return bootstrap.EndpointsSetup->SetupHostIcV2Endpoint(
            bootstrap,
            hostConfig,
            std::move(handler));
    }
    return bootstrap.EndpointsSetup->SetupHostRdmaEndpoint(
        bootstrap,
        hostConfig,
        std::move(handler));
}

TString DescribeDataEndpoint(const TCellHostConfig& hostConfig)
{
    if (hostConfig.GetTransport() == NProto::CELL_DATA_TRANSPORT_ICV2) {
        return TStringBuilder() << "IC V2 " << hostConfig.GetFqdn();
    }
    return TStringBuilder() << "RDMA " << hostConfig.GetFqdn() << ":"
                            << hostConfig.GetRdmaPort();
}

TResultOrError<ITransportSwitcherPtr> PrepareBindingTransportSwitching(
    const TBootstrap& bootstrap,
    const THostBinding& binding)
{
    const auto& hostConfig = binding.HostConfig;
    return PrepareTransportSwitching(
        binding.Sink,
        binding.DataEndpoint,
        [bootstrap, hostConfig](NCloud::IClientEndpointHandlerPtr handler)
        {
            return SetupHostDataEndpoint(
                bootstrap,
                hostConfig,
                std::move(handler));
        },
        bootstrap.Timer,
        bootstrap.Scheduler,
        bootstrap.Logging,
        DescribeDataEndpoint(hostConfig),
        TTransportSwitcherConfig{
            .SettleTime = hostConfig.GetEndpointSettleTime(),
        });
}

////////////////////////////////////////////////////////////////////////////////

// A connection serves through one binding at a time and moves between them.
// Asynchronous events - a ping verdict, a data endpoint callback, a mount
// response - arrive under whichever binding is current by then, so:
//
// 1. Every event names the binding it is about: a generation, or a host name
//    resolved to one under the lock.
// 2. An event about an older binding is dropped. One about the current or a
//    newer binding is acted on only when the connection is set up and no move
//    is in flight; otherwise it waits for the next settle point.
// 3. The settle points are the end of a move and the end of the initial
//    setup; both run RunPendingMoveAway.
class TCellConnection final
    : public ICellConnection
    , public ICellHostWatcher
    , public std::enable_shared_from_this<TCellConnection>
{
private:
    const TString ConnectionId = CreateGuidAsString();
    const TCellHostPoolPtr Pool;
    const TBootstrap Bootstrap;
    const ICellConnectionObserverPtr Observer;

    // outlive any binding: this is what makes a move invisible from outside
    const IEndpointRouterPtr ControlRouter;
    const IEndpointRouterPtr DataRouter;

    TLog Log;

    mutable TAdaptiveLock Lock;
    THostBindingPtr Binding;
    ui64 LastGeneration = 0;
    std::optional<NProto::ECellDataTransport> TransportOverride;

    TString MountedDiskId;
    TString MountedClientId;
    TString MountedTabletHost;

    // Ping and data verdicts are kept apart: each is taken back only by its
    // own kind of news.
    struct TAvoidance
    {
        TInstant PingUntil;
        TInstant DataUntil;

        [[nodiscard]] bool InForce(TInstant now) const
        {
            return PingUntil > now || DataUntil > now;
        }
    };

    // Kept here and not in the pool: the pool forgets a discovered host as
    // soon as nobody holds its channel.
    THashMap<TString, TAvoidance> Avoid;

    // Set for a whole chain of hops, so that queued targets are taken in
    // order and a move away cannot cut in between them.
    bool MigrationInFlight = false;
    TString PendingTarget;

    // Until the initial setup completes callbacks only record: a move
    // started earlier would race the rest of the setup.
    bool BindingInstalled = false;

    // Generations of the bindings that asked to be left, awaiting a settle
    // point. Not just the newest: a binding still being prepared can be
    // abandoned, and the request about the current one must survive that.
    THashSet<ui64> MoveAwayRequests;

public:
    TCellConnection(
            TCellHostPoolPtr pool,
            TBootstrap bootstrap,
            ICellConnectionObserverPtr observer,
            THostBindingPtr binding)
        : Pool(std::move(pool))
        , Bootstrap(std::move(bootstrap))
        , Observer(std::move(observer))
        , ControlRouter(CreateEndpointRouter(binding->ControlService))
        , DataRouter(CreateEndpointRouter(binding->ControlService))
        , Log(Bootstrap.Logging->CreateLog("BLOCKSTORE_CELLS"))
        , Binding(std::move(binding))
    {}

    ~TCellConnection() override
    {
        // no lock: every writer of Binding holds a strong reference to us
        Pool->ReleaseControlChannel(Binding->HostConfig.GetFqdn());
    }

    TString GetHost() const override
    {
        with_lock (Lock) {
            return Binding->HostConfig.GetFqdn();
        }
    }

    void NoteMount(
        const TString& diskId,
        const TString& clientId,
        const TString& tabletHost)
    {
        with_lock (Lock) {
            MountedDiskId = diskId;
            MountedClientId = clientId;
            MountedTabletHost = tabletHost;
        }
    }

    void NoteUnmount(const TString& diskId)
    {
        with_lock (Lock) {
            if (MountedDiskId == diskId) {
                MountedDiskId.clear();
                MountedClientId.clear();
                MountedTabletHost.clear();
            }
        }
    }

    const TString& GetConnectionId() const
    {
        return ConnectionId;
    }

    TCellMountStatus GetMountStatus() const
    {
        THostBindingPtr binding;
        ITransportSwitcherPtr switcher;
        TCellMountStatus status;
        with_lock (Lock) {
            binding = Binding;
            switcher = binding->Switcher;
            status.DiskId = MountedDiskId;
            status.ClientId = MountedClientId;
            status.TabletHost = MountedTabletHost;
        }

        // outside the lock: the switcher has a lock of its own
        status.CellId = Pool->GetCellId();
        status.Host = binding->HostConfig.GetFqdn();
        status.DataTransport = DescribeDataTransport(*binding, switcher);
        status.SelectedTransport = binding->HostConfig.GetTransport();
        status.ConnectionId = ConnectionId;
        status.GrpcDataFallbackEnabled =
            binding->HostConfig.GetGrpcDataFallbackEnabled();
        return status;
    }

    NProto::TError SwitchDataTransport(NProto::ECellDataTransport transport)
    {
        if (transport != NProto::CELL_DATA_TRANSPORT_GRPC &&
            !UsesEndpointHandler(transport))
        {
            return MakeError(E_ARGUMENT, "Unsupported cell data transport");
        }

        THostBindingPtr old;
        TString diskId;
        TString clientId;
        with_lock (Lock) {
            if (MountedDiskId.empty()) {
                return MakeError(E_NOT_FOUND, "Cell mount no longer exists");
            }
            if (!BindingInstalled || MigrationInFlight) {
                return MakeError(
                    E_REJECTED,
                    "Cell connection is changing; retry");
            }
            if (UsesEndpointHandler(transport) &&
                !Binding->HostConfig.GetGrpcDataFallbackEnabled())
            {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "Manual "
                        << NProto::ECellDataTransport_Name(transport)
                        << " selection requires gRPC data fallback");
            }
            if (Binding->HostConfig.GetTransport() == transport &&
                (!UsesEndpointHandler(transport) || Binding->Switcher))
            {
                TransportOverride = transport;
                return {};
            }
            MigrationInFlight = true;
            old = Binding;
            diskId = MountedDiskId;
            clientId = MountedClientId;
        }

        Y_DEFER
        {
            EndMigration();
        };
        THostBindingPtr binding;
        try {
            auto prepared = PrepareTransportChange(*old, transport);
            if (HasError(prepared)) {
                return prepared.GetError();
            }
            binding = prepared.ExtractResult();
        } catch (...) {
            return MakeError(E_FAIL, CurrentExceptionMessage());
        }

        auto error = CommitTransportChange(binding, diskId, clientId);
        if (HasError(error)) {
            return error;
        }
        if (old->Sink) {
            old->Sink->Detach();
        }
        // same host: the control channel and the pool subscription stay
        InstallBinding(binding);
        STORAGE_INFO(
            "Cell data transport switched: cell="
            << Pool->GetCellId() << " disk=" << diskId << " client=" << clientId
            << " host=" << old->HostConfig.GetFqdn() << " from="
            << NProto::ECellDataTransport_Name(old->HostConfig.GetTransport())
            << " to=" << NProto::ECellDataTransport_Name(transport));
        if (Pool->IsHostKnownDead(binding->HostConfig.GetFqdn())) {
            OnHostUnavailable(
                binding->HostConfig.GetFqdn(),
                binding->ChannelEpoch);
        }
        return {};
    }

    IBlockStorePtr GetService() override;

    IStoragePtr GetStorage() override
    {
        return CreateRemoteStorage(DataRouter, shared_from_this());
    }

    // Gives the binding its generation and the parts that need this
    // connection. Touches nothing the connection is serving through.
    NProto::TError PrepareBinding(const THostBindingPtr& binding);

    // Runs only once the binding is current.
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
        auto fqdn = response.GetTabletHost();
        if (!fqdn) {
            // the serving cell is older than this field
            return;
        }

        with_lock (Lock) {
            // a mount issued before a move is answered by the host we left
            if (generation != Binding->ControlGeneration ||
                fqdn == Binding->HostConfig.GetFqdn())
            {
                return;
            }
        }

        if (Observer) {
            Observer->OnTabletHostChanged(fqdn);
        }

        if (Pool->IsHostKnownDead(fqdn)) {
            return;
        }

        with_lock (Lock) {
            // again: the observer ran in between
            if (generation != Binding->ControlGeneration ||
                !IsWorthMovingToLocked(fqdn))
            {
                return;
            }

            if (MigrationInFlight) {
                // the latest target wins
                PendingTarget = std::move(fqdn);
                return;
            }

            MigrationInFlight = true;
        }

        STORAGE_INFO(
            "[" << GetHost() << "] moving to " << fqdn
                << ": the volume tablet lives there");

        StartMigration(std::move(fqdn));
    }

    void OnHostUnavailable(const TString& fqdn, ui64 epoch) noexcept override
    {
        with_lock (Lock) {
            const bool aboutCurrentHost =
                fqdn == Binding->HostConfig.GetFqdn();

            // stale: about the channel incarnation we had before a move took
            // us off this host and back
            if (aboutCurrentHost && epoch != Binding->ChannelEpoch) {
                return;
            }

            // also for a host we have left: the tablet may call us back there
            Avoid[fqdn].PingUntil = Bootstrap.Timer->Now() + AvoidCooldown;

            if (!aboutCurrentHost) {
                return;
            }

            MoveAwayRequests.insert(Binding->Generation);
        }

        RunPendingMoveAway();
    }

    // Only without a fallback: the host serves no data while the link is down.
    void OnDataLinkUnusable(ui64 generation, const TString& fqdn) noexcept
    {
        with_lock (Lock) {
            if (generation < Binding->Generation) {
                return;
            }

            // the binding may be one still being prepared
            Avoid[fqdn].DataUntil = Bootstrap.Timer->Now() + AvoidCooldown;
            MoveAwayRequests.insert(generation);
        }

        RunPendingMoveAway();
    }

    // The settle point: starts a move away if one is pending.
    void RunPendingMoveAway()
    {
        ui64 generation = 0;
        THashSet<TString> except;

        with_lock (Lock) {
            if (!IsMoveAwayPendingLocked()) {
                return;
            }

            // the request stays until the move is committed to below: the
            // data link coming up may take it back meanwhile
            generation = Binding->Generation;
            except = HostsToAvoidLocked();
        }

        // outside the lock: the pool has a lock of its own
        auto picked = Pool->PickHostExcept(except);
        if (HasError(picked)) {
            Pool->GetCounters().NoMigrationTarget->Inc();
            STORAGE_WARN(
                "[" << GetHost() << "] this host cannot serve us, and there "
                    << "is nowhere to move: "
                    << FormatError(picked.GetError()));
            return;
        }

        auto fqdn = picked.GetResult().GetFqdn();

        with_lock (Lock) {
            // again: the lock was released for the pick
            if (generation != Binding->Generation ||
                !IsMoveAwayPendingLocked())
            {
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

    void OnDataLinkUsable(ui64 generation, const TString& fqdn) noexcept
    {
        with_lock (Lock) {
            if (generation < Binding->Generation) {
                return;
            }

            auto* avoidance = Avoid.FindPtr(fqdn);
            if (!avoidance) {
                return;
            }

            // the pinger's verdict, if any, still stands
            avoidance->DataUntil = {};
            if (!avoidance->InForce(Bootstrap.Timer->Now())) {
                MoveAwayRequests.erase(generation);
            }
        }
    }

    void CompleteSetup()
    {
        // before the flag goes up, so that no move reports its host first
        ReportServingHost(GetHost());

        with_lock (Lock) {
            BindingInstalled = true;
        }

        RunPendingMoveAway();
    }

private:
    TResultOrError<THostBindingPtr> PrepareTransportChange(
        const THostBinding& old,
        NProto::ECellDataTransport transport)
    {
        auto config = old.HostConfig;
        config.SetTransport(transport);
        const bool hasGrpcDataEndpoint =
            old.HostConfig.GetTransport() == NProto::CELL_DATA_TRANSPORT_GRPC ||
            old.HostConfig.GetGrpcDataFallbackEnabled();
        auto built = BuildHostBinding(
            Bootstrap,
            config,
            old.ControlService,
            hasGrpcDataEndpoint ? old.DataEndpoint : nullptr);
        if (HasError(built)) {
            return built.GetError();
        }

        auto binding = built.ExtractResult();
        binding->ChannelEpoch = old.ChannelEpoch;
        binding->Generation = ReserveGeneration();
        binding->ControlGeneration = old.ControlGeneration;
        binding->BoundControlService = old.BoundControlService;
        if (!UsesEndpointHandler(transport)) {
            return binding;
        }

        binding->Sink = CreateDetachableTarget(DataRouter);
        auto prepared = PrepareBindingTransportSwitching(Bootstrap, *binding);
        if (HasError(prepared)) {
            return prepared.GetError();
        }
        binding->Switcher = prepared.ExtractResult();
        return binding;
    }

    NProto::TError CommitTransportChange(
        const THostBindingPtr& binding,
        const TString& diskId,
        const TString& clientId)
    {
        with_lock (Lock) {
            if (MountedDiskId != diskId || MountedClientId != clientId) {
                return MakeError(E_NOT_FOUND, "Cell mount no longer exists");
            }
            const auto* avoidance =
                Avoid.FindPtr(binding->HostConfig.GetFqdn());
            if (MoveAwayRequests.contains(Binding->Generation) && avoidance &&
                avoidance->PingUntil > Bootstrap.Timer->Now())
            {
                MoveAwayRequests.insert(binding->Generation);
            }
            Binding = binding;
            TransportOverride = binding->HostConfig.GetTransport();
            if (!UsesEndpointHandler(*TransportOverride)) {
                for (auto& [host, hostAvoidance]: Avoid) {
                    Y_UNUSED(host);
                    hostAvoidance.DataUntil = {};
                }
            }
        }
        return {};
    }

    void ReportServingHost(const TString& fqdn)
    {
        if (Observer) {
            Observer->OnServingHostChanged(fqdn);
        }
    }

    [[nodiscard]] bool IsMoveAwayPendingLocked()
    {
        if (!BindingInstalled || MigrationInFlight) {
            return false;
        }

        // nothing is in flight, so the others are about bindings we never
        // ended up on
        EraseNodesIf(
            MoveAwayRequests,
            [&](ui64 requested) { return requested != Binding->Generation; });

        if (MoveAwayRequests.empty()) {
            return false;
        }

        // the verdict may have run out while the request waited
        if (!IsAvoidedLocked(Binding->HostConfig.GetFqdn())) {
            MoveAwayRequests.clear();
            return false;
        }

        return true;
    }

    [[nodiscard]] bool IsAvoidedLocked(const TString& fqdn) const
    {
        const auto* avoidance = Avoid.FindPtr(fqdn);
        return avoidance && avoidance->InForce(Bootstrap.Timer->Now());
    }

    THashSet<TString> HostsToAvoidLocked()
    {
        THashSet<TString> except;

        // the pool may still see the host we are leaving as alive
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

    void StartMigration(TString fqdn)
    {
        // strong: the move owns a control channel that only its tail releases
        auto self = shared_from_this();

        TFuture<IBlockStorePtr> channel;
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

    // Builds the target binding while the old host is still serving requests.
    // With gRPC fallback, InstallBinding starts the switcher after the move.
    TResultOrError<THostBindingPtr> PrepareMigration(
        const TString& fqdn,
        const IBlockStorePtr& controlService)
    {
        auto hostConfig = Pool->MakeHostConfig(fqdn);
        with_lock (Lock) {
            if (TransportOverride) {
                hostConfig.SetTransport(*TransportOverride);
            }
        }

        auto built =
            BuildHostBinding(Bootstrap, hostConfig, controlService, {});
        if (HasError(built)) {
            return built.GetError();
        }

        auto binding = built.ExtractResult();
        binding->ChannelEpoch = Pool->GetChannelEpoch(fqdn);

        auto error = PrepareBinding(binding);
        if (HasError(error)) {
            return error;
        }
        return binding;
    }

    void OnChannelAcquired(
        TString fqdn,
        const IBlockStorePtr& controlService)
    {
        if (!controlService) {
            AbortMigration(fqdn, "no control channel", true);
            return;
        }

        TResultOrError<THostBindingPtr> prepared = MakeError(E_FAIL);
        try {
            prepared = PrepareMigration(fqdn, controlService);
        } catch (...) {
            AbortMigration(fqdn, CurrentExceptionMessage(), true);
            return;
        }

        if (HasError(prepared)) {
            AbortMigration(fqdn, FormatError(prepared.GetError()), true);
            return;
        }

        FinishMigration(prepared.GetResult());
    }

    void AbortMigration(
        const TString& fqdn,
        const TString& reason,
        bool channelAcquired)
    {
        Pool->GetCounters().MigrationFailures->Inc();
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

        // before InstallBinding: the old switcher is still alive and could
        // point the data back at the host we left
        if (old->Sink) {
            old->Sink->Detach();
        }

        try {
            InstallBinding(binding);
        } catch (...) {
            // the routers already point at the new host; only transport
            // switching is missing
            STORAGE_ERROR(
                "[" << binding->HostConfig.GetFqdn() << "] moved, but "
                    << "transport switching did not start: "
                    << CurrentExceptionMessage());
        }

        Pool->GetCounters().Migrations->Inc();
        ReportServingHost(binding->HostConfig.GetFqdn());

        auto self = shared_from_this();
        const bool targetDead =
            Pool->WatchHost(binding->HostConfig.GetFqdn(), self);
        Pool->UnwatchHost(old->HostConfig.GetFqdn(), self);
        Pool->ReleaseControlChannel(old->HostConfig.GetFqdn());

        if (targetDead) {
            // the death notification came before WatchHost subscribed;
            // replayed so that EndMigration moves us off again
            OnHostUnavailable(
                binding->HostConfig.GetFqdn(),
                binding->ChannelEpoch);
        }

        EndMigration();
    }

    // Every path that set MigrationInFlight must end here.
    void EndMigration()
    {
        TString next;
        with_lock (Lock) {
            next = std::move(PendingTarget);
            PendingTarget.clear();

            // the flag stays up for a queued target
            if (!next) {
                MigrationInFlight = false;
            }
        }

        if (next) {
            ContinueMigration(std::move(next));
            return;
        }

        // also after an aborted move: a deferred request would otherwise
        // wait for the next callback
        RunPendingMoveAway();
    }

    // Runs with MigrationInFlight still set.
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

    [[nodiscard]] bool IsWorthMovingToLocked(const TString& fqdn) const
    {
        if (Binding->HostConfig.GetFqdn() == fqdn) {
            return false;
        }

        // following the tablet there would undo the move that took us away
        return !IsAvoidedLocked(fqdn);
    }
};

////////////////////////////////////////////////////////////////////////////////

// Handed out by GetService(). Holds the connection alive: the caller may
// release its handle while requests are still in flight.
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
        return TMethod::Execute(
            Impl.get(),
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        // marks an inter-cell forward for the receiving host's authorization
        request->MutableHeaders()->SetCellId(CellId);

        // the data is encrypted on this side, before it leaves for the cell
        request->MutableEncryptionSpec()->Clear();
        request->SetForceDisableEncryption(true);

        auto diskId = request->GetDiskId();
        auto clientId = request->GetHeaders().GetClientId();
        return Impl->MountVolume(std::move(callContext), std::move(request))
            .Apply(
                [connection = std::weak_ptr<TCellConnection>(Connection),
                 diskId = std::move(diskId),
                 clientId = std::move(clientId)](const auto& f)
                {
                    const auto& response = f.GetValue();
                    if (!HasError(response)) {
                        if (auto self = connection.lock()) {
                            self->NoteMount(
                                diskId,
                                clientId,
                                response.GetTabletHost());
                        }
                    }
                    return response;
                });
    }

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override
    {
        request->MutableHeaders()->SetCellId(CellId);

        auto diskId = request->GetDiskId();
        return Impl->UnmountVolume(std::move(callContext), std::move(request))
            .Apply(
                [connection = std::weak_ptr<TCellConnection>(Connection),
                 diskId = std::move(diskId)](const auto& f)
                {
                    const auto& response = f.GetValue();
                    if (!HasError(response)) {
                        if (auto self = connection.lock()) {
                            self->NoteUnmount(diskId);
                        }
                    }
                    return response;
                });
    }
};

////////////////////////////////////////////////////////////////////////////////

// One host's control service. A mount response is attributed to the binding
// that served the request, not to the one current when it was sent.
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

// The data endpoint handler used when there is no fallback.
class TNoFallbackHandler final: public NCloud::IClientEndpointHandler
{
private:
    const std::weak_ptr<TCellConnection> Connection;
    const ui64 Generation;
    const TString Host;

public:
    TNoFallbackHandler(
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
            connection->OnDataLinkUsable(Generation, Host);
        }
    }

    void HandleDisconnected() override
    {}

    void HandleUnavailable() override
    {
        // repeats on every reconnect attempt, which retries the move
        if (auto connection = Connection.lock()) {
            connection->OnDataLinkUnusable(Generation, Host);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr TCellConnection::GetService()
{
    return std::make_shared<TControlService>(
        ControlRouter,
        shared_from_this(),
        Pool->GetCellId());
}

NProto::TError TCellConnection::PrepareBinding(
    const THostBindingPtr& binding)
{
    binding->Generation = ReserveGeneration();
    binding->ControlGeneration = binding->Generation;

    binding->BoundControlService = std::make_shared<TBoundControlService>(
        binding->ControlService,
        weak_from_this(),
        binding->ControlGeneration);

    const auto& hostConfig = binding->HostConfig;
    const bool noFallback = UsesEndpointHandler(hostConfig.GetTransport()) &&
                            !hostConfig.GetGrpcDataFallbackEnabled();

    if (!noFallback) {
        // the endpoint was built by BuildHostBinding
        return {};
    }

    binding->DataHandler = std::make_shared<TNoFallbackHandler>(
        weak_from_this(),
        binding->Generation,
        hostConfig.GetFqdn());

    auto endpoint =
        SetupHostDataEndpoint(Bootstrap, hostConfig, binding->DataHandler);

    if (HasError(endpoint)) {
        return endpoint.GetError();
    }

    binding->DataEndpoint = endpoint.GetResult();
    return {};
}

void TCellConnection::InstallBinding(const THostBindingPtr& binding)
{
    const auto& hostConfig = binding->HostConfig;

    ControlRouter->SetTarget(binding->BoundControlService);

    // before Activate: the switcher may retarget the router right away
    DataRouter->SetTarget(binding->DataEndpoint);

    if (!UsesEndpointHandler(hostConfig.GetTransport()) ||
        !hostConfig.GetGrpcDataFallbackEnabled())
    {
        return;
    }

    ITransportSwitcherPtr switcher;
    with_lock (Lock) {
        switcher = binding->Switcher;
    }
    if (!switcher) {
        binding->Sink = CreateDetachableTarget(DataRouter);
        auto prepared = PrepareBindingTransportSwitching(Bootstrap, *binding);
        if (HasError(prepared)) {
            return;
        }
        switcher = prepared.ExtractResult();
    }
    switcher->Activate();

    // the binding may already be current, and the mon page reads it
    with_lock (Lock) {
        binding->Switcher = std::move(switcher);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TCellConnectionRegistry
{
private:
    TAdaptiveLock Lock;
    TVector<std::weak_ptr<TCellConnection>> Connections;

public:
    void Add(const TCellConnectionPtr& connection)
    {
        with_lock (Lock) {
            // keeps the list bounded when the mon page is never opened
            EraseIf(Connections, [] (const auto& c) { return c.expired(); });
            Connections.push_back(connection);
        }
    }

    TVector<TCellMountStatus> GetMounts()
    {
        TVector<TCellMountStatus> mounts;
        for (const auto& connection: GetLiveConnections()) {
            auto status = connection->GetMountStatus();
            if (status.DiskId) {
                mounts.push_back(std::move(status));
            }
        }
        return mounts;
    }

    NProto::TError SwitchDataTransport(
        const TString& connectionId,
        NProto::ECellDataTransport transport)
    {
        if (connectionId.empty()) {
            return MakeError(E_ARGUMENT, "Connection must be specified");
        }

        TCellConnectionPtr selected;
        for (const auto& connection: GetLiveConnections()) {
            if (connection->GetConnectionId() == connectionId) {
                selected = connection;
                break;
            }
        }
        if (selected) {
            return selected->SwitchDataTransport(transport);
        }
        return MakeError(E_NOT_FOUND, "Cell mount no longer exists");
    }

private:
    TVector<TCellConnectionPtr> GetLiveConnections()
    {
        TVector<TCellConnectionPtr> live;
        with_lock (Lock) {
            EraseIf(Connections, [](const auto& c) { return c.expired(); });
            for (const auto& weak: Connections) {
                if (auto connection = weak.lock()) {
                    live.push_back(std::move(connection));
                }
            }
        }
        return live;
    }
};

TCellConnectionRegistryPtr CreateCellConnectionRegistry()
{
    return std::make_shared<TCellConnectionRegistry>();
}

TVector<TCellMountStatus> GetCellMounts(TCellConnectionRegistry& registry)
{
    return registry.GetMounts();
}

NProto::TError SwitchCellDataTransport(
    TCellConnectionRegistry& registry,
    const TString& connectionId,
    NProto::ECellDataTransport transport)
{
    return registry.SwitchDataTransport(connectionId, transport);
}

////////////////////////////////////////////////////////////////////////////////

TCellConnectionFuture CreateCellConnection(
    TCellHostPoolPtr pool,
    TCellHostConfig hostConfig,
    TBootstrap bootstrap,
    ICellConnectionObserverPtr observer)
{
    auto fqdn = hostConfig.GetFqdn();
    auto controlFuture = pool->AcquireControlChannel(fqdn);

    return controlFuture.Apply(
        [pool = std::move(pool),
         hostConfig = std::move(hostConfig),
         bootstrap = std::move(bootstrap),
         observer = std::move(observer),
         fqdn = std::move(fqdn)](
            const auto& f) mutable -> TResultOrError<ICellConnectionPtr>
        {
            auto controlService = f.GetValue();
            if (!controlService) {
                pool->ReleaseControlChannel(fqdn);
                return MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Can't set up a control channel to " << fqdn);
            }

            auto built =
                BuildHostBinding(bootstrap, hostConfig, controlService, {});
            if (HasError(built)) {
                pool->ReleaseControlChannel(fqdn);
                return built.GetError();
            }

            auto binding = built.ExtractResult();
            binding->ChannelEpoch = pool->GetChannelEpoch(fqdn);

            auto registry = bootstrap.Connections;
            auto connection = std::make_shared<TCellConnection>(
                pool,
                std::move(bootstrap),
                std::move(observer),
                binding);

            auto error = connection->PrepareBinding(binding);
            if (HasError(error)) {
                // no release here: the connection owns the channel now, and
                // its destructor releases it
                return error;
            }

            connection->InstallBinding(binding);

            if (pool->WatchHost(fqdn, connection)) {
                connection->OnHostUnavailable(
                    fqdn,
                    pool->GetChannelEpoch(fqdn));
            }

            // last: from here on callbacks may act on their own
            connection->CompleteSetup();

            if (registry) {
                registry->Add(connection);
            }

            return ICellConnectionPtr(std::move(connection));
        });
}

}   // namespace NCloud::NBlockStore::NCells
