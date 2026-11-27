#include "service.h"

#include "public.h"

#include "host_endpoint.h"
#include "service_private.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/icv2/data/protocol.h>
#include <cloud/blockstore/libs/icv2/data/target.h>

#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/icv2/handshake/handshake.h>
#include <cloud/storage/core/libs/icv2/peer/peer_manager.h>
#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>
#include <cloud/storage/core/libs/icv2/tcp/address_resolver.h>
#include <cloud/storage/core/libs/icv2/tcp/listener.h>

#include <contrib/ydb/library/actors/interconnect/interconnect_common.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/network/address.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NIcV2 {

using namespace NThreading;

using NCloud::NIcV2::CreatePeerManager;
using NCloud::NIcV2::EPeerState;
using NCloud::NIcV2::IPeer;
using NCloud::NIcV2::IPeerManagerPtr;
using NCloud::NIcV2::IPeerPtr;
using NCloud::NIcV2::IRequestChannelFactory;
using NCloud::NIcV2::TPeerStatus;
using NCloud::NIcV2::TRequestChannelResult;

using NCloud::NIcV2::CreateRequestChannel;
using NCloud::NIcV2::IRequestChannel;
using NCloud::NIcV2::IRequestChannelPtr;

using NCloud::NIcV2::CreateHandshakeProvider;
using NCloud::NIcV2::IHandshakeProviderPtr;
using NCloud::NIcV2::TAcceptHandshakeFuture;
using NCloud::NIcV2::TAcceptHandshakeResult;
using NCloud::NIcV2::THandshakeConfig;
using NCloud::NIcV2::THandshakeIncomplete;
using NCloud::NIcV2::THandshakeReady;

using NCloud::NIcV2::ITransportPtr;

using NCloud::NIcV2::TSocketPtr;

using NCloud::NIcV2::IListenerPtr;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

constexpr int TestListenerBacklog = 128;

enum class EServicePhase
{
    Created,
    Running,
    Stopping,
    Stopped,
};

struct TServiceState: public std::enable_shared_from_this<TServiceState>
{
    const NCells::TCellsConfigPtr Config;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    TLog Log;
    const ITaskQueuePtr Completions;
    const ITaskQueuePtr DataExecutor;
    IDataTargetPtr DataTarget;

    struct TDataBinding
    {
        std::weak_ptr<IDataEndpoint> Endpoint;
        TFuture<void> Closed;
        std::weak_ptr<IPeer> Peer;
    };

    TVector<TDataBinding> DataEndpoints;
    const TCounterPtr AcceptedConnections;
    const TCounterPtr RejectedConnections;
    const TCounterPtr ListenerErrors;
    const TCounterPtr Listening;
    const TCounterPtr HandshakeErrors;
    const TCounterPtr ActiveConnections;
    ITransportPtr Transport;
    IHandshakeProviderPtr Handshake;
    IPeerManagerPtr Peers;

    TMutex Mutex;
    EServicePhase Phase = EServicePhase::Created;
    NProto::TError ListenerError;
    size_t ConnectionCount = 0;
    size_t PendingDataEndpointCreations = 0;
    ui64 NextConnectionId = 1;
    THashMap<ui64, IRequestChannelPtr> Channels;

    TServiceState(
        NCells::TCellsConfigPtr config,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        TLog log,
        const NMonitoring::TDynamicCountersPtr& counters)
        : Config(std::move(config))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Log(std::move(log))
        , Completions(CreateThreadPool(
              "ICV2Result",
              Config->GetIcV2Config().GetOutgoingCompletionThreads()))
        , DataExecutor(CreateThreadPool(
              "ICV2Data",
              Config->GetIcV2Config().GetIncomingRequestThreads()))
        , AcceptedConnections(counters->GetCounter("AcceptedConnections", true))
        , RejectedConnections(counters->GetCounter("RejectedConnections", true))
        , ListenerErrors(counters->GetCounter("ListenerErrors", true))
        , Listening(counters->GetCounter("Listening", false))
        , HandshakeErrors(counters->GetCounter("HandshakeErrors", true))
        , ActiveConnections(counters->GetCounter("ActiveConnections", false))
    {}

    void RequestStop()
    {
        TVector<IDataEndpointPtr> endpoints;
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase == EServicePhase::Stopped) {
                return;
            }
            Phase = EServicePhase::Stopping;
            for (const auto& binding: DataEndpoints) {
                if (auto endpoint = binding.Endpoint.lock()) {
                    endpoints.push_back(std::move(endpoint));
                }
            }
            *Listening = 0;
        }
        for (const auto& endpoint: endpoints) {
            endpoint->RequestStop();
        }
        DataTarget->RequestStop();
        Handshake->RequestStop();
        Peers->RequestStop();
        Transport->RequestStop();
    }

    void OnListenerError(const NProto::TError& error)
    {
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase >= EServicePhase::Stopping) {
                return;
            }
            ListenerError = error;
            *Listening = 0;
            ++*ListenerErrors;
        }
        const auto message = FormatError(error);
        STORAGE_ERROR("IC V2 listener stopped: " << message);
        ReportIcV2ListenerFailure(message);
    }

    void Accept(TSocketPtr socket)
    {
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase >= EServicePhase::Stopping) {
                return;
            }
            const auto& options = Config->GetIcV2Config();
            // Reserve room for the auxiliary hello even when every main slot
            // is occupied. Logical channels have their own admission below.
            const ui64 socketLimit = ui64(options.GetMaxIncomingConnections()) *
                                     (options.GetEnableXdc() ? 2 : 1);
            if (ConnectionCount >= socketLimit) {
                ++*RejectedConnections;
                return;
            }
            ++ConnectionCount;
        }
        ++*AcceptedConnections;
        const auto timeout = Config->GetIcV2Config().GetHandshakeTimeout();
        auto future = Handshake->Accept(std::move(socket), timeout);
        future.Subscribe(
            [weak = weak_from_this()](const TAcceptHandshakeFuture& future)
            {
                if (auto state = weak.lock()) {
                    state->OnHello(future.GetValue());
                }
            });
    }

    void OnHello(const TAcceptHandshakeResult& result)
    {
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase >= EServicePhase::Stopping ||
                HasError(result.GetError()) ||
                std::holds_alternative<THandshakeIncomplete>(
                    result.GetResult()))
            {
                --ConnectionCount;
                if (Phase >= EServicePhase::Stopping) {
                    return;
                }
            }
        }
        if (HasError(result.GetError())) {
            ++*HandshakeErrors;
            STORAGE_WARN(
                "IC V2 hello failed: " << FormatError(result.GetError()));
            return;
        }
        if (std::holds_alternative<THandshakeIncomplete>(result.GetResult())) {
            return;
        }
        auto [channel, error] = MakeChannel(
            std::get<THandshakeReady>(result.GetResult()),
            false);   // The peer sends pings on connections it initiates.
        if (HasError(error)) {
            {
                TGuard<TMutex> guard(Mutex);
                --ConnectionCount;
            }
            STORAGE_ERROR(
                "IC V2 channel creation failed: " << FormatError(error));
            return;
        }
        ui64 id = 0;
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase < EServicePhase::Stopping &&
                Channels.size() <
                    Config->GetIcV2Config().GetMaxIncomingConnections())
            {
                Y_ABORT_UNLESS(
                    NextConnectionId,
                    "IC V2 connection IDs exhausted");
                id = NextConnectionId++;
                Channels.emplace(id, channel);
                ++*ActiveConnections;
            } else {
                --ConnectionCount;
                if (Phase < EServicePhase::Stopping) {
                    ++*RejectedConnections;
                }
            }
        }
        if (!id) {
            channel->Close();
            return;
        }
        channel->WhenClosed().Subscribe(
            [weak = weak_from_this(), id](const auto&)
            {
                if (auto state = weak.lock()) {
                    {
                        TGuard<TMutex> guard(state->Mutex);
                        state->Channels.erase(id);
                        --*state->ActiveConnections;
                        --state->ConnectionCount;
                    }
                    auto& Log = state->Log;
                    STORAGE_INFO("IC V2 connection closed: id=" << id);
                }
            });
        if (channel->Activate()) {
            STORAGE_INFO("IC V2 connection accepted: id=" << id);
        }
    }

    TRequestChannelResult MakeChannel(
        const THandshakeReady& connection,
        bool sendPings)
    {
        return CreateRequestChannel(
            Transport,
            connection.Socket,
            connection.XdcSocket,
            {
                .EventTypes = {DataEventTypes.begin(), DataEventTypes.end()},
                .SendPings = sendPings,
                .MaxPendingRequests =
                    Config->GetIcV2Config().GetMaxOutgoingInflightRequests(),
            },
            Timer,
            Scheduler,
            Completions,
            DataTarget);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TServiceRequestChannelFactory final: public IRequestChannelFactory
{
private:
    const std::weak_ptr<TServiceState> State;

public:
    explicit TServiceRequestChannelFactory(std::weak_ptr<TServiceState> state)
        : State(std::move(state))
    {}

    TRequestChannelResult CreateRequestChannel(
        const THandshakeReady& connection) override
    {
        if (auto state = State.lock()) {
            return state->MakeChannel(connection, true);
        }
        return MakeError(E_REJECTED, "IC V2 service has been destroyed");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TListenerHandler final: public NCloud::NIcV2::IListenerHandler
{
    const std::weak_ptr<TServiceState> State;

public:
    explicit TListenerHandler(std::weak_ptr<TServiceState> state)
        : State(std::move(state))
    {}

    void OnSocketAccepted(TSocketPtr socket) override
    {
        if (auto state = State.lock()) {
            state->Accept(std::move(socket));
        }
    }

    void OnError(const NProto::TError& error) override
    {
        if (auto state = State.lock()) {
            state->OnListenerError(error);
        }
    }
};

class TService final: public IService
{
    const std::shared_ptr<TServiceState> State;
    const IListenerPtr Listener;
    TMutex LifecycleMutex;

public:
    TService(std::shared_ptr<TServiceState> state, IListenerPtr listener)
        : State(std::move(state))
        , Listener(std::move(listener))
    {}

    ~TService() override
    {
        Stop();
    }

    const NInterconnect::TAddress& GetAddress() const override
    {
        return Listener->GetAddress();
    }

    NProto::TError GetError() const override
    {
        TGuard<TMutex> guard(State->Mutex);
        return State->ListenerError;
    }

    TResultOrError<IBlockStorePtr> CreateDataEndpoint(
        const TString& cellId,
        const TString& host,
        IClientEndpointHandlerPtr handler) override
    {
        const auto* cell = State->Config->GetCells().FindPtr(cellId);
        if (!cell || cellId == State->Config->GetCellId() || host.empty() ||
            !(*cell)->GetIcV2Port() || (*cell)->GetIcV2Port() > 65535)
        {
            return MakeError(
                E_ARGUMENT,
                "invalid IC V2 data host or cell port");
        }
        const auto& options = State->Config->GetIcV2Config();
        {
            TGuard<TMutex> guard(State->Mutex);
            if (State->Phase != EServicePhase::Running) {
                return MakeError(E_REJECTED, "IC V2 service is not running");
            }
            EraseIf(
                State->DataEndpoints,
                [](const TServiceState::TDataBinding& binding)
                {
                    return binding.Endpoint.expired() &&
                           binding.Closed.HasValue();
                });
            if (State->DataEndpoints.size() +
                    State->PendingDataEndpointCreations >=
                options.GetMaxOutgoingConnections())
            {
                return MakeError(
                    E_REJECTED,
                    "IC V2 data binding limit reached");
            }
            ++State->PendingDataEndpointCreations;
        }

        bool reserved = true;
        Y_DEFER
        {
            if (reserved) {
                TGuard<TMutex> guard(State->Mutex);
                --State->PendingDataEndpointCreations;
            }
        };

        auto [binding, error] = CreateHostDataEndpoint(
            host,
            static_cast<ui16>((*cell)->GetIcV2Port()),
            std::move(handler),
            State->Peers,
            {
                .MaxInflightRequests = options.GetMaxOutgoingInflightRequests(),
                .MaxInflightBytes = options.GetMaxOutgoingInflightBytes(),
            });
        if (HasError(error)) {
            return error;
        }
        auto endpoint = binding.GetEndpoint();
        TServiceState::TDataBinding entry{
            .Endpoint = endpoint,
            .Closed = endpoint->WhenClosed(),
            .Peer = binding.GetPeer(),
        };
        {
            TGuard<TMutex> guard(State->Mutex);
            if (State->Phase == EServicePhase::Running) {
                State->DataEndpoints.push_back(std::move(entry));
                --State->PendingDataEndpointCreations;
                reserved = false;
                return IBlockStorePtr(std::move(endpoint));
            }
        }
        endpoint->RequestStop();
        return MakeError(E_REJECTED, "IC V2 service is not running");
    }

    TPeerStatus GetDataEndpointStatus(const IBlockStorePtr& endpoint) override
    {
        IPeerPtr peer;
        {
            TGuard<TMutex> guard(State->Mutex);
            for (const auto& binding: State->DataEndpoints) {
                if (!binding.Endpoint.owner_before(endpoint) &&
                    !endpoint.owner_before(binding.Endpoint))
                {
                    peer = binding.Peer.lock();
                    break;
                }
            }
        }
        return peer ? peer->GetStatus()
                    : TPeerStatus{.State = EPeerState::Disconnected};
    }

    void Start() override
    {
        TGuard<TMutex> lifecycle(LifecycleMutex);
        {
            TGuard<TMutex> guard(State->Mutex);
            Y_ABORT_UNLESS(State->Phase == EServicePhase::Created);
        }

        State->Completions->Start();
        State->DataExecutor->Start();
        State->Handshake->Start();
        State->Peers->Start();
        *State->Listening = 1;
        Listener->Start();
        {
            TGuard<TMutex> guard(State->Mutex);
            if (State->Phase == EServicePhase::Created) {
                State->Phase = EServicePhase::Running;
            }
        }
        auto& Log = State->Log;
        STORAGE_INFO(
            "IC V2 listening: cell=" << State->Config->GetCellId()
                                     << " address=" << GetAddress().ToString());
    }

    void RequestStop() override
    {
        State->RequestStop();
        Listener->RequestStop();
    }

    void Stop() override
    {
        TGuard<TMutex> lifecycle(LifecycleMutex);
        {
            TGuard<TMutex> guard(State->Mutex);
            if (State->Phase == EServicePhase::Stopped) {
                return;
            }
        }
        RequestStop();
        Listener->Stop();
        State->Handshake->Stop();
        TVector<IRequestChannelPtr> channels;
        {
            TGuard<TMutex> guard(State->Mutex);
            channels.reserve(State->Channels.size());
            for (const auto& [id, channel]: State->Channels) {
                channels.push_back(channel);
            }
        }
        for (const auto& channel: channels) {
            channel->Close();
        }
        State->Peers->Stop();
        State->Transport->Stop();
        for (const auto& channel: channels) {
            channel->WhenClosed().Wait();
        }
        TVector<TFuture<void>> bindings;
        {
            TGuard<TMutex> guard(State->Mutex);
            for (const auto& binding: State->DataEndpoints) {
                bindings.push_back(binding.Closed);
            }
        }
        // The shutdown barrier survives the endpoint and covers its callbacks.
        for (const auto& closed: bindings) {
            closed.Wait();
        }
        State->DataTarget->Stop();
        State->DataExecutor->Stop();
        State->Completions->Stop();
        TGuard<TMutex> guard(State->Mutex);
        State->Phase = EServicePhase::Stopped;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IServicePtr> CreateService(
    NCells::TCellsConfigPtr config,
    IBlockStorePtr dataService,
    NCloud::NIcV2::IAddressResolverPtr resolver,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    NMonitoring::TDynamicCountersPtr counters)
{
    return NPrivate::CreateService(
        std::move(config),
        std::move(dataService),
        std::move(resolver),
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(counters),
        nullptr);
}

TResultOrError<IServicePtr> NPrivate::CreateService(
    NCells::TCellsConfigPtr config,
    IBlockStorePtr dataService,
    NCloud::NIcV2::IAddressResolverPtr resolver,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    NMonitoring::TDynamicCountersPtr counters,
    NCloud::NIcV2::NPrivate::IListenerErrorInjectorPtr listenerErrorInjector)
{
    if (!config || !dataService || !resolver || !timer || !scheduler ||
        !logging || !counters)
    {
        return MakeError(
            E_ARGUMENT,
            "IC V2 service requires config, data service, address resolver, "
            "timer, scheduler, logging and counters");
    }
    const auto& options = config->GetIcV2Config();
    if (!config->GetCellsEnabled() || !options.GetEnabled() ||
        !options.GetIcV2Config().HasPort() || options.GetPort() > 65535 ||
        !options.GetHandshakeTimeout() ||
        !options.GetMaxIncomingConnections() ||
        !options.GetMaxOutgoingConnections() ||
        !options.GetMaxOutgoingInflightRequests() ||
        !options.GetMaxOutgoingInflightBytes() ||
        !options.GetMaxIncomingInflightRequests() ||
        !options.GetMaxIncomingInflightBytes() ||
        !options.GetMaxTotalOutputQueueBytes() ||
        !options.GetNetworkIoThreads() ||
        !options.GetOutgoingCompletionThreads() ||
        !options.GetIncomingRequestThreads())
    {
        return MakeError(E_ARGUMENT, "invalid IC V2 service configuration");
    }
    NInterconnect::TAddress address;
    in_addr ipv4;
    in6_addr ipv6;
    if (inet_pton(AF_INET, options.GetListenAddress().c_str(), &ipv4) == 1) {
        address = NInterconnect::TAddress(ipv4, options.GetPort());
    } else if (
        inet_pton(AF_INET6, options.GetListenAddress().c_str(), &ipv6) == 1)
    {
        address = NInterconnect::TAddress(ipv6, options.GetPort());
    } else {
        return MakeError(
            E_ARGUMENT,
            "IC V2 listen address must be a numeric IPv4 or IPv6 address");
    }
    THandshakeConfig helloConfig{options.GetEnableXdc()};
    auto [hello, helloError] = CreateHandshakeProvider(helloConfig);
    if (HasError(helloError)) {
        return helloError;
    }
    auto state = std::make_shared<TServiceState>(
        config,
        std::move(timer),
        std::move(scheduler),
        logging->CreateLog("BLOCKSTORE_SERVER"),
        counters);
    auto [target, targetError] = CreateDataTarget(
        std::move(dataService),
        state->DataExecutor,
        {
            .MaxInflightRequests = options.GetMaxIncomingInflightRequests(),
            .MaxInflightBytes = options.GetMaxIncomingInflightBytes(),
        });
    if (HasError(targetError)) {
        return targetError;
    }
    state->DataTarget = std::move(target);
    state->Handshake = std::move(hello);
    const std::weak_ptr<TServiceState> weak = state;
    auto listenerHandler = std::make_shared<TListenerHandler>(weak);
    auto [listener, listenError] =
        listenerErrorInjector
            ? NCloud::NIcV2::NPrivate::CreateListener(
                  address,
                  TestListenerBacklog,
                  std::move(listenerHandler),
                  std::move(listenerErrorInjector))
            : NCloud::NIcV2::CreateListener(address, std::move(listenerHandler));
    if (HasError(listenError)) {
        return listenError;
    }
    auto common = MakeIntrusive<NActors::TInterconnectProxyCommon>();
    common->MonCounters = counters;
    common->Settings.V2.Threads = options.GetNetworkIoThreads();
    common->Settings.V2.ChecksumEvents = options.GetChecksumEvents();
    common->Settings.V2.EnableSQPOLL = false;
    state->Transport = NCloud::NIcV2::CreateTransport(
        NActors::CreateUringEngine(std::move(common)),
        options.GetMaxTotalOutputQueueBytes(),
        {});
    if (!state->Transport) {
        return MakeError(E_NOT_IMPLEMENTED, "IC V2 engine is unavailable");
    }
    auto [outgoingHello, outgoingHelloError] =
        CreateHandshakeProvider(std::move(helloConfig));
    if (HasError(outgoingHelloError)) {
        return outgoingHelloError;
    }
    auto [connector, connectorError] = NCloud::NIcV2::CreateConnector();
    if (HasError(connectorError)) {
        return connectorError;
    }
    auto [peers, peersError] = CreatePeerManager(
        {.ConnectTimeout = options.GetHandshakeTimeout()},
        std::move(connector),
        std::move(outgoingHello),
        state->Timer,
        state->Scheduler,
        std::move(resolver),
        state->Completions,
        std::make_shared<TServiceRequestChannelFactory>(weak));
    if (HasError(peersError)) {
        return peersError;
    }
    state->Peers = std::move(peers);
    return IServicePtr(
        std::make_shared<TService>(std::move(state), std::move(listener)));
}

}   // namespace NCloud::NBlockStore::NIcV2
