#include "service.h"

#include "host_endpoint.h"
#include "peer_connections.h"
#include "ping_client.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/icv2/data/protocol.h>
#include <cloud/blockstore/libs/icv2/transport/diagnostic.h>
#include <cloud/blockstore/libs/icv2/transport/handshake.h>
#include <cloud/blockstore/libs/icv2/transport/request_channel.h>

#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/icv2/tcp/listener.h>

#include <contrib/ydb/library/actors/interconnect/interconnect_common.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/network/address.h>

#include <mutex>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IListenerPtr;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

struct TChannelContext
{
    std::weak_ptr<IRequestChannel> Channel;
};

struct TServiceState: public std::enable_shared_from_this<TServiceState>
{
    const NCells::TCellsConfigConstPtr Config;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    TLog Log;
    const ITaskQueuePtr Completions;
    const ITaskQueuePtr DataExecutor;
    IDataTargetPtr DataTarget;
    bool Running = false;
    ui64 NextDataBindingId = 1;

    struct TDataBinding
    {
        TString CellId;
        std::weak_ptr<IDataEndpoint> Endpoint;
        NThreading::TFuture<void> Closed;
    };

    THashMap<ui64, TDataBinding> DataEndpoints;
    const TCounterPtr AcceptedConnections;
    const TCounterPtr RejectedConnections;
    const TCounterPtr ListenerErrors;
    const TCounterPtr Listening;
    const TCounterPtr HandshakeErrors;
    const TCounterPtr ActiveConnections;
    const TCounterPtr PingRequests;
    const TCounterPtr PingReplies;
    ITransportPtr Transport;
    IHandshakeProviderPtr Handshake;
    IPeerConnectionsPtr Peers;
    IPingClientPtr Pings;

    std::mutex Mutex;
    bool Stopping = false;
    NProto::TError Error;
    size_t ConnectionCount = 0;
    ui64 NextConnectionId = 1;
    THashMap<ui64, IRequestChannelPtr> Channels;

    TServiceState(
        NCells::TCellsConfigConstPtr config,
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
              Config->GetCellsConfig().GetIcV2().GetCompletionThreads()))
        , DataExecutor(CreateThreadPool(
              "ICV2Data",
              Config->GetCellsConfig().GetIcV2().GetDataWorkers()))
        , AcceptedConnections(counters->GetCounter("AcceptedConnections", true))
        , RejectedConnections(counters->GetCounter("RejectedConnections", true))
        , ListenerErrors(counters->GetCounter("ListenerErrors", true))
        , Listening(counters->GetCounter("Listening", false))
        , HandshakeErrors(counters->GetCounter("HandshakeErrors", true))
        , ActiveConnections(counters->GetCounter("ActiveConnections", false))
        , PingRequests(counters->GetCounter("PingRequests", true))
        , PingReplies(counters->GetCounter("PingReplies", true))
    {}

    void RequestStop()
    {
        TVector<IDataEndpointPtr> endpoints;
        {
            std::lock_guard guard(Mutex);
            Stopping = true;
            Running = false;
            for (const auto& [id, binding]: DataEndpoints) {
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
        Pings->RequestStop();
        Handshake->RequestStop();
        Peers->RequestStop();
        Transport->RequestStop();
    }

    void Accept(TSocketResult result)
    {
        if (HasError(result.GetError())) {
            {
                std::lock_guard guard(Mutex);
                if (Stopping) {
                    return;
                }
                Error = result.GetError();
                Stopping = true;
                *Listening = 0;
                ++*ListenerErrors;
            }
            STORAGE_ERROR(
                "IC V2 listener stopped: " << FormatError(result.GetError()));
            // The accept worker cannot join itself. The owner still calls the
            // external Stop barrier, which joins workers and result callbacks.
            RequestStop();
            return;
        }
        {
            std::lock_guard guard(Mutex);
            if (Stopping) {
                return;
            }
            const auto& options = Config->GetCellsConfig().GetIcV2();
            // Reserve room for the auxiliary hello even when every main slot
            // is occupied. Logical channels have their own admission below.
            const ui64 socketLimit = ui64(options.GetMaxConnections()) *
                                     (options.GetEnableXdc() ? 2 : 1);
            if (ConnectionCount >= socketLimit) {
                ++*RejectedConnections;
                return;
            }
            ++ConnectionCount;
        }
        ++*AcceptedConnections;
        const std::weak_ptr<TServiceState> weak = shared_from_this();
        const auto timeout = TDuration::MilliSeconds(
            Config->GetCellsConfig().GetIcV2().GetHandshakeTimeoutMs());
        auto future = Handshake->Accept(result.ExtractResult(), timeout);
        future.Subscribe(
            [weak](const TAcceptHandshakeFuture& future)
            {
                if (auto state = weak.lock()) {
                    state->OnHello(future.GetValue());
                }
            });
    }

    void OnHello(const TAcceptHandshakeResult& result)
    {
        {
            std::lock_guard guard(Mutex);
            if (Stopping || HasError(result.GetError()) ||
                std::holds_alternative<THandshakeIncomplete>(
                    result.GetResult()))
            {
                --ConnectionCount;
                if (Stopping) {
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
        auto [channel, error] =
            MakeChannel(std::get<THandshakeReady>(result.GetResult()));
        if (HasError(error)) {
            {
                std::lock_guard guard(Mutex);
                --ConnectionCount;
            }
            STORAGE_ERROR(
                "IC V2 channel creation failed: " << FormatError(error));
            return;
        }
        const std::weak_ptr<TServiceState> weak = shared_from_this();
        ui64 id = 0;
        {
            std::lock_guard guard(Mutex);
            if (!Stopping &&
                Channels.size() <
                    Config->GetCellsConfig().GetIcV2().GetMaxConnections())
            {
                Y_ABORT_UNLESS(
                    NextConnectionId,
                    "IC V2 connection IDs exhausted");
                id = NextConnectionId++;
                Channels.emplace(id, channel);
                ++*ActiveConnections;
            } else {
                --ConnectionCount;
                if (!Stopping) {
                    ++*RejectedConnections;
                }
            }
        }
        if (!id) {
            channel->Close();
            return;
        }
        channel->WhenClosed().Subscribe(
            [weak, id](const auto&)
            {
                if (auto state = weak.lock()) {
                    {
                        std::lock_guard guard(state->Mutex);
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

    TChannelResult MakeChannel(const THandshakeReady& connection)
    {
        auto context = std::make_shared<TChannelContext>();
        const std::weak_ptr<TServiceState> weak = shared_from_this();
        TVector<TRequestEventTypes> eventTypes(
            DataEventTypes.begin(),
            DataEventTypes.end());
        eventTypes.push_back(
            {TEvDiagnostic::EvPingRequest, TEvDiagnostic::EvPingResponse});
        auto [channel, error] = CreateRequestChannel(
            Transport,
            connection.Socket,
            connection.XdcSocket,
            {std::move(eventTypes),
             false,
             Config->GetCellsConfig().GetIcV2().GetMaxPendingRequests()},
            Timer,
            Scheduler,
            Completions,
            [weak, context](std::unique_ptr<NActors::IEventHandle> request)
            {
                if (auto state = weak.lock()) {
                    state->Reply(context, std::move(request));
                }
            });
        if (HasError(error)) {
            return error;
        }
        context->Channel = channel;
        return channel;
    }

    void Reply(
        const std::shared_ptr<TChannelContext>& context,
        std::unique_ptr<NActors::IEventHandle> request)
    {
        if (request->GetTypeRewrite() != TEvDiagnostic::EvPingRequest) {
            if (auto channel = context->Channel.lock()) {
                DataTarget->HandleRequest(
                    std::move(channel),
                    std::move(request));
            }
            return;
        }
        ++*PingRequests;
        TEvDiagnostic::TEvPingRequest* ping;
        try {
            ping = request->Get<TEvDiagnostic::TEvPingRequest>();
        } catch (const yexception& e) {
            STORAGE_WARN("IC V2 invalid ping: error=" << e.what());
            if (auto channel = context->Channel.lock()) {
                channel->Close();
            }
            return;
        }
        auto response = std::make_unique<TEvDiagnostic::TEvPingResponse>();
        response->Record.SetPayload(ping->Record.GetPayload());
        if (auto channel = context->Channel.lock()) {
            if (channel->Reply(request->Cookie, std::move(response))) {
                ++*PingReplies;
                STORAGE_DEBUG(
                    "IC V2 ping replied: cookie="
                    << request->Cookie
                    << " bytes=" << ping->Record.GetPayload().size());
            } else {
                // A rejected reply must not leave the peer waiting until its
                // deadline while this channel keeps accepting more requests.
                channel->Close();
            }
        }
    }
};

class TListenerClient final: public NCloud::NIcV2::IListenerClient
{
    const std::weak_ptr<TServiceState> State;

public:
    explicit TListenerClient(std::weak_ptr<TServiceState> state)
        : State(std::move(state))
    {}

    void OnConnection(TSocketPtr socket) override
    {
        if (auto state = State.lock()) {
            state->Accept(std::move(socket));
        }
    }

    void OnError(const NProto::TError& error) override
    {
        if (auto state = State.lock()) {
            state->Accept(error);
        }
    }
};

class TService final: public IService
{
    const std::shared_ptr<TServiceState> State;
    const IListenerPtr Listener;
    std::mutex LifecycleMutex;
    bool Started = false;
    bool Stopped = false;

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
        std::lock_guard guard(State->Mutex);
        return State->Error;
    }

    TPingFuture
    Ping(TString cellId, TString payload, TDuration timeout) override
    {
        return State->Pings->Ping(
            std::move(cellId),
            std::move(payload),
            timeout);
    }

    TResultOrError<IBlockStorePtr> CreateDataEndpoint(
        const TString& cellId,
        const TString& host) override
    {
        const auto it = State->Config->GetCells().find(cellId);
        if (it == State->Config->GetCells().end() ||
            cellId == State->Config->GetCellId() || host.empty() ||
            !it->second->GetIcV2Port() || it->second->GetIcV2Port() > 65535)
        {
            return MakeError(
                E_ARGUMENT,
                "invalid IC V2 data host or cell port");
        }
        TVector<NInterconnect::TAddress> addresses;
        try {
            TNetworkAddress resolved(host, it->second->GetIcV2Port());
            for (auto address = resolved.Begin(); address != resolved.End();
                 ++address)
            {
                if (address->ai_family == AF_INET ||
                    address->ai_family == AF_INET6)
                {
                    NAddr::TAddrInfo remote(&*address);
                    addresses.emplace_back(remote);
                }
            }
        } catch (const yexception& e) {
            return MakeError(
                E_FAIL,
                TStringBuilder()
                    << "IC V2 host resolution failed: " << e.what());
        }
        std::lock_guard guard(State->Mutex);
        if (!State->Running || State->Stopping) {
            return MakeError(E_REJECTED, "IC V2 service is not running");
        }
        Y_ABORT_UNLESS(State->NextDataBindingId, "IC V2 binding IDs exhausted");
        const auto id = State->NextDataBindingId++;
        const auto& options = State->Config->GetCellsConfig().GetIcV2();
        for (auto it = State->DataEndpoints.begin();
             it != State->DataEndpoints.end();)
        {
            if (it->second.Endpoint.expired() && it->second.Closed.HasValue()) {
                State->DataEndpoints.erase(it++);
            } else {
                ++it;
            }
        }
        if (State->DataEndpoints.size() >= options.GetMaxOutgoingConnections())
        {
            return MakeError(E_REJECTED, "IC V2 data binding limit reached");
        }
        auto [binding, error] = CreateHostDataEndpoint(
            cellId,
            id,
            std::move(addresses),
            State->Peers,
            State->Timer,
            State->Scheduler,
            State->Completions,
            {options.GetMaxDataRequestBytes(),
             options.GetMaxPendingRequests(),
             options.GetMaxDataInflightBytes()});
        if (HasError(error)) {
            return error;
        }
        State->DataEndpoints.emplace(
            id,
            TServiceState::TDataBinding{
                cellId,
                binding.Endpoint,
                binding.Closed});
        return IBlockStorePtr(std::move(binding.Endpoint));
    }

    THostChannelStatus GetDataEndpointStatus(
        const IBlockStorePtr& endpoint) override
    {
        TString cellId;
        ui64 bindingId = 0;
        {
            std::lock_guard guard(State->Mutex);
            for (const auto& [id, binding]: State->DataEndpoints) {
                if (auto current = binding.Endpoint.lock();
                    current.get() == endpoint.get())
                {
                    cellId = binding.CellId;
                    bindingId = id;
                    break;
                }
            }
        }
        return bindingId ? State->Peers->GetHostStatus(cellId, bindingId)
                         : THostChannelStatus{
                               .State = EHostChannelState::Disconnected};
    }

    void Start() override
    {
        std::lock_guard lifecycle(LifecycleMutex);
        Y_ABORT_UNLESS(!Started && !Stopped);
        State->Completions->Start();
        State->DataExecutor->Start();
        State->Handshake->Start();
        State->Peers->Start();
        State->Pings->Start();
        *State->Listening = 1;
        Listener->Start();
        Started = true;
        {
            std::lock_guard guard(State->Mutex);
            State->Running = !State->Stopping;
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
        std::lock_guard lifecycle(LifecycleMutex);
        if (Stopped) {
            return;
        }
        RequestStop();
        Listener->Stop();
        State->Handshake->Stop();
        TVector<IRequestChannelPtr> channels;
        {
            std::lock_guard guard(State->Mutex);
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
            channel->WhenClosed().GetValueSync();
        }
        TVector<IDataEndpointPtr> endpoints;
        TVector<NThreading::TFuture<void>> bindings;
        {
            std::lock_guard guard(State->Mutex);
            for (const auto& [id, binding]: State->DataEndpoints) {
                if (auto endpoint = binding.Endpoint.lock()) {
                    endpoints.push_back(std::move(endpoint));
                }
                bindings.push_back(binding.Closed);
            }
        }
        // Endpoint reservations also cover synchronous write snapshots before
        // a request reaches the connection-waiting client. Retired bindings
        // retain their result barrier even after the endpoint is destroyed.
        for (const auto& endpoint: endpoints) {
            endpoint->Stop();
        }
        for (const auto& closed: bindings) {
            closed.GetValueSync();
        }
        State->DataTarget->Stop();
        State->DataExecutor->Stop();
        State->Pings->Stop();
        State->Completions->Stop();
        Stopped = true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IServicePtr> CreateService(
    NCells::TCellsConfigConstPtr config,
    IBlockStorePtr dataService,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    NMonitoring::TDynamicCountersPtr counters)
{
    if (!config || !dataService || !timer || !scheduler || !logging ||
        !counters)
    {
        return MakeError(
            E_ARGUMENT,
            "IC V2 service requires config, data service, timer, scheduler, "
            "logging and "
            "counters");
    }
    const auto& options = config->GetCellsConfig().GetIcV2();
    if (!config->GetCellsEnabled() || !options.GetEnabled() ||
        !options.HasPort() || options.GetPort() > 65535 ||
        !options.GetThreads() || !options.GetHandshakeTimeoutMs() ||
        !options.GetMaxConnections() || !options.GetMaxPendingRequests() ||
        !options.GetMaxOutputQueueBytes() || !options.GetCompletionThreads() ||
        !options.GetMaxOutgoingConnections() || !options.GetDataWorkers() ||
        !options.GetMaxDataRequestBytes() || !options.GetMaxDataInflightBytes())
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
    THandshakeConfig helloConfig{
        options.GetChecksumEvents(),
        options.GetEnableXdc()};
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
        {options.GetMaxDataRequestBytes(),
         options.GetMaxPendingRequests(),
         options.GetMaxDataInflightBytes()});
    if (HasError(targetError)) {
        return targetError;
    }
    state->DataTarget = std::move(target);
    state->Handshake = std::move(hello);
    const std::weak_ptr<TServiceState> weak = state;
    auto [listener, listenError] = NCloud::NIcV2::CreateListener(
        address,
        std::make_shared<TListenerClient>(weak));
    if (HasError(listenError)) {
        return listenError;
    }
    auto common = MakeIntrusive<NActors::TInterconnectProxyCommon>();
    common->MonCounters = counters;
    common->Settings.V2.Threads = options.GetThreads();
    common->Settings.V2.ChecksumEvents = options.GetChecksumEvents();
    common->Settings.V2.EnableSQPOLL = false;
    state->Transport = NCloud::NIcV2::CreateTransport(
        NActors::CreateUringEngine(std::move(common)),
        options.GetMaxOutputQueueBytes(),
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
    auto [peers, peersError] = CreatePeerConnections(
        config,
        std::move(connector),
        std::move(outgoingHello),
        state->Timer,
        state->Completions,
        [weak](const THandshakeReady& connection) -> TChannelResult
        {
            if (auto state = weak.lock()) {
                return state->MakeChannel(connection);
            }
            return MakeError(E_REJECTED, "IC V2 service has been destroyed");
        });
    if (HasError(peersError)) {
        return peersError;
    }
    state->Peers = std::move(peers);
    auto [pings, pingError] = CreatePingClient(
        {options.GetMaxPendingRequests(), options.GetMaxOutputQueueBytes()},
        state->Peers,
        state->Timer,
        state->Scheduler,
        state->Completions);
    if (HasError(pingError)) {
        return pingError;
    }
    state->Pings = std::move(pings);
    return IServicePtr(
        std::make_shared<TService>(std::move(state), std::move(listener)));
}

}   // namespace NCloud::NBlockStore::NIcV2
