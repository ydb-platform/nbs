#include "host_endpoint.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/icv2/data/protocol.h>
#include <cloud/blockstore/libs/icv2/data/target.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/icv2/handshake/handshake.h>
#include <cloud/storage/core/libs/icv2/tcp/testlib/socket_test.h>
#include <cloud/storage/core/libs/icv2/transport/transport.h>

#include <contrib/ydb/library/actors/interconnect/interconnect_common.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>
#include <contrib/ydb/library/actors/interconnect/uring_context.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/mutex.h>

#include <functional>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NThreading;

using NCloud::NIcV2::CreateHandshakeProvider;
using NCloud::NIcV2::CreatePeerManager;
using NCloud::NIcV2::CreateRequestChannel;
using NCloud::NIcV2::CreateTransport;
using NCloud::NIcV2::EPeerState;
using NCloud::NIcV2::IAddressResolver;
using NCloud::NIcV2::IChannelFactory;
using NCloud::NIcV2::IHandshakeProviderPtr;
using NCloud::NIcV2::IIncomingRequestPtr;
using NCloud::NIcV2::IPeerManagerPtr;
using NCloud::NIcV2::IRequestChannelHandler;
using NCloud::NIcV2::IRequestChannelHandlerPtr;
using NCloud::NIcV2::IRequestChannelPtr;
using NCloud::NIcV2::ITransportPtr;
using NCloud::NIcV2::TChannelResult;
using NCloud::NIcV2::THandshakeIncomplete;
using NCloud::NIcV2::THandshakeReady;
using NCloud::NIcV2::TRequestChannelConfig;
using NCloud::NIcV2::TResolveFuture;
using NCloud::NIcV2::TResolveResult;
using NCloud::NIcV2::NTests::CreateTestConnector;
using NCloud::NIcV2::NTests::TTestListener;

constexpr ui32 BlockSize = 4_KB;
constexpr ui64 MaxOutputQueueBytes = 64_MB;
constexpr TDuration WaitTimeout = TDuration::Seconds(10);

template <typename T>
T Wait(const TFuture<T>& future)
{
    UNIT_ASSERT(future.Wait(WaitTimeout));
    return future.GetValue();
}

ITransportPtr CreateTestTransport()
{
    auto common = MakeIntrusive<NActors::TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    common->Settings.V2.Threads = 1;
    common->Settings.V2.EnableSQPOLL = false;
    return CreateTransport(
        NActors::CreateUringEngine(std::move(common)),
        MaxOutputQueueBytes,
        {});
}

////////////////////////////////////////////////////////////////////////////////

class TLoopbackResolver final: public IAddressResolver
{
public:
    TResolveFuture
    Resolve(const TString& host, ui16 port, TMonotonic deadline) override
    {
        Y_UNUSED(deadline);

        TVector<NInterconnect::TAddress> addresses;
        addresses.emplace_back(host.c_str(), port);
        return MakeFuture<TResolveResult>(std::move(addresses));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestRequestHandler final: public IRequestChannelHandler
{
    const std::function<void(IIncomingRequestPtr)> Handler;

public:
    explicit TTestRequestHandler(
        std::function<void(IIncomingRequestPtr)> handler)
        : Handler(std::move(handler))
    {}

    void HandleRequest(IIncomingRequestPtr request) override
    {
        Handler(std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestChannelFactory final: public IChannelFactory
{
    const ITransportPtr Transport;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ITaskQueuePtr Completions;

public:
    TTestChannelFactory(
        ITransportPtr transport,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ITaskQueuePtr completions)
        : Transport(std::move(transport))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Completions(std::move(completions))
    {}

    TChannelResult CreateChannel(const THandshakeReady& connection) override
    {
        return CreateChannel(
            connection,
            std::make_shared<TTestRequestHandler>(
                [](IIncomingRequestPtr request) { Y_UNUSED(request); }));
    }

    [[nodiscard]] TChannelResult CreateChannel(
        const THandshakeReady& connection,
        IRequestChannelHandlerPtr handler) const
    {
        TRequestChannelConfig config;
        config.EventTypes.assign(DataEventTypes.begin(), DataEventTypes.end());
        return CreateRequestChannel(
            Transport,
            connection.Socket,
            connection.XdcSocket,
            config,
            Timer,
            Scheduler,
            Completions,
            std::move(handler));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TNetworkFixture
{
    const bool EnableXdc;
    TTestListener Listener;
    const ITimerPtr Timer = CreateWallClockTimer();
    const ISchedulerPtr Scheduler = CreateScheduler(Timer);
    const ITaskQueuePtr Completions = CreateThreadPool("ICV2HostTest", 2);
    const ITransportPtr Transport = CreateTestTransport();
    const std::shared_ptr<TTestChannelFactory> ChannelFactory =
        std::make_shared<TTestChannelFactory>(
            Transport,
            Timer,
            Scheduler,
            Completions);
    const std::shared_ptr<TTestService> Service =
        std::make_shared<TTestService>();
    TPromise<void> ReadInterrupted = NewPromise<void>();
    TPromise<ui32> ReadRetryNumber = NewPromise<ui32>();
    TMutex DataMutex;
    TString Data = TString(BlockSize, '\0');
    IDataTargetPtr Target;
    IHandshakeProviderPtr ServerHandshake;
    IPeerManagerPtr Peers;
    THostDataEndpoint Host;
    TVector<IRequestChannelPtr> ServerChannels;

    explicit TNetworkFixture(bool enableXdc)
        : EnableXdc(enableXdc)
    {
        UNIT_ASSERT(Transport);
        auto [target, targetError] = CreateDataTarget(Service, Completions, {});
        UNIT_ASSERT_C(!HasError(targetError), FormatError(targetError));
        Target = std::move(target);

        Service->WriteBlocksLocalHandler = [this](const auto& request)
        {
            TGuard<TMutex> lock(DataMutex);
            auto guard = request->Sglist.Acquire();
            if (!guard ||
                SgListCopy(guard.Get(), {Data.Detach(), Data.size()}) !=
                    BlockSize)
            {
                return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                    TErrorResponse(E_ARGUMENT, "invalid test write buffer"));
            }
            return MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };
        Service->ReadBlocksLocalHandler = [this](const auto& request)
        {
            TGuard<TMutex> lock(DataMutex);
            auto guard = request->Sglist.Acquire();
            if (!guard || SgListCopy({Data.data(), Data.size()}, guard.Get()) !=
                              BlockSize)
            {
                return MakeFuture<NProto::TReadBlocksLocalResponse>(
                    TErrorResponse(E_ARGUMENT, "invalid test read buffer"));
            }
            ReadRetryNumber.TrySetValue(request->GetHeaders().GetRetryNumber());
            return MakeFuture<NProto::TReadBlocksLocalResponse>();
        };

        auto [serverHandshake, serverError] =
            CreateHandshakeProvider({.EnableXdc = EnableXdc});
        UNIT_ASSERT_C(!HasError(serverError), FormatError(serverError));
        ServerHandshake = std::move(serverHandshake);
        auto [clientHandshake, clientError] =
            CreateHandshakeProvider({.EnableXdc = EnableXdc});
        UNIT_ASSERT_C(!HasError(clientError), FormatError(clientError));
        auto [peers, peersError] = CreatePeerManager(
            NCloud::NIcV2::TPeerManagerConfig{},
            CreateTestConnector(),
            std::move(clientHandshake),
            Timer,
            Scheduler,
            std::make_shared<TLoopbackResolver>(),
            Completions,
            ChannelFactory);
        UNIT_ASSERT_C(!HasError(peersError), FormatError(peersError));
        Peers = std::move(peers);

        Completions->Start();
        Scheduler->Start();
        Listener.Listener->Start();
        ServerHandshake->Start();
        Peers->Start();
    }

    ~TNetworkFixture()
    {
        if (auto endpoint = Host.GetEndpoint()) {
            const auto stopped = endpoint->RequestStop();
            Y_ABORT_UNLESS(
                stopped.Wait(WaitTimeout),
                "IC V2 test endpoint did not stop");
            stopped.GetValue();
        }
        for (const auto& channel: ServerChannels) {
            channel->Close();
        }
        Peers->Stop();
        ServerHandshake->Stop();
        Transport->Stop();
        for (const auto& channel: ServerChannels) {
            const auto closed = channel->WhenClosed();
            Y_ABORT_UNLESS(
                closed.Wait(WaitTimeout),
                "IC V2 test server channel did not close");
            closed.GetValue();
        }
        Target->Stop();
        Scheduler->Stop();
        Completions->Stop();
    }

    void ConnectHost()
    {
        auto [host, error] = CreateHostDataEndpoint(
            "127.0.0.1",
            Listener.Listener->GetAddress().GetPort(),
            {},
            Peers,
            {});
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Host = std::move(host);
    }

    void AcceptChannel()
    {
        auto hello = ServerHandshake->Accept(Listener.Accept(), WaitTimeout);
        if (EnableXdc) {
            const auto auxiliary =
                Wait(ServerHandshake->Accept(Listener.Accept(), WaitTimeout));
            UNIT_ASSERT_C(
                !HasError(auxiliary),
                FormatError(auxiliary.GetError()));
            UNIT_ASSERT(
                std::holds_alternative<THandshakeIncomplete>(
                    auxiliary.GetResult()));
        }
        const auto accepted = Wait(hello);
        UNIT_ASSERT_C(!HasError(accepted), FormatError(accepted.GetError()));
        auto [channel, error] = ChannelFactory->CreateChannel(
            std::get<THandshakeReady>(accepted.GetResult()),
            std::make_shared<TTestRequestHandler>(
                [this](IIncomingRequestPtr request)
                {
                    if (request->GetEvent().Type ==
                            NStorage::TEvService::EvReadBlocksRequest &&
                        ReadInterrupted.TrySetValue())
                    {
                        request->CloseChannel();
                        return;
                    }
                    Target->HandleRequest(std::move(request));
                }));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        ServerChannels.push_back(std::move(channel));
        UNIT_ASSERT(ServerChannels.back()->Activate());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
std::shared_ptr<TRequest> MakeRequest()
{
    auto request = std::make_shared<TRequest>();
    request->SetDiskId("disk");
    request->SetBlockSize(BlockSize);
    request->MutableHeaders()->SetRequestTimeout(WaitTimeout.MilliSeconds());
    return request;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIcV2HostDataEndpointNetworkTest)
{
    Y_UNIT_TEST(DurableReadRetriesAfterTcpDisconnect)
    {
        if (!NActors::TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        for (bool enableXdc: {false, true}) {
            TNetworkFixture f(enableXdc);
            f.ConnectHost();
            f.AcceptChannel();
            auto config = std::make_shared<NClient::TClientAppConfig>();
            auto durable = NClient::CreateDurableClient(
                config,
                f.Host.GetEndpoint(),
                NClient::CreateRetryPolicy(config, NProto::STORAGE_MEDIA_SSD),
                CreateLoggingService("console"),
                f.Timer,
                f.Scheduler,
                CreateRequestStatsStub(),
                CreateVolumeStatsStub());

            const TString payload(BlockSize, 'w');
            auto write = MakeRequest<NProto::TWriteBlocksRequest>();
            write->MutableBlocks()->AddBuffers(payload);
            auto written = Wait(durable->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(write)));
            UNIT_ASSERT_C(!HasError(written), FormatError(written.GetError()));
            UNIT_ASSERT(f.Host.GetStatus().State == EPeerState::Connected);

            auto read = MakeRequest<NProto::TReadBlocksRequest>();
            read->SetBlocksCount(1);
            auto pending = durable->ReadBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(read));
            UNIT_ASSERT(f.ReadInterrupted.GetFuture().Wait(WaitTimeout));
            f.AcceptChannel();

            const auto response = Wait(pending);
            UNIT_ASSERT_C(
                !HasError(response),
                FormatError(response.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(1, response.GetBlocks().BuffersSize());
            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                response.GetBlocks().GetBuffers(0));
            UNIT_ASSERT_GT(Wait(f.ReadRetryNumber.GetFuture()), 0);
        }
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
