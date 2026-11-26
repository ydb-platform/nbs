#include "service.h"

#include "public.h"

#include "service_private.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/icv2/handshake/handshake.h>
#include <cloud/storage/core/libs/icv2/peer/peer_manager.h>
#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>
#include <cloud/storage/core/libs/icv2/tcp/connector.h>
#include <cloud/storage/core/libs/icv2/tcp/listener.h>
#include <cloud/storage/core/libs/icv2/tcp/testlib/socket_test.h>

#include <contrib/ydb/library/actors/dnsresolver/dnsresolver.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_common.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>
#include <contrib/ydb/library/actors/interconnect/uring_context.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/system/mutex.h>

#include <poll.h>

#include <atomic>
#include <cerrno>
#include <future>
#include <thread>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::CreatePeerManager;
using NCloud::NIcV2::EPeerState;
using NCloud::NIcV2::IChannelFactory;
using NCloud::NIcV2::IPeerManagerPtr;
using NCloud::NIcV2::TChannelFuture;
using NCloud::NIcV2::TChannelResult;

using NCloud::NIcV2::IConnectorPtr;
using NCloud::NIcV2::TSocketResult;

using NCloud::NIcV2::CreateRequestChannel;
using NCloud::NIcV2::IIncomingRequestPtr;
using NCloud::NIcV2::IRequestChannel;
using NCloud::NIcV2::IRequestChannelHandler;
using NCloud::NIcV2::IRequestChannelPtr;

using NCloud::NIcV2::CreateHandshakeProvider;
using NCloud::NIcV2::IHandshakeProviderPtr;
using NCloud::NIcV2::TConnectHandshakeResult;
using NCloud::NIcV2::THandshakeReady;

using NCloud::NIcV2::ITransportPtr;

using NCloud::NIcV2::TSocketPtr;

using NCloud::NIcV2::CreateListener;
using NCloud::NIcV2::NTests::TTestListenerClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUnexpectedRequestHandler final: public IRequestChannelHandler
{
public:
    void HandleRequest(IIncomingRequestPtr) override
    {
        UNIT_FAIL("client channel must not receive requests");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestChannelFactory final: public IChannelFactory
{
private:
    const ITransportPtr Transport;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;

public:
    TTestChannelFactory(
        ITransportPtr transport,
        ITimerPtr timer,
        ISchedulerPtr scheduler)
        : Transport(std::move(transport))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
    {}

    TChannelResult CreateChannel(const THandshakeReady& connection) override
    {
        return CreateRequestChannel(
            Transport,
            connection.Socket,
            {},
            {{{NStorage::TEvService::EvZeroBlocksRequest,
               NStorage::TEvService::EvZeroBlocksResponse}},
             false},
            Timer,
            Scheduler,
            CreateTaskQueueStub(),
            std::make_shared<TUnexpectedRequestHandler>());
    }
};

using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

class TListenerErrorInjector final
    : public NCloud::NIcV2::NPrivate::IListenerErrorInjector
{
public:
    std::atomic<int> AcceptError = 0;

    // IListenerErrorInjector implementation
    int BeforeAccept() override
    {
        return AcceptError.exchange(0);
    }

    int BeforePrepareSocket() override
    {
        return 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 TestMaxOutputQueueBytes = 64 * 1024 * 1024;

void WaitCounter(
    const NMonitoring::TDynamicCounters::TCounterPtr& counter,
    i64 value)
{
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (counter->Val() != value &&
           std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(1ms);
    }
    UNIT_ASSERT_VALUES_EQUAL(counter->Val(), value);
}

NProto::TCellsConfig MakeConfig(bool checksums = false)
{
    NProto::TCellsConfig config;
    config.SetCellsEnabled(true);
    config.SetCellId("cell-2");
    config.AddCells()->SetCellId("cell-1");
    auto& icv2 = *config.MutableIcV2();
    icv2.SetEnabled(true);
    icv2.SetListenAddress("127.0.0.1");
    icv2.SetPort(0);
    icv2.SetChecksumEvents(checksums);
    return config;
}

struct TFixture
{
    NActors::TTestActorRuntimeBase Runtime{1, true};
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>();
    ILoggingServicePtr Logging =
        CreateLoggingService("console", TLogSettings{});
    NMonitoring::TDynamicCountersPtr Counters =
        MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<TTestService> DataService =
        std::make_shared<TTestService>();
    ITaskQueuePtr DnsCompletions = CreateThreadPool("TestDns", 1);
    IServicePtr Service;
    IConnectorPtr Connector = NCloud::NIcV2::NTests::CreateTestConnector();
    IHandshakeProviderPtr Hello;
    ITransportPtr Transport;
    IRequestChannelPtr Channel;
    IPeerManagerPtr Peers;

    TFixture()
    {
        DataService->ZeroBlocksHandler = [](const auto&)
        {
            return NThreading::MakeFuture<NProto::TZeroBlocksResponse>();
        };
        DnsCompletions->Start();
        Runtime.Initialize();
        Runtime.RegisterService(
            NActors::NDnsResolver::MakeDnsResolverActorId(),
            Runtime.Register(
                NActors::NDnsResolver::CreateOnDemandDnsResolver()));
    }

    ~TFixture()
    {
        if (Peers) {
            Peers->Stop();
        }
        if (Channel) {
            Channel->Close();
        }
        if (Transport) {
            Transport->Stop();
        }
        Connector->Stop();
        if (Hello) {
            Hello->Stop();
        }
        if (Service) {
            Service->Stop();
        }
        DnsCompletions->Stop();
    }

    TResultOrError<IServicePtr> Create(NProto::TCellsConfig config)
    {
        return CreateService(
            std::make_shared<NCells::TCellsConfig>(std::move(config)),
            DataService,
            NCloud::NIcV2::CreateAddressResolver(
                Runtime.GetActorSystem(0),
                DnsCompletions),
            Timer,
            Scheduler,
            Logging,
            Counters);
    }

    void Start(bool checksums = false)
    {
        auto [service, error] = Create(MakeConfig(checksums));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Service = std::move(service);
        Service->Start();
        Connector->Start();
    }

    TSocketPtr Connect()
    {
        auto future =
            Connector->Connect(Service->GetAddress(), TDuration::Seconds(5));
        UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
        auto result = future.GetValue();
        UNIT_ASSERT_C(
            !HasError(result.GetError()),
            FormatError(result.GetError()));
        return result.ExtractResult();
    }

    TConnectHandshakeResult Handshake(TSocketPtr socket)
    {
        if (Hello) {
            Hello->Stop();
        }
        auto [hello, error] = CreateHandshakeProvider({});
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Hello = std::move(hello);
        Hello->Start();
        auto future = Hello->Connect(std::move(socket), TDuration::Seconds(5));
        UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
        return future.GetValue();
    }

    void ConnectChannel(bool checksums)
    {
        auto [connection, error] = Handshake(Connect());
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        auto common = MakeIntrusive<NActors::TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        common->Settings.V2.Threads = 1;
        common->Settings.V2.EnableSQPOLL = false;
        common->Settings.V2.ChecksumEvents = checksums;
        Transport = NCloud::NIcV2::CreateTransport(
            NActors::CreateUringEngine(std::move(common)),
            TestMaxOutputQueueBytes,
            {});
        UNIT_ASSERT(Transport);
        auto [channel, channelError] = CreateRequestChannel(
            Transport,
            std::move(connection.Socket),
            {},
            {{{NStorage::TEvService::EvZeroBlocksRequest,
               NStorage::TEvService::EvZeroBlocksResponse}},
             false},
            Timer,
            Scheduler,
            CreateTaskQueueStub(),
            std::make_shared<TUnexpectedRequestHandler>());
        UNIT_ASSERT_C(!HasError(channelError), FormatError(channelError));
        Channel = std::move(channel);
        UNIT_ASSERT(Channel->Activate());
    }
};

void StartPair(
    TFixture& first,
    TFixture& second,
    bool checksums,
    bool startSecond = true,
    bool firstXdc = false,
    bool secondXdc = false,
    ui32 maxConnections = 128,
    bool differentChecksums = false)
{
    auto [reserved, reserveError] = CreateListener(
        NInterconnect::TAddress("127.0.0.1", 0),
        std::make_shared<TTestListenerClient>([](TSocketResult) {}));
    UNIT_ASSERT_C(!HasError(reserveError), FormatError(reserveError));
    const auto secondPort = reserved->GetAddress().GetPort();
    auto firstConfig = MakeConfig(checksums);
    firstConfig.SetCellId("cell-1");
    firstConfig.MutableIcV2()->SetEnableXdc(firstXdc);
    firstConfig.MutableIcV2()->SetMaxIncomingConnections(maxConnections);
    auto& firstPeer = *firstConfig.MutableCells(0);
    firstPeer.SetCellId("cell-2");
    firstPeer.SetIcV2Port(secondPort);
    firstPeer.AddHosts()->SetFqdn("localhost");
    auto [firstService, firstError] = first.Create(std::move(firstConfig));
    UNIT_ASSERT_C(!HasError(firstError), FormatError(firstError));
    first.Service = std::move(firstService);
    auto secondConfig = MakeConfig(differentChecksums ? !checksums : checksums);
    secondConfig.MutableIcV2()->SetPort(secondPort);
    secondConfig.MutableIcV2()->SetEnableXdc(secondXdc);
    secondConfig.MutableIcV2()->SetMaxIncomingConnections(maxConnections);
    auto& secondPeer = *secondConfig.MutableCells(0);
    secondPeer.SetIcV2Port(first.Service->GetAddress().GetPort());
    secondPeer.AddHosts()->SetFqdn("127.0.0.1");
    reserved->Stop();
    auto [secondService, secondError] = second.Create(std::move(secondConfig));
    UNIT_ASSERT_C(!HasError(secondError), FormatError(secondError));
    second.Service = std::move(secondService);
    first.Service->Start();
    if (startSecond) {
        second.Service->Start();
    }
}

bool IsAvailable()
{
    if (!NActors::TUringContext::IsAvailable()) {
        Cerr << "io_uring not available; skipping" << Endl;
        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TDataConnectionHandler final: public IClientEndpointHandler
{
public:
    NThreading::TPromise<void> Connected = NThreading::NewPromise();

    void HandleConnected() noexcept override
    {
        Connected.TrySetValue();
    }

    void HandleDisconnected() noexcept override
    {}

    void HandleUnavailable() noexcept override
    {}
};

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2ServiceTest)
{
    Y_UNIT_TEST(ZeroBlocksWithOptionalXdcAndDifferentChecksums)
    {
        if (!IsAvailable()) {
            return;
        }
        for (bool firstXdc: {false, true}) {
            for (bool secondXdc: {false, true}) {
                TFixture first;
                TFixture second;
                StartPair(
                    first,
                    second,
                    false,
                    true,
                    firstXdc,
                    secondXdc,
                    1,
                    true);
                TVector<IBlockStorePtr> endpoints;
                for (const auto& [service, peer]:
                     {std::pair{first.Service, TString("cell-2")},
                      std::pair{second.Service, TString("cell-1")}})
                {
                    auto handler = std::make_shared<TDataConnectionHandler>();
                    auto [endpoint, error] =
                        service->CreateDataEndpoint(peer, "127.0.0.1", handler);
                    UNIT_ASSERT_C(!HasError(error), FormatError(error));
                    UNIT_ASSERT(handler->Connected.GetFuture().Wait(
                        TDuration::Seconds(5)));

                    auto request =
                        std::make_shared<NProto::TZeroBlocksRequest>();
                    request->SetDiskId("disk");
                    request->SetBlocksCount(1);
                    request->MutableHeaders()->SetRequestTimeout(5000);
                    auto future = endpoint->ZeroBlocks(
                        MakeIntrusive<TCallContext>(),
                        std::move(request));
                    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
                    const auto& result = future.GetValue();
                    UNIT_ASSERT_C(
                        !HasError(result.GetError()),
                        FormatError(result.GetError()));
                    // Keep the connection open while checking admission below.
                    endpoints.push_back(std::move(endpoint));
                }
                if (secondXdc) {
                    first.Connector->Start();
                    auto extra = first.Connector->Connect(
                        second.Service->GetAddress(),
                        TDuration::Seconds(5));
                    UNIT_ASSERT(extra.Wait(TDuration::Seconds(5)));
                    UNIT_ASSERT_C(
                        !HasError(extra.GetValue().GetError()),
                        FormatError(extra.GetValue().GetError()));
                    auto hello = first.Handshake(extra.GetValue().GetResult());
                    UNIT_ASSERT_C(
                        !HasError(hello.GetError()),
                        FormatError(hello.GetError()));
                    WaitCounter(
                        second.Counters->GetCounter(
                            "RejectedConnections",
                            true),
                        1);
                    WaitCounter(
                        second.Counters->GetCounter("ActiveConnections", false),
                        1);
                }
                first.Service->Stop();
                second.Service->Stop();
                WaitCounter(
                    first.Counters->GetCounter("ActiveConnections", false),
                    0);
            }
        }
    }

    Y_UNIT_TEST(DataBindingsExchangeBlocksWithBothChecksumModes)
    {
        if (!IsAvailable()) {
            return;
        }
        for (bool checksums: {false, true}) {
            TMutex storedMutex;
            TString stored(8192, 'x');
            TFixture first;
            TFixture second;
            second.DataService->WriteBlocksLocalHandler = [&](auto request)
            {
                UNIT_ASSERT_VALUES_EQUAL(request->GetDiskId(), "disk");
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);

                TGuard<TMutex> lock(storedMutex);
                SgListCopy(guard.Get(), {stored.Detach(), stored.size()});
                return NThreading::MakeFuture<
                    NProto::TWriteBlocksLocalResponse>();
            };
            second.DataService->ReadBlocksLocalHandler = [&](auto request)
            {
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);

                TGuard<TMutex> lock(storedMutex);
                SgListCopy({stored.data(), stored.size()}, guard.Get());
                return NThreading::MakeFuture<
                    NProto::TReadBlocksLocalResponse>();
            };
            second.DataService->ZeroBlocksHandler = [&](auto request)
            {
                UNIT_ASSERT_VALUES_EQUAL(request->GetBlocksCount(), 2);

                TGuard<TMutex> lock(storedMutex);
                stored = TString(8192, '\0');
                return NThreading::MakeFuture<NProto::TZeroBlocksResponse>();
            };
            StartPair(first, second, checksums);
            IDataEndpointFactoryPtr factory = first.Service;
            auto handler = std::make_shared<TDataConnectionHandler>();
            auto [endpoint, error] =
                factory->CreateDataEndpoint("cell-2", "127.0.0.1", handler);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            UNIT_ASSERT(
                handler->Connected.GetFuture().Wait(TDuration::Seconds(5)));
            TString data(8192, 'w');
            auto write = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            write->SetDiskId("disk");
            write->SetBlockSize(4096);
            write->BlocksCount = 2;
            write->MutableHeaders()->SetRequestTimeout(5000);
            write->Sglist = TGuardedSgList({{data.data(), data.size()}});
            auto written = endpoint->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                write);
            UNIT_ASSERT(written.Wait(TDuration::Seconds(5)));
            UNIT_ASSERT_C(
                !HasError(written.GetValue()),
                FormatError(written.GetValue().GetError()));
            {
                TGuard<TMutex> lock(storedMutex);
                UNIT_ASSERT_VALUES_EQUAL(stored, data);
            }
            auto read = std::make_shared<NProto::TReadBlocksLocalRequest>();
            read->SetDiskId("disk");
            read->SetBlockSize(4096);
            read->SetBlocksCount(2);
            read->MutableHeaders()->SetRequestTimeout(5000);
            TString received(8192, 'r');
            read->Sglist =
                TGuardedSgList({{received.Detach(), received.size()}});
            Y_DEFER
            {
                read->Sglist.Close();
            };
            auto response =
                endpoint->ReadBlocksLocal(MakeIntrusive<TCallContext>(), read);
            UNIT_ASSERT(response.Wait(TDuration::Seconds(5)));
            UNIT_ASSERT_C(
                !HasError(response.GetValue()),
                FormatError(response.GetValue().GetError()));
            UNIT_ASSERT_VALUES_EQUAL(received, data);
            auto zero = std::make_shared<NProto::TZeroBlocksRequest>();
            zero->SetDiskId("disk");
            zero->SetBlocksCount(2);
            zero->MutableHeaders()->SetRequestTimeout(5000);
            auto zeroed =
                endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), zero);
            UNIT_ASSERT(zeroed.Wait(TDuration::Seconds(5)));
            UNIT_ASSERT_C(
                !HasError(zeroed.GetValue()),
                FormatError(zeroed.GetValue().GetError()));
            {
                TGuard<TMutex> lock(storedMutex);
                UNIT_ASSERT_VALUES_EQUAL(stored, TString(8192, '\0'));
            }
            UNIT_ASSERT(
                factory->GetDataEndpointStatus(endpoint).State ==
                EPeerState::Connected);
            first.Service->Stop();
            auto rejected =
                endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), zero);
            UNIT_ASSERT(rejected.Wait(TDuration::Seconds(5)));
            UNIT_ASSERT(HasError(rejected.GetValue()));
        }
    }
    Y_UNIT_TEST(ConnectsDataEndpointBeforeAnyIoAndKeepsItAlive)
    {
        if (!IsAvailable()) {
            return;
        }

        TFixture first;
        TFixture second;
        StartPair(first, second, false);
        auto handler = std::make_shared<TDataConnectionHandler>();
        IDataEndpointFactoryPtr factory = first.Service;
        auto [endpoint, error] =
            factory->CreateDataEndpoint("cell-2", "127.0.0.1", handler);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT(handler->Connected.GetFuture().Wait(TDuration::Seconds(5)));
        const auto status = factory->GetDataEndpointStatus(endpoint);
        UNIT_ASSERT(status.State == EPeerState::Connected);
        UNIT_ASSERT_VALUES_EQUAL(1, status.ConnectionAttempts);

        // Exceed the engine's default 10s idle timeout before sending I/O.
        std::this_thread::sleep_for(12s);
        auto zero = std::make_shared<NProto::TZeroBlocksRequest>();
        zero->SetDiskId("disk");
        zero->SetBlocksCount(1);
        zero->MutableHeaders()->SetRequestTimeout(5000);
        auto reply = endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), zero);
        UNIT_ASSERT(reply.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_C(
            !HasError(reply.GetValue()),
            FormatError(reply.GetValue().GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            factory->GetDataEndpointStatus(endpoint).ConnectionAttempts);
        endpoint->Stop();
    }

    Y_UNIT_TEST(ReleasedEndpointStillDrainsItsResultCallbackDuringServiceStop)
    {
        if (!IsAvailable()) {
            return;
        }

        TFixture first;
        TFixture second;
        auto storageEntered = NThreading::NewPromise();
        auto storageDone =
            NThreading::NewPromise<NProto::TZeroBlocksResponse>();
        second.DataService->ZeroBlocksHandler =
            [storageEntered, storageDone](auto request) mutable
        {
            Y_UNUSED(request);

            storageEntered.SetValue();
            return storageDone.GetFuture();
        };
        StartPair(first, second, false);
        auto handler = std::make_shared<TDataConnectionHandler>();
        auto [endpoint, error] =
            first.Service->CreateDataEndpoint("cell-2", "127.0.0.1", handler);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT(handler->Connected.GetFuture().Wait(TDuration::Seconds(5)));
        const std::weak_ptr<IBlockStore> weak = endpoint;
        auto zero = std::make_shared<NProto::TZeroBlocksRequest>();
        zero->SetDiskId("disk");
        zero->SetBlocksCount(1);
        zero->MutableHeaders()->SetRequestTimeout(5000);
        auto response =
            endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), zero);
        auto subscribed = NThreading::NewPromise();
        auto released = NThreading::NewPromise();
        auto release = NThreading::NewPromise();
        auto stopEntered = NThreading::NewPromise();
        std::future<void> subscribing;
        std::future<void> completing;
        std::future<void> stopping;
        Y_DEFER
        {
            release.TrySetValue();
            storageDone.TrySetValue(NProto::TZeroBlocksResponse{});
        };

        UNIT_ASSERT(storageEntered.GetFuture().Wait(TDuration::Seconds(5)));
        subscribing = std::async(
            std::launch::async,
            [endpoint = std::move(endpoint),
             response,
             subscribed,
             released,
             release]() mutable
            {
                response.Subscribe(
                    [endpoint = std::move(endpoint), released, release](
                        const auto& completed) mutable
                    {
                        Y_UNUSED(completed);

                        endpoint.reset();
                        released.SetValue();
                        release.GetFuture().GetValueSync();
                    });
                subscribed.SetValue();
            });
        UNIT_ASSERT(subscribed.GetFuture().Wait(TDuration::Seconds(5)));
        subscribing.get();
        completing = std::async(
            std::launch::async,
            [storageDone]() mutable
            { storageDone.SetValue(NProto::TZeroBlocksResponse{}); });
        UNIT_ASSERT(released.GetFuture().Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(weak.expired());
        stopping = std::async(
            std::launch::async,
            [service = first.Service, stopEntered]() mutable
            {
                stopEntered.SetValue();
                service->Stop();
            });
        UNIT_ASSERT(stopEntered.GetFuture().Wait(TDuration::Seconds(5)));
        const auto status = stopping.wait_for(50ms);
        release.SetValue();
        UNIT_ASSERT(status == std::future_status::timeout);
        UNIT_ASSERT(stopping.wait_for(5s) == std::future_status::ready);
        stopping.get();
        completing.get();
        UNIT_ASSERT(response.Wait(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(StopWaitsForAdmittedStorageOperation)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture first;
        TFixture second;
        auto entered = NThreading::NewPromise<void>();
        auto completed = NThreading::NewPromise<NProto::TZeroBlocksResponse>();
        second.DataService->ZeroBlocksHandler = [&](auto)
        {
            entered.SetValue();
            return completed.GetFuture();
        };
        StartPair(first, second, false);
        auto handler = std::make_shared<TDataConnectionHandler>();
        auto [endpoint, error] =
            first.Service->CreateDataEndpoint("cell-2", "127.0.0.1", handler);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT(handler->Connected.GetFuture().Wait(TDuration::Seconds(5)));
        auto zero = std::make_shared<NProto::TZeroBlocksRequest>();
        zero->SetDiskId("disk");
        zero->SetBlocksCount(1);
        zero->MutableHeaders()->SetRequestTimeout(5000);
        auto response =
            endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), zero);
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));
        auto stopped =
            std::async(std::launch::async, [&] { second.Service->Stop(); });
        const auto status = stopped.wait_for(50ms);
        completed.SetValue({});
        UNIT_ASSERT(status == std::future_status::timeout);
        UNIT_ASSERT(stopped.wait_for(5s) == std::future_status::ready);
        stopped.get();
        UNIT_ASSERT(response.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(HasError(response.GetValue()));
    }

    Y_UNIT_TEST(DataRequestBeforeConnectionFailsWithoutSending)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture first;
        TFixture second;
        std::atomic<size_t> calls = 0;
        second.DataService->ZeroBlocksHandler = [&](auto)
        {
            ++calls;
            return NThreading::MakeFuture<NProto::TZeroBlocksResponse>();
        };
        StartPair(first, second, false, false);
        auto handler = std::make_shared<TDataConnectionHandler>();
        auto [endpoint, error] =
            first.Service->CreateDataEndpoint("cell-2", "127.0.0.1", handler);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        auto zero = std::make_shared<NProto::TZeroBlocksRequest>();
        zero->SetDiskId("disk");
        zero->SetBlocksCount(1);
        zero->MutableHeaders()->SetRequestTimeout(5000);
        auto unavailable =
            endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), zero);
        UNIT_ASSERT(unavailable.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            E_UNAVAILABLE,
            unavailable.GetValue().GetError().GetCode());
        second.Service->Start();
        UNIT_ASSERT(handler->Connected.GetFuture().Wait(TDuration::Seconds(5)));
        auto next = endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), zero);
        UNIT_ASSERT(next.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_C(
            !HasError(next.GetValue()),
            FormatError(next.GetValue().GetError()));
        UNIT_ASSERT_VALUES_EQUAL(calls.load(), 1);
    }

    Y_UNIT_TEST(DataBindingLimitRecoversAfterClosedEndpointIsReleased)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture fixture;
        auto config = MakeConfig();
        config.MutableIcV2()->SetMaxOutgoingConnections(1);
        auto& peer = *config.MutableCells(0);
        peer.SetIcV2Port(1234);
        peer.AddHosts()->SetFqdn("127.0.0.1");
        auto [service, serviceError] = fixture.Create(std::move(config));
        UNIT_ASSERT_C(!HasError(serviceError), FormatError(serviceError));
        fixture.Service = std::move(service);
        fixture.Service->Start();
        auto [endpoint, error] =
            fixture.Service->CreateDataEndpoint("cell-1", "127.0.0.1", {});
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        endpoint->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Service->CreateDataEndpoint("cell-1", "127.0.0.1", {})
                .GetError()
                .GetCode(),
            E_REJECTED);
        endpoint.reset();
        auto next =
            fixture.Service->CreateDataEndpoint("cell-1", "127.0.0.1", {});
        UNIT_ASSERT_C(!HasError(next.GetError()), FormatError(next.GetError()));
    }

    Y_UNIT_TEST(OutgoingCacheExchangesMessagesAndReconnectsToRestartedService)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture fixture;
        fixture.Start();
        auto common = MakeIntrusive<NActors::TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        common->Settings.V2.Threads = 1;
        common->Settings.V2.EnableSQPOLL = false;
        fixture.Transport = NCloud::NIcV2::CreateTransport(
            NActors::CreateUringEngine(std::move(common)),
            TestMaxOutputQueueBytes,
            {});
        auto [hello, helloError] = CreateHandshakeProvider({false});
        UNIT_ASSERT_C(!HasError(helloError), FormatError(helloError));
        auto [peers, error] = CreatePeerManager(
            {.ConnectTimeout = TDuration::Seconds(5)},
            NCloud::NIcV2::NTests::CreateTestConnector(),
            std::move(hello),
            fixture.Timer,
            fixture.Scheduler,
            NCloud::NIcV2::CreateAddressResolver(
                fixture.Runtime.GetActorSystem(0),
                CreateTaskQueueStub()),
            CreateTaskQueueStub(),
            std::make_shared<TTestChannelFactory>(
                fixture.Transport,
                fixture.Timer,
                fixture.Scheduler));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        fixture.Peers = std::move(peers);
        fixture.Peers->Start();
        const auto [peer, createError] = fixture.Peers->CreatePeer(
            "localhost",
            fixture.Service->GetAddress().GetPort(),
            {});
        UNIT_ASSERT_C(!HasError(createError), FormatError(createError));
        TVector<TChannelFuture> connections;
        for (size_t i = 0; i < 32; ++i) {
            connections.push_back(peer->GetChannel());
        }
        UNIT_ASSERT(connections[0].Wait(TDuration::Seconds(5)));
        auto [first, connectError] = connections[0].GetValue();
        UNIT_ASSERT_C(!HasError(connectError), FormatError(connectError));
        for (const auto& connection: connections) {
            UNIT_ASSERT(connection.Wait(TDuration::Seconds(5)));
            UNIT_ASSERT(connection.GetValue().GetResult() == first);
        }
        auto zeroBlocks = [](const IRequestChannelPtr& channel)
        {
            auto request =
                std::make_unique<NStorage::TEvService::TEvZeroBlocksRequest>();
            request->Record.SetDiskId("disk");
            request->Record.SetBlocksCount(1);
            auto result =
                channel->Request(std::move(request), TDuration::Seconds(5));
            UNIT_ASSERT(result.Wait(TDuration::Seconds(5)));
            auto [event, error] = result.ExtractValue();
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            const auto& response =
                event->Get<NStorage::TEvService::TEvZeroBlocksResponse>()
                    ->Record;
            UNIT_ASSERT_C(
                !HasError(response),
                FormatError(response.GetError()));
        };
        zeroBlocks(first);
        WaitCounter(
            fixture.Counters->GetCounter("AcceptedConnections", true),
            1);
        const auto port = fixture.Service->GetAddress().GetPort();
        fixture.Service->Stop();
        UNIT_ASSERT(first->WhenClosed().Wait(TDuration::Seconds(5)));
        auto failed = peer->GetChannel();
        UNIT_ASSERT(failed.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(HasError(failed.GetValue().GetError()));
        auto restarted = MakeConfig();
        restarted.MutableIcV2()->SetPort(port);
        auto [service, serviceError] = fixture.Create(std::move(restarted));
        UNIT_ASSERT_C(!HasError(serviceError), FormatError(serviceError));
        fixture.Service = std::move(service);
        fixture.Service->Start();
        TChannelResult connected = MakeError(E_UNAVAILABLE);
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        do {
            auto future = peer->GetChannel();
            UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
            connected = future.GetValue();
            if (!HasError(connected.GetError())) {
                break;
            }
            fixture.Timer->AdvanceTime(TDuration::MilliSeconds(10));
            fixture.Scheduler->AdvanceTime(TDuration::MilliSeconds(10));
            fixture.Scheduler->RunAllScheduledTasksUntilNow();
            std::this_thread::sleep_for(10ms);
        } while (std::chrono::steady_clock::now() < deadline);
        auto [second, reconnectError] = connected;
        UNIT_ASSERT_C(!HasError(reconnectError), FormatError(reconnectError));
        UNIT_ASSERT(second != first);
        zeroBlocks(second);
        WaitCounter(
            fixture.Counters->GetCounter("AcceptedConnections", true),
            2);
    }

    Y_UNIT_TEST(AcceptsHelloWithoutCellIdentity)
    {
        if (!IsAvailable()) {
            return;
        }

        TFixture fixture;
        fixture.Start();
        auto result = fixture.Handshake(fixture.Connect());
        UNIT_ASSERT_C(
            !HasError(result.GetError()),
            FormatError(result.GetError()));
        WaitCounter(
            fixture.Counters->GetCounter("ActiveConnections", false),
            1);
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Counters->GetCounter("HandshakeErrors", true)->Val(),
            0);
        fixture.Service->Stop();
    }

    Y_UNIT_TEST(StopCancelsSilentHelloWithoutWaitingForDeadline)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture fixture;
        auto config = MakeConfig();
        config.MutableIcV2()->SetHandshakeTimeout(30000);
        auto [service, error] = fixture.Create(std::move(config));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        fixture.Service = std::move(service);
        fixture.Service->Start();
        fixture.Connector->Start();
        auto socket = fixture.Connect();
        WaitCounter(
            fixture.Counters->GetCounter("AcceptedConnections", true),
            1);
        auto first =
            std::async(std::launch::async, [&] { fixture.Service->Stop(); });
        auto second =
            std::async(std::launch::async, [&] { fixture.Service->Stop(); });
        UNIT_ASSERT(first.wait_for(5s) == std::future_status::ready);
        UNIT_ASSERT(second.wait_for(5s) == std::future_status::ready);
        first.get();
        second.get();
        pollfd readable{*socket, POLLIN, 0};
        UNIT_ASSERT_VALUES_EQUAL(poll(&readable, 1, 1000), 1);
        char byte;
        UNIT_ASSERT(recv(*socket, &byte, 1, 0) <= 0);
    }

    Y_UNIT_TEST(StopBeforeStartReleasesPort)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture fixture;
        auto config = MakeConfig();
        config.MutableCells(0)->SetIcV2Port(1234);
        auto [service, error] = fixture.Create(std::move(config));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        const auto address = service->GetAddress();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            service->CreateDataEndpoint("cell-1", "127.0.0.1", {})
                .GetError()
                .GetCode());
        service->Stop();
        service->Stop();
        auto [listener, listenError] = CreateListener(
            address,
            std::make_shared<TTestListenerClient>([](TSocketResult) {}));
        UNIT_ASSERT_C(!HasError(listenError), FormatError(listenError));
        listener->Stop();
    }

    Y_UNIT_TEST(ConnectionLimitIncludesSilentHelloAndActiveChannels)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture fixture;
        auto config = MakeConfig();
        config.MutableIcV2()->SetMaxIncomingConnections(1);
        auto [service, error] = fixture.Create(std::move(config));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        fixture.Service = std::move(service);
        fixture.Service->Start();
        fixture.Connector->Start();
        auto silent = fixture.Connect();
        WaitCounter(
            fixture.Counters->GetCounter("AcceptedConnections", true),
            1);
        auto rejected = fixture.Connect();
        WaitCounter(
            fixture.Counters->GetCounter("RejectedConnections", true),
            1);
        silent.Reset();
        WaitCounter(fixture.Counters->GetCounter("HandshakeErrors", true), 1);
        fixture.ConnectChannel(false);
        WaitCounter(
            fixture.Counters->GetCounter("ActiveConnections", false),
            1);
        rejected = fixture.Connect();
        WaitCounter(
            fixture.Counters->GetCounter("RejectedConnections", true),
            2);
        fixture.Channel->Close();
        WaitCounter(
            fixture.Counters->GetCounter("ActiveConnections", false),
            0);
        fixture.Transport->Stop();
        fixture.Hello->Stop();
        fixture.ConnectChannel(false);
        WaitCounter(
            fixture.Counters->GetCounter("ActiveConnections", false),
            1);
        UNIT_ASSERT(!HasError(fixture.Service->GetError()));
    }

    Y_UNIT_TEST(TerminalAcceptErrorLeavesDataChannelsWorking)
    {
        if (!IsAvailable()) {
            return;
        }

        TFixture fixture;
        TFixture remote;
        remote.Start();
        auto config = MakeConfig();
        config.SetCellId("cell-1");
        auto& peer = *config.MutableCells(0);
        peer.SetCellId("cell-2");
        peer.SetIcV2Port(remote.Service->GetAddress().GetPort());
        auto injector = std::make_shared<TListenerErrorInjector>();
        auto [service, error] = NPrivate::CreateService(
            std::make_shared<NCells::TCellsConfig>(std::move(config)),
            fixture.DataService,
            NCloud::NIcV2::CreateAddressResolver(
                fixture.Runtime.GetActorSystem(0),
                fixture.DnsCompletions),
            fixture.Timer,
            fixture.Scheduler,
            fixture.Logging,
            fixture.Counters,
            injector);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        fixture.Service = std::move(service);
        fixture.Service->Start();
        fixture.Connector->Start();
        fixture.ConnectChannel(false);
        WaitCounter(
            fixture.Counters->GetCounter("ActiveConnections", false),
            1);
        const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        UNIT_ASSERT(client >= 0);
        TSocketPtr socket = MakeIntrusive<NInterconnect::TStreamSocket>(client);

        injector->AcceptError = EMFILE;
        const auto address = fixture.Service->GetAddress();
        const int connected =
            connect(client, address.SockAddr(), address.Size());
        // The previous accept loop may observe the error and close first.
        UNIT_ASSERT(connected == 0 || errno == ECONNREFUSED);
        WaitCounter(fixture.Counters->GetCounter("ListenerErrors", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            MAKE_SYSTEM_ERROR(EMFILE),
            fixture.Service->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            fixture.Counters->GetCounter("Listening", false)->Val());

        // The established inbound channel still serves requests.
        auto incoming =
            std::make_unique<NStorage::TEvService::TEvZeroBlocksRequest>();
        incoming->Record.SetDiskId("disk");
        incoming->Record.SetBlocksCount(1);
        auto reply = fixture.Channel->Request(
            std::move(incoming),
            TDuration::Seconds(5));
        UNIT_ASSERT(reply.Wait(TDuration::Seconds(5)));
        auto [event, replyError] = reply.ExtractValue();
        UNIT_ASSERT_C(!HasError(replyError), FormatError(replyError));
        const auto& result =
            event->Get<NStorage::TEvService::TEvZeroBlocksResponse>()->Record;
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));

        // The service can still establish outbound channels.
        auto handler = std::make_shared<TDataConnectionHandler>();
        auto [endpoint, endpointError] =
            fixture.Service->CreateDataEndpoint("cell-2", "127.0.0.1", handler);
        UNIT_ASSERT_C(!HasError(endpointError), FormatError(endpointError));
        UNIT_ASSERT(handler->Connected.GetFuture().Wait(TDuration::Seconds(5)));
        auto outgoing = std::make_shared<NProto::TZeroBlocksRequest>();
        outgoing->SetDiskId("disk");
        outgoing->SetBlocksCount(1);
        outgoing->MutableHeaders()->SetRequestTimeout(5000);
        auto response =
            endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), outgoing);
        UNIT_ASSERT(response.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_C(
            !HasError(response.GetValue()),
            FormatError(response.GetValue().GetError()));

        fixture.Service->Stop();
        UNIT_ASSERT(fixture.Channel->WhenClosed().Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            MAKE_SYSTEM_ERROR(EMFILE),
            fixture.Service->GetError().GetCode());
    }

    Y_UNIT_TEST(RejectsInvalidConfigurationBeforeEngineCreation)
    {
        TFixture fixture;
        for (size_t variant = 0; variant < 16; ++variant) {
            auto config = MakeConfig();
            auto& icv2 = *config.MutableIcV2();
            switch (variant) {
                case 0:
                    config.SetCellsEnabled(false);
                    break;
                case 1:
                    icv2.SetEnabled(false);
                    break;
                case 2:
                    icv2.SetListenAddress("invalid-address");
                    break;
                case 3:
                    icv2.SetPort(65536);
                    break;
                case 4:
                    icv2.ClearPort();
                    break;
                case 5:
                    icv2.SetThreads(0);
                    break;
                case 6:
                    icv2.SetHandshakeTimeout(0);
                    break;
                case 7:
                    icv2.SetDataWorkers(0);
                    break;
                case 8:
                    icv2.SetMaxIncomingConnections(0);
                    break;
                case 9:
                    icv2.SetMaxOutgoingConnections(0);
                    break;
                case 10:
                    icv2.SetMaxOutgoingInflightRequests(0);
                    break;
                case 11:
                    icv2.SetMaxOutgoingInflightBytes(0);
                    break;
                case 12:
                    icv2.SetMaxIncomingInflightRequests(0);
                    break;
                case 13:
                    icv2.SetMaxIncomingInflightBytes(0);
                    break;
                case 14:
                    icv2.SetMaxTotalOutputQueueBytes(0);
                    break;
                case 15:
                    icv2.SetCompletionThreads(0);
                    break;
            }
            auto result = fixture.Create(std::move(config));
            UNIT_ASSERT_VALUES_EQUAL_C(
                result.GetError().GetCode(),
                E_ARGUMENT,
                variant);
        }
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
