#include "service.h"

#include "public.h"

#include "service_private.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/icv2/data/endpoint.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/icv2/handshake/handshake.h>
#include <cloud/storage/core/libs/icv2/peer/peer_manager.h>
#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>
#include <cloud/storage/core/libs/icv2/tcp/connector.h>
#include <cloud/storage/core/libs/icv2/tcp/listener.h>
#include <cloud/storage/core/libs/icv2/tcp/testlib/socket_test.h>

#include <contrib/ydb/library/actors/interconnect/interconnect_common.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>
#include <contrib/ydb/library/actors/interconnect/uring_context.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/generic/scope.h>
#include <util/system/mutex.h>

#include <poll.h>

#include <atomic>
#include <cerrno>
#include <functional>
#include <future>
#include <thread>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::CreatePeerManager;
using NCloud::NIcV2::EPeerState;
using NCloud::NIcV2::IPeerManagerPtr;
using NCloud::NIcV2::IRequestChannelFactory;
using NCloud::NIcV2::TRequestChannelFuture;
using NCloud::NIcV2::TRequestChannelResult;

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
using NCloud::NIcV2::NTests::TTestListenerHandler;

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

class TTestRequestChannelFactory final: public IRequestChannelFactory
{
private:
    const ITransportPtr Transport;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;

public:
    TTestRequestChannelFactory(
        ITransportPtr transport,
        ITimerPtr timer,
        ISchedulerPtr scheduler)
        : Transport(std::move(transport))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
    {}

    TRequestChannelResult CreateRequestChannel(
        const THandshakeReady& connection) override
    {
        return NCloud::NIcV2::CreateRequestChannel(
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
    UNIT_ASSERT_VALUES_EQUAL(value, counter->Val());
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

////////////////////////////////////////////////////////////////////////////////

class TTestAddressResolver final: public NCloud::NIcV2::IAddressResolver
{
public:
    NProto::TError Error;

    NCloud::NIcV2::TResolveFuture
    Resolve(const TString& host, ui16 port, TMonotonic deadline) override
    {
        Y_UNUSED(deadline);

        if (HasError(Error)) {
            return NThreading::MakeFuture<NCloud::NIcV2::TResolveResult>(Error);
        }

        TVector<NInterconnect::TAddress> addresses;
        addresses.emplace_back(host.c_str(), port);
        return NThreading::MakeFuture<NCloud::NIcV2::TResolveResult>(
            std::move(addresses));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture
{
    TPortManager PortManager;
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>();
    ILoggingServicePtr Logging =
        CreateLoggingService("console", TLogSettings{});
    NMonitoring::TDynamicCountersPtr Counters =
        MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<TTestService> DataService =
        std::make_shared<TTestService>();
    NCloud::NIcV2::IAddressResolverPtr Resolver =
        std::make_shared<TTestAddressResolver>();
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
    }

    TResultOrError<IServicePtr> Create(
        NProto::TCellsConfig config,
        NCloud::NIcV2::NPrivate::IListenerErrorInjectorPtr injector = {})
    {
        return NPrivate::CreateService(
            std::make_shared<NCells::TCellsConfig>(std::move(config)),
            DataService,
            Resolver,
            Timer,
            Scheduler,
            Logging,
            Counters,
            std::move(injector));
    }

    void Start(
        NProto::TCellsConfig config = MakeConfig(),
        NCloud::NIcV2::NPrivate::IListenerErrorInjectorPtr injector = {})
    {
        auto [service, error] = Create(std::move(config), std::move(injector));
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

struct TPairConfig
{
    bool FirstChecksums = false;
    bool SecondChecksums = false;
    bool StartSecond = true;
    bool FirstXdc = false;
    bool SecondXdc = false;
    ui32 MaxIncomingConnections = 128;
    ui32 FirstMaxOutgoingConnections = 128;
};

void StartPair(TFixture& first, TFixture& second, TPairConfig config = {})
{
    const auto secondPort = second.PortManager.GetTcpPort();
    auto firstConfig = MakeConfig(config.FirstChecksums);
    firstConfig.SetCellId("cell-1");
    firstConfig.MutableIcV2()->SetEnableXdc(config.FirstXdc);
    firstConfig.MutableIcV2()->SetMaxIncomingConnections(
        config.MaxIncomingConnections);
    firstConfig.MutableIcV2()->SetMaxOutgoingConnections(
        config.FirstMaxOutgoingConnections);
    auto& firstPeer = *firstConfig.MutableCells(0);
    firstPeer.SetCellId("cell-2");
    firstPeer.SetIcV2Port(secondPort);
    firstPeer.AddHosts()->SetFqdn("127.0.0.1");
    auto [firstService, firstError] = first.Create(std::move(firstConfig));
    UNIT_ASSERT_C(!HasError(firstError), FormatError(firstError));
    first.Service = std::move(firstService);
    auto secondConfig = MakeConfig(config.SecondChecksums);
    secondConfig.MutableIcV2()->SetPort(secondPort);
    secondConfig.MutableIcV2()->SetEnableXdc(config.SecondXdc);
    secondConfig.MutableIcV2()->SetMaxIncomingConnections(
        config.MaxIncomingConnections);
    auto& secondPeer = *secondConfig.MutableCells(0);
    secondPeer.SetIcV2Port(first.Service->GetAddress().GetPort());
    secondPeer.AddHosts()->SetFqdn("127.0.0.1");
    auto [secondService, secondError] = second.Create(std::move(secondConfig));
    UNIT_ASSERT_C(!HasError(secondError), FormatError(secondError));
    second.Service = std::move(secondService);
    first.Service->Start();
    if (config.StartSecond) {
        second.Service->Start();
    }
}

bool HasIoUring(NUnitTest::TTestContext& context)
{
    const bool available = NActors::TUringContext::IsAvailable();
    context.Metrics["io_uring_available"] = available;
    if (!available) {
        Cerr << "io_uring not available; skipping" << Endl;
    }
    return available;
}

////////////////////////////////////////////////////////////////////////////////

class TDataConnectionHandler final: public IClientConnectionHandler
{
public:
    NThreading::TPromise<void> Connected = NThreading::NewPromise();
    std::function<void()> OnUnavailable;

    void HandleConnected() noexcept override
    {
        Connected.TrySetValue();
    }

    void HandleDisconnected() noexcept override
    {}

    void HandleUnavailable() noexcept override
    {
        if (OnUnavailable) {
            OnUnavailable();
        }
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2ServiceTest)
{
    Y_UNIT_TEST(ZeroBlocksWithOptionalXdcAndDifferentChecksums)
    {
        if (!HasIoUring(ut_context)) {
            return;
        }
        for (bool firstXdc: {false, true}) {
            for (bool secondXdc: {false, true}) {
                TFixture first;
                TFixture second;
                StartPair(
                    first,
                    second,
                    {
                        .SecondChecksums = true,
                        .FirstXdc = firstXdc,
                        .SecondXdc = secondXdc,
                        .MaxIncomingConnections = 1,
                    });
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
        if (!HasIoUring(ut_context)) {
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
            StartPair(
                first,
                second,
                {.FirstChecksums = checksums, .SecondChecksums = checksums});
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
        if (!HasIoUring(ut_context)) {
            return;
        }

        TFixture first;
        TFixture second;
        StartPair(first, second);
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

    Y_UNIT_TEST(ReleasedEndpointHoldsItsSlotUntilResultCallbackCompletes)
    {
        if (!HasIoUring(ut_context)) {
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
        StartPair(first, second, {.FirstMaxOutgoingConnections = 1});
        auto handler = std::make_shared<TDataConnectionHandler>();
        auto [endpoint, error] =
            first.Service->CreateDataEndpoint("cell-2", "127.0.0.1", handler);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT(handler->Connected.GetFuture().Wait(TDuration::Seconds(5)));
        const std::weak_ptr<IBlockStore> weak = endpoint;
        const auto closed = static_cast<IDataEndpoint&>(*endpoint).WhenClosed();
        auto zero = std::make_shared<NProto::TZeroBlocksRequest>();
        zero->SetDiskId("disk");
        zero->SetBlocksCount(1);
        zero->MutableHeaders()->SetRequestTimeout(5000);
        auto response =
            endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), zero);
        auto released = NThreading::NewPromise();
        auto release = NThreading::NewPromise();
        std::future<void> subscribing;
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
            });
        UNIT_ASSERT(subscribing.wait_for(5s) == std::future_status::ready);
        subscribing.get();
        storageDone.SetValue({});
        UNIT_ASSERT(released.GetFuture().Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT(!closed.HasValue());
        const auto rejected =
            first.Service->CreateDataEndpoint("cell-2", "127.0.0.1", {});
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, rejected.GetError().GetCode());

        release.SetValue();
        UNIT_ASSERT(closed.Wait(TDuration::Seconds(5)));
        const auto next =
            first.Service->CreateDataEndpoint("cell-2", "127.0.0.1", {});
        UNIT_ASSERT_C(!HasError(next.GetError()), FormatError(next.GetError()));
    }

    Y_UNIT_TEST(StopWaitsForAdmittedStorageOperation)
    {
        if (!HasIoUring(ut_context)) {
            return;
        }
        TFixture first;
        TFixture second;
        auto entered = NThreading::NewPromise<void>();
        auto completed = NThreading::NewPromise<NProto::TZeroBlocksResponse>();
        std::future<void> stopped;
        Y_DEFER
        {
            completed.TrySetValue(NProto::TZeroBlocksResponse{});
        };
        second.DataService->ZeroBlocksHandler = [&](auto)
        {
            entered.SetValue();
            return completed.GetFuture();
        };
        StartPair(first, second);
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
        stopped =
            std::async(std::launch::async, [&] { second.Service->Stop(); });
        WaitCounter(second.Counters->GetCounter("ActiveConnections", false), 0);
        UNIT_ASSERT(response.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(HasError(response.GetValue()));
        UNIT_ASSERT(stopped.wait_for(50ms) == std::future_status::timeout);

        completed.SetValue({});
        UNIT_ASSERT(stopped.wait_for(5s) == std::future_status::ready);
        stopped.get();
    }

    Y_UNIT_TEST(DataRequestBeforeConnectionFailsWithoutSending)
    {
        if (!HasIoUring(ut_context)) {
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
        StartPair(first, second, {.StartSecond = false});
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

    Y_UNIT_TEST(EndpointCreationReservesSlotAndAllowsConcurrentStop)
    {
        if (!HasIoUring(ut_context)) {
            return;
        }

        TFixture fixture;
        auto resolver = std::make_shared<TTestAddressResolver>();
        resolver->Error = MakeError(E_UNAVAILABLE, "test resolution failure");
        fixture.Resolver = std::move(resolver);
        auto config = MakeConfig();
        config.MutableIcV2()->SetMaxOutgoingConnections(1);
        config.MutableCells(0)->SetIcV2Port(1234);
        fixture.Start(std::move(config));

        auto reentrantError = NThreading::NewPromise<ui32>();
        auto release = NThreading::NewPromise();
        auto handler = std::make_shared<TDataConnectionHandler>();
        handler->OnUnavailable = [&]
        {
            auto result =
                fixture.Service->CreateDataEndpoint("cell-1", "127.0.0.1", {});
            reentrantError.SetValue(result.GetError().GetCode());
            release.GetFuture().GetValueSync();
        };

        std::future<TResultOrError<IBlockStorePtr>> creating;
        std::future<void> stopping;
        Y_DEFER
        {
            release.TrySetValue();
        };
        creating = std::async(
            std::launch::async,
            [&]
            {
                return fixture.Service->CreateDataEndpoint(
                    "cell-1",
                    "127.0.0.1",
                    handler);
            });
        const auto reentered = reentrantError.GetFuture();
        UNIT_ASSERT(reentered.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, reentered.GetValue());

        stopping = std::async(
            std::launch::async,
            [&] { fixture.Service->RequestStop(); });
        UNIT_ASSERT(stopping.wait_for(5s) == std::future_status::ready);
        stopping.get();

        release.SetValue();
        UNIT_ASSERT(creating.wait_for(5s) == std::future_status::ready);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            creating.get().GetError().GetCode());
    }

    Y_UNIT_TEST(DataBindingLimitRecoversAfterClosedEndpointIsReleased)
    {
        if (!HasIoUring(ut_context)) {
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
        if (!HasIoUring(ut_context)) {
            return;
        }
        TFixture fixture;
        const auto port = fixture.PortManager.GetTcpPort();
        auto config = MakeConfig();
        config.MutableIcV2()->SetPort(port);
        fixture.Start(std::move(config));
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
            fixture.Resolver,
            CreateTaskQueueStub(),
            std::make_shared<TTestRequestChannelFactory>(
                fixture.Transport,
                fixture.Timer,
                fixture.Scheduler));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        fixture.Peers = std::move(peers);
        fixture.Peers->Start();
        const auto [peer, createError] =
            fixture.Peers->CreatePeer("127.0.0.1", port, {});
        UNIT_ASSERT_C(!HasError(createError), FormatError(createError));
        TVector<TRequestChannelFuture> connections;
        for (size_t i = 0; i < 32; ++i) {
            connections.push_back(peer->GetRequestChannel());
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
        fixture.Service->Stop();
        UNIT_ASSERT(first->WhenClosed().Wait(TDuration::Seconds(5)));
        auto failed = peer->GetRequestChannel();
        UNIT_ASSERT(failed.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(HasError(failed.GetValue().GetError()));
        auto restarted = MakeConfig();
        restarted.MutableIcV2()->SetPort(port);
        auto [service, serviceError] = fixture.Create(std::move(restarted));
        UNIT_ASSERT_C(!HasError(serviceError), FormatError(serviceError));
        fixture.Service = std::move(service);
        fixture.Service->Start();
        TRequestChannelResult connected = MakeError(E_UNAVAILABLE);
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        do {
            auto future = peer->GetRequestChannel();
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
        if (!HasIoUring(ut_context)) {
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
        if (!HasIoUring(ut_context)) {
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
        if (!HasIoUring(ut_context)) {
            return;
        }
        TFixture fixture;
        auto config = MakeConfig();
        config.MutableIcV2()->SetPort(fixture.PortManager.GetTcpPort());
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
            std::make_shared<TTestListenerHandler>([](TSocketResult) {}));
        UNIT_ASSERT_C(!HasError(listenError), FormatError(listenError));
        listener->Stop();
    }

    Y_UNIT_TEST(ConnectionLimitIncludesSilentHelloAndActiveChannels)
    {
        if (!HasIoUring(ut_context)) {
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

    Y_UNIT_TEST(AcceptsNewChannelsAfterTemporaryAcceptError)
    {
        if (!HasIoUring(ut_context)) {
            return;
        }

        TFixture fixture;
        auto injector = std::make_shared<TListenerErrorInjector>();
        injector->AcceptError = EMFILE;
        fixture.Start(MakeConfig(), injector);
        fixture.ConnectChannel(false);
        WaitCounter(
            fixture.Counters->GetCounter("ActiveConnections", false),
            1);
        UNIT_ASSERT_VALUES_EQUAL(0, injector->AcceptError.load());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            fixture.Counters->GetCounter("ListenerErrors", true)->Val());
        UNIT_ASSERT(!HasError(fixture.Service->GetError()));
    }

    Y_UNIT_TEST(TerminalAcceptErrorLeavesDataChannelsWorking)
    {
        if (!HasIoUring(ut_context)) {
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
        fixture.Start(std::move(config), injector);
        fixture.ConnectChannel(false);
        WaitCounter(
            fixture.Counters->GetCounter("ActiveConnections", false),
            1);
        const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        UNIT_ASSERT(client >= 0);
        TSocketPtr socket = MakeIntrusive<NInterconnect::TStreamSocket>(client);

        injector->AcceptError = EBADF;
        const auto address = fixture.Service->GetAddress();
        const int connected =
            connect(client, address.SockAddr(), address.Size());
        // The previous accept loop may observe the error and close first.
        UNIT_ASSERT(connected == 0 || errno == ECONNREFUSED);
        WaitCounter(fixture.Counters->GetCounter("ListenerErrors", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            MAKE_SYSTEM_ERROR(EBADF),
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
            MAKE_SYSTEM_ERROR(EBADF),
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
                    icv2.SetNetworkIoThreads(0);
                    break;
                case 6:
                    icv2.SetHandshakeTimeout(0);
                    break;
                case 7:
                    icv2.SetIncomingRequestThreads(0);
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
                    icv2.SetOutgoingCompletionThreads(0);
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
