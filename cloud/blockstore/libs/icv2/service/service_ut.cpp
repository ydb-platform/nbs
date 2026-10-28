#include "service.h"

#include "peer_connections.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/icv2/transport/diagnostic.h>
#include <cloud/blockstore/libs/icv2/transport/handshake.h>
#include <cloud/blockstore/libs/icv2/transport/request_channel.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/icv2/tcp/connector.h>
#include <cloud/storage/core/libs/icv2/tcp/listener.h>
#include <cloud/storage/core/libs/icv2/tcp/testlib/socket_test.h>

#include <contrib/ydb/library/actors/interconnect/interconnect_common.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>
#include <contrib/ydb/library/actors/interconnect/uring_context.h>

#include <library/cpp/testing/unittest/registar.h>

#include <fcntl.h>
#include <poll.h>
#include <sys/resource.h>

#include <future>
#include <thread>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::CreateListener;
using NCloud::NIcV2::NTests::TTestListenerClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;

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

struct TInvalidPing final
    : NActors::TEventPreSerializedPB<
          TInvalidPing,
          NProto::TIcV2PingRequest,
          TEvDiagnostic::EvPingRequest>
{
};

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
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>();
    ILoggingServicePtr Logging =
        CreateLoggingService("console", TLogSettings{});
    NMonitoring::TDynamicCountersPtr Counters =
        MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<TTestService> DataService =
        std::make_shared<TTestService>();
    IServicePtr Service;
    IConnectorPtr Connector = NCloud::NIcV2::NTests::CreateTestConnector();
    IHandshakeProviderPtr Hello;
    ITransportPtr Transport;
    IRequestChannelPtr Channel;
    IPeerConnectionsPtr Peers;

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

    TResultOrError<IServicePtr> Create(NProto::TCellsConfig config)
    {
        return CreateService(
            std::make_shared<NCells::TCellsConfig>(std::move(config)),
            DataService,
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

    THandshakeResult Handshake(TSocketPtr socket, bool checksums)
    {
        auto [hello, error] = CreateHandshakeProvider({checksums});
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Hello = std::move(hello);
        Hello->Start();
        auto future = Hello->Connect(std::move(socket), TDuration::Seconds(5));
        UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
        return future.GetValue();
    }

    void ConnectChannel(bool checksums)
    {
        auto [connection, error] = Handshake(Connect(), checksums);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        auto common = MakeIntrusive<NActors::TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        common->Settings.V2.Threads = 1;
        common->Settings.V2.EnableSQPOLL = false;
        common->Settings.V2.ChecksumEvents = checksums;
        Transport = NCloud::NIcV2::CreateTransport(
            NActors::CreateUringEngine(std::move(common)),
            0,
            {});
        UNIT_ASSERT(Transport);
        auto [channel, channelError] = CreateRequestChannel(
            Transport,
            std::move(connection.Socket),
            {},
            {{{TEvDiagnostic::EvPingRequest, TEvDiagnostic::EvPingResponse}},
             false},
            Timer,
            Scheduler,
            CreateTaskQueueStub(),
            [](std::unique_ptr<NActors::IEventHandle>)
            { UNIT_FAIL("echo service must not originate requests"); });
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
    ui32 maxConnections = 128)
{
    auto [reserved, reserveError] = CreateListener(
        NInterconnect::TAddress("127.0.0.1", 0),
        std::make_shared<TTestListenerClient>([](TSocketResult) {}));
    UNIT_ASSERT_C(!HasError(reserveError), FormatError(reserveError));
    const auto secondPort = reserved->GetAddress().GetPort();
    auto firstConfig = MakeConfig(checksums);
    firstConfig.SetCellId("cell-1");
    firstConfig.MutableIcV2()->SetEnableXdc(firstXdc);
    firstConfig.MutableIcV2()->SetMaxConnections(maxConnections);
    auto& firstPeer = *firstConfig.MutableCells(0);
    firstPeer.SetCellId("cell-2");
    firstPeer.SetIcV2Port(secondPort);
    firstPeer.AddHosts()->SetFqdn("localhost");
    auto [firstService, firstError] = first.Create(std::move(firstConfig));
    UNIT_ASSERT_C(!HasError(firstError), FormatError(firstError));
    first.Service = std::move(firstService);
    auto secondConfig = MakeConfig(checksums);
    secondConfig.MutableIcV2()->SetPort(secondPort);
    secondConfig.MutableIcV2()->SetEnableXdc(secondXdc);
    secondConfig.MutableIcV2()->SetMaxConnections(maxConnections);
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

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2ServiceTest)
{
    Y_UNIT_TEST(PingsWithOptionalXdcInBothDirections)
    {
        if (!IsAvailable()) {
            return;
        }
        for (bool firstXdc: {false, true}) {
            for (bool secondXdc: {false, true}) {
                TFixture first;
                TFixture second;
                StartPair(first, second, false, true, firstXdc, secondXdc, 1);
                for (const auto& [service, peer]:
                     {std::pair{first.Service, TString("cell-2")},
                      std::pair{second.Service, TString("cell-1")}})
                {
                    auto future = service->Ping(
                        peer,
                        "optional-xdc",
                        TDuration::Seconds(5));
                    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
                    const auto& result = future.GetValue();
                    UNIT_ASSERT_C(
                        !HasError(result.GetError()),
                        FormatError(result.GetError()));
                    UNIT_ASSERT_VALUES_EQUAL(
                        result.GetResult().Payload,
                        "optional-xdc");
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
                    auto hello =
                        first.Handshake(extra.GetValue().GetResult(), false);
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
            TFixture first;
            TFixture second;
            TString stored(8192, 'x');
            second.DataService->WriteBlocksLocalHandler = [&](auto request)
            {
                UNIT_ASSERT_VALUES_EQUAL(request->GetDiskId(), "disk");
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                SgListCopy(guard.Get(), {stored.Detach(), stored.size()});
                return NThreading::MakeFuture<
                    NProto::TWriteBlocksLocalResponse>();
            };
            second.DataService->ReadBlocksLocalHandler = [&](auto request)
            {
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                SgListCopy({stored.data(), stored.size()}, guard.Get());
                return NThreading::MakeFuture<
                    NProto::TReadBlocksLocalResponse>();
            };
            second.DataService->ZeroBlocksHandler = [&](auto request)
            {
                UNIT_ASSERT_VALUES_EQUAL(request->GetBlocksCount(), 2);
                stored = TString(8192, '\0');
                return NThreading::MakeFuture<NProto::TZeroBlocksResponse>();
            };
            StartPair(first, second, checksums);
            auto [endpoint, error] =
                first.Service->CreateDataEndpoint("cell-2", "127.0.0.1");
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
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
            UNIT_ASSERT_VALUES_EQUAL(stored, data);
            auto read = std::make_shared<NProto::TReadBlocksLocalRequest>();
            read->SetDiskId("disk");
            read->SetBlockSize(4096);
            read->SetBlocksCount(2);
            read->MutableHeaders()->SetRequestTimeout(5000);
            TString received(8192, 'r');
            read->Sglist =
                TGuardedSgList({{received.Detach(), received.size()}});
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
            UNIT_ASSERT_VALUES_EQUAL(stored, TString(8192, '\0'));
            first.Service->Stop();
            auto rejected =
                endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), zero);
            UNIT_ASSERT(rejected.Wait(TDuration::Seconds(5)));
            UNIT_ASSERT(HasError(rejected.GetValue()));
        }
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
        auto [endpoint, error] =
            first.Service->CreateDataEndpoint("cell-2", "127.0.0.1");
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
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

    Y_UNIT_TEST(DataDeadlineIncludesWaitingForConnectionAndDoesNotResend)
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
        auto [endpoint, error] =
            first.Service->CreateDataEndpoint("cell-2", "127.0.0.1");
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        auto zero = std::make_shared<NProto::TZeroBlocksRequest>();
        zero->SetDiskId("disk");
        zero->SetBlocksCount(1);
        zero->MutableHeaders()->SetRequestTimeout(10);
        auto expired =
            endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), zero);
        first.Timer->AdvanceTime(TDuration::MilliSeconds(10));
        first.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT(expired.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            expired.GetValue().GetError().GetCode(),
            E_TIMEOUT);
        second.Service->Start();
        zero->MutableHeaders()->SetRequestTimeout(5000);
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
            fixture.Service->CreateDataEndpoint("cell-1", "127.0.0.1");
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        endpoint->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Service->CreateDataEndpoint("cell-1", "127.0.0.1")
                .GetError()
                .GetCode(),
            E_REJECTED);
        endpoint.reset();
        auto next = fixture.Service->CreateDataEndpoint("cell-1", "127.0.0.1");
        UNIT_ASSERT_C(!HasError(next.GetError()), FormatError(next.GetError()));
    }

    Y_UNIT_TEST(EchoesPingWithBothChecksumModesAndPublishesCounters)
    {
        if (!IsAvailable()) {
            return;
        }
        for (bool checksums: {false, true}) {
            TFixture fixture;
            fixture.Start(checksums);
            fixture.ConnectChannel(checksums);
            const TString payload(256 * 1024, '\0');
            auto request = std::make_unique<TEvDiagnostic::TEvPingRequest>();
            request->Record.SetPayload(payload);
            auto future = fixture.Channel->Request(
                std::move(request),
                TDuration::Seconds(5));
            UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
            auto [event, error] = future.GetValue();
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(
                event->Get<TEvDiagnostic::TEvPingResponse>()
                    ->Record.GetPayload(),
                payload);
            fixture.Service->Stop();
            UNIT_ASSERT(
                fixture.Channel->WhenClosed().Wait(TDuration::Seconds(5)));
            UNIT_ASSERT_VALUES_EQUAL(
                fixture.Counters->GetCounter("AcceptedConnections", true)
                    ->Val(),
                1);
            UNIT_ASSERT_VALUES_EQUAL(
                fixture.Counters->GetCounter("HandshakeErrors", true)->Val(),
                0);
            UNIT_ASSERT_VALUES_EQUAL(
                fixture.Counters->GetCounter("ActiveConnections", false)->Val(),
                0);
            UNIT_ASSERT_VALUES_EQUAL(
                fixture.Counters->GetCounter("PingRequests", true)->Val(),
                1);
            UNIT_ASSERT_VALUES_EQUAL(
                fixture.Counters->GetCounter("PingReplies", true)->Val(),
                1);
        }
    }

    Y_UNIT_TEST(TypedPingExchangesBinaryMessagesBetweenServicesInBothDirections)
    {
        if (!IsAvailable()) {
            return;
        }
        for (bool checksums: {false, true}) {
            TFixture first;
            TFixture second;
            StartPair(first, second, checksums);
            const TString payload(256 * 1024, '\0');
            TVector<TPingFuture> forward;
            TVector<TPingFuture> reverse;
            for (size_t i = 0; i < 32; ++i) {
                forward.push_back(first.Service->Ping(
                    "cell-2",
                    ToString(i) + payload,
                    TDuration::Seconds(5)));
                reverse.push_back(second.Service->Ping(
                    "cell-1",
                    ToString(i) + payload,
                    TDuration::Seconds(5)));
            }
            for (size_t i = 0; i < forward.size(); ++i) {
                for (bool outgoing: {false, true}) {
                    const auto& future = outgoing ? forward[i] : reverse[i];
                    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
                    auto [response, error] = future.GetValue();
                    UNIT_ASSERT_C(!HasError(error), FormatError(error));
                    UNIT_ASSERT_VALUES_EQUAL(
                        response.PeerCellId,
                        outgoing ? "cell-2" : "cell-1");
                    UNIT_ASSERT_VALUES_EQUAL(
                        response.Payload,
                        ToString(i) + payload);
                }
            }
            for (auto* fixture: {&first, &second}) {
                WaitCounter(
                    fixture->Counters->GetCounter("AcceptedConnections", true),
                    1);
                WaitCounter(
                    fixture->Counters->GetCounter("PingRequests", true),
                    32);
                WaitCounter(
                    fixture->Counters->GetCounter("PingReplies", true),
                    32);
            }
        }
    }

    Y_UNIT_TEST(TypedPingTimeoutDoesNotCancelConnectionForLongerWaitingCall)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture first;
        TFixture second;
        StartPair(first, second, false, false);
        auto shortRequest =
            first.Service->Ping("cell-2", "short", TDuration::MilliSeconds(10));
        auto longRequest =
            first.Service->Ping("cell-2", "long", TDuration::Seconds(5));
        first.Timer->AdvanceTime(TDuration::MilliSeconds(10));
        first.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT(shortRequest.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            shortRequest.GetValue().GetError().GetCode(),
            E_TIMEOUT);
        second.Service->Start();
        UNIT_ASSERT(longRequest.Wait(TDuration::Seconds(5)));
        auto [response, error] = longRequest.GetValue();
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(response.Payload, "long");
        WaitCounter(
            second.Counters->GetCounter("AcceptedConnections", true),
            1);
        WaitCounter(second.Counters->GetCounter("PingRequests", true), 1);
    }

    Y_UNIT_TEST(StopWaitsForTypedPingContinuation)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture first;
        TFixture second;
        StartPair(first, second, false, false);
        auto result =
            first.Service->Ping("cell-2", "ping", TDuration::Seconds(5));
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        result.Subscribe(
            [&](const auto&)
            {
                first.Service->RequestStop();
                entered.SetValue();
                release.GetFuture().GetValueSync();
            });
        second.Service->Start();
        const bool started = entered.GetFuture().Wait(TDuration::Seconds(5));
        auto stopped =
            std::async(std::launch::async, [&] { first.Service->Stop(); });
        const bool waits =
            stopped.wait_for(30ms) == std::future_status::timeout;
        release.SetValue();
        stopped.get();
        UNIT_ASSERT(started);
        UNIT_ASSERT(waits);
        UNIT_ASSERT_VALUES_EQUAL(
            first.Service->Ping("cell-2", "after stop", TDuration::Seconds(1))
                .GetValue()
                .GetError()
                .GetCode(),
            E_CANCELLED);
    }

    Y_UNIT_TEST(TypedPingRejectsUnknownAndUnconfiguredPeer)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture fixture;
        fixture.Start();
        for (const TString cellId: {"cell-1", "cell-2", "unknown"}) {
            auto future =
                fixture.Service->Ping(cellId, "ping", TDuration::Seconds(5));
            UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
            UNIT_ASSERT_VALUES_EQUAL(
                future.GetValue().GetError().GetCode(),
                E_ARGUMENT);
        }
        UNIT_ASSERT(!HasError(fixture.Service->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Counters->GetCounter("AcceptedConnections", true)->Val(),
            0);
    }

    Y_UNIT_TEST(OutgoingCacheExchangesMessagesAndReconnectsToRestartedService)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture fixture;
        fixture.Start();
        auto config = MakeConfig();
        config.SetCellId("cell-1");
        auto& peer = *config.MutableCells(0);
        peer.SetCellId("cell-2");
        peer.SetIcV2Port(fixture.Service->GetAddress().GetPort());
        peer.AddHosts()->SetFqdn("localhost");
        auto common = MakeIntrusive<NActors::TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        common->Settings.V2.Threads = 1;
        common->Settings.V2.EnableSQPOLL = false;
        fixture.Transport = NCloud::NIcV2::CreateTransport(
            NActors::CreateUringEngine(std::move(common)),
            0,
            {});
        auto [hello, helloError] = CreateHandshakeProvider({false});
        UNIT_ASSERT_C(!HasError(helloError), FormatError(helloError));
        auto [peers, error] = CreatePeerConnections(
            std::make_shared<NCells::TCellsConfig>(std::move(config)),
            NCloud::NIcV2::NTests::CreateTestConnector(),
            std::move(hello),
            fixture.Timer,
            CreateTaskQueueStub(),
            [&](const THandshakeReady& connection)
            {
                return CreateRequestChannel(
                    fixture.Transport,
                    connection.Socket,
                    {},
                    {{{TEvDiagnostic::EvPingRequest,
                       TEvDiagnostic::EvPingResponse}},
                     false},
                    fixture.Timer,
                    fixture.Scheduler,
                    CreateTaskQueueStub(),
                    [](std::unique_ptr<NActors::IEventHandle>)
                    {
                        UNIT_FAIL("remote service must not originate requests");
                    });
            });
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        fixture.Peers = std::move(peers);
        fixture.Peers->Start();
        TVector<TChannelFuture> connections;
        for (size_t i = 0; i < 32; ++i) {
            connections.push_back(fixture.Peers->GetChannel("cell-2"));
        }
        UNIT_ASSERT(connections[0].Wait(TDuration::Seconds(5)));
        auto [first, connectError] = connections[0].GetValue();
        UNIT_ASSERT_C(!HasError(connectError), FormatError(connectError));
        for (const auto& connection: connections) {
            UNIT_ASSERT(connection.Wait(TDuration::Seconds(5)));
            UNIT_ASSERT(connection.GetValue().GetResult() == first);
        }
        auto ping =
            [&](const IRequestChannelPtr& channel, const TString& payload)
        {
            auto request = std::make_unique<TEvDiagnostic::TEvPingRequest>();
            request->Record.SetPayload(payload);
            auto result =
                channel->Request(std::move(request), TDuration::Seconds(5));
            UNIT_ASSERT(result.Wait(TDuration::Seconds(5)));
            auto [event, pingError] = result.GetValue();
            UNIT_ASSERT_C(!HasError(pingError), FormatError(pingError));
            UNIT_ASSERT_VALUES_EQUAL(
                event->Get<TEvDiagnostic::TEvPingResponse>()
                    ->Record.GetPayload(),
                payload);
        };
        ping(first, TString(256 * 1024, '\0'));
        WaitCounter(
            fixture.Counters->GetCounter("AcceptedConnections", true),
            1);
        const auto port = fixture.Service->GetAddress().GetPort();
        fixture.Service->Stop();
        UNIT_ASSERT(first->WhenClosed().Wait(TDuration::Seconds(5)));
        auto failed = fixture.Peers->GetChannel("cell-2");
        UNIT_ASSERT(failed.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(HasError(failed.GetValue().GetError()));
        auto restarted = MakeConfig();
        restarted.MutableIcV2()->SetPort(port);
        auto [service, serviceError] = fixture.Create(std::move(restarted));
        UNIT_ASSERT_C(!HasError(serviceError), FormatError(serviceError));
        fixture.Service = std::move(service);
        fixture.Service->Start();
        auto connected = fixture.Peers->GetChannel("cell-2");
        UNIT_ASSERT(connected.Wait(TDuration::Seconds(5)));
        auto [second, reconnectError] = connected.GetValue();
        UNIT_ASSERT_C(!HasError(reconnectError), FormatError(reconnectError));
        UNIT_ASSERT(second != first);
        ping(second, "after restart");
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
        auto result = fixture.Handshake(fixture.Connect(), false);
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
        config.MutableIcV2()->SetHandshakeTimeoutMs(30000);
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
        auto [service, error] = fixture.Create(MakeConfig());
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        const auto address = service->GetAddress();
        UNIT_ASSERT_VALUES_EQUAL(
            service->Ping("cell-1", "", TDuration::Seconds(1))
                .GetValue()
                .GetError()
                .GetCode(),
            E_CANCELLED);
        service->Stop();
        service->Stop();
        auto [listener, listenError] = CreateListener(
            address,
            std::make_shared<TTestListenerClient>([](TSocketResult) {}));
        UNIT_ASSERT_C(!HasError(listenError), FormatError(listenError));
        listener->Stop();
    }

    Y_UNIT_TEST(MalformedPingClosesChannelWithoutStoppingService)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture fixture;
        fixture.Start();
        fixture.ConnectChannel(false);
        auto request = std::make_unique<TInvalidPing>();
        request->PreSerializedData = TString(1, '\xff');
        auto future =
            fixture.Channel->Request(std::move(request), TDuration::Seconds(5));
        UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            future.GetValue().GetError().GetCode(),
            E_TRANSPORT_ERROR);
        fixture.Transport->Stop();
        fixture.Hello->Stop();
        fixture.ConnectChannel(false);
        auto ping = std::make_unique<TEvDiagnostic::TEvPingRequest>();
        ping->Record.SetPayload("still running");
        auto reply =
            fixture.Channel->Request(std::move(ping), TDuration::Seconds(5));
        UNIT_ASSERT(reply.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_C(
            !HasError(reply.GetValue().GetError()),
            FormatError(reply.GetValue().GetError()));
    }

    Y_UNIT_TEST(ConnectionLimitIncludesSilentHelloAndActiveChannels)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture fixture;
        auto config = MakeConfig();
        config.MutableIcV2()->SetMaxConnections(1);
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

    Y_UNIT_TEST(RejectedReplyClosesChannelInsteadOfLeavingPeerWaiting)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture fixture;
        auto config = MakeConfig();
        config.MutableIcV2()->SetMaxOutputQueueBytes(16);
        auto [service, error] = fixture.Create(std::move(config));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        fixture.Service = std::move(service);
        fixture.Service->Start();
        fixture.Connector->Start();
        fixture.ConnectChannel(false);
        auto request = std::make_unique<TEvDiagnostic::TEvPingRequest>();
        request->Record.SetPayload(TString(1024, 'x'));
        auto response = fixture.Channel->Request(
            std::move(request),
            TDuration::Seconds(30));
        UNIT_ASSERT(response.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            response.GetValue().GetError().GetCode(),
            E_TRANSPORT_ERROR);
        WaitCounter(
            fixture.Counters->GetCounter("ActiveConnections", false),
            0);
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Counters->GetCounter("PingReplies", true)->Val(),
            0);
    }

    SIMPLE_UNIT_FORKED_TEST(TerminalAcceptErrorIsObservableAndClosesChannels)
    {
        if (!IsAvailable()) {
            return;
        }
        TFixture fixture;
        fixture.Start();
        fixture.ConnectChannel(false);
        WaitCounter(
            fixture.Counters->GetCounter("ActiveConnections", false),
            1);
        const int client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        UNIT_ASSERT(client >= 0);
        TSocketPtr socket = MakeIntrusive<NInterconnect::TStreamSocket>(client);

        struct TFdLimit
        {
            rlimit Saved;
            TVector<int> Files;

            TFdLimit()
            {
                UNIT_ASSERT_VALUES_EQUAL(getrlimit(RLIMIT_NOFILE, &Saved), 0);
            }

            ~TFdLimit()
            {
                for (int fd: Files) {
                    close(fd);
                }
                setrlimit(RLIMIT_NOFILE, &Saved);
            }

            void Exhaust()
            {
                auto limit = Saved;
                limit.rlim_cur = Min<rlim_t>(256, Saved.rlim_cur);
                UNIT_ASSERT_VALUES_EQUAL(setrlimit(RLIMIT_NOFILE, &limit), 0);
                while (true) {
                    int fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
                    if (fd < 0) {
                        UNIT_ASSERT_VALUES_EQUAL(errno, EMFILE);
                        break;
                    }
                    Files.push_back(fd);
                }
            }
        } limit;

        limit.Exhaust();
        const auto address = fixture.Service->GetAddress();
        UNIT_ASSERT_VALUES_EQUAL(
            connect(client, address.SockAddr(), address.Size()),
            0);
        WaitCounter(fixture.Counters->GetCounter("ListenerErrors", true), 1);
        UNIT_ASSERT(HasError(fixture.Service->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Counters->GetCounter("Listening", false)->Val(),
            0);
        UNIT_ASSERT(fixture.Channel->WhenClosed().Wait(TDuration::Seconds(5)));
        fixture.Service->Stop();
        UNIT_ASSERT(HasError(fixture.Service->GetError()));
    }

    Y_UNIT_TEST(RejectsInvalidConfigurationBeforeEngineCreation)
    {
        TFixture fixture;
        for (size_t variant = 0; variant < 13; ++variant) {
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
                    icv2.SetHandshakeTimeoutMs(0);
                    break;
                case 7:
                    icv2.SetDataWorkers(0);
                    break;
                case 8:
                    icv2.SetMaxConnections(0);
                    break;
                case 9:
                    icv2.SetMaxPendingRequests(0);
                    break;
                case 10:
                    icv2.SetMaxOutputQueueBytes(0);
                    break;
                case 11:
                    icv2.SetCompletionThreads(0);
                    break;
                case 12:
                    icv2.SetMaxOutgoingConnections(0);
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
