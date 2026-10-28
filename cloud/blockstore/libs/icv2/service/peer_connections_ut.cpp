#include "peer_connections.h"

#include <cloud/blockstore/libs/cells/iface/config.h>

#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <atomic>
#include <future>
#include <mutex>
#include <thread>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IConnector;

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;

struct TTestConnector final: IConnector
{
    struct TRequest
    {
        NInterconnect::TAddress Address;
        TDuration Timeout;
        NThreading::TPromise<TSocketResult> Promise =
            NThreading::NewPromise<TSocketResult>();
    };

    std::mutex Mutex;
    TVector<TRequest> Requests;

    void Start() override
    {}

    void Stop() override
    {}

    void RequestStop() override
    {}

    TSocketFuture Connect(
        const NInterconnect::TAddress& address,
        TDuration timeout) override
    {
        std::lock_guard guard(Mutex);
        auto& request = Requests.emplace_back(address, timeout);
        return request.Promise.GetFuture();
    }
};

struct TTestHandshake final: IHandshakeProvider
{
    struct TRequest
    {
        TSocketPtr Socket;
        TDuration Timeout;
        NThreading::TPromise<THandshakeResult> Promise =
            NThreading::NewPromise<THandshakeResult>();
    };

    TVector<TRequest> Requests;

    void Start() override
    {}

    void Stop() override
    {}

    void RequestStop() override
    {}

    THandshakeFuture Connect(TSocketPtr socket, TDuration timeout) override
    {
        auto& request = Requests.emplace_back(std::move(socket), timeout);
        return request.Promise.GetFuture();
    }

    TAcceptHandshakeFuture Accept(TSocketPtr, TDuration) override
    {
        UNIT_FAIL("outgoing connections must initiate hello");
        return {};
    }
};

struct TTestChannel final: IRequestChannel
{
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();
    std::atomic<bool> Closing = false;
    std::function<void()> OnActivate;

    bool Activate() override
    {
        if (OnActivate) {
            OnActivate();
        }
        return true;
    }

    TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase>,
        TDuration) override
    {
        UNIT_FAIL("channel requests are outside cache admission");
        return {};
    }

    bool Reply(ui64, std::unique_ptr<NActors::IEventBase>) override
    {
        UNIT_FAIL("channel replies are outside cache admission");
        return false;
    }

    void Close() override
    {
        if (!Closing.exchange(true)) {
            Closed.SetValue();
        }
    }

    NThreading::TFuture<void> WhenClosed() const override
    {
        return Closed.GetFuture();
    }
};

NProto::TCellsConfig MakeConfig()
{
    NProto::TCellsConfig config;
    config.SetCellId("cell-1");
    config.MutableIcV2()->SetHandshakeTimeoutMs(100);
    auto& peer = *config.AddCells();
    peer.SetCellId("cell-2");
    peer.SetIcV2Port(39602);
    peer.AddHosts()->SetFqdn("127.0.0.1");
    return config;
}

TSocketPtr MakeSocket()
{
    const int fd =
        socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    UNIT_ASSERT(fd >= 0);
    return MakeIntrusive<NInterconnect::TStreamSocket>(fd);
}

TChannelResult Wait(const TChannelFuture& future)
{
    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
    return future.GetValue();
}

struct TFixture
{
    std::shared_ptr<TTestConnector> Connector =
        std::make_shared<TTestConnector>();
    std::shared_ptr<TTestHandshake> Handshake =
        std::make_shared<TTestHandshake>();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    TVector<std::shared_ptr<TTestChannel>> Channels;
    std::function<void(TTestChannel&)> PrepareChannel;
    IPeerConnectionsPtr Peers;

    explicit TFixture(
        NProto::TCellsConfig config = MakeConfig(),
        ITaskQueuePtr completions = CreateTaskQueueStub())
    {
        auto [peers, error] = CreatePeerConnections(
            std::make_shared<NCells::TCellsConfig>(std::move(config)),
            Connector,
            Handshake,
            Timer,
            std::move(completions),
            [this](const THandshakeReady&) -> TChannelResult
            {
                auto channel = std::make_shared<TTestChannel>();
                Channels.push_back(channel);
                if (PrepareChannel) {
                    PrepareChannel(*channel);
                }
                return IRequestChannelPtr(std::move(channel));
            });
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Peers = std::move(peers);
    }

    ~TFixture()
    {
        Peers->Stop();
    }

    void Connect(size_t index = 0)
    {
        auto promise = Connector->Requests.at(index).Promise;
        promise.SetValue(MakeSocket());
    }

    void Hello(size_t index = 0)
    {
        auto request = Handshake->Requests.at(index);
        request.Promise.SetValue(THandshakeReady{request.Socket, {}});
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2PeerConnectionsTest)
{
    Y_UNIT_TEST(ConnectsWhenLocalXdcIsEnabled)
    {
        auto config = MakeConfig();
        config.MutableIcV2()->SetEnableXdc(true);
        TFixture f(std::move(config));
        f.Peers->Start();
        auto channel = f.Peers->GetChannel("cell-2");
        f.Connect();
        UNIT_ASSERT_VALUES_EQUAL(f.Handshake->Requests.size(), 1);
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(channel).GetError()));
    }

    Y_UNIT_TEST(ReportsHostChannelStateWithoutStartingAConnection)
    {
        TFixture f;
        f.Peers->Start();
        UNIT_ASSERT(!HasError(f.Peers->RegisterHost(
            "cell-2",
            1,
            {NInterconnect::TAddress("127.0.0.1", 1111)})));
        const auto idle = f.Peers->GetHostStatus("cell-2", 1);
        UNIT_ASSERT(idle.State == EHostChannelState::Idle);
        UNIT_ASSERT_VALUES_EQUAL(idle.ConnectionAttempts, 0);
        UNIT_ASSERT(f.Connector->Requests.empty());

        auto failed = f.Peers->GetHostChannel("cell-2", 1);
        const auto connecting = f.Peers->GetHostStatus("cell-2", 1);
        UNIT_ASSERT(connecting.State == EHostChannelState::Connecting);
        UNIT_ASSERT_VALUES_EQUAL(connecting.ConnectionAttempts, 1);
        f.Connector->Requests[0].Promise.SetValue(
            MakeError(E_IO, "connection refused"));
        UNIT_ASSERT_VALUES_EQUAL(Wait(failed).GetError().GetCode(), E_IO);
        const auto disconnected = f.Peers->GetHostStatus("cell-2", 1);
        UNIT_ASSERT(disconnected.State == EHostChannelState::Disconnected);
        UNIT_ASSERT_VALUES_EQUAL(disconnected.ConnectionFailures, 1);

        auto retry = f.Peers->GetHostChannel("cell-2", 1);
        f.Connect(1);
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(retry).GetError()));
        const auto connected = f.Peers->GetHostStatus("cell-2", 1);
        UNIT_ASSERT(connected.State == EHostChannelState::Connected);
        UNIT_ASSERT_VALUES_EQUAL(connected.ConnectionAttempts, 2);
        UNIT_ASSERT_VALUES_EQUAL(connected.ConnectionFailures, 1);

        f.Channels[0]->Close();
        UNIT_ASSERT(
            f.Peers->GetHostStatus("cell-2", 1).State ==
            EHostChannelState::Disconnected);
    }

    Y_UNIT_TEST(DataBindingsStayOnTheirHostAndIgnoreRetiredAttempts)
    {
        TFixture f;
        f.Peers->Start();
        UNIT_ASSERT(!HasError(f.Peers->RegisterHost(
            "cell-2",
            1,
            {NInterconnect::TAddress("127.0.0.1", 1111)})));
        UNIT_ASSERT(!HasError(f.Peers->RegisterHost(
            "cell-2",
            2,
            {NInterconnect::TAddress("127.0.0.1", 2222)})));
        auto retired = f.Peers->GetHostChannel("cell-2", 1);
        auto current = f.Peers->GetHostChannel("cell-2", 2);
        UNIT_ASSERT_VALUES_EQUAL(
            f.Connector->Requests[0].Address.GetPort(),
            1111);
        UNIT_ASSERT_VALUES_EQUAL(
            f.Connector->Requests[1].Address.GetPort(),
            2222);
        f.Peers->UnregisterHost("cell-2", 1);
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(retired).GetError().GetCode(),
            E_REJECTED);
        f.Connect(0);
        UNIT_ASSERT(f.Handshake->Requests.empty());
        f.Connect(1);
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(current).GetError()));
        UNIT_ASSERT(
            f.Peers->GetHostChannel("cell-2", 2).GetValue().GetResult() ==
            f.Channels[0]);
        UNIT_ASSERT(HasError(f.Peers->RegisterHost(
            "cell-2",
            1,
            {NInterconnect::TAddress("127.0.0.1", 3333)})));
        f.Peers->UnregisterHost("cell-2", 2);
        UNIT_ASSERT(f.Channels[0]->Closing);
    }

    Y_UNIT_TEST(UnregisterDuringHelloReleasesBindingQuota)
    {
        auto config = MakeConfig();
        config.MutableIcV2()->SetMaxOutgoingConnections(1);
        TFixture f(config);
        f.Peers->Start();
        UNIT_ASSERT(!HasError(f.Peers->RegisterHost(
            "cell-2",
            1,
            {NInterconnect::TAddress("127.0.0.1", 1111)})));
        UNIT_ASSERT_VALUES_EQUAL(
            f.Peers
                ->RegisterHost(
                    "cell-2",
                    2,
                    {NInterconnect::TAddress("127.0.0.1", 2222)})
                .GetCode(),
            E_REJECTED);
        auto retired = f.Peers->GetHostChannel("cell-2", 1);
        f.Connect();
        f.Peers->UnregisterHost("cell-2", 1);
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(retired).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT(!HasError(f.Peers->RegisterHost(
            "cell-2",
            2,
            {NInterconnect::TAddress("127.0.0.1", 2222)})));
        auto current = f.Peers->GetHostChannel("cell-2", 2);
        f.Hello();
        UNIT_ASSERT(f.Channels.empty());
        f.Connect(1);
        f.Hello(1);
        UNIT_ASSERT(!HasError(Wait(current).GetError()));
    }
    Y_UNIT_TEST(CoalescesConcurrentCallsAndReusesEstablishedChannel)
    {
        TFixture f;
        f.Peers->Start();
        std::array<std::future<TChannelFuture>, 32> callers;
        for (auto& caller: callers) {
            caller = std::async(
                std::launch::async,
                [&] { return f.Peers->GetChannel("cell-2"); });
        }
        TVector<TChannelFuture> results;
        for (auto& caller: callers) {
            results.push_back(caller.get());
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Connector->Requests.size(), 1);
        f.Connect();
        UNIT_ASSERT_VALUES_EQUAL(f.Handshake->Requests.size(), 1);
        f.Hello();
        for (const auto& result: results) {
            auto [channel, error] = Wait(result);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            UNIT_ASSERT(channel == f.Channels[0]);
        }
        UNIT_ASSERT(
            Wait(f.Peers->GetChannel("cell-2")).GetResult() == f.Channels[0]);
        UNIT_ASSERT_VALUES_EQUAL(f.Connector->Requests.size(), 1);
    }

    Y_UNIT_TEST(UsesOneDeadlineForTcpAndHello)
    {
        TFixture f;
        f.Peers->Start();
        auto result = f.Peers->GetChannel("cell-2");
        UNIT_ASSERT_VALUES_EQUAL(
            f.Connector->Requests[0].Timeout,
            TDuration::MilliSeconds(100));
        f.Timer->AdvanceTime(TDuration::MilliSeconds(70));
        f.Connect();
        UNIT_ASSERT_VALUES_EQUAL(
            f.Handshake->Requests[0].Timeout,
            TDuration::MilliSeconds(30));
        f.Timer->AdvanceTime(TDuration::MilliSeconds(30));
        f.Hello();
        UNIT_ASSERT_VALUES_EQUAL(Wait(result).GetError().GetCode(), E_TIMEOUT);
        UNIT_ASSERT(f.Channels.empty());
    }

    Y_UNIT_TEST(ActivationCannotExtendTheConnectionDeadline)
    {
        TFixture f;
        f.PrepareChannel = [&](TTestChannel& channel)
        {
            channel.OnActivate = [&]
            {
                f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
            };
        };
        f.Peers->Start();
        auto result = f.Peers->GetChannel("cell-2");
        f.Connect();
        f.Hello();
        UNIT_ASSERT_VALUES_EQUAL(Wait(result).GetError().GetCode(), E_TIMEOUT);
        UNIT_ASSERT(f.Channels[0]->Closing);
    }

    Y_UNIT_TEST(DoesNotStartHelloAfterTcpBudgetExpires)
    {
        TFixture f;
        f.Peers->Start();
        auto result = f.Peers->GetChannel("cell-2");
        f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
        f.Connect();
        UNIT_ASSERT(f.Handshake->Requests.empty());
        UNIT_ASSERT_VALUES_EQUAL(Wait(result).GetError().GetCode(), E_TIMEOUT);
    }

    Y_UNIT_TEST(ReconnectsAfterDisconnectOnlyOnANewCall)
    {
        TFixture f;
        f.Peers->Start();
        auto first = f.Peers->GetChannel("cell-2");
        f.Connect();
        f.Hello();
        auto channel = Wait(first).ExtractResult();
        channel->Close();
        UNIT_ASSERT_VALUES_EQUAL(f.Connector->Requests.size(), 1);
        auto next = f.Peers->GetChannel("cell-2");
        f.Connect(1);
        f.Hello(1);
        auto [replacement, error] = Wait(next);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT(replacement != channel);
    }

    Y_UNIT_TEST(EarlyDisconnectAndLateActivationDoNotReplaceNewGeneration)
    {
        TFixture f;
        f.Peers->Start();
        TChannelFuture next;
        f.PrepareChannel = [&](TTestChannel& channel)
        {
            if (f.Channels.size() == 1) {
                channel.OnActivate = [&]
                {
                    f.Channels[0]->Close();
                    next = f.Peers->GetChannel("cell-2");
                    f.Connect(1);
                    f.Hello(1);
                };
            }
        };
        auto first = f.Peers->GetChannel("cell-2");
        f.Connect();
        f.Hello();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(first).GetError().GetCode(),
            E_TRANSPORT_ERROR);
        UNIT_ASSERT(Wait(next).GetResult() == f.Channels[1]);
        UNIT_ASSERT(
            Wait(f.Peers->GetChannel("cell-2")).GetResult() == f.Channels[1]);
    }

    Y_UNIT_TEST(LateCloseCallbackCannotEvictNewGeneration)
    {
        TFixture f;
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        f.PrepareChannel = [&](TTestChannel& channel)
        {
            if (f.Channels.size() == 1) {
                channel.WhenClosed().Subscribe(
                    [entered, release](const auto&) mutable
                    {
                        entered.SetValue();
                        release.GetFuture().GetValueSync();
                    });
            }
        };
        f.Peers->Start();
        auto first = f.Peers->GetChannel("cell-2");
        f.Connect();
        f.Hello();
        auto channel = Wait(first).ExtractResult();
        auto closing =
            std::async(std::launch::async, [channel] { channel->Close(); });
        const bool delayed = entered.GetFuture().Wait(TDuration::Seconds(5));
        TChannelFuture next;
        if (delayed) {
            // The close future is ready, but the cache's close callback has
            // not run. A new generation can already replace the old channel.
            next = f.Peers->GetChannel("cell-2");
            if (f.Connector->Requests.size() == 2) {
                f.Connect(1);
                f.Hello(1);
            }
        }
        release.SetValue();
        closing.get();
        UNIT_ASSERT(delayed);
        UNIT_ASSERT(next.HasValue());
        UNIT_ASSERT(!HasError(next.GetValue().GetError()));
        auto reused = f.Peers->GetChannel("cell-2");
        UNIT_ASSERT(reused.HasValue());
        UNIT_ASSERT(
            reused.GetValue().GetResult() == next.GetValue().GetResult());
        UNIT_ASSERT_VALUES_EQUAL(f.Connector->Requests.size(), 2);
    }

    Y_UNIT_TEST(DoesNotPublishChannelUntilActivationReturns)
    {
        TFixture f;
        f.Peers->Start();
        TChannelFuture joined;
        f.PrepareChannel = [&](TTestChannel& channel)
        {
            channel.OnActivate = [&]
            {
                joined = f.Peers->GetChannel("cell-2");
                UNIT_ASSERT(!joined.HasValue());
            };
        };
        auto first = f.Peers->GetChannel("cell-2");
        f.Connect();
        f.Hello();
        UNIT_ASSERT(Wait(first).GetResult() == Wait(joined).GetResult());
    }

    Y_UNIT_TEST(StopRejectsAttemptsAndIgnoresLateConnectOrHello)
    {
        for (bool connected: {false, true}) {
            TFixture f;
            f.Peers->Start();
            auto result = f.Peers->GetChannel("cell-2");
            if (connected) {
                f.Connect();
            }
            f.Peers->Stop();
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(result).GetError().GetCode(),
                E_REJECTED);
            if (connected) {
                f.Hello();
            } else {
                f.Connect();
                UNIT_ASSERT(f.Handshake->Requests.empty());
            }
            UNIT_ASSERT(f.Channels.empty());
        }
    }

    Y_UNIT_TEST(LimitIncludesPendingConnectionsAndRecoversAfterFailure)
    {
        auto config = MakeConfig();
        config.MutableIcV2()->SetMaxOutgoingConnections(1);
        auto other = *config.MutableCells(0);
        other.SetCellId("cell-3");
        *config.AddCells() = std::move(other);
        TFixture f(std::move(config));
        f.Peers->Start();
        auto first = f.Peers->GetChannel("cell-2");
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Peers->GetChannel("cell-3")).GetError().GetCode(),
            E_REJECTED);
        f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
        f.Connect();
        UNIT_ASSERT_VALUES_EQUAL(Wait(first).GetError().GetCode(), E_TIMEOUT);
        auto second = f.Peers->GetChannel("cell-3");
        f.Connect(1);
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(second).GetError()));
    }

    Y_UNIT_TEST(LimitIncludesQueuedFailureCompletions)
    {
        auto queue = CreateThreadPool("ICV2PeerTest", 1);
        queue->Start();
        auto release = NThreading::NewPromise<void>();
        queue->ExecuteSimple([release] { release.GetFuture().GetValueSync(); });
        auto config = MakeConfig();
        config.MutableIcV2()->SetMaxOutgoingConnections(1);
        TFixture f(std::move(config), queue);
        f.Peers->Start();
        auto first = f.Peers->GetChannel("cell-2");
        auto promise = f.Connector->Requests[0].Promise;
        promise.SetValue(MakeError(E_IO, "connection refused"));
        const bool queued = !first.HasValue();
        const auto rejected = f.Peers->GetChannel("cell-2");
        const bool bounded = f.Connector->Requests.size() == 1;
        release.SetValue();
        UNIT_ASSERT_VALUES_EQUAL(Wait(first).GetError().GetCode(), E_IO);
        queue->Execute([] {}).GetValueSync();
        UNIT_ASSERT(queued);
        UNIT_ASSERT(bounded);
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(rejected).GetError().GetCode(),
            E_REJECTED);
        auto next = f.Peers->GetChannel("cell-2");
        f.Connect(1);
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(next).GetError()));
        f.Peers->Stop();
        queue->Stop();
    }

    Y_UNIT_TEST(RequestStopFromContinuationAndExternalStopWaitsForItsReturn)
    {
        auto queue = CreateThreadPool("ICV2PeerTest", 1);
        queue->Start();
        TFixture f(MakeConfig(), queue);
        f.Peers->Start();
        auto result = f.Peers->GetChannel("cell-2");
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        result.Subscribe(
            [&](const auto&)
            {
                f.Peers->RequestStop();
                entered.SetValue();
                release.GetFuture().GetValueSync();
            });
        f.Connect();
        f.Hello();
        const bool started = entered.GetFuture().Wait(TDuration::Seconds(5));
        auto stop = std::async(std::launch::async, [&] { f.Peers->Stop(); });
        const bool waits = stop.wait_for(30ms) == std::future_status::timeout;
        release.SetValue();
        stop.get();
        queue->Stop();
        UNIT_ASSERT(started);
        UNIT_ASSERT(waits);
        UNIT_ASSERT(f.Channels[0]->Closing);
    }

    Y_UNIT_TEST(RejectsCallsBeforeStartAfterStopAndWithoutEndpoint)
    {
        auto config = MakeConfig();
        config.MutableCells(0)->ClearIcV2Port();
        TFixture f(std::move(config));
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Peers->GetChannel("cell-2")).GetError().GetCode(),
            E_REJECTED);
        f.Peers->Start();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Peers->GetChannel("cell-2")).GetError().GetCode(),
            E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Peers->GetChannel("cell-unknown")).GetError().GetCode(),
            E_ARGUMENT);
        f.Peers->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Peers->GetChannel("cell-2")).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT(f.Connector->Requests.empty());
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
