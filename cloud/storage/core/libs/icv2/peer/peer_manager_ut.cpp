#include "peer_manager.h"

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <array>
#include <atomic>
#include <future>
#include <mutex>
#include <thread>

namespace NCloud::NIcV2 {

using NCloud::NIcV2::IRequestChannel;
using NCloud::NIcV2::IRequestChannelPtr;
using NCloud::NIcV2::TResponseFuture;

using NCloud::NIcV2::IHandshakeProvider;
using NCloud::NIcV2::TAcceptHandshakeFuture;
using NCloud::NIcV2::TConnectHandshakeFuture;
using NCloud::NIcV2::TConnectHandshakeResult;
using NCloud::NIcV2::THandshakeReady;

using NCloud::NIcV2::TSocketPtr;

using NCloud::NIcV2::IConnector;

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

struct TTestHandshake final: IHandshakeProvider
{
    struct TRequest
    {
        TSocketPtr Socket;
        TDuration Timeout;
        NThreading::TPromise<TConnectHandshakeResult> Promise =
            NThreading::NewPromise<TConnectHandshakeResult>();
    };

    TVector<TRequest> Requests;

    void Start() override
    {}

    void Stop() override
    {}

    void RequestStop() override
    {}

    TConnectHandshakeFuture Connect(
        TSocketPtr socket,
        TDuration timeout) override
    {
        auto& request = Requests.emplace_back(std::move(socket), timeout);
        return request.Promise.GetFuture();
    }

    TAcceptHandshakeFuture Accept(TSocketPtr socket, TDuration timeout) override
    {
        Y_UNUSED(socket);
        Y_UNUSED(timeout);

        UNIT_FAIL("outgoing connections must initiate hello");
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestResolver final: NCloud::NIcV2::IAddressResolver
{
    struct TRequest
    {
        TString Host;
        ui16 Port;
        TMonotonic Deadline;
        NThreading::TPromise<NCloud::NIcV2::TResolveResult> Promise =
            NThreading::NewPromise<NCloud::NIcV2::TResolveResult>();
    };

    bool Deferred = false;
    TVector<TRequest> Requests;

    NCloud::NIcV2::TResolveFuture
    Resolve(const TString& host, ui16 port, TMonotonic deadline) override
    {
        auto& request = Requests.emplace_back(host, port, deadline);
        auto future = request.Promise.GetFuture();
        if (!Deferred) {
            request.Promise.SetValue(
                TVector<NInterconnect::TAddress>{
                    NInterconnect::TAddress("127.0.0.1", port),
                });
        }
        return future;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestHostHandler final: IClientEndpointHandler
{
    std::atomic<size_t> Disconnects = 0;
    std::atomic<size_t> Connects = 0;
    std::atomic<size_t> Unavailable = 0;
    std::function<void()> OnConnected;
    std::function<void()> OnDisconnected;

    void HandleConnected() noexcept override
    {
        ++Connects;
        if (OnConnected) {
            OnConnected();
        }
    }

    void HandleUnavailable() noexcept override
    {
        ++Unavailable;
    }

    void HandleDisconnected() noexcept override
    {
        ++Disconnects;
        if (OnDisconnected) {
            OnDisconnected();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestChannel final: IRequestChannel
{
    NThreading::TPromise<NProto::TError> Closed =
        NThreading::NewPromise<NProto::TError>();
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
        std::unique_ptr<NActors::IEventBase> request,
        TDuration timeout) override
    {
        Y_UNUSED(request);
        Y_UNUSED(timeout);

        UNIT_FAIL("channel requests are outside cache admission");
        return {};
    }

    void Close() override
    {
        if (!Closing.exchange(true)) {
            Closed.SetValue(NProto::TError{});
        }
    }

    NThreading::TFuture<NProto::TError> WhenClosed() const override
    {
        return Closed.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestChannelFactory final: IChannelFactory
{
    TVector<std::shared_ptr<TTestChannel>> Channels;
    std::function<void(TTestChannel&)> PrepareChannel;

    TChannelResult CreateChannel(const THandshakeReady& connection) override
    {
        Y_UNUSED(connection);

        auto channel = std::make_shared<TTestChannel>();
        Channels.push_back(channel);
        if (PrepareChannel) {
            PrepareChannel(*channel);
        }
        return IRequestChannelPtr(std::move(channel));
    }
};

TPeerConnectionManagerConfig MakeConfig()
{
    return {
        .ConnectTimeout = TDuration::MilliSeconds(100),
        .MaxConnections = 128,
    };
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

////////////////////////////////////////////////////////////////////////////////

struct TFixture
{
    std::shared_ptr<TTestConnector> Connector =
        std::make_shared<TTestConnector>();
    std::shared_ptr<TTestHandshake> Handshake =
        std::make_shared<TTestHandshake>();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>(Timer->Now());
    std::shared_ptr<TTestResolver> Resolver = std::make_shared<TTestResolver>();
    const std::shared_ptr<TTestChannelFactory> ChannelFactory =
        std::make_shared<TTestChannelFactory>();
    IPeerConnectionManagerPtr Peers;

    explicit TFixture(
        TPeerConnectionManagerConfig config = MakeConfig(),
        ITaskQueuePtr completions = CreateTaskQueueStub())
    {
        auto [peers, error] = CreatePeerConnectionManager(
            config,
            Connector,
            Handshake,
            Timer,
            Scheduler,
            Resolver,
            std::move(completions),
            ChannelFactory);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Peers = std::move(peers);
    }

    ~TFixture()
    {
        Peers->Stop();
    }

    IPeerPtr RegisterPeer(
        TString host,
        ui16 port,
        IClientEndpointHandlerPtr handler) const
    {
        auto [peer, error] =
            Peers->RegisterPeer(std::move(host), port, std::move(handler));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        return peer;
    }

    void Retry() const
    {
        Timer->AdvanceTime(TDuration::Seconds(1));
        Scheduler->AdvanceTime(TDuration::Seconds(1));
        Scheduler->RunAllScheduledTasksUntilNow();
    }

    void Connect(size_t index = 0) const
    {
        auto promise = Connector->Requests.at(index).Promise;
        promise.SetValue(MakeSocket());
    }

    void Hello(size_t index = 0) const
    {
        auto request = Handshake->Requests.at(index);
        request.Promise.SetValue(
            THandshakeReady{.Socket = request.Socket, .XdcSocket = {}});
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2PeerConnectionManagerTest)
{
    Y_UNIT_TEST(ConnectsRegisteredPeer)
    {
        TFixture f;
        f.Peers->Start();
        const auto peer = f.RegisterPeer("127.0.0.1", 39602, {});
        auto channel = peer->GetChannel();
        f.Connect();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Handshake->Requests.size());
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(channel).GetError()));
    }

    Y_UNIT_TEST(ReportsHostChannelStateAndRetriesWithoutIo)
    {
        TFixture f;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto peer = f.RegisterPeer("selected-host", 1111, handler);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Resolver->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL("selected-host", f.Resolver->Requests[0].Host);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
        UNIT_ASSERT(peer->GetStatus().State == EPeerState::Connecting);
        auto failed = peer->GetChannel();
        f.Connector->Requests[0].Promise.SetValue(
            MakeError(E_IO, "connection refused"));
        UNIT_ASSERT_VALUES_EQUAL(E_IO, Wait(failed).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Unavailable.load());
        UNIT_ASSERT_VALUES_EQUAL(0, handler->Disconnects.load());
        UNIT_ASSERT_VALUES_EQUAL(1, peer->GetStatus().ConnectionFailures);
        UNIT_ASSERT_VALUES_EQUAL(
            E_UNAVAILABLE,
            Wait(peer->GetChannel()).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
        f.Retry();
        UNIT_ASSERT_VALUES_EQUAL(2, f.Connector->Requests.size());
        f.Connect(1);
        f.Hello();
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Connects.load());
        UNIT_ASSERT(peer->GetStatus().State == EPeerState::Connected);
        f.ChannelFactory->Channels[0]->Close();
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Disconnects.load());
        f.Retry();
        UNIT_ASSERT_VALUES_EQUAL(3, f.Connector->Requests.size());
    }

    Y_UNIT_TEST(ReportsRepeatedDnsFailuresAndRecoversWithoutIo)
    {
        TFixture f;
        f.Resolver->Deferred = true;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto registered = f.RegisterPeer("host", 1111, handler);
        UNIT_ASSERT(f.Connector->Requests.empty());
        f.Resolver->Requests[0].Promise.SetValue(
            MakeError(E_UNAVAILABLE, "DNS failed"));
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Unavailable.load());
        f.Retry();
        UNIT_ASSERT_VALUES_EQUAL(2, f.Resolver->Requests.size());
        f.Resolver->Requests[1].Promise.SetValue(
            MakeError(E_UNAVAILABLE, "DNS still failed"));
        UNIT_ASSERT_VALUES_EQUAL(2, handler->Unavailable.load());
        f.Retry();
        f.Resolver->Requests[2].Promise.SetValue(
            TVector<NInterconnect::TAddress>{
                NInterconnect::TAddress("127.0.0.1", 1111)});
        f.Connect();
        f.Hello();
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Connects.load());
    }

    Y_UNIT_TEST(PendingDnsDoesNotBlockAnotherBindingAndRetiredResultIsIgnored)
    {
        TFixture f;
        f.Resolver->Deferred = true;
        auto retired = std::make_shared<TTestHostHandler>();
        auto current = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto peer = f.RegisterPeer("slow-host", 1111, retired);
        const auto registered = f.RegisterPeer("other-host", 2222, current);
        UNIT_ASSERT_VALUES_EQUAL(2, f.Resolver->Requests.size());
        f.Resolver->Requests[1].Promise.SetValue(
            TVector<NInterconnect::TAddress>{
                NInterconnect::TAddress("127.0.0.1", 2222)});
        f.Connect();
        f.Hello();
        UNIT_ASSERT_VALUES_EQUAL(1, current->Connects.load());
        peer->Close();
        f.Resolver->Requests[0].Promise.SetValue(
            TVector<NInterconnect::TAddress>{
                NInterconnect::TAddress("127.0.0.1", 1111)});
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(0, retired->Connects.load());
        UNIT_ASSERT_VALUES_EQUAL(0, retired->Unavailable.load());
    }

    Y_UNIT_TEST(RegistrationsAreIndependent)
    {
        TFixture f;
        f.Peers->Start();
        const auto first = f.RegisterPeer("host", 1111, {});
        const auto second = f.RegisterPeer("host", 2222, {});
        UNIT_ASSERT(bool(first));
        UNIT_ASSERT(bool(second));
        UNIT_ASSERT(first != second);
        first->Close();
        const auto third = f.RegisterPeer("host", 3333, {});
        UNIT_ASSERT(bool(third));
        UNIT_ASSERT(third != first);
        UNIT_ASSERT(third != second);
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            Wait(first->GetChannel()).GetError().GetCode());
    }

    Y_UNIT_TEST(ReleasingLastPeerCancelsAttemptAndReleasesQuota)
    {
        auto config = MakeConfig();
        config.MaxConnections = 1;
        TFixture f(config);
        f.Peers->Start();
        auto peer = f.RegisterPeer("host", 1111, {});
        auto pending = peer->GetChannel();
        auto retained = peer;
        peer.reset();
        UNIT_ASSERT(!pending.HasValue());
        retained.reset();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(pending).GetError().GetCode());
        const auto replacement = f.RegisterPeer("other-host", 2222, {});
        replacement->Close();
        replacement->Close();
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            Wait(replacement->GetChannel()).GetError().GetCode());
    }

    Y_UNIT_TEST(UnregisterCancelsScheduledReconnect)
    {
        TFixture f;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto peer = f.RegisterPeer("host", 1111, handler);
        f.Connector->Requests[0].Promise.SetValue(MakeError(E_IO, "failed"));
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Unavailable.load());
        peer->Close();
        f.Retry();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Resolver->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
    }

    Y_UNIT_TEST(SerializesReentrantHostNotificationsOutsideCacheLock)
    {
        IPeerPtr peer;
        TFixture f;
        auto handler = std::make_shared<TTestHostHandler>();
        bool connectedCallback = false;
        bool nested = false;
        handler->OnConnected = [&]
        {
            connectedCallback = true;
            Y_UNUSED(peer->GetStatus());
            f.ChannelFactory->Channels[0]->Close();
            connectedCallback = false;
        };
        handler->OnDisconnected = [&]
        {
            nested = connectedCallback;
        };
        f.Peers->Start();
        peer = f.RegisterPeer("host", 1111, handler);
        f.Connect();
        f.Hello();
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Connects.load());
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Disconnects.load());
        UNIT_ASSERT(!nested);
    }

    Y_UNIT_TEST(ShutdownIgnoresResolutionCompletingAfterRequestStop)
    {
        TFixture f;
        f.Resolver->Deferred = true;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto registered = f.RegisterPeer("host", 1111, handler);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Resolver->Requests.size());
        auto resolved = f.Resolver->Requests[0].Promise;
        Y_DEFER
        {
            resolved.TrySetValue(
                MakeError(E_UNAVAILABLE, "lookup interrupted"));
        };

        f.Peers->RequestStop();
        resolved.SetValue(
            TVector<NInterconnect::TAddress>{
                NInterconnect::TAddress("127.0.0.1", 1111)});
        f.Peers->Stop();
        f.Retry();
        UNIT_ASSERT(f.Connector->Requests.empty());
        UNIT_ASSERT_VALUES_EQUAL(0, handler->Connects.load());
        UNIT_ASSERT_VALUES_EQUAL(0, handler->Unavailable.load());
    }

    Y_UNIT_TEST(NotifiesHostAboutDisconnectedChannel)
    {
        TFixture f;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto peer = f.RegisterPeer("127.0.0.1", 1111, handler);

        auto connected = peer->GetChannel();
        f.Connect();
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(connected).GetError()));
        f.ChannelFactory->Channels[0]->Close();
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Disconnects.load());
        f.Retry();

        auto reconnected = peer->GetChannel();
        f.Connect(1);
        f.Hello(1);
        UNIT_ASSERT(!HasError(Wait(reconnected).GetError()));
        peer->Close();
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Disconnects.load());
    }

    Y_UNIT_TEST(DataBindingsStayOnTheirHostAndIgnoreRetiredAttempts)
    {
        TFixture f;
        f.Peers->Start();
        const auto firstPeer = f.RegisterPeer("127.0.0.1", 1111, {});
        const auto secondPeer = f.RegisterPeer("127.0.0.1", 2222, {});
        auto retired = firstPeer->GetChannel();
        auto current = secondPeer->GetChannel();
        UNIT_ASSERT_VALUES_EQUAL(
            1111,
            f.Connector->Requests[0].Address.GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            2222,
            f.Connector->Requests[1].Address.GetPort());
        firstPeer->Close();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(retired).GetError().GetCode());
        f.Connect(0);
        UNIT_ASSERT(f.Handshake->Requests.empty());
        f.Connect(1);
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(current).GetError()));
        UNIT_ASSERT(
            secondPeer->GetChannel().GetValue().GetResult() ==
            f.ChannelFactory->Channels[0]);
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            Wait(firstPeer->GetChannel()).GetError().GetCode());
        secondPeer->Close();
        UNIT_ASSERT(f.ChannelFactory->Channels[0]->Closing);
    }

    Y_UNIT_TEST(UnregisterDuringHelloReleasesBindingQuota)
    {
        auto config = MakeConfig();
        config.MaxConnections = 1;
        TFixture f(config);
        f.Peers->Start();
        const auto firstPeer = f.RegisterPeer("127.0.0.1", 1111, {});
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            f.Peers->RegisterPeer("127.0.0.1", 2222, {}).GetError().GetCode());
        auto retired = firstPeer->GetChannel();
        f.Connect();
        firstPeer->Close();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(retired).GetError().GetCode());
        const auto secondPeer = f.RegisterPeer("127.0.0.1", 2222, {});
        auto current = secondPeer->GetChannel();
        f.Hello();
        UNIT_ASSERT(f.ChannelFactory->Channels.empty());
        f.Connect(1);
        f.Hello(1);
        UNIT_ASSERT(!HasError(Wait(current).GetError()));
    }
    Y_UNIT_TEST(CoalescesConcurrentCallsAndReusesEstablishedChannel)
    {
        TFixture f;
        f.Peers->Start();
        const auto peer = f.RegisterPeer("127.0.0.1", 39602, {});
        std::array<std::future<TChannelFuture>, 32> callers;
        for (auto& caller: callers) {
            caller = std::async(
                std::launch::async,
                [&] { return peer->GetChannel(); });
        }
        TVector<TChannelFuture> results;
        for (auto& caller: callers) {
            results.push_back(caller.get());
        }
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
        f.Connect();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Handshake->Requests.size());
        f.Hello();
        for (const auto& result: results) {
            auto [channel, error] = Wait(result);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            UNIT_ASSERT(channel == f.ChannelFactory->Channels[0]);
        }
        UNIT_ASSERT(
            Wait(peer->GetChannel()).GetResult() ==
            f.ChannelFactory->Channels[0]);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
    }

    Y_UNIT_TEST(UsesOneDeadlineForTcpAndHello)
    {
        TFixture f;
        f.Peers->Start();
        const auto peer = f.RegisterPeer("127.0.0.1", 39602, {});
        auto result = peer->GetChannel();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            f.Connector->Requests[0].Timeout);
        f.Timer->AdvanceTime(TDuration::MilliSeconds(70));
        f.Connect();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(30),
            f.Handshake->Requests[0].Timeout);
        f.Timer->AdvanceTime(TDuration::MilliSeconds(30));
        f.Hello();
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, Wait(result).GetError().GetCode());
        UNIT_ASSERT(f.ChannelFactory->Channels.empty());
    }

    Y_UNIT_TEST(ActivationCannotExtendTheConnectionDeadline)
    {
        TFixture f;
        f.ChannelFactory->PrepareChannel = [&](TTestChannel& channel)
        {
            channel.OnActivate = [&]
            {
                f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
            };
        };
        f.Peers->Start();
        const auto peer = f.RegisterPeer("127.0.0.1", 39602, {});
        auto result = peer->GetChannel();
        f.Connect();
        f.Hello();
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, Wait(result).GetError().GetCode());
        UNIT_ASSERT(f.ChannelFactory->Channels[0]->Closing);
    }

    Y_UNIT_TEST(DoesNotStartHelloAfterTcpBudgetExpires)
    {
        TFixture f;
        f.Peers->Start();
        const auto peer = f.RegisterPeer("127.0.0.1", 39602, {});
        auto result = peer->GetChannel();
        f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
        f.Connect();
        UNIT_ASSERT(f.Handshake->Requests.empty());
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, Wait(result).GetError().GetCode());
    }

    Y_UNIT_TEST(ReconnectsAfterDisconnectInBackground)
    {
        TFixture f;
        f.Peers->Start();
        const auto peer = f.RegisterPeer("127.0.0.1", 39602, {});
        auto first = peer->GetChannel();
        f.Connect();
        f.Hello();
        auto channel = Wait(first).ExtractResult();
        channel->Close();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
        f.Retry();
        auto next = peer->GetChannel();
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
        const auto peer = f.RegisterPeer("127.0.0.1", 39602, {});
        TChannelFuture next;
        f.ChannelFactory->PrepareChannel = [&](TTestChannel& channel)
        {
            if (f.ChannelFactory->Channels.size() == 1) {
                channel.OnActivate = [&]
                {
                    f.ChannelFactory->Channels[0]->Close();
                    f.Retry();
                    next = peer->GetChannel();
                    f.Connect(1);
                    f.Hello(1);
                };
            }
        };
        auto first = peer->GetChannel();
        f.Connect();
        f.Hello();
        UNIT_ASSERT_VALUES_EQUAL(
            E_TRANSPORT_ERROR,
            Wait(first).GetError().GetCode());
        UNIT_ASSERT(Wait(next).GetResult() == f.ChannelFactory->Channels[1]);
        UNIT_ASSERT(
            Wait(peer->GetChannel()).GetResult() ==
            f.ChannelFactory->Channels[1]);
    }

    Y_UNIT_TEST(LateCloseCallbackCannotEvictNewGeneration)
    {
        TFixture f;
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        f.ChannelFactory->PrepareChannel = [&](TTestChannel& channel)
        {
            if (f.ChannelFactory->Channels.size() == 1) {
                channel.WhenClosed().Subscribe(
                    [entered, release](const auto&) mutable
                    {
                        entered.SetValue();
                        release.GetFuture().GetValueSync();
                    });
            }
        };
        f.Peers->Start();
        const auto peer = f.RegisterPeer("127.0.0.1", 39602, {});
        auto first = peer->GetChannel();
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
            next = peer->GetChannel();
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
        auto reused = peer->GetChannel();
        UNIT_ASSERT(reused.HasValue());
        UNIT_ASSERT(
            reused.GetValue().GetResult() == next.GetValue().GetResult());
        UNIT_ASSERT_VALUES_EQUAL(2, f.Connector->Requests.size());
    }

    Y_UNIT_TEST(DoesNotPublishChannelUntilActivationReturns)
    {
        TFixture f;
        f.Peers->Start();
        const auto peer = f.RegisterPeer("127.0.0.1", 39602, {});
        TChannelFuture joined;
        f.ChannelFactory->PrepareChannel = [&](TTestChannel& channel)
        {
            channel.OnActivate = [&]
            {
                joined = peer->GetChannel();
                UNIT_ASSERT(!joined.HasValue());
            };
        };
        auto first = peer->GetChannel();
        f.Connect();
        f.Hello();
        UNIT_ASSERT(Wait(first).GetResult() == Wait(joined).GetResult());
    }

    Y_UNIT_TEST(StopRejectsAttemptsAndIgnoresLateConnectOrHello)
    {
        for (bool connected: {false, true}) {
            TFixture f;
            f.Peers->Start();
            const auto peer = f.RegisterPeer("127.0.0.1", 39602, {});
            auto result = peer->GetChannel();
            if (connected) {
                f.Connect();
            }
            f.Peers->Stop();
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                Wait(result).GetError().GetCode());
            if (connected) {
                f.Hello();
            } else {
                f.Connect();
                UNIT_ASSERT(f.Handshake->Requests.empty());
            }
            UNIT_ASSERT(f.ChannelFactory->Channels.empty());
        }
    }

    Y_UNIT_TEST(LimitIncludesRegisteredPeersAndRecoversAfterUnregister)
    {
        auto config = MakeConfig();
        config.MaxConnections = 1;
        TFixture f(config);
        f.Peers->Start();
        const auto first = f.RegisterPeer("host", 1111, {});
        auto pending = first->GetChannel();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            f.Peers->RegisterPeer("host", 2222, {}).GetError().GetCode());
        first->Close();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(pending).GetError().GetCode());
        const auto second = f.RegisterPeer("host", 2222, {});
        auto connected = second->GetChannel();
        f.Connect(0);
        UNIT_ASSERT(f.Handshake->Requests.empty());
        f.Connect(1);
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(connected).GetError()));
    }

    Y_UNIT_TEST(LimitIncludesQueuedFailureCompletions)
    {
        auto queue = CreateThreadPool("ICV2PeerTest", 1);
        queue->Start();
        auto release = NThreading::NewPromise<void>();
        queue->ExecuteSimple([release] { release.GetFuture().GetValueSync(); });
        auto config = MakeConfig();
        config.MaxConnections = 1;
        TFixture f(config, queue);
        f.Peers->Start();
        const auto firstPeer = f.RegisterPeer("host", 1111, {});
        auto first = firstPeer->GetChannel();
        f.Connector->Requests[0].Promise.SetValue(
            MakeError(E_IO, "connection refused"));
        const bool queued = !first.HasValue();
        firstPeer->Close();
        const auto secondPeer = f.RegisterPeer("host", 2222, {});
        const bool bounded = f.Connector->Requests.size() == 1;
        release.SetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_IO, Wait(first).GetError().GetCode());
        queue->Execute([] {}).GetValueSync();
        UNIT_ASSERT(queued);
        UNIT_ASSERT(bounded);
        f.Retry();
        auto next = secondPeer->GetChannel();
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
        const auto peer = f.RegisterPeer("127.0.0.1", 39602, {});
        auto result = peer->GetChannel();
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
        auto otherStop =
            std::async(std::launch::async, [&] { f.Peers->Stop(); });
        const bool waits = stop.wait_for(30ms) == std::future_status::timeout;
        const bool otherWaits =
            otherStop.wait_for(30ms) == std::future_status::timeout;
        release.SetValue();
        stop.get();
        otherStop.get();
        queue->Stop();
        UNIT_ASSERT(started);
        UNIT_ASSERT(waits);
        UNIT_ASSERT(otherWaits);
        UNIT_ASSERT(f.ChannelFactory->Channels[0]->Closing);
    }

    Y_UNIT_TEST(RejectsCallsBeforeStartAfterStopAndUnknownPeers)
    {
        TFixture f;
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            f.Peers->RegisterPeer("host", 1111, {}).GetError().GetCode());
        f.Peers->Start();
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            f.Peers->RegisterPeer("", 1111, {}).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            f.Peers->RegisterPeer("host", 0, {}).GetError().GetCode());
        const auto peer = f.RegisterPeer("host", 1111, {});
        f.Peers->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(peer->GetChannel()).GetError().GetCode());
    }
}

}   // namespace NCloud::NIcV2
