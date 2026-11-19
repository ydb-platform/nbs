#include "peer_manager.h"

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>

#include <array>
#include <atomic>
#include <future>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace NCloud::NIcV2 {

using namespace NThreading;

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
        TPromise<TSocketResult> Promise = NewPromise<TSocketResult>();
    };

    bool ThrowOnConnect = false;
    TSocketFuture ConnectResult;
    std::function<void()> OnStop;
    std::mutex Mutex;
    TVector<TRequest> Requests;

    void Start() override
    {}

    void Stop() override
    {
        if (OnStop) {
            OnStop();
        }
    }

    void RequestStop() override
    {}

    TSocketFuture Connect(
        const NInterconnect::TAddress& address,
        TDuration timeout) override
    {
        if (ThrowOnConnect) {
            throw std::runtime_error("connect failed");
        }
        std::lock_guard guard(Mutex);
        auto& request = Requests.emplace_back(address, timeout);
        return ConnectResult.Initialized() ? ConnectResult
                                           : request.Promise.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestHandshake final: IHandshakeProvider
{
    struct TRequest
    {
        TSocketPtr Socket;
        TDuration Timeout;
        TPromise<TConnectHandshakeResult> Promise =
            NewPromise<TConnectHandshakeResult>();
    };

    bool ThrowOnConnect = false;
    TConnectHandshakeFuture ConnectResult;
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
        if (ThrowOnConnect) {
            throw std::runtime_error("hello failed");
        }
        auto& request = Requests.emplace_back(std::move(socket), timeout);
        return ConnectResult.Initialized() ? ConnectResult
                                           : request.Promise.GetFuture();
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
        TPromise<NCloud::NIcV2::TResolveResult> Promise =
            NewPromise<NCloud::NIcV2::TResolveResult>();
    };

    bool Deferred = false;
    bool ThrowOnResolve = false;
    TVector<TRequest> Requests;

    NCloud::NIcV2::TResolveFuture
    Resolve(const TString& host, ui16 port, TMonotonic deadline) override
    {
        if (ThrowOnResolve) {
            throw std::runtime_error("resolve failed");
        }
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

struct TTestCompletionQueue final: ITaskQueue
{
    bool ThrowOnEnqueue = false;

    void Start() override
    {}

    void Stop() override
    {}

    void Enqueue(ITaskPtr task) override
    {
        if (ThrowOnEnqueue) {
            throw std::runtime_error("enqueue failed");
        }
        task->Execute();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TThrowingScheduler final: IScheduler
{
    void Start() override
    {}

    void Stop() override
    {}

    void Schedule(
        ITaskQueue* taskQueue,
        TInstant deadline,
        TCallback callback) override
    {
        Y_UNUSED(taskQueue);
        Y_UNUSED(deadline);
        Y_UNUSED(callback);

        throw std::runtime_error("schedule failed");
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
    std::function<void()> OnUnavailable;
    std::function<void()> OnDestroyed;

    ~TTestHostHandler() override
    {
        if (OnDestroyed) {
            OnDestroyed();
        }
    }

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
        if (OnUnavailable) {
            OnUnavailable();
        }
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
    TPromise<NProto::TError> Closed = NewPromise<NProto::TError>();
    std::atomic<bool> Closing = false;
    std::function<void()> OnActivate;
    std::function<void()> OnClose;
    std::function<void()> OnWhenClosed;

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
            if (OnClose) {
                OnClose();
            }
            Closed.SetValue(NProto::TError{});
        }
    }

    TFuture<NProto::TError> WhenClosed() const override
    {
        if (OnWhenClosed) {
            OnWhenClosed();
        }
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

TPeerManagerConfig MakeConfig()
{
    return {
        .ConnectTimeout = TDuration::MilliSeconds(100),
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
    IPeerManagerPtr Peers;

    explicit TFixture(
        TPeerManagerConfig config = MakeConfig(),
        ITaskQueuePtr completions = CreateTaskQueueStub(),
        ISchedulerPtr scheduler = {})
    {
        auto [peers, error] = CreatePeerManager(
            config,
            Connector,
            Handshake,
            Timer,
            scheduler ? std::move(scheduler) : Scheduler,
            Resolver,
            std::move(completions),
            ChannelFactory);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Peers = std::move(peers);
    }

    ~TFixture()
    {
        Peers->RequestStop();
        for (auto& request: Resolver->Requests) {
            request.Promise.TrySetValue(
                MakeError(E_CANCELLED, "DNS lookup cancelled"));
        }
        Peers->Stop();
    }

    IPeerPtr
    CreatePeer(TString host, ui16 port, IClientEndpointHandlerPtr handler) const
    {
        auto [peer, error] =
            Peers->CreatePeer(std::move(host), port, std::move(handler));
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

Y_UNIT_TEST_SUITE(TIcV2PeerManagerTest)
{
    Y_UNIT_TEST(ConnectsCreatedPeer)
    {
        TFixture f;
        f.Peers->Start();
        const auto peer = f.CreatePeer("127.0.0.1", 39602, {});
        auto channel = peer->GetChannel();
        f.Connect();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Handshake->Requests.size());
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(channel).GetError()));
    }

    Y_UNIT_TEST(ResolverExceptionFailsAttemptAndAllowsRetry)
    {
        TFixture f;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Resolver->ThrowOnResolve = true;
        f.Peers->Start();
        const auto peer = f.CreatePeer("host", 1111, handler);
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Unavailable.load());
        UNIT_ASSERT_VALUES_EQUAL(1, peer->GetStatus().ConnectionFailures);

        f.Resolver->ThrowOnResolve = false;
        f.Retry();
        const auto pending = peer->GetChannel();
        f.Connect();
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(pending).GetError()));
    }

    Y_UNIT_TEST(ConnectionStepExceptionsFailAttemptAndAllowRetry)
    {
        enum class EStep
        {
            Connect,
            Hello,
            Channel,
            Activate,
        };

        for (const auto step:
             {EStep::Connect, EStep::Hello, EStep::Channel, EStep::Activate})
        {
            TFixture f;
            auto handler = std::make_shared<TTestHostHandler>();
            f.Resolver->Deferred = true;
            f.Connector->ThrowOnConnect = step == EStep::Connect;
            f.Handshake->ThrowOnConnect = step == EStep::Hello;
            f.ChannelFactory->PrepareChannel = [step](TTestChannel& channel)
            {
                if (step == EStep::Channel) {
                    throw std::runtime_error("create channel failed");
                }
                if (step == EStep::Activate) {
                    channel.OnActivate = []
                    {
                        throw std::runtime_error("activate failed");
                    };
                }
            };
            f.Peers->Start();
            const auto peer = f.CreatePeer("host", 1111, handler);
            const auto pending = peer->GetChannel();
            f.Resolver->Requests.at(0).Promise.SetValue(
                TVector<NInterconnect::TAddress>{
                    NInterconnect::TAddress("127.0.0.1", 1111)});
            if (step != EStep::Connect) {
                f.Connect();
            }
            if (step == EStep::Channel || step == EStep::Activate) {
                f.Hello();
            }
            UNIT_ASSERT_VALUES_EQUAL(
                E_FAIL,
                Wait(pending).GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, handler->Unavailable.load());
            UNIT_ASSERT_VALUES_EQUAL(1, peer->GetStatus().ConnectionFailures);

            f.Connector->ThrowOnConnect = false;
            f.Handshake->ThrowOnConnect = false;
            f.ChannelFactory->PrepareChannel = {};
            const auto connectIndex = f.Connector->Requests.size();
            const auto helloIndex = f.Handshake->Requests.size();
            f.Retry();
            const auto retry = peer->GetChannel();
            f.Resolver->Requests.at(1).Promise.SetValue(
                TVector<NInterconnect::TAddress>{
                    NInterconnect::TAddress("127.0.0.1", 1111)});
            f.Connect(connectIndex);
            f.Hello(helloIndex);
            UNIT_ASSERT(!HasError(Wait(retry).GetError()));
        }
    }

    Y_UNIT_TEST(ExceptionalConnectionFuturesFailAttempt)
    {
        enum class EStep
        {
            Resolve,
            Connect,
            Hello,
        };

        for (const auto step: {EStep::Resolve, EStep::Connect, EStep::Hello}) {
            TFixture f;
            f.Resolver->Deferred = true;
            f.Peers->Start();
            const auto peer = f.CreatePeer("host", 1111, {});
            const auto pending = peer->GetChannel();
            const auto exception =
                std::make_exception_ptr(std::runtime_error("future failed"));
            if (step == EStep::Resolve) {
                f.Resolver->Requests.at(0).Promise.SetException(exception);
            } else {
                f.Resolver->Requests.at(0).Promise.SetValue(
                    TVector<NInterconnect::TAddress>{
                        NInterconnect::TAddress("127.0.0.1", 1111)});
                if (step == EStep::Connect) {
                    f.Connector->Requests.at(0).Promise.SetException(exception);
                } else {
                    f.Connect();
                    f.Handshake->Requests.at(0).Promise.SetException(exception);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(
                E_FAIL,
                Wait(pending).GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, peer->GetStatus().ConnectionFailures);
        }
    }

    Y_UNIT_TEST(SchedulerExceptionClosesPeerAndPreservesResult)
    {
        for (const bool connected: {false, true}) {
            TFixture f(
                MakeConfig(),
                CreateTaskQueueStub(),
                std::make_shared<TThrowingScheduler>());
            f.Peers->Start();
            const auto peer = f.CreatePeer("host", 1111, {});
            const auto pending = peer->GetChannel();
            if (connected) {
                f.Connect();
                f.Hello();
                f.ChannelFactory->Channels.at(0)->Close();
            } else {
                f.Connector->Requests.at(0).Promise.SetValue(
                    MakeError(E_IO, "connect failed"));
            }
            UNIT_ASSERT_VALUES_EQUAL(
                connected ? S_OK : E_IO,
                Wait(pending).GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                E_FAIL,
                Wait(peer->GetChannel()).GetError().GetCode());
            UNIT_ASSERT(peer->GetStatus().State == EPeerState::Disconnected);
        }
    }

    Y_UNIT_TEST(CompletionSubmissionExceptionPreservesResult)
    {
        for (const bool success: {false, true}) {
            auto queue = std::make_shared<TTestCompletionQueue>();
            TFixture f(MakeConfig(), queue);
            f.Peers->Start();
            const auto peer = f.CreatePeer("host", 1111, {});
            const auto pending = peer->GetChannel();
            queue->ThrowOnEnqueue = true;
            if (success) {
                f.Connect();
                f.Hello();
            } else {
                f.Connector->Requests.at(0).Promise.SetValue(
                    MakeError(E_IO, "connect failed"));
            }
            UNIT_ASSERT_VALUES_EQUAL(
                success ? S_OK : E_IO,
                Wait(pending).GetError().GetCode());
            peer->Close();
        }
    }

    Y_UNIT_TEST(ReleasingLastPeerSurvivesChannelCloseException)
    {
        TFixture f;
        f.Peers->Start();
        auto peer = f.CreatePeer("host", 1111, {});
        std::weak_ptr<IPeer> weak = peer;
        f.Connect();
        f.Hello();
        const auto channel = f.ChannelFactory->Channels.at(0);
        Y_DEFER
        {
            channel->Closed.TrySetValue(NProto::TError{});
        };

        channel->OnClose = []
        {
            throw std::runtime_error("close failed");
        };
        peer.reset();

        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT(channel->Closing.load());
        UNIT_ASSERT(!channel->Closed.GetFuture().HasValue());
        f.Peers->RequestStop();
        channel->Closed.SetValue(NProto::TError{});
        f.Peers->Stop();
    }

    Y_UNIT_TEST(CloseDuringActivationCompletesAttemptWhenChannelCloseThrows)
    {
        IPeerPtr peer;
        TFixture f;
        Y_DEFER
        {
            for (const auto& channel: f.ChannelFactory->Channels) {
                channel->Closed.TrySetValue(NProto::TError{});
            }
        };

        f.ChannelFactory->PrepareChannel = [&](TTestChannel& channel)
        {
            channel.OnClose = []
            {
                throw std::runtime_error("close failed");
            };
            channel.OnActivate = [&]
            {
                peer->Close();
            };
        };
        f.Peers->Start();
        peer = f.CreatePeer("host", 1111, {});
        const auto pending = peer->GetChannel();
        f.Connect();
        f.Hello();

        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(pending).GetError().GetCode());
        UNIT_ASSERT(peer->GetStatus().State == EPeerState::Disconnected);
    }

    Y_UNIT_TEST(ActivationFailureCompletesAttemptWhenChannelCloseThrows)
    {
        TFixture f;
        Y_DEFER
        {
            for (const auto& channel: f.ChannelFactory->Channels) {
                channel->Closed.TrySetValue(NProto::TError{});
            }
        };

        auto handler = std::make_shared<TTestHostHandler>();
        f.ChannelFactory->PrepareChannel = [](TTestChannel& channel)
        {
            channel.OnClose = []
            {
                throw std::runtime_error("close failed");
            };
            channel.OnActivate = []
            {
                throw std::runtime_error("activate failed");
            };
        };
        f.Peers->Start();
        const auto peer = f.CreatePeer("host", 1111, handler);
        const auto pending = peer->GetChannel();
        f.Connect();
        f.Hello();

        const auto error = Wait(pending).GetError();
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("activate failed", error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Unavailable.load());
        f.ChannelFactory->PrepareChannel = {};
        f.Retry();
        const auto next = peer->GetChannel();
        f.Connect(1);
        f.Hello(1);
        UNIT_ASSERT(!HasError(Wait(next).GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Connects.load());
    }

    Y_UNIT_TEST(ReportsHostChannelStateAndRetriesWithoutIo)
    {
        TFixture f;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto peer = f.CreatePeer("selected-host", 1111, handler);
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

    Y_UNIT_TEST(EnforcesRetryBackoffDuringHostNotifications)
    {
        for (bool disconnect: {false, true}) {
            TChannelFuture reentered;
            TFixture f;
            auto handler = std::make_shared<TTestHostHandler>();
            f.Peers->Start();
            const auto peer = f.CreatePeer("host", 1111, handler);
            auto getChannel = [&]
            {
                reentered = peer->GetChannel();
            };
            handler->OnUnavailable = getChannel;
            handler->OnDisconnected = getChannel;
            if (disconnect) {
                f.Connect();
                f.Hello();
                f.ChannelFactory->Channels.at(0)->Close();
            } else {
                f.Connector->Requests.at(0).Promise.SetValue(
                    MakeError(E_IO, "connection refused"));
            }
            UNIT_ASSERT(reentered.Initialized() && reentered.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(
                E_UNAVAILABLE,
                Wait(reentered).GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, f.Resolver->Requests.size());
            UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());

            f.Retry();
            UNIT_ASSERT_VALUES_EQUAL(2, f.Resolver->Requests.size());
            UNIT_ASSERT_VALUES_EQUAL(2, f.Connector->Requests.size());
            f.Connect(1);
            f.Hello(disconnect ? 1 : 0);
            UNIT_ASSERT(!HasError(Wait(peer->GetChannel()).GetError()));
        }
    }

    Y_UNIT_TEST(ReportsRepeatedDnsFailuresAndRecoversWithoutIo)
    {
        TFixture f;
        f.Resolver->Deferred = true;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto registered = f.CreatePeer("host", 1111, handler);
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

    Y_UNIT_TEST(RetriesAllAddressesAndUsesNewDnsResultForNextAttempt)
    {
        TFixture f;
        f.Resolver->Deferred = true;
        f.Peers->Start();
        const auto peer = f.CreatePeer("host", 1111, {});
        const auto first = peer->GetChannel();
        f.Resolver->Requests.at(0).Promise.SetValue(
            TVector<NInterconnect::TAddress>{
                NInterconnect::TAddress("127.0.0.1", 1111),
                NInterconnect::TAddress("127.0.0.1", 2222),
            });
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            1111,
            f.Connector->Requests.at(0).Address.GetPort());
        auto firstConnect = f.Connector->Requests.at(0).Promise;
        firstConnect.SetValue(MakeError(E_IO, "first address failed"));
        UNIT_ASSERT_VALUES_EQUAL(2, f.Connector->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            2222,
            f.Connector->Requests.at(1).Address.GetPort());
        auto secondConnect = f.Connector->Requests.at(1).Promise;
        secondConnect.SetValue(MakeError(E_IO, "second address failed"));
        UNIT_ASSERT_VALUES_EQUAL(E_IO, Wait(first).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, peer->GetStatus().ConnectionAttempts);
        UNIT_ASSERT_VALUES_EQUAL(1, peer->GetStatus().ConnectionFailures);

        f.Retry();
        const auto next = peer->GetChannel();
        f.Resolver->Requests.at(1).Promise.SetValue(
            TVector<NInterconnect::TAddress>{
                NInterconnect::TAddress("127.0.0.1", 3333),
            });
        UNIT_ASSERT_VALUES_EQUAL(3, f.Connector->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            3333,
            f.Connector->Requests.at(2).Address.GetPort());
        f.Connect(2);
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(next).GetError()));
        UNIT_ASSERT_VALUES_EQUAL(2, peer->GetStatus().ConnectionAttempts);
        UNIT_ASSERT_VALUES_EQUAL(1, peer->GetStatus().ConnectionFailures);
    }

    Y_UNIT_TEST(ImmediateConnectionFailuresDoNotRecurse)
    {
        enum class EStep
        {
            Connect,
            Hello,
        };

        constexpr size_t AddressCount = 10000;
        for (const auto step: {EStep::Connect, EStep::Hello}) {
            TFixture f;
            f.Resolver->Deferred = true;
            auto handler = std::make_shared<TTestHostHandler>();
            if (step == EStep::Connect) {
                f.Connector->ConnectResult = MakeFuture<TSocketResult>(
                    MakeError(E_REJECTED, "connect rejected"));
            } else {
                f.Connector->ConnectResult =
                    MakeFuture<TSocketResult>(MakeSocket());
                f.Handshake->ConnectResult =
                    MakeFuture<TConnectHandshakeResult>(
                        MakeError(E_REJECTED, "hello rejected"));
            }
            TVector<NInterconnect::TAddress> addresses(
                AddressCount,
                NInterconnect::TAddress("127.0.0.1", 1111));
            f.Peers->Start();
            const auto peer = f.CreatePeer("host", 1111, handler);
            const auto pending = peer->GetChannel();
            f.Resolver->Requests.at(0).Promise.SetValue(std::move(addresses));

            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                Wait(pending).GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                AddressCount,
                f.Connector->Requests.size());
            UNIT_ASSERT_VALUES_EQUAL(
                step == EStep::Hello ? AddressCount : 0,
                f.Handshake->Requests.size());
            UNIT_ASSERT_VALUES_EQUAL(1, peer->GetStatus().ConnectionFailures);
            UNIT_ASSERT_VALUES_EQUAL(1, handler->Unavailable.load());
        }
    }

    Y_UNIT_TEST(EmptyDnsResultFailsAndRetries)
    {
        TFixture f;
        f.Resolver->Deferred = true;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto peer = f.CreatePeer("host", 1111, handler);
        auto pending = peer->GetChannel();
        f.Resolver->Requests.at(0).Promise.SetValue(
            TVector<NInterconnect::TAddress>{});

        UNIT_ASSERT_VALUES_EQUAL(
            E_UNAVAILABLE,
            Wait(pending).GetError().GetCode());
        UNIT_ASSERT(f.Connector->Requests.empty());
        UNIT_ASSERT(f.ChannelFactory->Channels.empty());
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Unavailable.load());
        UNIT_ASSERT_VALUES_EQUAL(1, peer->GetStatus().ConnectionFailures);
        UNIT_ASSERT(peer->GetStatus().State == EPeerState::Disconnected);

        f.Resolver->Deferred = false;
        f.Retry();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
        f.Connect();
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(peer->GetChannel()).GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Connects.load());
    }

    Y_UNIT_TEST(PassesDnsDeadlineAndRetriesAfterResolverTimeout)
    {
        const auto config = MakeConfig();
        TFixture f(config);
        f.Resolver->Deferred = true;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto before = TMonotonic::Now();
        const auto peer = f.CreatePeer("host", 1111, handler);
        const auto after = TMonotonic::Now();
        auto pending = peer->GetChannel();
        const auto deadline = f.Resolver->Requests.at(0).Deadline;
        f.Resolver->Requests.at(0).Promise.SetValue(
            MakeError(E_TIMEOUT, "DNS deadline expired"));

        UNIT_ASSERT(deadline >= before + config.ConnectTimeout);
        UNIT_ASSERT(deadline <= after + config.ConnectTimeout);
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, Wait(pending).GetError().GetCode());
        UNIT_ASSERT(f.Connector->Requests.empty());
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Unavailable.load());
        UNIT_ASSERT_VALUES_EQUAL(1, peer->GetStatus().ConnectionFailures);

        f.Resolver->Deferred = false;
        f.Retry();
        auto replacement = peer->GetChannel();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
        f.Connect();
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(replacement).GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Connects.load());
    }

    Y_UNIT_TEST(PendingDnsDoesNotBlockAnotherBindingAndRetiredResultIsIgnored)
    {
        TFixture f;
        f.Resolver->Deferred = true;
        auto retired = std::make_shared<TTestHostHandler>();
        auto current = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto peer = f.CreatePeer("slow-host", 1111, retired);
        const auto registered = f.CreatePeer("other-host", 2222, current);
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

    Y_UNIT_TEST(PeersAreIndependent)
    {
        TFixture f;
        f.Peers->Start();
        const auto first = f.CreatePeer("host", 1111, {});
        const auto second = f.CreatePeer("host", 2222, {});
        UNIT_ASSERT(bool(first));
        UNIT_ASSERT(bool(second));
        UNIT_ASSERT(first != second);
        first->Close();
        const auto third = f.CreatePeer("host", 3333, {});
        UNIT_ASSERT(bool(third));
        UNIT_ASSERT(third != first);
        UNIT_ASSERT(third != second);
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            Wait(first->GetChannel()).GetError().GetCode());
    }

    Y_UNIT_TEST(DestroysHandlerOutsideManagerLock)
    {
        std::future<TPeerStatus> status;
        bool destroyedOutsideLock = false;
        TFixture f;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto peer = f.CreatePeer("host", 1111, handler);
        handler->OnDestroyed = [&, weak = std::weak_ptr<IPeer>(peer)]
        {
            if (auto peer = weak.lock()) {
                status = std::async(
                    std::launch::async,
                    [peer] { return peer->GetStatus(); });
                destroyedOutsideLock =
                    status.wait_for(5s) == std::future_status::ready;
            }
        };
        handler.reset();
        peer->Close();

        UNIT_ASSERT(status.valid());
        UNIT_ASSERT(status.get().State == EPeerState::Disconnected);
        UNIT_ASSERT(destroyedOutsideLock);
    }

    Y_UNIT_TEST(ReleasingLastPeerCancelsAttempt)
    {
        TFixture f;
        f.Peers->Start();
        auto peer = f.CreatePeer("host", 1111, {});
        auto pending = peer->GetChannel();
        auto retained = peer;
        peer.reset();
        UNIT_ASSERT(!pending.HasValue());
        retained.reset();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(pending).GetError().GetCode());
        const auto replacement = f.CreatePeer("other-host", 2222, {});
        replacement->Close();
        replacement->Close();
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            Wait(replacement->GetChannel()).GetError().GetCode());
    }

    Y_UNIT_TEST(ReleasingLastConnectedPeerClosesChannel)
    {
        TFixture f;
        f.Peers->Start();
        auto peer = f.CreatePeer("host", 1111, {});
        std::weak_ptr<IPeer> weak = peer;
        f.Connect();
        f.Hello();
        const auto channel = Wait(peer->GetChannel()).ExtractResult();
        peer.reset();

        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT(channel->WhenClosed().HasValue());
        f.Retry();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
    }

    Y_UNIT_TEST(ReleasingPeerFromConnectedCallback)
    {
        IPeerPtr peer;
        TFixture f;
        auto handler = std::make_shared<TTestHostHandler>();
        handler->OnConnected = [&]
        {
            peer.reset();
        };
        f.Peers->Start();
        peer = f.CreatePeer("host", 1111, handler);
        std::weak_ptr<IPeer> weak = peer;
        const auto pending = peer->GetChannel();
        f.Connect();
        f.Hello();

        const auto [channel, error] = Wait(pending);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT(channel->WhenClosed().HasValue());
        f.Retry();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
    }

    Y_UNIT_TEST(ReleasingPeerFromCancelledContinuation)
    {
        IPeerPtr peer;
        TFixture f;
        f.Peers->Start();
        peer = f.CreatePeer("host", 1111, {});
        std::weak_ptr<IPeer> weak = peer;
        const auto pending = peer->GetChannel();
        pending.Subscribe([&](const auto&) { peer.reset(); });
        peer->Close();

        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(pending).GetError().GetCode());
    }

    Y_UNIT_TEST(ReleasingLastPeerRacesStop)
    {
        for (size_t i = 0; i < 32; ++i) {
            TFixture f;
            f.Peers->Start();
            auto peer = f.CreatePeer("host", 1111, {});
            const auto pending = peer->GetChannel();
            auto released = std::async(
                std::launch::async,
                [peer = std::move(peer)]() mutable { peer.reset(); });
            f.Peers->Stop();
            const bool completed = pending.HasValue();
            released.get();

            UNIT_ASSERT(completed);
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                Wait(pending).GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ClosingPeerCancelsScheduledReconnect)
    {
        TFixture f;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto peer = f.CreatePeer("host", 1111, handler);
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
        peer = f.CreatePeer("host", 1111, handler);
        f.Connect();
        f.Hello();
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Connects.load());
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Disconnects.load());
        UNIT_ASSERT(!nested);
    }

    Y_UNIT_TEST(CloseFromNotificationPreservesQueuedEvents)
    {
        IPeerPtr peer;
        TFixture f;
        auto handler = std::make_shared<TTestHostHandler>();
        handler->OnConnected = [&]
        {
            f.ChannelFactory->Channels.at(0)->Close();
            peer->Close();
        };
        f.Peers->Start();
        peer = f.CreatePeer("host", 1111, handler);
        const auto pending = peer->GetChannel();
        f.Connect();
        f.Hello();

        UNIT_ASSERT(!HasError(Wait(pending).GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Connects.load());
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Disconnects.load());
        UNIT_ASSERT(peer->GetStatus().State == EPeerState::Disconnected);
    }

    Y_UNIT_TEST(ShutdownIgnoresResolutionCompletingAfterRequestStop)
    {
        TFixture f;
        f.Resolver->Deferred = true;
        auto handler = std::make_shared<TTestHostHandler>();
        f.Peers->Start();
        const auto registered = f.CreatePeer("host", 1111, handler);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Resolver->Requests.size());
        auto resolved = f.Resolver->Requests[0].Promise;
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
        const auto peer = f.CreatePeer("127.0.0.1", 1111, handler);

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
        const auto firstPeer = f.CreatePeer("127.0.0.1", 1111, {});
        const auto secondPeer = f.CreatePeer("127.0.0.1", 2222, {});
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

    Y_UNIT_TEST(ClosingPeerDuringHelloIgnoresLateResult)
    {
        TFixture f;
        f.Peers->Start();
        const auto firstPeer = f.CreatePeer("127.0.0.1", 1111, {});
        auto retired = firstPeer->GetChannel();
        f.Connect();
        firstPeer->Close();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(retired).GetError().GetCode());
        const auto secondPeer = f.CreatePeer("127.0.0.1", 2222, {});
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
        const auto peer = f.CreatePeer("127.0.0.1", 39602, {});
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

    Y_UNIT_TEST(EstablishedPeerUsesCachedCloseFuture)
    {
        TFixture f;
        f.Peers->Start();
        const auto peer = f.CreatePeer("host", 1111, {});
        f.Connect();
        f.Hello();
        const auto channel = f.ChannelFactory->Channels.at(0);
        channel->OnWhenClosed = []
        {
            throw std::runtime_error("use the cached close future");
        };

        UNIT_ASSERT(peer->GetStatus().State == EPeerState::Connected);
        UNIT_ASSERT(Wait(peer->GetChannel()).GetResult() == channel);
        channel->Close();
        UNIT_ASSERT(peer->GetStatus().State == EPeerState::Disconnected);
        UNIT_ASSERT_VALUES_EQUAL(
            E_UNAVAILABLE,
            Wait(peer->GetChannel()).GetError().GetCode());
    }

    Y_UNIT_TEST(OtherPeersRemainUsableWhileOneChannelGetterIsBlocked)
    {
        TFixture f;
        f.Peers->Start();
        const auto first = f.CreatePeer("host-a", 1111, {});
        const auto second = f.CreatePeer("host-b", 2222, {});
        f.Connect(0);
        f.Connect(1);
        f.Hello(1);
        const auto secondChannel = f.ChannelFactory->Channels.at(0);

        auto entered = NewPromise<void>();
        auto release = NewPromise<void>();
        std::future<void> activation;
        std::future<TPeerStatus> otherStatus;
        std::future<TChannelFuture> lookup;
        Y_DEFER
        {
            release.TrySetValue();
        };

        f.ChannelFactory->PrepareChannel = [&](TTestChannel& channel)
        {
            channel.OnWhenClosed = [&]
            {
                entered.TrySetValue();
                release.GetFuture().GetValueSync();
            };
        };
        activation = std::async(std::launch::async, [&] { f.Hello(0); });
        const bool blocked = entered.GetFuture().Wait(TDuration::Seconds(5));
        lookup = std::async(
            std::launch::async,
            [&] { return second->GetChannel(); });
        const bool ready = lookup.wait_for(5s) == std::future_status::ready;
        otherStatus =
            std::async(std::launch::async, [&] { return second->GetStatus(); });
        const bool statusReady =
            otherStatus.wait_for(5s) == std::future_status::ready;
        release.SetValue();

        activation.get();
        UNIT_ASSERT(first->GetStatus().State == EPeerState::Connected);
        const auto channel = lookup.get();
        UNIT_ASSERT(blocked);
        UNIT_ASSERT(ready);
        UNIT_ASSERT(statusReady);
        UNIT_ASSERT(otherStatus.get().State == EPeerState::Connected);
        UNIT_ASSERT(Wait(channel).GetResult() == secondChannel);
    }

    Y_UNIT_TEST(WithdrawsReadyChannelBeforeClosingIt)
    {
        for (bool stopManager: {false, true}) {
            TFixture f;
            f.Peers->Start();
            const auto peer = f.CreatePeer("host", 1111, {});
            f.Connect();
            f.Hello();
            TChannelFuture duringClose;
            f.ChannelFactory->Channels.at(0)->OnClose = [&]
            {
                duringClose = peer->GetChannel();
            };
            if (stopManager) {
                f.Peers->RequestStop();
            } else {
                peer->Close();
            }
            UNIT_ASSERT_VALUES_EQUAL(
                stopManager ? E_REJECTED : E_CANCELLED,
                Wait(duringClose).GetError().GetCode());
        }
    }

    Y_UNIT_TEST(AcceptsSuccessfulHelloAfterConnectDeadline)
    {
        TFixture f;
        f.Resolver->Deferred = true;
        f.Peers->Start();
        const auto peer = f.CreatePeer("127.0.0.1", 39602, {});
        auto result = peer->GetChannel();
        f.Timer->AdvanceTime(TDuration::MilliSeconds(30));
        f.Resolver->Requests.at(0).Promise.SetValue(
            TVector<NInterconnect::TAddress>{
                NInterconnect::TAddress("127.0.0.1", 39602),
            });
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(70),
            f.Connector->Requests[0].Timeout);
        f.Timer->AdvanceTime(TDuration::MilliSeconds(40));
        f.Connect();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(30),
            f.Handshake->Requests[0].Timeout);
        f.Timer->AdvanceTime(TDuration::MilliSeconds(30));
        f.Hello();
        const auto ready = Wait(result);
        UNIT_ASSERT_C(
            !HasError(ready.GetError()),
            FormatError(ready.GetError()));
        UNIT_ASSERT(ready.GetResult() == f.ChannelFactory->Channels.at(0));
        f.Retry();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Connector->Requests.size());
    }

    Y_UNIT_TEST(PreservesHelloErrorAfterConnectDeadline)
    {
        TFixture f;
        f.Peers->Start();
        const auto peer = f.CreatePeer("127.0.0.1", 39602, {});
        auto result = peer->GetChannel();
        f.Connect();
        f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
        f.Handshake->Requests.at(0).Promise.SetValue(
            MakeError(E_TRANSPORT_ERROR, "hello rejected"));
        UNIT_ASSERT_VALUES_EQUAL(
            E_TRANSPORT_ERROR,
            Wait(result).GetError().GetCode());
        UNIT_ASSERT(f.ChannelFactory->Channels.empty());
    }

    Y_UNIT_TEST(KeepsChannelWhenActivationFinishesAfterConnectDeadline)
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
        const auto peer = f.CreatePeer("127.0.0.1", 39602, {});
        auto result = peer->GetChannel();
        f.Connect();
        f.Hello();
        const auto ready = Wait(result);
        UNIT_ASSERT_C(
            !HasError(ready.GetError()),
            FormatError(ready.GetError()));
        UNIT_ASSERT(ready.GetResult() == f.ChannelFactory->Channels.at(0));
        UNIT_ASSERT(!ready.GetResult()->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(DoesNotStartHelloAfterTcpBudgetExpires)
    {
        TFixture f;
        f.Peers->Start();
        const auto peer = f.CreatePeer("127.0.0.1", 39602, {});
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
        const auto peer = f.CreatePeer("127.0.0.1", 39602, {});
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
        const auto peer = f.CreatePeer("127.0.0.1", 39602, {});
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
        auto entered = NewPromise<void>();
        auto release = NewPromise<void>();
        IPeerPtr peer;
        std::future<void> closing;
        Y_DEFER
        {
            release.TrySetValue();
        };

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
        peer = f.CreatePeer("127.0.0.1", 39602, {});
        auto first = peer->GetChannel();
        f.Connect();
        f.Hello();
        auto channel = Wait(first).ExtractResult();
        closing =
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
        const auto peer = f.CreatePeer("127.0.0.1", 39602, {});
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
            const auto peer = f.CreatePeer("127.0.0.1", 39602, {});
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

    Y_UNIT_TEST(KeepsOneThousandPeersConnected)
    {
        constexpr size_t peerCount = 1000;
        TFixture f;
        f.Peers->Start();
        TVector<IPeerPtr> peers;
        peers.reserve(peerCount);
        for (size_t i = 0; i < peerCount; ++i) {
            peers.push_back(
                f.CreatePeer(TStringBuilder() << "host-" << i, 1111, {}));
        }
        UNIT_ASSERT_VALUES_EQUAL(peerCount, f.Connector->Requests.size());

        // The fake channel factory does not use the socket.
        const auto socket = MakeSocket();
        for (size_t i = 0; i < peerCount; ++i) {
            f.Connector->Requests.at(i).Promise.SetValue(socket);
            f.Hello(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(peerCount, f.ChannelFactory->Channels.size());
        for (size_t i = 0; i < peerCount; ++i) {
            const auto channel = Wait(peers.at(i)->GetChannel());
            UNIT_ASSERT_C(
                !HasError(channel.GetError()),
                FormatError(channel.GetError()));
            UNIT_ASSERT(
                channel.GetResult() == f.ChannelFactory->Channels.at(i));
            UNIT_ASSERT(!channel.GetResult()->WhenClosed().HasValue());
        }
    }

    Y_UNIT_TEST(QueuedFailureCompletionDoesNotBlockOtherPeers)
    {
        auto queue = CreateThreadPool("ICV2PeerTest", 1);
        queue->Start();
        auto release = NewPromise<void>();
        TFixture f(MakeConfig(), queue);
        Y_DEFER
        {
            release.TrySetValue();
            f.Peers->Stop();
            queue->Stop();
        };

        queue->ExecuteSimple([release] { release.GetFuture().GetValueSync(); });
        f.Peers->Start();
        const auto firstPeer = f.CreatePeer("host", 1111, {});
        auto first = firstPeer->GetChannel();
        f.Connector->Requests[0].Promise.SetValue(
            MakeError(E_IO, "connection refused"));
        const bool queued = !first.HasValue();
        firstPeer->Close();
        const auto secondPeer = f.CreatePeer("host", 2222, {});
        const auto started = f.Connector->Requests.size();
        release.SetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_IO, Wait(first).GetError().GetCode());
        queue->Execute([] {}).GetValueSync();
        UNIT_ASSERT(queued);
        UNIT_ASSERT_VALUES_EQUAL(2, started);
        auto next = secondPeer->GetChannel();
        f.Connect(1);
        f.Hello();
        UNIT_ASSERT(!HasError(Wait(next).GetError()));
    }

    Y_UNIT_TEST(RequestStopFromContinuationAndExternalStopWaitsForItsReturn)
    {
        auto queue = CreateThreadPool("ICV2PeerTest", 1);
        queue->Start();
        auto entered = NewPromise<void>();
        auto release = NewPromise<void>();
        auto stopEntered = NewPromise<void>();
        auto otherStopEntered = NewPromise<void>();
        TFixture f(MakeConfig(), queue);
        f.Connector->OnStop = [stopEntered]() mutable
        {
            stopEntered.TrySetValue();
        };
        f.Peers->Start();
        const auto peer = f.CreatePeer("127.0.0.1", 39602, {});
        auto result = peer->GetChannel();
        std::future<void> stop;
        std::future<void> otherStop;
        Y_DEFER
        {
            release.TrySetValue();
            f.Peers->Stop();
            queue->Stop();
        };

        result.Subscribe(
            [&](const auto&)
            {
                f.Peers->RequestStop();
                entered.SetValue();
                release.GetFuture().GetValueSync();
            });
        f.Connect();
        f.Hello();
        const bool callbackStarted =
            entered.GetFuture().Wait(TDuration::Seconds(5));
        stop = std::async(std::launch::async, [&] { f.Peers->Stop(); });
        const bool stopStarted =
            stopEntered.GetFuture().Wait(TDuration::Seconds(5));
        otherStop = std::async(
            std::launch::async,
            [&]
            {
                otherStopEntered.SetValue();
                f.Peers->Stop();
            });
        const bool otherStarted =
            otherStopEntered.GetFuture().Wait(TDuration::Seconds(5));
        const bool waits = stop.wait_for(1s) == std::future_status::timeout;
        const bool otherWaits =
            otherStop.wait_for(1s) == std::future_status::timeout;
        release.SetValue();
        stop.get();
        otherStop.get();
        UNIT_ASSERT(callbackStarted);
        UNIT_ASSERT(stopStarted);
        UNIT_ASSERT(otherStarted);
        UNIT_ASSERT(waits);
        UNIT_ASSERT(otherWaits);
        UNIT_ASSERT(f.ChannelFactory->Channels[0]->Closing);
    }

    Y_UNIT_TEST(RejectsCallsBeforeStartAfterStopAndUnknownPeers)
    {
        TFixture f;
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            f.Peers->CreatePeer("host", 1111, {}).GetError().GetCode());
        f.Peers->Start();
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            f.Peers->CreatePeer("", 1111, {}).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            f.Peers->CreatePeer("host", 0, {}).GetError().GetCode());
        const auto peer = f.CreatePeer("host", 1111, {});
        f.Peers->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(peer->GetChannel()).GetError().GetCode());
    }
}

}   // namespace NCloud::NIcV2
