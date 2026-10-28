#include "request_channel.h"

#include "diagnostic.h"
#include "handshake.h"

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/icv2/tcp/testlib/socket_test.h>

#include <contrib/ydb/library/actors/interconnect/interconnect_common.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>
#include <contrib/ydb/library/actors/interconnect/uring_context.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <atomic>
#include <future>
#include <thread>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud::NIcV2::NTests;
using namespace std::chrono_literals;
using TEvent = std::unique_ptr<NActors::IEventHandle>;

const TRequestChannelConfig Config{
    {{TEvDiagnostic::EvPingRequest, TEvDiagnostic::EvPingResponse}},
    false};

struct TOtherRequest final
    : NActors::TEventPB<
          TOtherRequest,
          NProto::TIcV2PingRequest,
          TEvDiagnostic::EvPingResponse + 1>
{
};

struct TOtherResponse final
    : NActors::TEventPB<
          TOtherResponse,
          NProto::TIcV2PingResponse,
          TEvDiagnostic::EvPingResponse + 2>
{
};

const TRequestChannelConfig MultiConfig{
    {{TEvDiagnostic::EvPingRequest, TEvDiagnostic::EvPingResponse},
     {TOtherRequest::EventType, TOtherResponse::EventType}},
    false};

template <typename T>
auto MakePayload(TString payload)
{
    auto event = std::make_unique<T>();
    event->Record.SetPayload(std::move(payload));
    return event;
}

auto MakePing(TString payload)
{
    return MakePayload<TEvDiagnostic::TEvPingRequest>(std::move(payload));
}

std::unique_ptr<NActors::IEventBase> MakeRequest(bool other, TString payload)
{
    if (other) {
        return MakePayload<TOtherRequest>(std::move(payload));
    }
    return MakePing(std::move(payload));
}

auto MakeReply(TString payload)
{
    return MakePayload<TEvDiagnostic::TEvPingResponse>(std::move(payload));
}

template <typename T = TEvDiagnostic::TEvPingResponse>
TEvent MakeResponse(ui64 cookie, TString payload)
{
    return std::make_unique<NActors::IEventHandle>(
        NActors::TActorId(),
        NActors::TActorId(),
        MakePayload<T>(std::move(payload)).release(),
        0,
        cookie);
}

TResponseResult WaitResponse(const TResponseFuture& future)
{
    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
    return future.GetValue();
}

template <typename T = TEvDiagnostic::TEvPingResponse>
TString GetPayload(const TResponseFuture& future)
{
    auto [event, error] = WaitResponse(future);
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
    return event->Get<T>()->Record.GetPayload();
}

template <typename TRequest, typename TResponse>
void ReplyWithPayload(const IRequestChannelPtr& channel, const TEvent& request)
{
    UNIT_ASSERT(channel->Reply(
        request->Cookie,
        MakePayload<TResponse>(request->Get<TRequest>()->Record.GetPayload())));
}

class TTestConnection final: public IConnection
{
    std::mutex Mutex;
    std::shared_ptr<IConnectionHandler> Handler;
    TVector<TEvent> Sent;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();
    bool Active = false;
    bool Closing = false;

public:
    std::function<void(NActors::IEventHandle&)> OnSend;
    std::function<void()> BeforeClose;

    explicit TTestConnection(std::shared_ptr<IConnectionHandler> handler)
        : Handler(std::move(handler))
    {}

    bool Activate() override
    {
        std::lock_guard guard(Mutex);
        if (Active || Closing) {
            return false;
        }
        Active = true;
        return true;
    }

    bool Send(TEvent event) override
    {
        // Complete inline to exercise response-before-Send-returns admission.
        if (OnSend) {
            OnSend(*event);
        }
        std::lock_guard guard(Mutex);
        if (!Active || Closing) {
            return false;
        }
        Sent.push_back(std::move(event));
        return true;
    }

    void Close() override
    {
        if (BeforeClose) {
            BeforeClose();
        }
        {
            std::lock_guard guard(Mutex);
            if (Closing) {
                return;
            }
            Closing = true;
            Handler.reset();
        }
        Closed.SetValue();
    }

    NThreading::TFuture<void> WhenClosed() const override
    {
        return Closed.GetFuture();
    }

    TVector<TEvent> TakeSent()
    {
        TVector<TEvent> result;
        std::lock_guard guard(Mutex);
        result.swap(Sent);
        return result;
    }

    void Receive(TEvent event)
    {
        if (auto handler = GetHandler()) {
            handler->HandleEvent(std::move(event));
        }
    }

    void Disconnect()
    {
        if (auto handler = GetHandler()) {
            handler->HandleDisconnect("test peer disconnected");
        }
    }

private:
    std::shared_ptr<IConnectionHandler> GetHandler()
    {
        std::lock_guard guard(Mutex);
        return Handler;
    }
};

struct TTestTransport final: ITransport
{
    std::shared_ptr<TTestConnection> Connection;

    IConnectionPtr CreateConnection(
        TSocketPtr,
        TSocketPtr,
        std::shared_ptr<IConnectionHandler> handler,
        bool) override
    {
        Connection = std::make_shared<TTestConnection>(std::move(handler));
        return Connection;
    }

    void RequestStop() override
    {
        if (Connection) {
            Connection->Close();
        }
    }

    void Stop() override
    {
        RequestStop();
    }
};

class TCountingScheduler final: public IScheduler
{
    TTestScheduler Scheduler;

public:
    std::atomic<size_t> Tasks = 0;
    std::atomic<size_t> Scheduled = 0;

    void Start() override
    {}

    void Stop() override
    {}

    void Schedule(ITaskQueue*, TInstant deadline, TCallback callback) override
    {
        ++Tasks;
        ++Scheduled;
        Scheduler.Schedule(
            nullptr,
            deadline,
            [this, callback = std::move(callback)]
            {
                --Tasks;
                callback();
            });
    }

    void RunAllScheduledTasks()
    {
        Scheduler.RunAllScheduledTasks();
    }
};

class TManualTaskQueue final: public ITaskQueue
{
    TVector<ITaskPtr> Tasks;

public:
    void Start() override
    {}

    void Stop() override
    {}

    void Enqueue(ITaskPtr task) override
    {
        Tasks.push_back(std::move(task));
    }

    void RunAll()
    {
        TVector<ITaskPtr> tasks;
        tasks.swap(Tasks);
        for (auto& task: tasks) {
            task->Execute();
        }
    }
};

TSocketPtr CreateTestSocket()
{
    const int fd =
        ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    UNIT_ASSERT(fd >= 0);
    return MakeIntrusive<NInterconnect::TStreamSocket>(fd);
}

struct TFixture
{
    std::shared_ptr<TTestTransport> Transport =
        std::make_shared<TTestTransport>();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TCountingScheduler> Scheduler =
        std::make_shared<TCountingScheduler>();
    IRequestChannelPtr Channel;

    explicit TFixture(
        TRequestHandler handler = [](TEvent) {},
        TRequestChannelConfig config = Config,
        ITaskQueuePtr completions = CreateTaskQueueStub())
    {
        auto [channel, error] = CreateRequestChannel(
            Transport,
            CreateTestSocket(),
            {},
            config,
            Timer,
            Scheduler,
            std::move(completions),
            std::move(handler));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Channel = std::move(channel);
    }

    ~TFixture()
    {
        if (Channel) {
            Channel->Close();
        }
        Transport->Stop();
    }

    TResponseFuture Request(
        TString payload = "ping",
        TDuration timeout = TDuration::Seconds(5))
    {
        return Channel->Request(MakePing(std::move(payload)), timeout);
    }
};

struct TNetworkFixture
{
    const bool Echo;
    NThreading::TPromise<void> RequestReceived = NThreading::NewPromise<void>();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>();
    std::array<ITransportPtr, 2> Transports;
    std::array<IRequestChannelPtr, 2> Channels;

    explicit TNetworkFixture(
        bool echo = true,
        TRequestChannelConfig config = Config)
        : Echo(echo)
    {
        TTestListener listener;
        listener.Listener->Start();
        auto connector = NCloud::NIcV2::NTests::CreateTestConnector();
        connector->Start();
        std::array<TSocketPtr, 2> sockets{
            WaitSocket(connector->Connect(
                listener.Listener->GetAddress(),
                TDuration::Seconds(5))),
            listener.Accept()};
        connector->Stop();
        listener.Listener->Stop();
        std::array<IHandshakeProviderPtr, 2> hellos;
        for (size_t i = 0; i < hellos.size(); ++i) {
            auto [hello, error] =
                CreateHandshakeProvider(THandshakeConfig{false});
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            hellos[i] = std::move(hello);
            hellos[i]->Start();
        }
        const auto outgoing =
            hellos[0]->Connect(std::move(sockets[0]), TDuration::Seconds(5));
        const auto incoming =
            hellos[1]->Accept(std::move(sockets[1]), TDuration::Seconds(5));
        UNIT_ASSERT(outgoing.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(incoming.Wait(TDuration::Seconds(5)));
        const auto& outgoingResult = outgoing.GetValue();
        const auto& incomingResult = incoming.GetValue();
        UNIT_ASSERT_C(
            !HasError(outgoingResult.GetError()),
            FormatError(outgoingResult.GetError()));
        UNIT_ASSERT_C(
            !HasError(incomingResult.GetError()),
            FormatError(incomingResult.GetError()));
        const std::array<THandshakeReady, 2> connected{
            outgoingResult.GetResult(),
            std::get<THandshakeReady>(incomingResult.GetResult())};
        for (size_t i = 0; i < Transports.size(); ++i) {
            const auto& connection = connected[i];
            hellos[i]->Stop();
            auto common = MakeIntrusive<NActors::TInterconnectProxyCommon>();
            common->MonCounters =
                MakeIntrusive<NMonitoring::TDynamicCounters>();
            common->Settings.V2.Threads = 1;
            common->Settings.V2.EnableSQPOLL = false;
            Transports[i] = NCloud::NIcV2::CreateTransport(
                NActors::CreateUringEngine(std::move(common)),
                0,
                {});
            UNIT_ASSERT(Transports[i]);
            auto [channel, error] = CreateRequestChannel(
                Transports[i],
                connection.Socket,
                {},
                config,
                Timer,
                Scheduler,
                CreateTaskQueueStub(),
                [this, i](TEvent request)
                {
                    if (!Echo) {
                        RequestReceived.SetValue();
                        return;
                    }
                    if (request->Type == TOtherRequest::EventType) {
                        ReplyWithPayload<TOtherRequest, TOtherResponse>(
                            Channels[i],
                            request);
                    } else {
                        ReplyWithPayload<
                            TEvDiagnostic::TEvPingRequest,
                            TEvDiagnostic::TEvPingResponse>(
                            Channels[i],
                            request);
                    }
                });
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            Channels[i] = std::move(channel);
        }
        for (auto& channel: Channels) {
            UNIT_ASSERT(channel->Activate());
        }
    }

    ~TNetworkFixture()
    {
        for (auto& channel: Channels) {
            if (channel) {
                channel->Close();
            }
        }
        for (auto& transport: Transports) {
            if (transport) {
                transport->Stop();
            }
        }
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2RequestChannelTest)
{
    Y_UNIT_TEST(RejectsEmptyZeroDuplicateAndOverlappingEventTypes)
    {
        const TVector<TVector<TRequestEventTypes>> invalid{
            {},
            {{0, 2}},
            {{1, 0}},
            {{1, 1}},
            {{1, 2}, {1, 3}},
            {{1, 2}, {3, 2}},
            {{1, 2}, {2, 3}},
            {{1, 2}, {3, 1}},
            {{1, 2}, {1, 2}}};
        for (const auto& types: invalid) {
            auto config = Config;
            config.EventTypes = types;
            auto transport = std::make_shared<TTestTransport>();
            auto [channel, error] = CreateRequestChannel(
                transport,
                CreateTestSocket(),
                {},
                config,
                std::make_shared<TTestTimer>(),
                std::make_shared<TCountingScheduler>(),
                CreateTaskQueueStub(),
                [](TEvent) {});
            UNIT_ASSERT(!channel);
            UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), E_ARGUMENT);
            UNIT_ASSERT(!transport->Connection);
        }
    }

    Y_UNIT_TEST(CorrelatesParallelMixedRequestsAndOutOfOrderResponses)
    {
        TFixture fixture([](TEvent) {}, MultiConfig);
        fixture.Channel->Activate();
        std::array<std::future<TResponseFuture>, 32> callers;
        for (size_t i = 0; i < callers.size(); ++i) {
            callers[i] = std::async(
                std::launch::async,
                [&, i]
                {
                    return fixture.Channel->Request(
                        MakeRequest(i % 2, ToString(i)),
                        TDuration::Seconds(5));
                });
        }
        TVector<TResponseFuture> futures;
        for (auto& caller: callers) {
            futures.push_back(caller.get());
        }
        auto sent = fixture.Transport->Connection->TakeSent();
        UNIT_ASSERT_VALUES_EQUAL(sent.size(), callers.size());
        THashSet<ui64> cookies;
        for (size_t i = sent.size(); i--;) {
            const auto& request = sent[i];
            UNIT_ASSERT(cookies.insert(request->Cookie).second);
            if (request->Type == TOtherRequest::EventType) {
                fixture.Transport->Connection->Receive(
                    MakeResponse<TOtherResponse>(
                        request->Cookie,
                        request->Get<TOtherRequest>()->Record.GetPayload()));
                // Even another configured type is ignored after completion.
                fixture.Transport->Connection->Receive(
                    MakeResponse(request->Cookie, "duplicate"));
            } else {
                fixture.Transport->Connection->Receive(MakeResponse(
                    request->Cookie,
                    request->Get<TEvDiagnostic::TEvPingRequest>()
                        ->Record.GetPayload()));
                fixture.Transport->Connection->Receive(
                    MakeResponse<TOtherResponse>(request->Cookie, "duplicate"));
            }
        }
        fixture.Transport->Connection->Receive(
            MakeResponse<TOtherResponse>(10000, "unknown"));
        for (size_t i = 0; i < futures.size(); ++i) {
            const auto payload = i % 2 ? GetPayload<TOtherResponse>(futures[i])
                                       : GetPayload(futures[i]);
            UNIT_ASSERT_VALUES_EQUAL(payload, ToString(i));
        }
        UNIT_ASSERT(!fixture.Channel->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(WrongConfiguredResponseTypeClosesChannelAndFailsPendingRequests)
    {
        for (bool other: {false, true}) {
            auto queue = std::make_shared<TManualTaskQueue>();
            TFixture fixture([](TEvent) {}, MultiConfig, queue);
            fixture.Channel->Activate();
            auto first = fixture.Request();
            auto second = fixture.Channel->Request(
                MakePayload<TOtherRequest>("other"),
                TDuration::Seconds(5));
            auto sent = fixture.Transport->Connection->TakeSent();
            if (other) {
                fixture.Transport->Connection->Receive(
                    MakeResponse(sent[1]->Cookie, "wrong response"));
            } else {
                fixture.Transport->Connection->Receive(
                    MakeResponse<TOtherResponse>(
                        sent[0]->Cookie,
                        "wrong response"));
            }
            UNIT_ASSERT(!first.HasValue());
            UNIT_ASSERT(!second.HasValue());
            UNIT_ASSERT(!fixture.Channel->WhenClosed().HasValue());
            UNIT_ASSERT_VALUES_EQUAL(
                WaitResponse(fixture.Request()).GetError().GetCode(),
                E_REJECTED);
            queue->RunAll();
            UNIT_ASSERT_VALUES_EQUAL(
                WaitResponse(first).GetError().GetCode(),
                E_BADMSG);
            UNIT_ASSERT_VALUES_EQUAL(
                WaitResponse(second).GetError().GetCode(),
                E_BADMSG);
            UNIT_ASSERT(fixture.Channel->WhenClosed().HasValue());
        }
    }

    Y_UNIT_TEST(
        LateConfiguredResponseTypeDoesNotCloseChannelOrCompleteAnotherCall)
    {
        for (bool runTimer: {false, true}) {
            TFixture fixture([](TEvent) {}, MultiConfig);
            fixture.Channel->Activate();
            auto first = fixture.Request("short", TDuration::MilliSeconds(10));
            auto second = fixture.Channel->Request(
                MakePayload<TOtherRequest>("other"),
                TDuration::Seconds(5));
            auto sent = fixture.Transport->Connection->TakeSent();
            fixture.Timer->AdvanceTime(TDuration::MilliSeconds(10));
            if (runTimer) {
                fixture.Scheduler->RunAllScheduledTasks();
            }
            fixture.Transport->Connection->Receive(
                MakeResponse<TOtherResponse>(sent[0]->Cookie, "late"));
            UNIT_ASSERT_VALUES_EQUAL(
                WaitResponse(first).GetError().GetCode(),
                E_TIMEOUT);
            UNIT_ASSERT(!second.HasValue());
            fixture.Transport->Connection->Receive(
                MakeResponse<TOtherResponse>(sent[1]->Cookie, "other"));
            UNIT_ASSERT_VALUES_EQUAL(
                GetPayload<TOtherResponse>(second),
                "other");
            UNIT_ASSERT(!fixture.Channel->WhenClosed().HasValue());
        }
    }

    Y_UNIT_TEST(RequestAdmissionLimitIsSharedAcrossEventTypes)
    {
        auto config = MultiConfig;
        config.MaxPendingRequests = 1;
        TFixture fixture([](TEvent) {}, config);
        fixture.Channel->Activate();
        auto first = fixture.Request();
        auto rejected = fixture.Channel->Request(
            MakePayload<TOtherRequest>("other"),
            TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(rejected).GetError().GetCode(),
            E_REJECTED);
        auto sent = fixture.Transport->Connection->TakeSent();
        UNIT_ASSERT_VALUES_EQUAL(sent.size(), 1);
        fixture.Transport->Connection->Receive(
            MakeResponse(sent[0]->Cookie, "ping"));
        UNIT_ASSERT_VALUES_EQUAL(GetPayload(first), "ping");
        auto second = fixture.Channel->Request(
            MakePayload<TOtherRequest>("other"),
            TDuration::Seconds(5));
        sent = fixture.Transport->Connection->TakeSent();
        UNIT_ASSERT_VALUES_EQUAL(sent.size(), 1);
        fixture.Transport->Connection->Receive(
            MakeResponse<TOtherResponse>(sent[0]->Cookie, "other"));
        UNIT_ASSERT_VALUES_EQUAL(GetPayload<TOtherResponse>(second), "other");
    }

    Y_UNIT_TEST(DoesNotAccumulateTimersForCompletedRequests)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        for (size_t i = 0; i < 10000; ++i) {
            auto response = fixture.Request();
            auto sent = fixture.Transport->Connection->TakeSent();
            fixture.Transport->Connection->Receive(
                MakeResponse(sent[0]->Cookie, "reply"));
            UNIT_ASSERT_VALUES_EQUAL(GetPayload(response), "reply");
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler->Tasks.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler->Scheduled.load(), 1);
        fixture.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler->Tasks.load(), 0);
    }

    Y_UNIT_TEST(NewEarlierDeadlineExpiresOnTheNextTick)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        auto longRequest = fixture.Request("long", TDuration::Hours(1));
        auto shortRequest =
            fixture.Request("short", TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler->Tasks.load(), 1);
        fixture.Timer->AdvanceTime(TDuration::MilliSeconds(10));
        fixture.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(shortRequest).GetError().GetCode(),
            E_TIMEOUT);
        UNIT_ASSERT(!longRequest.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler->Tasks.load(), 1);
        fixture.Channel->Close();
        fixture.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler->Tasks.load(), 0);
    }

    Y_UNIT_TEST(RequestLimitIncludesQueuedCompletionsAndRecovers)
    {
        auto queue = std::make_shared<TManualTaskQueue>();
        auto config = Config;
        config.MaxPendingRequests = 1;
        TFixture fixture([](TEvent) {}, config, queue);
        fixture.Channel->Activate();
        auto first = fixture.Request();
        auto sent = fixture.Transport->Connection->TakeSent();
        fixture.Transport->Connection->Receive(
            MakeResponse(sent[0]->Cookie, "reply"));
        UNIT_ASSERT(!first.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(fixture.Request()).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT(fixture.Transport->Connection->TakeSent().empty());
        queue->RunAll();
        UNIT_ASSERT_VALUES_EQUAL(GetPayload(first), "reply");
        auto second = fixture.Request();
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Transport->Connection->TakeSent().size(),
            1);
        fixture.Channel->Close();
        UNIT_ASSERT(!fixture.Channel->WhenClosed().HasValue());
        queue->RunAll();
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(second).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT(fixture.Channel->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(ContinuationsDoNotBlockIoTimerOrCloseAndRemainInTheBarrier)
    {
        for (size_t completion: {size_t(0), size_t(1), size_t(2)}) {
            auto queue = CreateThreadPool("ICV2TestResult", 1);
            queue->Start();
            TFixture fixture([](TEvent) {}, Config, queue);
            fixture.Channel->Activate();
            auto response = fixture.Request();
            auto entered = NThreading::NewPromise<void>();
            auto release = NThreading::NewPromise<void>();
            const auto caller = std::this_thread::get_id();
            std::thread::id callbackThread;
            response.Subscribe(
                [&](const auto&)
                {
                    callbackThread = std::this_thread::get_id();
                    fixture.Channel->Close();
                    entered.SetValue();
                    release.GetFuture().GetValueSync();
                });
            if (completion == 0) {
                auto sent = fixture.Transport->Connection->TakeSent();
                fixture.Transport->Connection->Receive(
                    MakeResponse(sent[0]->Cookie, "reply"));
            } else if (completion == 1) {
                fixture.Timer->AdvanceTime(TDuration::Seconds(5));
                fixture.Scheduler->RunAllScheduledTasks();
            } else {
                fixture.Channel->Close();
            }
            const bool started =
                entered.GetFuture().Wait(TDuration::Seconds(5));
            fixture.Transport->Stop();
            const bool waits = !fixture.Channel->WhenClosed().Wait(
                TDuration::MilliSeconds(30));
            release.SetValue();
            const bool closed =
                fixture.Channel->WhenClosed().Wait(TDuration::Seconds(5));
            queue->Stop();
            UNIT_ASSERT(started);
            UNIT_ASSERT(callbackThread != caller);
            UNIT_ASSERT(waits);
            UNIT_ASSERT(closed);
        }
    }

    Y_UNIT_TEST(CorrelatesOutOfOrderResponsesAndIgnoresDuplicates)
    {
        TFixture fixture;
        UNIT_ASSERT(fixture.Channel->Activate());
        TVector<TResponseFuture> responses;
        constexpr size_t Count = 64;
        for (size_t i = 0; i < Count; ++i) {
            responses.push_back(fixture.Request(ToString(i)));
        }
        auto sent = fixture.Transport->Connection->TakeSent();
        UNIT_ASSERT_VALUES_EQUAL(sent.size(), Count);
        THashSet<ui64> cookies;
        for (size_t i = Count; i--;) {
            UNIT_ASSERT(cookies.insert(sent[i]->Cookie).second);
            fixture.Transport->Connection->Receive(
                MakeResponse(sent[i]->Cookie, ToString(i)));
            fixture.Transport->Connection->Receive(
                MakeResponse(sent[i]->Cookie, "duplicate"));
        }
        fixture.Transport->Connection->Receive(MakeResponse(10000, "unknown"));
        for (size_t i = 0; i < Count; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(GetPayload(responses[i]), ToString(i));
        }
        fixture.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT(!fixture.Channel->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(AdmitsResponseBeforeSendReturns)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        fixture.Transport->Connection->OnSend =
            [&](NActors::IEventHandle& request)
        {
            fixture.Transport->Connection->Receive(
                MakeResponse(request.Cookie, "inline"));
        };
        UNIT_ASSERT_VALUES_EQUAL(GetPayload(fixture.Request()), "inline");
    }

    Y_UNIT_TEST(TimesOutAndDiscardsLateResponseWithoutReusingCookie)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        auto first = fixture.Request("first", TDuration::MilliSeconds(10));
        auto sent = fixture.Transport->Connection->TakeSent();
        fixture.Timer->AdvanceTime(TDuration::MilliSeconds(10));
        fixture.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(first).GetError().GetCode(),
            E_TIMEOUT);
        auto next = fixture.Request("next");
        auto nextSent = fixture.Transport->Connection->TakeSent();
        UNIT_ASSERT_VALUES_UNEQUAL(sent[0]->Cookie, nextSent[0]->Cookie);
        fixture.Transport->Connection->Receive(
            MakeResponse(sent[0]->Cookie, "late"));
        UNIT_ASSERT(!next.HasValue());
        fixture.Transport->Connection->Receive(
            MakeResponse(nextSent[0]->Cookie, "next"));
        UNIT_ASSERT_VALUES_EQUAL(GetPayload(next), "next");
    }

    Y_UNIT_TEST(ChecksDeadlineEvenBeforeTimerCallbackRuns)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        auto response = fixture.Request("ping", TDuration::MilliSeconds(10));
        auto sent = fixture.Transport->Connection->TakeSent();
        fixture.Timer->AdvanceTime(TDuration::MilliSeconds(10));
        fixture.Transport->Connection->Receive(
            MakeResponse(sent[0]->Cookie, "late"));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(response).GetError().GetCode(),
            E_TIMEOUT);
    }

    Y_UNIT_TEST(CloseAndTransportStopFailPendingRequests)
    {
        for (bool stopTransport: {false, true}) {
            TFixture fixture;
            fixture.Channel->Activate();
            auto response = fixture.Request();
            if (stopTransport) {
                fixture.Transport->Stop();
            } else {
                fixture.Channel->Close();
            }
            UNIT_ASSERT_VALUES_EQUAL(
                WaitResponse(response).GetError().GetCode(),
                E_REJECTED);
            UNIT_ASSERT(
                fixture.Channel->WhenClosed().Wait(TDuration::Seconds(5)));
            UNIT_ASSERT_VALUES_EQUAL(
                WaitResponse(fixture.Request()).GetError().GetCode(),
                E_REJECTED);
            UNIT_ASSERT(!fixture.Channel->Reply(1, MakeReply("closed")));
        }
    }

    Y_UNIT_TEST(DisconnectFailsPendingRequests)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        auto response = fixture.Request();
        fixture.Transport->Connection->Disconnect();
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(response).GetError().GetCode(),
            E_TRANSPORT_ERROR);
        UNIT_ASSERT(fixture.Channel->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(DroppingChannelClosesConnectionAndReleasesTimerState)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        auto response = fixture.Request();
        auto closed = fixture.Channel->WhenClosed();
        std::weak_ptr<IRequestChannel> weak = fixture.Channel;
        fixture.Channel.reset();
        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(response).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT(closed.Wait(TDuration::Seconds(5)));
        fixture.Scheduler->RunAllScheduledTasks();
    }

    Y_UNIT_TEST(ContinuationCanReleaseLastChannelReference)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        auto response = fixture.Request();
        auto closed = fixture.Channel->WhenClosed();
        std::weak_ptr<IRequestChannel> weak = fixture.Channel;
        response.Subscribe([&](const auto&) { fixture.Channel.reset(); });
        fixture.Channel->Close();
        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT(closed.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(response).GetError().GetCode(),
            E_REJECTED);
    }

    Y_UNIT_TEST(ConcurrentCloseRejectsSendsBeforeReturning)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        auto response = fixture.Request();
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        std::atomic<size_t> calls = 0;
        fixture.Transport->Connection->BeforeClose = [&]
        {
            if (++calls == 1) {
                entered.SetValue();
                release.GetFuture().GetValueSync();
            }
        };
        auto first =
            std::async(std::launch::async, [&] { fixture.Channel->Close(); });
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));
        fixture.Channel->Close();
        const bool rejected = !fixture.Channel->Reply(1, MakeReply("closed"));
        release.SetValue();
        first.get();
        fixture.Transport->Connection->BeforeClose = {};
        UNIT_ASSERT(rejected);
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(response).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT(fixture.Channel->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(CloseFromRequestCallbackWaitsForItsReturn)
    {
        IRequestChannelPtr channel;
        TFixture fixture(
            [&](TEvent event)
            {
                UNIT_ASSERT_VALUES_EQUAL(event->Cookie, 42);
                channel->Close();
                UNIT_ASSERT(!channel->WhenClosed().HasValue());
            });
        channel = fixture.Channel;
        channel->Activate();
        fixture.Transport->Connection->Receive(
            std::make_unique<NActors::IEventHandle>(
                NActors::TActorId(),
                NActors::TActorId(),
                MakePing("request").release(),
                0,
                42));
        UNIT_ASSERT(channel->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(WhenClosedWaitsForRunningTimeoutContinuation)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        auto response = fixture.Request();
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        response.Subscribe(
            [&](const auto&)
            {
                fixture.Channel->Close();
                entered.SetValue();
                release.GetFuture().GetValueSync();
            });
        fixture.Timer->AdvanceTime(TDuration::Seconds(5));
        auto timer = std::async(
            std::launch::async,
            [&] { fixture.Scheduler->RunAllScheduledTasks(); });
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));
        fixture.Channel->Close();
        const bool waits =
            !fixture.Channel->WhenClosed().Wait(TDuration::MilliSeconds(30));
        release.SetValue();
        timer.get();
        UNIT_ASSERT(waits);
        UNIT_ASSERT(fixture.Channel->WhenClosed().Wait(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(ResponseTimeoutAndCloseRaceCompleteEachRequestOnce)
    {
        TFixture fixture([](TEvent) {}, MultiConfig);
        fixture.Channel->Activate();
        std::atomic<size_t> completions = 0;
        TVector<TResponseFuture> responses;
        for (size_t i = 0; i < 128; ++i) {
            auto future = fixture.Channel->Request(
                MakeRequest(i % 2, "request"),
                TDuration::Seconds(5));
            future.Subscribe([&](const auto&) { ++completions; });
            responses.push_back(std::move(future));
        }
        auto sent = fixture.Transport->Connection->TakeSent();
        fixture.Timer->AdvanceTime(TDuration::Seconds(5));
        auto replies = std::async(
            std::launch::async,
            [&]
            {
                for (const auto& request: sent) {
                    auto response =
                        request->Type == TOtherRequest::EventType
                            ? MakeResponse<TOtherResponse>(
                                  request->Cookie,
                                  "reply")
                            : MakeResponse(request->Cookie, "reply");
                    fixture.Transport->Connection->Receive(std::move(response));
                }
            });
        auto timeouts = std::async(
            std::launch::async,
            [&] { fixture.Scheduler->RunAllScheduledTasks(); });
        fixture.Channel->Close();
        replies.get();
        timeouts.get();
        for (const auto& response: responses) {
            const auto code = WaitResponse(response).GetError().GetCode();
            UNIT_ASSERT(code == E_TIMEOUT || code == E_REJECTED);
        }
        UNIT_ASSERT_VALUES_EQUAL(completions.load(), responses.size());
        UNIT_ASSERT(fixture.Channel->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(RejectsInvalidEventsAndInactiveSend)
    {
        TFixture fixture;
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(fixture.Request()).GetError().GetCode(),
            E_REJECTED);
        fixture.Channel->Activate();
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(fixture.Channel->Request({}, TDuration::Seconds(5)))
                .GetError()
                .GetCode(),
            E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(fixture.Channel->Request(
                             MakeReply("wrong type"),
                             TDuration::Seconds(5)))
                .GetError()
                .GetCode(),
            E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(fixture.Request("expired", TDuration::Zero()))
                .GetError()
                .GetCode(),
            E_TIMEOUT);
        UNIT_ASSERT(!fixture.Channel->Reply(0, MakeReply("no cookie")));
        UNIT_ASSERT(!fixture.Channel->Reply(1, MakePing("wrong type")));
        auto pending = fixture.Request();
        fixture.Transport->Connection->Receive(
            std::make_unique<NActors::IEventHandle>(
                NActors::TActorId(),
                NActors::TActorId(),
                MakePing("zero cookie").release(),
                0,
                0));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(pending).GetError().GetCode(),
            E_BADMSG);
    }

    Y_UNIT_TEST(BidirectionalPingOverTcpAndHelloWithoutActorSystem)
    {
        if (!NActors::TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        TNetworkFixture fixture;
        std::array<TVector<TResponseFuture>, 2> responses;
        const TString payload(256 * 1024, '\0');
        for (size_t i = 0; i < 32; ++i) {
            for (size_t peer = 0; peer < fixture.Channels.size(); ++peer) {
                responses[peer].push_back(fixture.Channels[peer]->Request(
                    MakePing(ToString(peer) + ":" + ToString(i) + payload),
                    TDuration::Seconds(5)));
            }
        }
        for (size_t peer = 0; peer < fixture.Channels.size(); ++peer) {
            for (size_t i = 0; i < responses[peer].size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    GetPayload(responses[peer][i]),
                    ToString(peer) + ":" + ToString(i) + payload);
            }
        }
    }

    Y_UNIT_TEST(BidirectionalMixedRequestsOverTcpAndHelloWithoutActorSystem)
    {
        if (!NActors::TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        TNetworkFixture fixture(true, MultiConfig);
        std::array<TVector<TResponseFuture>, 2> responses;
        const TString payload(256 * 1024, '\0');
        for (size_t i = 0; i < 32; ++i) {
            for (size_t peer = 0; peer < fixture.Channels.size(); ++peer) {
                const TString message =
                    ToString(peer) + ":" + ToString(i) + payload;
                responses[peer].push_back(fixture.Channels[peer]->Request(
                    MakeRequest(i % 2, message),
                    TDuration::Seconds(5)));
            }
        }
        for (size_t peer = 0; peer < responses.size(); ++peer) {
            for (size_t i = 0; i < responses[peer].size(); ++i) {
                const auto received =
                    i % 2 ? GetPayload<TOtherResponse>(responses[peer][i])
                          : GetPayload(responses[peer][i]);
                UNIT_ASSERT_VALUES_EQUAL(
                    received,
                    ToString(peer) + ":" + ToString(i) + payload);
            }
        }
    }

    Y_UNIT_TEST(TransportStopFailsPendingRequestLocallyAndOnPeer)
    {
        if (!NActors::TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        for (size_t stoppedPeer: {size_t(0), size_t(1)}) {
            TNetworkFixture fixture(false);
            auto response = fixture.Channels[0]->Request(
                MakePing("pending"),
                TDuration::Seconds(5));
            UNIT_ASSERT(fixture.RequestReceived.GetFuture().Wait(
                TDuration::Seconds(5)));
            fixture.Transports[stoppedPeer]->Stop();
            UNIT_ASSERT_VALUES_EQUAL(
                WaitResponse(response).GetError().GetCode(),
                stoppedPeer == 0 ? E_REJECTED : E_TRANSPORT_ERROR);
            UNIT_ASSERT(
                fixture.Channels[0]->WhenClosed().Wait(TDuration::Seconds(5)));
        }
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
