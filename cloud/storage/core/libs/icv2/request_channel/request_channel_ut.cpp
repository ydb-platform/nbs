#include "request_channel.h"

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/icv2/handshake/handshake.h>
#include <cloud/storage/core/libs/icv2/tcp/testlib/socket_test.h>

#include <contrib/ydb/library/actors/core/event_pb.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_common.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>
#include <contrib/ydb/library/actors/interconnect/uring_context.h>
#include <contrib/ydb/library/actors/protos/unittests.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/system/event.h>
#include <util/system/mutex.h>

#include <atomic>
#include <functional>
#include <future>
#include <thread>

namespace NCloud::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NTests;
using namespace std::chrono_literals;
using TEvent = std::unique_ptr<NActors::IEventHandle>;

struct TEvDiagnostic
{
    enum EEvents
    {
        EvPingRequest = EventSpaceBegin(NActors::TEvents::ES_USERSPACE) + 1,
        EvPingResponse,
    };

    struct TEvPingRequest final
        : NActors::TEventPB<TEvPingRequest, TMessageWithPayload, EvPingRequest>
    {
    };

    struct TEvPingResponse final
        : NActors::
              TEventPB<TEvPingResponse, TMessageWithPayload, EvPingResponse>
    {
    };
};

const TRequestChannelConfig Config{
    {{TEvDiagnostic::EvPingRequest, TEvDiagnostic::EvPingResponse}},
    false};

////////////////////////////////////////////////////////////////////////////////

struct TOtherRequest final
    : NActors::TEventPB<
          TOtherRequest,
          TMessageWithPayload,
          TEvDiagnostic::EvPingResponse + 1>
{
};

////////////////////////////////////////////////////////////////////////////////

struct TOtherResponse final
    : NActors::TEventPB<
          TOtherResponse,
          TMessageWithPayload,
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
    event->Record.AddSomeData(std::move(payload));
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

TResponseResult WaitResponse(TResponseFuture future)
{
    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
    return future.ExtractValue();
}

template <typename T = TEvDiagnostic::TEvPingResponse>
TString GetPayload(const TResponseFuture& future)
{
    auto [event, error] = WaitResponse(future);
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
    return event->Get<T>()->Record.GetSomeData(0);
}

template <typename TRequest, typename TResponse>
void ReplyWithPayload(IIncomingRequest& request)
{
    const auto error = request.Reply(
        MakePayload<TResponse>(
            request.GetEvent().Get<TRequest>()->Record.GetSomeData(0)));
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
}

////////////////////////////////////////////////////////////////////////////////

class TTestConnection final: public IConnection
{
    enum class EPhase
    {
        Created,
        Active,
        Closing,
        Closed,
    };

    TMutex Mutex;
    std::shared_ptr<IConnectionHandler> Handler;
    TVector<TEvent> Sent;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();
    EPhase Phase = EPhase::Created;
    size_t Operations = 0;

public:
    std::function<void(NActors::IEventHandle&)> OnSend;
    std::function<void()> BeforeClose;
    ESendResult SendResult = ESendResult::Accepted;

    explicit TTestConnection(std::shared_ptr<IConnectionHandler> handler)
        : Handler(std::move(handler))
    {}

    bool Activate() override
    {
        TGuard<TMutex> guard(Mutex);
        if (Phase != EPhase::Created) {
            return false;
        }

        Phase = EPhase::Active;
        return true;
    }

    ESendResult Send(TEvent event) override
    {
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase != EPhase::Active) {
                return ESendResult::NotActive;
            }

            ++Operations;
        }
        Y_DEFER
        {
            FinishOperation();
        };

        // Complete inline to exercise response-before-Send-returns admission.
        if (OnSend) {
            OnSend(*event);
        }
        TGuard<TMutex> guard(Mutex);
        if (SendResult != ESendResult::Accepted) {
            return SendResult;
        }

        Sent.push_back(std::move(event));
        return ESendResult::Accepted;
    }

    void Close() override
    {
        if (BeforeClose) {
            BeforeClose();
        }
        std::shared_ptr<IConnectionHandler> handler;
        bool publish = false;
        {
            TGuard<TMutex> guard(Mutex);
            if (Phase == EPhase::Closing || Phase == EPhase::Closed) {
                return;
            }

            Phase = EPhase::Closing;
            handler = std::move(Handler);
            publish = PrepareClosedLocked();
        }
        handler.reset();
        if (publish) {
            Closed.SetValue();
        }
    }

    NThreading::TFuture<void> WhenClosed() const override
    {
        return Closed.GetFuture();
    }

    TVector<TEvent> TakeSent()
    {
        TVector<TEvent> result;
        TGuard<TMutex> guard(Mutex);
        result.swap(Sent);
        return result;
    }

    void Receive(TEvent event)
    {
        if (auto handler = BeginCallback()) {
            Y_DEFER
            {
                handler.reset();
                FinishOperation();
            };
            handler->HandleEvent(std::move(event));
        }
    }

    void Disconnect()
    {
        if (auto handler = BeginCallback()) {
            Y_DEFER
            {
                handler.reset();
                FinishOperation();
            };
            handler->HandleDisconnect("test peer disconnected");
        }
    }

private:
    std::shared_ptr<IConnectionHandler> BeginCallback()
    {
        TGuard<TMutex> guard(Mutex);
        if (Phase != EPhase::Active) {
            return {};
        }

        ++Operations;
        return Handler;
    }

    void FinishOperation()
    {
        bool publish = false;
        {
            TGuard<TMutex> guard(Mutex);
            Y_ABORT_UNLESS(Operations);

            --Operations;
            publish = PrepareClosedLocked();
        }
        if (publish) {
            Closed.SetValue();
        }
    }

    bool PrepareClosedLocked()
    {
        if (Phase != EPhase::Closing || Operations) {
            return false;
        }

        Phase = EPhase::Closed;
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

class TAdvancingTimer final: public ITimer
{
    const TDuration Step;
    TInstant Timestamp = TInstant::Zero();

public:
    explicit TAdvancingTimer(TDuration step)
        : Step(step)
    {}

    TInstant Now() override
    {
        const auto now = Timestamp;
        Timestamp += Step;
        return now;
    }

    void Sleep(TDuration duration) override
    {
        Timestamp += duration;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCountingScheduler final: public IScheduler
{
    TTestScheduler Scheduler;

public:
    std::atomic<size_t> Tasks = 0;
    std::atomic<size_t> Scheduled = 0;
    std::atomic<ui64> LastDeadline = 0;
    std::function<void()> OnSchedule;

    void Start() override
    {}

    void Stop() override
    {}

    void Schedule(ITaskQueue*, TInstant deadline, TCallback callback) override
    {
        ++Tasks;
        ++Scheduled;
        LastDeadline = deadline.MicroSeconds();
        Scheduler.Schedule(
            nullptr,
            deadline,
            [this, callback = std::move(callback)]
            {
                --Tasks;
                callback();
            });
        if (OnSchedule) {
            OnSchedule();
        }
    }

    void RunAllScheduledTasks()
    {
        Scheduler.RunAllScheduledTasks();
    }
};

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

class TBlockingTaskQueue final: public ITaskQueue
{
public:
    enum class EBlockAt
    {
        AfterExecute,
        BeforeEnqueueReturn,
    };

    struct TEvents
    {
        TManualEvent TaskEnqueued;
        TManualEvent TaskExecuted;
        TManualEvent DestructionStarted;
        TManualEvent Release;
    };

private:
    const ITaskQueuePtr Queue;
    const EBlockAt BlockAt;
    const std::shared_ptr<TEvents> Events;

public:
    TBlockingTaskQueue(
        ITaskQueuePtr queue,
        EBlockAt blockAt,
        std::shared_ptr<TEvents> events)
        : Queue(std::move(queue))
        , BlockAt(blockAt)
        , Events(std::move(events))
    {}

    ~TBlockingTaskQueue() override
    {
        Events->DestructionStarted.Signal();
    }

    void Start() override
    {
        Queue->Start();
    }

    void Stop() override
    {
        Queue->Stop();
    }

    void Enqueue(ITaskPtr task) override
    {
        // Signals remain alive while the executor's destruction is in progress.
        Queue->ExecuteSimple(
            [task = std::move(task), events = Events, blockAt = BlockAt]
            {
                task->Execute();
                events->TaskExecuted.Signal();
                if (blockAt == EBlockAt::AfterExecute) {
                    events->Release.WaitI();
                }
            });
        Events->TaskEnqueued.Signal();
        if (BlockAt == EBlockAt::BeforeEnqueueReturn) {
            Events->Release.WaitI();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void UnblockAndStopExecutor(
    const std::weak_ptr<ITaskQueue>& executor,
    TManualEvent& release,
    const std::future<void>& destroying)
{
    // Retain ownership during failure cleanup before unblocking the worker.
    const auto owner = executor.lock();
    release.Signal();
    if (destroying.valid()) {
        destroying.wait();
    }
    if (owner) {
        owner->Stop();
    }
}

TSocketPtr CreateTestSocket()
{
    const int fd =
        ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    UNIT_ASSERT(fd >= 0);
    return MakeIntrusive<NInterconnect::TStreamSocket>(fd);
}

////////////////////////////////////////////////////////////////////////////////

class TTestRequestHandler final: public IRequestChannelHandler
{
public:
    using TCallback = std::function<void(IIncomingRequestPtr)>;

private:
    const TCallback Callback;

public:
    explicit TTestRequestHandler(TCallback callback)
        : Callback(std::move(callback))
    {}

    void HandleRequest(IIncomingRequestPtr request) override
    {
        Callback(std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture
{
    std::shared_ptr<TTestTransport> Transport =
        std::make_shared<TTestTransport>();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TCountingScheduler> Scheduler =
        std::make_shared<TCountingScheduler>();
    IRequestChannelPtr Channel;

    explicit TFixture(
        TTestRequestHandler::TCallback handler = [](IIncomingRequestPtr) {},
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
            std::make_shared<TTestRequestHandler>(std::move(handler)));
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

////////////////////////////////////////////////////////////////////////////////

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
        auto connector = NTests::CreateTestConnector();
        connector->Start();
        std::array<TSocketPtr, 2> sockets{
            WaitSocket(connector->Connect(
                listener.Listener->GetAddress(),
                TDuration::Seconds(5))),
            listener.Accept()};
        connector->Stop();
        listener.Listener->Stop();
        std::array<IHandshakeProviderPtr, 2> hellos;
        Y_DEFER
        {
            for (const auto& hello: hellos) {
                if (hello) {
                    hello->Stop();
                }
            }
        };
        for (size_t i = 0; i < hellos.size(); ++i) {
            auto [hello, error] = CreateHandshakeProvider(THandshakeConfig{});
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
            Transports[i] = CreateTransport(
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
                std::make_shared<TTestRequestHandler>(
                    [this](IIncomingRequestPtr request)
                    {
                        if (!Echo) {
                            RequestReceived.SetValue();
                            return;
                        }
                        if (request->GetEvent().Type ==
                            TOtherRequest::EventType) {
                            ReplyWithPayload<TOtherRequest, TOtherResponse>(
                                *request);
                        } else {
                            ReplyWithPayload<
                                TEvDiagnostic::TEvPingRequest,
                                TEvDiagnostic::TEvPingResponse>(*request);
                        }
                    }));
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
                std::make_shared<TTestRequestHandler>(
                    [](IIncomingRequestPtr) {}));
            UNIT_ASSERT(!channel);
            UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), E_ARGUMENT);
            UNIT_ASSERT(!transport->Connection);
        }
    }

    Y_UNIT_TEST(CorrelatesParallelMixedRequestsAndOutOfOrderResponses)
    {
        TFixture fixture([](IIncomingRequestPtr) {}, MultiConfig);
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
                        request->Get<TOtherRequest>()->Record.GetSomeData(0)));
                // Even another configured type is ignored after completion.
                fixture.Transport->Connection->Receive(
                    MakeResponse(request->Cookie, "duplicate"));
            } else {
                fixture.Transport->Connection->Receive(MakeResponse(
                    request->Cookie,
                    request->Get<TEvDiagnostic::TEvPingRequest>()
                        ->Record.GetSomeData(0)));
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
            TFixture fixture([](IIncomingRequestPtr) {}, MultiConfig, queue);
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
            TFixture fixture([](IIncomingRequestPtr) {}, MultiConfig);
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
        TFixture fixture([](IIncomingRequestPtr) {}, config);
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

    Y_UNIT_TEST(SchedulerCanCloseChannelBeforeSend)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        fixture.Scheduler->OnSchedule = [&]
        {
            fixture.Channel->Close();
        };
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(fixture.Request()).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT(fixture.Transport->Connection->TakeSent().empty());
        UNIT_ASSERT(fixture.Channel->WhenClosed().Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(!HasError(fixture.Channel->WhenClosed().GetValue()));
    }

    Y_UNIT_TEST(CompletedRequestCancelsItsTimer)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        auto response = fixture.Request();
        auto sent = fixture.Transport->Connection->TakeSent();
        fixture.Transport->Connection->Receive(
            MakeResponse(sent.at(0)->Cookie, "reply"));
        UNIT_ASSERT_VALUES_EQUAL(GetPayload(response), "reply");
        fixture.Timer->AdvanceTime(TDuration::Seconds(5));
        fixture.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler->Tasks.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler->Scheduled.load(), 1);
        UNIT_ASSERT(!fixture.Channel->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(SchedulesAtDeadlineAndRearmsForEarlierRequest)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        const auto longDeadline = fixture.Timer->Now() + TDuration::Hours(1);
        auto longRequest = fixture.Request("long", TDuration::Hours(1));
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Scheduler->LastDeadline.load(),
            longDeadline.MicroSeconds());
        const auto shortDeadline =
            fixture.Timer->Now() + TDuration::MilliSeconds(1);
        auto shortRequest =
            fixture.Request("short", TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Scheduler->LastDeadline.load(),
            shortDeadline.MicroSeconds());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler->Scheduled.load(), 2);
        fixture.Timer->AdvanceTime(TDuration::MilliSeconds(1));
        fixture.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(shortRequest).GetError().GetCode(),
            E_TIMEOUT);
        UNIT_ASSERT(!longRequest.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler->Tasks.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Scheduler->LastDeadline.load(),
            longDeadline.MicroSeconds());
        fixture.Timer->AdvanceTime(TDuration::Hours(1));
        fixture.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(longRequest).GetError().GetCode(),
            E_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler->Tasks.load(), 0);
    }

    Y_UNIT_TEST(RequestLimitIncludesQueuedCompletionsAndRecovers)
    {
        auto queue = std::make_shared<TManualTaskQueue>();
        auto config = Config;
        config.MaxPendingRequests = 1;
        TFixture fixture([](IIncomingRequestPtr) {}, config, queue);
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

    Y_UNIT_TEST(CloseCompletesIndependentRequestsOnSeparateWorkers)
    {
        auto queue = CreateThreadPool("ICV2CloseTest", 2);
        queue->Start();
        TFixture fixture([](IIncomingRequestPtr) {}, Config, queue);
        fixture.Channel->Activate();
        auto first = fixture.Request("first");
        auto second = fixture.Request("second");
        std::atomic<bool> firstSawSecond = false;
        std::atomic<bool> secondSawFirst = false;
        Y_DEFER
        {
            fixture.Channel->Close();
            queue->Stop();
        };
        first.Subscribe(
            [second, &firstSawSecond](const auto&)
            { firstSawSecond = second.Wait(TDuration::Seconds(5)); });
        second.Subscribe(
            [first, &secondSawFirst](const auto&)
            { secondSawFirst = first.Wait(TDuration::Seconds(5)); });
        fixture.Channel->Close();
        UNIT_ASSERT(fixture.Channel->WhenClosed().Wait(TDuration::Seconds(15)));
        UNIT_ASSERT(firstSawSecond.load());
        UNIT_ASSERT(secondSawFirst.load());
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(first).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(second).GetError().GetCode(),
            E_REJECTED);
    }

    Y_UNIT_TEST(ContinuationsDoNotBlockIoTimerOrCloseAndRemainInTheBarrier)
    {
        for (size_t completion: {size_t(0), size_t(1), size_t(2)}) {
            auto queue = CreateThreadPool("ICV2TestResult", 1);
            queue->Start();
            TFixture fixture([](IIncomingRequestPtr) {}, Config, queue);
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

    Y_UNIT_TEST(DoesNotSendWhenTimeoutExpiresBeforeAdmission)
    {
        const auto timeout = TDuration::MilliSeconds(10);
        auto transport = std::make_shared<TTestTransport>();
        auto scheduler = std::make_shared<TCountingScheduler>();
        auto config = Config;
        config.MaxPendingRequests = 1;
        auto [channel, error] = CreateRequestChannel(
            transport,
            CreateTestSocket(),
            {},
            config,
            std::make_shared<TAdvancingTimer>(timeout),
            scheduler,
            CreateTaskQueueStub(),
            std::make_shared<TTestRequestHandler>(
                [](IIncomingRequestPtr request) { Y_UNUSED(request); }));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        Y_DEFER
        {
            channel->Close();
            transport->Stop();
        };
        UNIT_ASSERT(channel->Activate());

        auto expired = channel->Request(MakePing("expired"), timeout);
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(expired).GetError().GetCode(),
            E_TIMEOUT);
        UNIT_ASSERT(transport->Connection->TakeSent().empty());

        auto next = channel->Request(MakePing("next"), TDuration::Seconds(1));
        auto sent = transport->Connection->TakeSent();
        UNIT_ASSERT_VALUES_EQUAL(sent.size(), 1);

        transport->Connection->Receive(MakeResponse(sent[0]->Cookie, "reply"));
        UNIT_ASSERT_VALUES_EQUAL(GetPayload(next), "reply");
    }

    Y_UNIT_TEST(TimeoutDuringTimerSchedulingDoesNotPreventSend)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        fixture.Scheduler->OnSchedule = [&]
        {
            fixture.Timer->AdvanceTime(TDuration::MilliSeconds(10));
            fixture.Scheduler->RunAllScheduledTasks();
        };

        auto response = fixture.Request("ping", TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(response).GetError().GetCode(),
            E_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Transport->Connection->TakeSent().size(),
            1);
    }

    Y_UNIT_TEST(TimeoutAfterAdmissionDoesNotPreventSend)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        fixture.Transport->Connection->OnSend = [&](NActors::IEventHandle&)
        {
            fixture.Timer->AdvanceTime(TDuration::MilliSeconds(10));
            fixture.Scheduler->RunAllScheduledTasks();
        };

        auto response = fixture.Request("ping", TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(response).GetError().GetCode(),
            E_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Transport->Connection->TakeSent().size(),
            1);
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

    Y_UNIT_TEST(CanDestroyCompletionExecutorAfterWhenClosed)
    {
        const auto events = std::make_shared<TBlockingTaskQueue::TEvents>();
        auto queue = std::make_shared<TBlockingTaskQueue>(
            CreateThreadPool("ICV2DestroyTest", 1),
            TBlockingTaskQueue::EBlockAt::AfterExecute,
            events);
        queue->Start();
        TFixture fixture(
            [](IIncomingRequestPtr request) { Y_UNUSED(request); },
            Config,
            queue);
        const std::weak_ptr<ITaskQueue> weak = queue;
        std::future<void> destroying;
        Y_DEFER
        {
            UnblockAndStopExecutor(weak, events->Release, destroying);
        };
        UNIT_ASSERT(fixture.Channel->Activate());

        const auto response = fixture.Request();
        const auto closed = fixture.Channel->WhenClosed();
        fixture.Channel->Close();
        UNIT_ASSERT(closed.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(events->TaskExecuted.WaitT(TDuration::Seconds(5)));

        fixture.Channel.reset();
        destroying = std::async(
            std::launch::async,
            [queue = std::move(queue)]() mutable { queue.reset(); });
        const bool releasesExecutor =
            events->DestructionStarted.WaitT(TDuration::Seconds(5));
        const bool joinsWorker =
            destroying.wait_for(0ms) == std::future_status::timeout;
        UnblockAndStopExecutor(weak, events->Release, destroying);
        destroying.get();
        UNIT_ASSERT(releasesExecutor);
        UNIT_ASSERT(joinsWorker);
        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(response).GetError().GetCode(),
            E_REJECTED);
    }

    Y_UNIT_TEST(WhenClosedWaitsForResultEnqueueToReturn)
    {
        const auto events = std::make_shared<TBlockingTaskQueue::TEvents>();
        auto queue = std::make_shared<TBlockingTaskQueue>(
            CreateThreadPool("ICV2EnqueueTest", 1),
            TBlockingTaskQueue::EBlockAt::BeforeEnqueueReturn,
            events);
        queue->Start();
        TFixture fixture(
            [](IIncomingRequestPtr request) { Y_UNUSED(request); },
            Config,
            queue);
        std::future<void> timer;
        Y_DEFER
        {
            events->Release.Signal();
            if (timer.valid()) {
                timer.wait();
            }
            fixture.Channel->Close();
            queue->Stop();
        };
        UNIT_ASSERT(fixture.Channel->Activate());

        const auto response = fixture.Request();
        response.Subscribe(
            [&fixture](const auto& result)
            {
                Y_UNUSED(result);

                fixture.Channel->Close();
            });
        const auto closed = fixture.Channel->WhenClosed();
        fixture.Timer->AdvanceTime(TDuration::Seconds(5));
        timer = std::async(
            std::launch::async,
            [&fixture] { fixture.Scheduler->RunAllScheduledTasks(); });
        UNIT_ASSERT(events->TaskEnqueued.WaitT(TDuration::Seconds(5)));
        UNIT_ASSERT(events->TaskExecuted.WaitT(TDuration::Seconds(5)));
        UNIT_ASSERT(!closed.HasValue());

        events->Release.Signal();
        timer.get();
        UNIT_ASSERT(closed.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(response).GetError().GetCode(),
            E_TIMEOUT);
    }

    Y_UNIT_TEST(ReleasesCompletionExecutorWithoutPendingRequests)
    {
        for (bool activate: {false, true}) {
            auto queue = std::make_shared<TManualTaskQueue>();
            const std::weak_ptr<ITaskQueue> weak = queue;
            TFixture fixture(
                [](IIncomingRequestPtr request) { Y_UNUSED(request); },
                Config,
                queue);
            if (activate) {
                UNIT_ASSERT(fixture.Channel->Activate());
            }
            fixture.Channel->Close();
            UNIT_ASSERT(fixture.Channel->WhenClosed().HasValue());
            queue.reset();
            UNIT_ASSERT(weak.expired());
        }
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
        IIncomingRequestPtr incoming;
        TFixture fixture([&](IIncomingRequestPtr request)
                         { incoming = std::move(request); });
        fixture.Channel->Activate();
        fixture.Transport->Connection->Receive(
            MakeResponse<TEvDiagnostic::TEvPingRequest>(42, "request"));
        UNIT_ASSERT(incoming);
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
        std::future<void> first;
        Y_DEFER
        {
            release.TrySetValue();
            if (first.valid()) {
                first.wait();
            }
            fixture.Transport->Connection->BeforeClose = {};
        };
        first =
            std::async(std::launch::async, [&] { fixture.Channel->Close(); });
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));
        fixture.Channel->Close();
        const bool rejected = HasError(incoming->Reply(MakeReply("closed")));
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
        IIncomingRequestPtr incoming;
        TFixture fixture(
            [&](IIncomingRequestPtr request)
            {
                UNIT_ASSERT_VALUES_EQUAL(request->GetEvent().Cookie, 42);
                request->CloseChannel();
                UNIT_ASSERT(!fixture.Channel->WhenClosed().HasValue());
                incoming = std::move(request);
            });
        fixture.Channel->Activate();
        fixture.Transport->Connection->Receive(
            MakeResponse<TEvDiagnostic::TEvPingRequest>(42, "request"));
        UNIT_ASSERT(incoming);
        UNIT_ASSERT(fixture.Channel->WhenClosed().HasValue());
        std::weak_ptr<IRequestChannel> weak = fixture.Channel;
        fixture.Channel.reset();
        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT(HasError(incoming->Reply(MakeReply("closed"))));
        incoming->CloseChannel();
    }

    Y_UNIT_TEST(CloseWaitsForConcurrentIncomingRequest)
    {
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        std::atomic<size_t> calls = 0;
        TFixture fixture(
            [&](IIncomingRequestPtr request)
            {
                Y_UNUSED(request);

                ++calls;
                entered.SetValue();
                release.GetFuture().GetValueSync();
            });
        fixture.Channel->Activate();
        std::future<void> receive;
        Y_DEFER
        {
            release.TrySetValue();
            if (receive.valid()) {
                receive.wait();
            }
        };
        receive = std::async(
            std::launch::async,
            [&]
            {
                fixture.Transport->Connection->Receive(
                    MakeResponse<TEvDiagnostic::TEvPingRequest>(42, "request"));
            });
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));

        fixture.Channel->Close();
        UNIT_ASSERT(!fixture.Channel->WhenClosed().HasValue());
        UNIT_ASSERT(!fixture.Transport->Connection->WhenClosed().HasValue());

        fixture.Transport->Connection->Receive(
            MakeResponse<TEvDiagnostic::TEvPingRequest>(43, "after close"));
        UNIT_ASSERT_VALUES_EQUAL(calls.load(), 1);

        release.SetValue();
        receive.get();
        UNIT_ASSERT(fixture.Transport->Connection->WhenClosed().HasValue());
        UNIT_ASSERT(fixture.Channel->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(IncomingRequestAllowsOneMatchingReply)
    {
        IIncomingRequestPtr incoming;
        TFixture fixture(
            [&](IIncomingRequestPtr request) { incoming = std::move(request); },
            MultiConfig);
        fixture.Channel->Activate();
        for (ui32 mode = 0; mode < 3; ++mode) {
            fixture.Transport->Connection->Receive(
                MakeResponse<TEvDiagnostic::TEvPingRequest>(
                    42 + mode,
                    "request"));
            UNIT_ASSERT(incoming);
            if (mode == 0) {
                UNIT_ASSERT(!HasError(incoming->Reply(MakeReply("reply"))));
                auto sent = fixture.Transport->Connection->TakeSent();
                UNIT_ASSERT_VALUES_EQUAL(sent.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(sent[0]->Cookie, 42);
            } else if (mode == 1) {
                UNIT_ASSERT_VALUES_EQUAL(
                    incoming->Reply(MakePayload<TOtherResponse>("wrong"))
                        .GetCode(),
                    E_ARGUMENT);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    incoming->Reply({}).GetCode(),
                    E_ARGUMENT);
            }
            UNIT_ASSERT_VALUES_EQUAL(
                incoming->Reply(MakeReply("again")).GetCode(),
                E_INVALID_STATE);
            UNIT_ASSERT(fixture.Transport->Connection->TakeSent().empty());
            UNIT_ASSERT(!fixture.Channel->WhenClosed().HasValue());
        }
        fixture.Transport->Connection->Receive(
            MakeResponse<TEvDiagnostic::TEvPingRequest>(45, "no reply"));
        incoming.reset();
        UNIT_ASSERT(fixture.Transport->Connection->TakeSent().empty());
        UNIT_ASSERT(!fixture.Channel->WhenClosed().HasValue());
    }

    Y_UNIT_TEST(ClosureReasonIsStableWithoutPendingRequests)
    {
        for (ui32 mode = 0; mode < 3; ++mode) {
            TFixture fixture;
            fixture.Channel->Activate();
            auto closed = fixture.Channel->WhenClosed();
            if (mode == 0) {
                fixture.Channel->Close();
            } else if (mode == 1) {
                fixture.Transport->Connection->Disconnect();
            } else {
                fixture.Transport->Connection->Receive(
                    MakeResponse<TOtherResponse>(42, "unexpected"));
            }
            UNIT_ASSERT(closed.Wait(TDuration::Seconds(5)));
            const ui32 expected = mode == 0   ? S_OK
                                  : mode == 1 ? E_TRANSPORT_ERROR
                                              : E_BADMSG;
            UNIT_ASSERT_VALUES_EQUAL(closed.GetValue().GetCode(), expected);
            fixture.Channel->Close();
            UNIT_ASSERT_VALUES_EQUAL(
                fixture.Channel->WhenClosed().GetValue().GetCode(),
                expected);
        }
    }

    Y_UNIT_TEST(ContinuationCanStartNextRequestAtAdmissionLimit)
    {
        auto queue = std::make_shared<TManualTaskQueue>();
        auto config = Config;
        config.MaxPendingRequests = 1;
        TFixture fixture([](IIncomingRequestPtr) {}, config, queue);
        fixture.Channel->Activate();
        auto first = fixture.Request("first");
        TResponseFuture second;
        first.Subscribe([&](const auto&)
                        { second = fixture.Request("second"); });
        auto sent = fixture.Transport->Connection->TakeSent();
        fixture.Transport->Connection->Receive(
            MakeResponse(sent.at(0)->Cookie, "first"));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitResponse(fixture.Request()).GetError().GetCode(),
            E_REJECTED);
        queue->RunAll();
        sent = fixture.Transport->Connection->TakeSent();
        UNIT_ASSERT_VALUES_EQUAL(sent.size(), 1);
        UNIT_ASSERT(!second.HasValue());
        fixture.Transport->Connection->Receive(
            MakeResponse(sent[0]->Cookie, "second"));
        queue->RunAll();
        UNIT_ASSERT_VALUES_EQUAL(GetPayload(second), "second");
    }

    Y_UNIT_TEST(ConcurrentRequestsReuseAdmissionSlotsAfterCompletion)
    {
        constexpr ui32 WorkerCount = 4;
        auto completions = CreateThreadPool("ICV2TestResult", WorkerCount);
        completions->Start();
        Y_DEFER
        {
            completions->Stop();
        };
        auto config = Config;
        config.MaxPendingRequests = WorkerCount;
        TFixture fixture(
            [](IIncomingRequestPtr request) { Y_UNUSED(request); },
            config,
            completions);
        fixture.Channel->Activate();
        fixture.Transport->Connection->OnSend =
            [&](NActors::IEventHandle& event)
        {
            fixture.Transport->Connection->Receive(
                MakeResponse(event.Cookie, "reply"));
        };
        std::array<std::future<void>, WorkerCount> callers;
        Y_DEFER
        {
            for (auto& caller: callers) {
                if (caller.valid()) {
                    caller.wait();
                }
            }
        };
        for (auto& caller: callers) {
            caller = std::async(
                std::launch::async,
                [&]
                {
                    for (size_t i = 0; i < 64; ++i) {
                        UNIT_ASSERT_VALUES_EQUAL(
                            GetPayload(fixture.Request()),
                            "reply");
                    }
                });
        }
        for (auto& caller: callers) {
            caller.get();
        }
        fixture.Channel->Close();
        UNIT_ASSERT(fixture.Channel->WhenClosed().Wait(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(WhenClosedWaitsForRunningTimeoutContinuation)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        auto response = fixture.Request();
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        std::future<void> timer;
        Y_DEFER
        {
            release.TrySetValue();
            if (timer.valid()) {
                timer.wait();
            }
            fixture.Channel->Close();
        };
        response.Subscribe(
            [&](const auto&)
            {
                fixture.Channel->Close();
                entered.SetValue();
                release.GetFuture().GetValueSync();
            });
        fixture.Timer->AdvanceTime(TDuration::Seconds(5));
        timer = std::async(
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
        TFixture fixture([](IIncomingRequestPtr) {}, MultiConfig);
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

    Y_UNIT_TEST(ResponseOwnershipSurvivesFutureAndChannelDestruction)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        auto future = fixture.Request();
        auto sent = fixture.Transport->Connection->TakeSent();
        fixture.Transport->Connection->Receive(
            MakeResponse(sent.at(0)->Cookie, "owned response"));
        UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
        auto [event, error] = future.ExtractValue();
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        future = {};
        fixture.Channel.reset();
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(
            event->Get<TEvDiagnostic::TEvPingResponse>()->Record.GetSomeData(0),
            "owned response");
    }

    Y_UNIT_TEST(ReportsQueueLimitAndRecovers)
    {
        TFixture fixture;
        fixture.Channel->Activate();
        fixture.Transport->Connection->SendResult = ESendResult::QueueFull;
        auto rejected = WaitResponse(fixture.Request());
        UNIT_ASSERT_VALUES_EQUAL(rejected.GetError().GetCode(), E_REJECTED);
        UNIT_ASSERT_STRING_CONTAINS(
            rejected.GetError().GetMessage(),
            "queue limit");
        UNIT_ASSERT(fixture.Transport->Connection->TakeSent().empty());
        fixture.Transport->Connection->SendResult = ESendResult::Accepted;
        auto accepted = fixture.Request();
        auto sent = fixture.Transport->Connection->TakeSent();
        UNIT_ASSERT_VALUES_EQUAL(sent.size(), 1);
        fixture.Transport->Connection->Receive(
            MakeResponse(sent[0]->Cookie, "accepted"));
        UNIT_ASSERT_VALUES_EQUAL(GetPayload(accepted), "accepted");
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

}   // namespace NCloud::NIcV2
