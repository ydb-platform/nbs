#include "ping_client.h"

#include <library/cpp/testing/unittest/registar.h>

#include <cloud/blockstore/libs/icv2/transport/diagnostic.h>
#include <cloud/blockstore/libs/icv2/transport/event_test.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <array>
#include <atomic>
#include <future>
#include <mutex>
#include <thread>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;

struct TTestPeers final: IPeerConnections
{
    std::mutex Mutex;
    size_t Calls = 0;
    NThreading::TPromise<TChannelResult> Connected =
        NThreading::NewPromise<TChannelResult>();

    void Start() override
    {}

    void Stop() override
    {}

    void RequestStop() override
    {}

    NProto::TError RegisterHost(const TString&, ui64,
                                TVector<NInterconnect::TAddress>) override
    {
        UNIT_FAIL("Unexpected host binding");
        return MakeError(E_NOT_IMPLEMENTED);
    }

    TChannelFuture GetHostChannel(const TString&, ui64) override
    {
        UNIT_FAIL("Unexpected host binding");
        return {};
    }

    THostChannelStatus GetHostStatus(const TString&, ui64) override
    {
        UNIT_FAIL("Unexpected host binding");
        return {};
    }

    void UnregisterHost(const TString&, ui64) override
    {
        UNIT_FAIL("Unexpected host binding");
    }

    TChannelFuture GetChannel(const TString&) override
    {
        std::lock_guard guard(Mutex);
        ++Calls;
        return Connected.GetFuture();
    }
};

struct TTestChannel final: IRequestChannel
{
    struct TRequest
    {
        TString Payload;
        TDuration Timeout;
        NThreading::TPromise<TResponseResult> Promise =
            NThreading::NewPromise<TResponseResult>();
    };

    std::mutex Mutex;
    TVector<TRequest> Requests;
    std::atomic<bool> Closing = false;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

    bool Activate() override
    {
        return true;
    }

    TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) override
    {
        auto* ping = static_cast<TEvDiagnostic::TEvPingRequest*>(event.get());
        std::lock_guard guard(Mutex);
        auto& request =
            Requests.emplace_back(ping->Record.GetPayload(), timeout);
        return request.Promise.GetFuture();
    }

    bool Reply(ui64, std::unique_ptr<NActors::IEventBase>) override
    {
        UNIT_FAIL("ping client must only originate requests");
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

struct TInvalidResponse final
    : NActors::TEventPreSerializedPB<
          TInvalidResponse,
          NProto::TIcV2PingResponse,
          TEvDiagnostic::EvPingResponse>
{
};

TResponseResult MakeResponse(TString payload, bool malformed = false)
{
    std::unique_ptr<NActors::IEventBase> event;
    if (malformed) {
        auto response = std::make_unique<TInvalidResponse>();
        response->PreSerializedData = TString(1, '\xff');
        event = std::move(response);
    } else {
        auto response = std::make_unique<TEvDiagnostic::TEvPingResponse>();
        response->Record.SetPayload(std::move(payload));
        event = std::move(response);
    }
    auto handle = std::make_shared<NActors::IEventHandle>(
        NActors::TActorId(),
        NActors::TActorId(),
        event.release());
    NTests::PreserializeEvent(handle);
    return handle;
}

TPingResult Wait(const TPingFuture& future)
{
    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
    return future.GetValue();
}

struct TFixture
{
    std::shared_ptr<TTestPeers> Peers = std::make_shared<TTestPeers>();
    std::shared_ptr<TTestChannel> Channel = std::make_shared<TTestChannel>();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>();
    IPingClientPtr Client;

    explicit TFixture(
        TPingClientConfig config = {},
        ITaskQueuePtr queue = CreateTaskQueueStub())
    {
        auto [client, error] =
            CreatePingClient(config, Peers, Timer, Scheduler, std::move(queue));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Client = std::move(client);
    }

    ~TFixture()
    {
        Client->Stop();
    }

    void Connect()
    {
        Peers->Connected.SetValue(IRequestChannelPtr(Channel));
    }

    TPingFuture Ping(
        TString payload = "ping",
        TDuration timeout = TDuration::MilliSeconds(100))
    {
        return Client->Ping("cell-2", std::move(payload), timeout);
    }

    void Reply(size_t index = 0, bool malformed = false)
    {
        auto request = Channel->Requests.at(index);
        request.Promise.SetValue(MakeResponse(request.Payload, malformed));
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2PingClientTest)
{
    Y_UNIT_TEST(UsesRemainingDeadlineAndReturnsTypedPayloadAndDuration)
    {
        TFixture f;
        f.Client->Start();
        const TString payload("binary\0payload", 14);
        auto future = f.Ping(payload);
        f.Timer->AdvanceTime(TDuration::MilliSeconds(70));
        f.Connect();
        UNIT_ASSERT_VALUES_EQUAL(
            f.Channel->Requests[0].Timeout,
            TDuration::MilliSeconds(30));
        f.Timer->AdvanceTime(TDuration::MilliSeconds(20));
        f.Reply();
        auto [response, error] = Wait(future);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(response.PeerCellId, "cell-2");
        UNIT_ASSERT_VALUES_EQUAL(response.Payload, payload);
        UNIT_ASSERT_VALUES_EQUAL(
            response.Duration,
            TDuration::MilliSeconds(90));
    }

    Y_UNIT_TEST(TimesOutWhileWaitingForConnectionAndDoesNotSendLater)
    {
        TFixture f;
        f.Client->Start();
        auto future = f.Ping();
        f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
        f.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(Wait(future).GetError().GetCode(), E_TIMEOUT);
        f.Connect();
        UNIT_ASSERT(f.Channel->Requests.empty());
    }

    Y_UNIT_TEST(
        ChecksDeadlineWithoutWaitingForTimerAndDoesNotResetItForResponse)
    {
        for (bool connected: {false, true}) {
            TFixture f;
            f.Client->Start();
            auto future = f.Ping();
            if (connected) {
                f.Timer->AdvanceTime(TDuration::MilliSeconds(70));
                f.Connect();
                UNIT_ASSERT_VALUES_EQUAL(
                    f.Channel->Requests[0].Timeout,
                    TDuration::MilliSeconds(30));
                f.Timer->AdvanceTime(TDuration::MilliSeconds(30));
                f.Reply();
            } else {
                f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
                f.Connect();
                UNIT_ASSERT(f.Channel->Requests.empty());
            }
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(future).GetError().GetCode(),
                E_TIMEOUT);
        }
    }

    Y_UNIT_TEST(WaitingCallsHaveIndependentDeadlinesAndShareOneSubscription)
    {
        TFixture f;
        f.Client->Start();
        auto shortRequest = f.Ping("short", TDuration::MilliSeconds(10));
        auto longRequest = f.Ping("long");
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
        f.Timer->AdvanceTime(TDuration::MilliSeconds(10));
        f.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(shortRequest).GetError().GetCode(),
            E_TIMEOUT);
        UNIT_ASSERT(!longRequest.HasValue());
        f.Connect();
        UNIT_ASSERT_VALUES_EQUAL(f.Channel->Requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Channel->Requests[0].Payload, "long");
        UNIT_ASSERT_VALUES_EQUAL(
            f.Channel->Requests[0].Timeout,
            TDuration::MilliSeconds(90));
        f.Reply();
        UNIT_ASSERT(!HasError(Wait(longRequest).GetError()));
    }

    Y_UNIT_TEST(RepeatedTimeoutsDoNotAccumulateConnectionSubscriptions)
    {
        TFixture f;
        f.Client->Start();
        for (size_t i = 0; i < 1000; ++i) {
            auto future = f.Ping("ping", TDuration::MilliSeconds(10));
            f.Timer->AdvanceTime(TDuration::MilliSeconds(10));
            f.Scheduler->RunAllScheduledTasks();
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(future).GetError().GetCode(),
                E_TIMEOUT);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
        f.Connect();
        UNIT_ASSERT(f.Channel->Requests.empty());
        auto next = f.Ping();
        f.Reply();
        UNIT_ASSERT(!HasError(Wait(next).GetError()));
    }

    Y_UNIT_TEST(CompletedPingsDoNotAccumulateTimers)
    {
        TFixture f;
        f.Client->Start();
        f.Connect();
        auto first = f.Ping();
        f.Reply();
        UNIT_ASSERT(!HasError(Wait(first).GetError()));
        auto scheduled = f.Scheduler->WaitForTaskSchedule();
        for (size_t i = 0; i < 1000; ++i) {
            auto future = f.Ping();
            f.Reply(i + 1);
            UNIT_ASSERT(!HasError(Wait(future).GetError()));
        }
        UNIT_ASSERT(!scheduled.HasValue());
        f.Scheduler->RunAllScheduledTasks();
        auto next = f.Ping();
        UNIT_ASSERT(scheduled.HasValue());
        f.Reply(f.Channel->Requests.size() - 1);
        UNIT_ASSERT(!HasError(Wait(next).GetError()));
    }

    Y_UNIT_TEST(ParallelCallsReturnTheirOwnOutOfOrderResponses)
    {
        TFixture f;
        f.Client->Start();
        f.Connect();
        std::array<std::future<TPingFuture>, 32> callers;
        for (size_t i = 0; i < callers.size(); ++i) {
            callers[i] = std::async(
                std::launch::async,
                [&, i] { return f.Ping(ToString(i)); });
        }
        TVector<TPingFuture> futures;
        for (auto& caller: callers) {
            futures.push_back(caller.get());
        }
        for (size_t i = f.Channel->Requests.size(); i--;) {
            f.Reply(i);
        }
        for (size_t i = 0; i < futures.size(); ++i) {
            auto [response, error] = Wait(futures[i]);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(response.Payload, ToString(i));
        }
    }

    Y_UNIT_TEST(RejectsMalformedOrMismatchingResponseAndClosesChannel)
    {
        for (bool malformed: {false, true}) {
            TFixture f;
            f.Client->Start();
            f.Connect();
            auto future = f.Ping();
            if (malformed) {
                f.Reply(0, true);
            } else {
                f.Channel->Requests[0].Promise.SetValue(
                    MakeResponse("wrong payload"));
            }
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(future).GetError().GetCode(),
                E_BADMSG);
            UNIT_ASSERT(f.Channel->Closing);
        }
    }

    Y_UNIT_TEST(DiscardsLateResponseWithoutDeserializingIt)
    {
        TFixture f;
        f.Client->Start();
        f.Connect();
        auto future = f.Ping();
        f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
        f.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(Wait(future).GetError().GetCode(), E_TIMEOUT);
        f.Reply(0, true);
        UNIT_ASSERT(!f.Channel->Closing);
    }

    Y_UNIT_TEST(PropagatesConnectionAndTransportErrorsWithoutResending)
    {
        for (bool connected: {false, true}) {
            TFixture f;
            f.Client->Start();
            auto future = f.Ping();
            if (connected) {
                f.Connect();
                f.Channel->Requests[0].Promise.SetValue(
                    MakeError(E_TRANSPORT_ERROR, "disconnected"));
            } else {
                f.Peers->Connected.SetValue(
                    MakeError(E_ARGUMENT, "unknown peer"));
            }
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(future).GetError().GetCode(),
                connected ? E_TRANSPORT_ERROR : E_ARGUMENT);
            UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
            UNIT_ASSERT_VALUES_EQUAL(
                f.Channel->Requests.size(),
                connected ? 1 : 0);
        }
    }

    Y_UNIT_TEST(AdmissionBoundsOutstandingRequestsAndTheirPayloadBytes)
    {
        TFixture f({2, 6});
        f.Client->Start();
        auto first = f.Ping("1234");
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Ping("123")).GetError().GetCode(),
            E_REJECTED);
        auto second = f.Ping("12");
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Ping("")).GetError().GetCode(),
            E_REJECTED);
        f.Connect();
        const size_t firstIndex =
            f.Channel->Requests[0].Payload == "1234" ? 0 : 1;
        f.Reply(1 - firstIndex);
        UNIT_ASSERT(!HasError(Wait(second).GetError()));
        auto next = f.Ping("12");
        f.Reply(firstIndex);
        f.Reply(2);
        UNIT_ASSERT(!HasError(Wait(first).GetError()));
        UNIT_ASSERT(!HasError(Wait(next).GetError()));
    }

    Y_UNIT_TEST(
        QueuedCompletionKeepsAdmissionBudgetAndChecksDeadlineAtPublication)
    {
        auto queue = CreateThreadPool("ICV2PingTest", 1);
        queue->Start();
        auto release = NThreading::NewPromise<void>();
        queue->ExecuteSimple([release] { release.GetFuture().GetValueSync(); });
        TFixture f({1, 4}, queue);
        f.Client->Start();
        f.Connect();
        auto result = f.Ping();
        f.Reply();
        const bool queued = !result.HasValue();
        auto rejected = f.Ping();
        f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
        release.SetValue();
        UNIT_ASSERT(queued);
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(rejected).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(Wait(result).GetError().GetCode(), E_TIMEOUT);
        f.Client->Stop();
        queue->Stop();
    }

    Y_UNIT_TEST(StopCancelsResultAlreadyQueuedForPublication)
    {
        auto queue = CreateThreadPool("ICV2PingTest", 1);
        queue->Start();
        auto release = NThreading::NewPromise<void>();
        queue->ExecuteSimple([release] { release.GetFuture().GetValueSync(); });
        TFixture f({}, queue);
        f.Client->Start();
        f.Connect();
        auto result = f.Ping();
        f.Channel->Requests[0].Promise.SetValue(
            MakeError(E_TRANSPORT_ERROR, "disconnected"));
        const bool queued = !result.HasValue();
        f.Client->RequestStop();
        release.SetValue();
        f.Client->Stop();
        queue->Stop();
        UNIT_ASSERT(queued);
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(result).GetError().GetCode(), E_CANCELLED);
    }

    Y_UNIT_TEST(RequestStopFromContinuationAndExternalStopWaitsForIt)
    {
        auto queue = CreateThreadPool("ICV2PingTest", 1);
        queue->Start();
        TFixture f({}, queue);
        f.Client->Start();
        f.Connect();
        auto result = f.Ping();
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        std::thread::id callbackThread;
        const auto caller = std::this_thread::get_id();
        result.Subscribe(
            [&](const auto&)
            {
                callbackThread = std::this_thread::get_id();
                f.Client->RequestStop();
                entered.SetValue();
                release.GetFuture().GetValueSync();
            });
        f.Reply();
        const bool started = entered.GetFuture().Wait(TDuration::Seconds(5));
        auto stopped =
            std::async(std::launch::async, [&] { f.Client->Stop(); });
        const bool waits =
            stopped.wait_for(30ms) == std::future_status::timeout;
        release.SetValue();
        stopped.get();
        queue->Stop();
        UNIT_ASSERT(started);
        UNIT_ASSERT(waits);
        UNIT_ASSERT(callbackThread != caller);
    }

    Y_UNIT_TEST(ResponseTimeoutAndStopRaceCompleteEachPingOnce)
    {
        TFixture f;
        f.Client->Start();
        f.Connect();
        TVector<TPingFuture> futures;
        std::atomic<size_t> completions = 0;
        for (size_t i = 0; i < 64; ++i) {
            auto future = f.Ping();
            future.Subscribe([&](const auto&) { ++completions; });
            futures.push_back(std::move(future));
        }
        f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
        auto replies = std::async(
            std::launch::async,
            [&]
            {
                for (size_t i = 0; i < futures.size(); ++i) {
                    f.Reply(i);
                }
            });
        auto timer = std::async(
            std::launch::async,
            [&] { f.Scheduler->RunAllScheduledTasks(); });
        f.Client->RequestStop();
        replies.get();
        timer.get();
        f.Client->Stop();
        for (const auto& future: futures) {
            const auto code = Wait(future).GetError().GetCode();
            UNIT_ASSERT(code == E_TIMEOUT || code == E_CANCELLED);
        }
        UNIT_ASSERT_VALUES_EQUAL(completions.load(), futures.size());
    }

    Y_UNIT_TEST(StopRejectsWaitingCallsAndIgnoresLateConnectAndResponse)
    {
        for (bool connected: {false, true}) {
            TFixture f;
            f.Client->Start();
            auto future = f.Ping();
            if (connected) {
                f.Connect();
            }
            f.Client->Stop();
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(future).GetError().GetCode(), E_CANCELLED);
            if (connected) {
                f.Reply();
            } else {
                f.Connect();
                UNIT_ASSERT(f.Channel->Requests.empty());
            }
        }
    }

    Y_UNIT_TEST(RejectsCallsBeforeStartAfterStopAndInvalidArguments)
    {
        TFixture f;
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Ping()).GetError().GetCode(), E_CANCELLED);
        f.Client->Start();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Client->Ping("", "", TDuration::Seconds(1)))
                .GetError()
                .GetCode(), E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Ping("", TDuration::Zero())).GetError().GetCode(),
            E_TIMEOUT);
        f.Client->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Ping()).GetError().GetCode(), E_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 0);
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
