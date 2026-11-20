#include "host_endpoint.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/icv2/data/protocol.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/icv2/transport/testlib/event_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IPeer;
using NCloud::NIcV2::IPeerManager;
using NCloud::NIcV2::IPeerManagerPtr;
using NCloud::NIcV2::IPeerPtr;
using NCloud::NIcV2::IRequestChannel;
using NCloud::NIcV2::IRequestChannelPtr;
using NCloud::NIcV2::TChannelFuture;
using NCloud::NIcV2::TChannelResult;
using NCloud::NIcV2::TPeerStatus;
using NCloud::NIcV2::TResponseFuture;
using NCloud::NIcV2::TResponseResult;

namespace {

using NStorage::TEvService;
constexpr ui32 BlockSize = 4096;

template <typename T>
T Wait(const NThreading::TFuture<T>& future)
{
    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
    return future.GetValue();
}

struct TTestChannel final: IRequestChannel
{
    struct TRequest
    {
        std::unique_ptr<NActors::IEventBase> Event;
        TDuration Timeout;
        NThreading::TPromise<TResponseResult> Promise =
            NThreading::NewPromise<TResponseResult>();
    };

    TVector<TRequest> Requests;
    TCallback BeforeRequestReturns;
    bool Closing = false;

    bool Activate() override
    {
        return true;
    }

    TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) override
    {
        if (Closing) {
            return NThreading::MakeFuture<TResponseResult>(
                MakeError(E_REJECTED, "test channel is closed"));
        }
        auto future = Requests.emplace_back(std::move(event), timeout)
                          .Promise.GetFuture();
        if (BeforeRequestReturns) {
            BeforeRequestReturns();
        }
        return future;
    }

    void Close() override
    {
        Closing = true;
        const auto error = MakeError(E_REJECTED, "test channel is closed");
        for (auto& request: Requests) {
            request.Promise.TrySetValue(error);
        }
    }

    NThreading::TFuture<NProto::TError> WhenClosed() const override
    {
        return NThreading::MakeFuture(NProto::TError{});
    }

    void
    Respond(size_t index, const NProto::TError& error = {}, char readData = 'r')
    {
        std::unique_ptr<NActors::IEventBase> event;
        switch (Requests.at(index).Event->Type()) {
            case TEvService::EvReadBlocksRequest: {
                auto response =
                    std::make_unique<TEvService::TEvReadBlocksResponse>();
                *response->Record.MutableError() = error;
                if (!HasError(error)) {
                    response->AddPayload(TRope(TString(BlockSize, readData)));
                }
                event = std::move(response);
                break;
            }
            case TEvService::EvWriteBlocksRequest:
                event =
                    std::make_unique<TEvService::TEvWriteBlocksResponse>(error);
                break;
            case TEvService::EvZeroBlocksRequest:
                event =
                    std::make_unique<TEvService::TEvZeroBlocksResponse>(error);
                break;
            default:
                UNIT_FAIL("unexpected request type");
        }
        auto handle = std::make_unique<NActors::IEventHandle>(
            NActors::TActorId(),
            NActors::TActorId(),
            event.release());
        NCloud::NIcV2::NTests::PreserializeEvent(handle);
        auto promise = Requests.at(index).Promise;
        promise.SetValue(std::move(handle));
    }

    void Fail(size_t index, const NProto::TError& error)
    {
        auto promise = Requests.at(index).Promise;
        promise.SetValue(error);
    }
};

struct TTestPeers final
    : IPeerManager
    , IPeer
    , std::enable_shared_from_this<TTestPeers>
{
    std::shared_ptr<TTestChannel> Channel = std::make_shared<TTestChannel>();
    size_t CreatePeerCalls = 0;
    size_t Calls = 0;
    size_t Unregistered = 0;
    bool WaitForConnect = false;
    NProto::TError ConnectError;
    NThreading::TPromise<TChannelResult> Connected =
        NThreading::NewPromise<TChannelResult>();

    void Start() override
    {}

    void Stop() override
    {}

    void RequestStop() override
    {}

    TResultOrError<IPeerPtr> CreatePeer(
        TString host,
        ui16 port,
        IClientEndpointHandlerPtr handler) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        Y_UNUSED(handler);

        ++CreatePeerCalls;
        return IPeerPtr(shared_from_this());
    }

    TChannelFuture GetChannel() override
    {
        ++Calls;
        if (WaitForConnect) {
            return Connected.GetFuture();
        }
        if (HasError(ConnectError)) {
            return NThreading::MakeFuture<TChannelResult>(ConnectError);
        }
        return NThreading::MakeFuture<TChannelResult>(
            IRequestChannelPtr(Channel));
    }

    TPeerStatus GetStatus() override
    {
        return {};
    }

    void Close() override
    {
        Unregistered = 1;
        Channel->Close();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestCompletionQueue final: ITaskQueue
{
    bool Paused = false;
    TVector<ITaskPtr> Tasks;
    TCallback BeforeEnqueueReturns;

    // ITaskQueue implementation

    void Start() override
    {}

    void Stop() override
    {
        Resume();
    }

    void Enqueue(ITaskPtr task) override
    {
        if (Paused) {
            Tasks.push_back(std::move(task));
        } else {
            task->Execute();
        }
        if (BeforeEnqueueReturns) {
            BeforeEnqueueReturns();
        }
    }

    void Resume()
    {
        Paused = false;
        for (auto& task: Tasks) {
            task->Execute();
        }
        Tasks.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture
{
    const std::shared_ptr<TTestPeers> Peers = std::make_shared<TTestPeers>();
    const std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    const std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>(Timer->Now());
    // Drive retry backoff independently of the per-attempt deadline timer.
    const std::shared_ptr<TTestScheduler> Retries =
        std::make_shared<TTestScheduler>();
    const std::shared_ptr<TTestCompletionQueue> Completions =
        std::make_shared<TTestCompletionQueue>();
    IDataEndpointPtr Endpoint;
    NThreading::TFuture<void> Closed;
    IBlockStorePtr Durable;
    TString Data = TString(BlockSize, 'w');

    explicit TFixture(TDataConfig dataConfig = {})
    {
        auto [host, error] = CreateHostDataEndpoint(
            "host",
            1234,
            {},
            Peers,
            Timer,
            Scheduler,
            Completions,
            dataConfig);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Endpoint = std::move(host.Endpoint);
        Closed = std::move(host.Closed);
        auto config = std::make_shared<NClient::TClientAppConfig>();
        Durable = NClient::CreateDurableClient(
            config,
            Endpoint,
            NClient::CreateRetryPolicy(config, NProto::STORAGE_MEDIA_SSD),
            CreateLoggingService("console"),
            Timer,
            Retries,
            CreateRequestStatsStub(),
            CreateVolumeStatsStub());
    }

    ~TFixture()
    {
        Completions->Resume();
        Endpoint->Stop();
    }

    std::shared_ptr<NProto::TWriteBlocksRequest> MakeWrite() const
    {
        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        request->SetDiskId("disk");
        request->SetBlockSize(BlockSize);
        request->MutableHeaders()->SetRequestTimeout(100);
        request->MutableBlocks()->AddBuffers(Data);
        return request;
    }

    static std::shared_ptr<NProto::TZeroBlocksRequest> MakeZero()
    {
        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        request->SetDiskId("disk");
        request->SetBlocksCount(1);
        request->MutableHeaders()->SetRequestId(123);
        request->MutableHeaders()->SetRequestTimeout(100);
        return request;
    }

    NThreading::TFuture<NProto::TZeroBlocksResponse> Zero(bool durable = false)
    {
        return (durable ? Durable : Endpoint)
            ->ZeroBlocks(MakeIntrusive<TCallContext>(), MakeZero());
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2HostDataEndpointTest)
{
    Y_UNIT_TEST(RejectsInvalidConfigurationBeforeCreatingPeer)
    {
        const auto peers = std::make_shared<TTestPeers>();
        const auto timer = std::make_shared<TTestTimer>();
        const auto scheduler = std::make_shared<TTestScheduler>();
        const auto completions = CreateTaskQueueStub();
        auto create = [&](IPeerManagerPtr manager,
                          ISchedulerPtr taskScheduler,
                          TDataConfig config)
        {
            auto [endpoint, error] = CreateHostDataEndpoint(
                "host",
                1234,
                {},
                std::move(manager),
                timer,
                std::move(taskScheduler),
                completions,
                config);
            UNIT_ASSERT(!endpoint.Endpoint);
            return error.GetCode();
        };

        TDataConfig config;
        config.MaxInflightRequests = 0;
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, create(peers, scheduler, config));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, create(peers, {}, {}));
        UNIT_ASSERT_VALUES_EQUAL(0, peers->CreatePeerCalls);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, create({}, scheduler, {}));
    }

    Y_UNIT_TEST(ClassifiesConnectionAndChannelErrorsWithoutResending)
    {
        for (bool connect: {false, true}) {
            for (ui32 code:
                 TVector<ui32>{
                     E_TRANSPORT_ERROR,
                     E_IO,
                     E_ABORTED,
                     static_cast<ui32>(MAKE_SYSTEM_ERROR(ECONNRESET)),
                     E_ARGUMENT,
                     E_BADMSG,
                     E_TIMEOUT,
                     E_REJECTED,
                     E_CANCELLED})
            {
                TFixture f;
                auto error = MakeError(code, "original reason");
                error.SetFlags(17);
                if (connect) {
                    f.Peers->ConnectError = error;
                }
                auto future = f.Zero();
                if (!connect) {
                    f.Peers->Channel->Fail(0, error);
                }
                const auto response = Wait(future);
                const bool normalizeToUnavailable =
                    code == E_TRANSPORT_ERROR || code == E_IO ||
                    code == E_ABORTED || code == E_BADMSG ||
                    FACILITY_FROM_CODE(code) == FACILITY_SYSTEM;
                UNIT_ASSERT_VALUES_EQUAL(
                    normalizeToUnavailable ? E_UNAVAILABLE : code,
                    response.GetError().GetCode());
                UNIT_ASSERT_VALUES_EQUAL(
                    response.GetError().GetMessage(),
                    error.GetMessage());
                UNIT_ASSERT_VALUES_EQUAL(response.GetError().GetFlags(), 17);
                UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
                UNIT_ASSERT_VALUES_EQUAL(
                    f.Peers->Channel->Requests.size(),
                    connect ? 0 : 1);
            }
        }
    }

    Y_UNIT_TEST(ReportsStorageErrorsWithMessageAndFlags)
    {
        for (ui32 code:
             TVector<ui32>{
                 E_TRANSPORT_ERROR,
                 E_IO,
                 E_ABORTED,
                 E_UNAVAILABLE,
                 E_BS_INVALID_SESSION,
                 E_BADMSG,
                 E_CANCELLED})
        {
            TFixture f;
            auto future = f.Zero();
            auto error = MakeError(code, "storage reason");
            error.SetFlags(17);
            f.Peers->Channel->Respond(0, error);
            const auto response = Wait(future);
            UNIT_ASSERT_VALUES_EQUAL(
                code == E_BADMSG ? E_UNAVAILABLE : code,
                response.GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetError().GetMessage(),
                error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(response.GetError().GetFlags(), 17);
            UNIT_ASSERT_VALUES_EQUAL(f.Peers->Unregistered, 0);
        }
    }

    Y_UNIT_TEST(DurableClientRetriesConnectionFailure)
    {
        TFixture f;
        f.Peers->ConnectError = MakeError(E_IO, "EOF during hello");
        auto future = f.Zero(true);
        UNIT_ASSERT(!future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
        UNIT_ASSERT(f.Peers->Channel->Requests.empty());
        f.Peers->ConnectError.Clear();
        f.Retries->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 2);
        const auto& record = static_cast<TEvService::TEvZeroBlocksRequest*>(
                                 f.Peers->Channel->Requests.at(0).Event.get())
                                 ->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetHeaders().GetRequestId(), 123);
        UNIT_ASSERT_VALUES_EQUAL(record.GetHeaders().GetRetryNumber(), 1);
        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(future)));
    }

    Y_UNIT_TEST(DurableClientRetriesRequestsAfterProtocolError)
    {
        constexpr ui64 InterruptedRequestId = 124;
        TFixture f;
        const auto closedChannel = f.Peers->Channel;
        auto malformed = f.Zero(true);
        auto request = TFixture::MakeZero();
        request->MutableHeaders()->SetRequestId(InterruptedRequestId);
        auto interrupted = f.Durable->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(2, closedChannel->Requests.size());

        closedChannel->Fail(1, MakeError(E_ABORTED, "channel closed"));
        closedChannel->Fail(0, MakeError(E_BADMSG, "unexpected response type"));
        UNIT_ASSERT(!malformed.HasValue());
        UNIT_ASSERT(!interrupted.HasValue());

        auto replacement = std::make_shared<TTestChannel>();
        f.Peers->Channel = replacement;
        f.Retries->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(2, replacement->Requests.size());
        for (const auto& attempt: replacement->Requests) {
            const auto* event = static_cast<TEvService::TEvZeroBlocksRequest*>(
                attempt.Event.get());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                event->Record.GetHeaders().GetRetryNumber());
        }

        replacement->Respond(0);
        replacement->Respond(1);
        UNIT_ASSERT(!HasError(Wait(malformed)));
        UNIT_ASSERT(!HasError(Wait(interrupted)));
    }

    Y_UNIT_TEST(ReportsInvalidReadPayloadAsUnavailable)
    {
        TFixture f;
        auto request = std::make_shared<NProto::TReadBlocksRequest>();
        request->SetDiskId("disk");
        request->SetBlockSize(BlockSize);
        request->SetBlocksCount(2);
        request->MutableHeaders()->SetRequestTimeout(100);

        auto future = f.Endpoint->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        f.Peers->Channel->Respond(0);

        const auto response = Wait(future);
        UNIT_ASSERT_VALUES_EQUAL(E_UNAVAILABLE, response.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "invalid IC V2 read payload",
            response.GetError().GetMessage());
    }

    Y_UNIT_TEST(DurableClientCanExecuteWriteAgainAfterLostResponse)
    {
        TFixture f;
        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request->SetDiskId("disk");
        request->SetStartIndex(42);
        request->SetBlockSize(BlockSize);
        request->BlocksCount = 1;
        request->Sglist =
            TGuardedSgList({TBlockDataRef(f.Data.data(), f.Data.size())});
        request->MutableHeaders()->SetRequestId(456);
        request->MutableHeaders()->SetRequestTimeout(100);
        auto future =
            f.Durable->WriteBlocksLocal(MakeIntrusive<TCallContext>(), request);
        auto snapshot = [&f](size_t index)
        {
            const auto* event = static_cast<TEvService::TEvWriteBlocksRequest*>(
                f.Peers->Channel->Requests.at(index).Event.get());
            UNIT_ASSERT_VALUES_EQUAL(
                event->Record.GetBlocks().BuffersSize(),
                0);
            UNIT_ASSERT_VALUES_EQUAL(event->GetPayloadCount(), 1);
            return std::pair{
                event->Record,
                event->GetPayload(0).ConvertToString()};
        };
        // The first write reached storage; only its reply was lost.
        const auto [first, stored] = snapshot(0);
        f.Peers->Channel->Fail(0, MakeError(E_TRANSPORT_ERROR, "lost reply"));
        UNIT_ASSERT(!future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Channel->Requests.size(), 1);
        f.Retries->RunAllScheduledTasks();
        const auto [second, retried] = snapshot(1);
        UNIT_ASSERT_VALUES_EQUAL(second.GetDiskId(), first.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(second.GetStartIndex(), first.GetStartIndex());
        UNIT_ASSERT_VALUES_EQUAL(second.GetHeaders().GetRequestId(), 456);
        UNIT_ASSERT_VALUES_EQUAL(second.GetHeaders().GetRetryNumber(), 1);
        UNIT_ASSERT_VALUES_EQUAL(first.GetHeaders().GetRetryNumber(), 0);
        UNIT_ASSERT_VALUES_EQUAL(retried, stored);
        f.Peers->Channel->Respond(1);
        UNIT_ASSERT(!HasError(Wait(future)));
        UNIT_ASSERT_VALUES_EQUAL(stored, f.Data);
    }

    Y_UNIT_TEST(DurableWriteKeepsSuccessWhenCompletionIsDelayed)
    {
        TFixture f;
        f.Completions->Paused = true;
        auto future = f.Durable->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            f.MakeWrite());
        f.Timer->AdvanceTime(TDuration::MilliSeconds(90));
        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!future.HasValue());

        f.Timer->AdvanceTime(TDuration::MilliSeconds(20));
        f.Scheduler->RunAllScheduledTasks();
        f.Completions->Resume();

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT(!HasError(Wait(future)));
        f.Retries->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());
    }

    Y_UNIT_TEST(UsesRemainingTimeoutAfterWaitingForChannel)
    {
        TFixture f;
        f.Peers->WaitForConnect = true;
        auto first = f.Zero();
        f.Timer->AdvanceTime(TDuration::MilliSeconds(40));
        auto second = f.Zero();
        f.Timer->AdvanceTime(TDuration::MilliSeconds(60));
        f.Scheduler->RunAllScheduledTasks();

        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, Wait(first).GetError().GetCode());
        UNIT_ASSERT(!second.HasValue());
        UNIT_ASSERT(f.Peers->Channel->Requests.empty());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Calls);

        f.Peers->Connected.SetValue(IRequestChannelPtr(f.Peers->Channel));
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(40),
            f.Peers->Channel->Requests.front().Timeout);

        f.Timer->AdvanceTime(TDuration::MilliSeconds(40));
        f.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT(!second.HasValue());
        f.Peers->Channel->Fail(0, MakeError(E_TIMEOUT, "channel timed out"));
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, Wait(second).GetError().GetCode());
    }

    Y_UNIT_TEST(ReadyChannelDoesNotScheduleConnectionTimer)
    {
        TFixture f;
        auto timerScheduled = f.Scheduler->WaitForTaskSchedule();
        auto future = f.Zero();

        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());
        UNIT_ASSERT(!timerScheduled.HasValue());

        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(future)));
    }

    Y_UNIT_TEST(RearmsConnectionTimerForEarlierDeadline)
    {
        TFixture f;
        f.Peers->WaitForConnect = true;
        auto advance = [&](TDuration duration)
        {
            f.Timer->AdvanceTime(duration);
            f.Scheduler->AdvanceTime(duration);
            f.Scheduler->RunAllScheduledTasksUntilNow();
        };

        auto firstSchedule = f.Scheduler->WaitForTaskSchedule();
        auto later = f.Zero();
        UNIT_ASSERT(firstSchedule.HasValue());

        auto nextSchedule = f.Scheduler->WaitForTaskSchedule();
        advance(TDuration::MilliSeconds(10));
        UNIT_ASSERT(!nextSchedule.HasValue());

        auto request = TFixture::MakeZero();
        request->MutableHeaders()->SetRequestTimeout(5);
        auto earlier = f.Endpoint->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT(nextSchedule.HasValue());

        advance(TDuration::MilliSeconds(5));
        UNIT_ASSERT(earlier.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, Wait(earlier).GetError().GetCode());
        UNIT_ASSERT(!later.HasValue());

        advance(TDuration::MilliSeconds(85));
        UNIT_ASSERT(later.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, Wait(later).GetError().GetCode());
    }

    Y_UNIT_TEST(DurableReadRetriesAfterChannelTimeout)
    {
        TFixture f;
        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->SetDiskId("disk");
        request->SetBlocksCount(1);
        request->SetBlockSize(BlockSize);
        request->Sglist =
            TGuardedSgList({TBlockDataRef(f.Data.data(), f.Data.size())});
        request->MutableHeaders()->SetRequestId(789);
        request->MutableHeaders()->SetRequestTimeout(100);
        auto future =
            f.Durable->ReadBlocksLocal(MakeIntrusive<TCallContext>(), request);
        f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
        f.Peers->Channel->Fail(0, MakeError(E_TIMEOUT, "channel timed out"));
        UNIT_ASSERT(!future.HasValue());
        f.Retries->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Channel->Requests.size(), 2);
        const auto& record = static_cast<TEvService::TEvReadBlocksRequest*>(
                                 f.Peers->Channel->Requests.at(1).Event.get())
                                 ->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetHeaders().GetRequestId(), 789);
        UNIT_ASSERT_VALUES_EQUAL(record.GetHeaders().GetRetryNumber(), 1);
        UNIT_ASSERT(record.GetHeaders().GetRequestTimeout() > 100);
        f.Peers->Channel->Respond(1, {}, 'b');
        UNIT_ASSERT(!HasError(Wait(future)));
        UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, 'b'), f.Data);
    }

    Y_UNIT_TEST(OwnerStopCancelsDurableRequestWithoutRetry)
    {
        for (bool connecting: {false, true}) {
            TFixture f;
            f.Peers->WaitForConnect = connecting;
            auto future = f.Zero(true);
            f.Endpoint->Stop();
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(future).GetError().GetCode(),
                E_CANCELLED);
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(f.Zero(true)).GetError().GetCode(),
                E_CANCELLED);
            f.Retries->RunAllScheduledTasks();
            if (connecting) {
                f.Peers->Connected.SetValue(
                    IRequestChannelPtr(f.Peers->Channel));
            } else {
                auto& promise = f.Peers->Channel->Requests.front().Promise;
                UNIT_ASSERT(!promise.TrySetValue(
                    MakeError(E_TRANSPORT_ERROR, "late")));
            }
            UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
            UNIT_ASSERT_VALUES_EQUAL(
                f.Peers->Channel->Requests.size(),
                connecting ? 0 : 1);
            UNIT_ASSERT_VALUES_EQUAL(f.Peers->Unregistered, 1);
        }
    }

    Y_UNIT_TEST(WhenClosedWaitsForReadyChannelRequestToReturn)
    {
        bool closedInsideRequest = true;
        TFixture f;
        f.Peers->Channel->BeforeRequestReturns = [&]
        {
            f.Endpoint->RequestStop();
            closedInsideRequest = f.Closed.HasValue();
        };

        auto response = f.Zero();
        f.Peers->Channel->BeforeRequestReturns = {};

        UNIT_ASSERT(!closedInsideRequest);
        UNIT_ASSERT(f.Closed.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            Wait(response).GetError().GetCode());
    }

    Y_UNIT_TEST(WhenClosedWaitsForEnqueueToReturn)
    {
        enum class EScenario
        {
            Response,
            CancelSent,
            CancelWaiting,
        };
        for (const auto scenario:
             {EScenario::Response,
              EScenario::CancelSent,
              EScenario::CancelWaiting})
        {
            bool closedBeforeEnqueueReturned = true;
            TFixture f;
            f.Peers->WaitForConnect = scenario == EScenario::CancelWaiting;
            auto response = f.Zero();
            f.Completions->BeforeEnqueueReturns = [&]
            {
                f.Endpoint->RequestStop();
                closedBeforeEnqueueReturned = f.Closed.HasValue();
            };

            if (scenario == EScenario::Response) {
                f.Peers->Channel->Respond(0);
            } else {
                f.Endpoint->RequestStop();
            }
            f.Completions->BeforeEnqueueReturns = {};

            UNIT_ASSERT(!closedBeforeEnqueueReturned);
            UNIT_ASSERT(f.Closed.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(
                scenario == EScenario::Response ? S_OK : E_CANCELLED,
                Wait(response).GetError().GetCode());
        }
    }

    Y_UNIT_TEST(StopDuringDurableBackoffPreventsAnotherNetworkAttempt)
    {
        TFixture f;
        auto future = f.Zero(true);
        f.Peers->Channel->Fail(0, MakeError(E_TRANSPORT_ERROR, "disconnected"));
        UNIT_ASSERT(!future.HasValue());
        f.Endpoint->Stop();
        f.Retries->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(future).GetError().GetCode(),
            E_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Channel->Requests.size(), 1);
    }

    Y_UNIT_TEST(DurableClientDoesNotRetryFatalArgumentOrStorageErrors)
    {
        for (bool storage: {false, true}) {
            for (ui32 code: {E_ARGUMENT, E_IO, E_TRANSPORT_ERROR}) {
                if (!storage && (code == E_IO || code == E_TRANSPORT_ERROR)) {
                    continue;   // These transport-origin errors are retriable.
                }
                TFixture f;
                auto future = f.Zero(true);
                const auto error = MakeError(code, "fatal reason");
                if (storage) {
                    f.Peers->Channel->Respond(0, error);
                } else {
                    f.Peers->Channel->Fail(0, error);
                }
                const auto response = Wait(future);
                UNIT_ASSERT_VALUES_EQUAL(response.GetError().GetCode(), code);
                UNIT_ASSERT_VALUES_EQUAL(
                    response.GetError().GetMessage(),
                    error.GetMessage());
                f.Retries->RunAllScheduledTasks();
                UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
            }
        }
    }

    Y_UNIT_TEST(RequestStopCancelsNewDurableRequests)
    {
        TFixture f;
        f.Endpoint->RequestStop();
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            Wait(f.Zero(true)).GetError().GetCode());
        f.Retries->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(0, f.Peers->Calls);
        UNIT_ASSERT(f.Peers->Channel->Requests.empty());
    }

    Y_UNIT_TEST(AdmissionOverloadRemainsRetriable)
    {
        TDataConfig config;
        config.MaxInflightRequests = 1;
        TFixture f(config);
        auto first = f.Zero();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Zero()).GetError().GetCode(),
            E_REJECTED);
        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(first)));
        f.Endpoint->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Zero()).GetError().GetCode(),
            E_CANCELLED);
    }

    Y_UNIT_TEST(WriteAtPayloadLimitIsAdmitted)
    {
        TDataConfig config;
        config.MaxInflightBytes = BlockSize;
        TFixture f(config);

        auto request = f.MakeWrite();
        auto first =
            f.Endpoint->WriteBlocks(MakeIntrusive<TCallContext>(), request);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());
        auto second =
            f.Endpoint->WriteBlocks(MakeIntrusive<TCallContext>(), request);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, Wait(second).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());

        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(first)));
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
