#include "host_endpoint.h"

#include "peer_request_channel.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/switchable_client.h>
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
        if (!timeout) {
            return NThreading::MakeFuture<TResponseResult>(
                MakeError(E_TIMEOUT, "test request timed out"));
        }
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
        if (Unregistered) {
            return NThreading::MakeFuture<TChannelResult>(
                MakeError(E_REJECTED, "test peer is closed"));
        }
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
            task.reset();
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
        Durable = MakeDurable(Endpoint);
    }

    ~TFixture()
    {
        Completions->Resume();
        if (Endpoint) {
            Endpoint->Stop();
        }
    }

    IBlockStorePtr MakeDurable(IBlockStorePtr client) const
    {
        auto config = std::make_shared<NClient::TClientAppConfig>();
        return NClient::CreateDurableClient(
            config,
            std::move(client),
            NClient::CreateRetryPolicy(config, NProto::STORAGE_MEDIA_SSD),
            CreateLoggingService("console"),
            Timer,
            Retries,
            CreateRequestStatsStub(),
            CreateVolumeStatsStub());
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
            for (auto [code, channelCode]:
                 TVector<std::pair<ui32, ui32>>{
                     {E_TRANSPORT_ERROR, E_REJECTED},
                     {E_UNAVAILABLE, E_REJECTED},
                     {E_IO, E_REJECTED},
                     {E_ABORTED, E_REJECTED},
                     {E_FAIL, E_REJECTED},
                     {MAKE_SYSTEM_ERROR(ECONNRESET), E_REJECTED},
                     {E_BADMSG, E_REJECTED},
                     {E_REJECTED, E_REJECTED},
                     {E_ARGUMENT, E_ARGUMENT},
                     {E_TIMEOUT, E_TIMEOUT},
                     {E_CANCELLED, E_REJECTED}})
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
                UNIT_ASSERT_VALUES_EQUAL(
                    connect ? E_REJECTED : channelCode,
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
                 E_ARGUMENT,
                 E_TIMEOUT,
                 E_CANCELLED})
        {
            TFixture f;
            auto future = f.Zero();
            auto error = MakeError(code, "storage reason");
            error.SetFlags(17);
            f.Peers->Channel->Respond(0, error);
            const auto response = Wait(future);
            UNIT_ASSERT_VALUES_EQUAL(code, response.GetError().GetCode());
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

    Y_UNIT_TEST(DurableClientRetriesRejectionWhilePeerIsOpen)
    {
        for (bool connecting: {false, true}) {
            TFixture f;
            const auto rejection = MakeError(E_REJECTED, "temporary refusal");
            if (connecting) {
                f.Peers->ConnectError = rejection;
            }
            auto future = f.Zero(true);
            if (!connecting) {
                f.Peers->Channel->Fail(0, rejection);
            }
            UNIT_ASSERT(!future.IsReady());

            f.Peers->ConnectError.Clear();
            f.Retries->RunAllScheduledTasks();
            UNIT_ASSERT_VALUES_EQUAL(2, f.Peers->Calls);
            UNIT_ASSERT_VALUES_EQUAL(
                connecting ? 1 : 2,
                f.Peers->Channel->Requests.size());
            f.Peers->Channel->Respond(f.Peers->Channel->Requests.size() - 1);
            UNIT_ASSERT(!HasError(Wait(future)));
        }
    }

    Y_UNIT_TEST(ReportsPeerClosureAsRejected)
    {
        enum class EClose
        {
            BeforeRequest,
            WhileConnecting,
            WithRequestInFlight,
        };
        for (auto close:
             {EClose::BeforeRequest,
              EClose::WhileConnecting,
              EClose::WithRequestInFlight})
        {
            TFixture f;
            f.Peers->WaitForConnect = close == EClose::WhileConnecting;
            if (close == EClose::BeforeRequest) {
                f.Peers->Close();
            }
            auto future = f.Zero();
            if (close != EClose::BeforeRequest) {
                UNIT_ASSERT(!future.IsReady());
                f.Peers->Close();
            }
            if (close == EClose::WhileConnecting) {
                f.Peers->Connected.SetValue(
                    MakeError(E_REJECTED, "test peer is closed"));
            }

            UNIT_ASSERT(future.IsReady());
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                Wait(future).GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Calls);
            UNIT_ASSERT_VALUES_EQUAL(
                close == EClose::WithRequestInFlight ? 1 : 0,
                f.Peers->Channel->Requests.size());
        }
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

    Y_UNIT_TEST(ReportsInvalidReadPayloadAsRejected)
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
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response.GetError().GetCode());
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
        f.Completions->ExecuteSimple([channel = f.Peers->Channel]
                                     { channel->Respond(0); });
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
        UNIT_ASSERT_VALUES_EQUAL(2, f.Peers->Calls);

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

    Y_UNIT_TEST(DoesNotSendExpiredRequestWhenChannelBecomesReady)
    {
        TFixture f;
        f.Peers->WaitForConnect = true;
        auto future = f.Zero();
        f.Timer->AdvanceTime(TDuration::MilliSeconds(110));
        UNIT_ASSERT(!future.IsReady());

        f.Peers->Connected.SetValue(IRequestChannelPtr(f.Peers->Channel));

        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, Wait(future).GetError().GetCode());
        UNIT_ASSERT(f.Peers->Channel->Requests.empty());
    }

    Y_UNIT_TEST(UsesReadyChannelWhilePreviousAttemptResultIsPending)
    {
        TFixture f;
        f.Peers->WaitForConnect = true;
        auto waiting = f.Zero();
        UNIT_ASSERT(f.Peers->Channel->Requests.empty());

        f.Peers->WaitForConnect = false;
        auto immediate = f.Zero();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());
        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(immediate)));
        UNIT_ASSERT(!waiting.IsReady());

        f.Peers->Connected.SetValue(IRequestChannelPtr(f.Peers->Channel));
        UNIT_ASSERT_VALUES_EQUAL(2, f.Peers->Channel->Requests.size());
        f.Peers->Channel->Respond(1);
        UNIT_ASSERT(!HasError(Wait(waiting)));
    }

    Y_UNIT_TEST(AwaitsTheChannelForEachConnectionAttempt)
    {
        TFixture f;
        f.Peers->WaitForConnect = true;
        auto firstAttempt = f.Peers->Connected;
        auto first = f.Zero();

        f.Peers->Connected = NThreading::NewPromise<TChannelResult>();
        auto second = f.Zero();
        f.Peers->Connected.SetValue(IRequestChannelPtr(f.Peers->Channel));
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());
        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(second)));
        UNIT_ASSERT(!first.IsReady());

        firstAttempt.SetValue(MakeError(E_UNAVAILABLE, "first attempt failed"));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, Wait(first).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());
    }

    Y_UNIT_TEST(ReusesEarlierConnectionTimerAcrossAttempts)
    {
        TFixture f;
        f.Peers->WaitForConnect = true;
        const auto advance = [&](TDuration duration)
        {
            f.Timer->AdvanceTime(duration);
            f.Scheduler->AdvanceTime(duration);
            f.Scheduler->RunAllScheduledTasksUntilNow();
        };

        auto initialTimer = f.Scheduler->WaitForTaskSchedule();
        auto first = f.Zero();
        UNIT_ASSERT(initialTimer.IsReady());
        f.Peers->Connected.SetValue(IRequestChannelPtr(f.Peers->Channel));
        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(first)));

        advance(TDuration::MilliSeconds(40));
        f.Peers->Connected = NThreading::NewPromise<TChannelResult>();
        auto nextTimer = f.Scheduler->WaitForTaskSchedule();
        auto second = f.Zero();
        UNIT_ASSERT(!nextTimer.IsReady());

        advance(TDuration::MilliSeconds(60));
        UNIT_ASSERT(nextTimer.IsReady());
        UNIT_ASSERT(!second.IsReady());

        advance(TDuration::MilliSeconds(40));
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, Wait(second).GetError().GetCode());
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

    Y_UNIT_TEST(EndpointStopRejectsPendingRequests)
    {
        for (bool connecting: {false, true}) {
            TFixture f;
            f.Peers->WaitForConnect = connecting;
            auto future = f.Zero();
            f.Endpoint->Stop();
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                Wait(future).GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                Wait(f.Zero()).GetError().GetCode());
            if (connecting) {
                f.Peers->Connected.SetValue(
                    IRequestChannelPtr(f.Peers->Channel));
            } else {
                auto& promise = f.Peers->Channel->Requests.front().Promise;
                UNIT_ASSERT(
                    !promise.TrySetValue(MakeError(E_TRANSPORT_ERROR, "late")));
            }
            UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Calls);
            UNIT_ASSERT_VALUES_EQUAL(
                connecting ? 0 : 1,
                f.Peers->Channel->Requests.size());
            UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Unregistered);
        }
    }

    Y_UNIT_TEST(LastOwnerRejectsPendingPeerRequests)
    {
        for (bool connecting: {false, true}) {
            const auto peer = std::make_shared<TTestPeers>();
            peer->WaitForConnect = connecting;
            const auto timer = std::make_shared<TTestTimer>();
            const auto scheduler = std::make_shared<TTestScheduler>();
            const auto completions = std::make_shared<TTestCompletionQueue>();
            completions->Paused = true;
            auto channel =
                CreatePeerRequestChannel(peer, timer, scheduler, completions);
            UNIT_ASSERT(channel->Activate());
            auto closed = channel->WhenClosed();
            auto response = channel->Request(
                std::make_unique<TEvService::TEvZeroBlocksRequest>(),
                TDuration::Seconds(1));

            channel.reset();
            UNIT_ASSERT_VALUES_EQUAL(1, peer->Unregistered);
            UNIT_ASSERT_VALUES_EQUAL(!connecting, response.IsReady());
            UNIT_ASSERT_VALUES_EQUAL(!connecting, closed.IsReady());

            completions->Resume();
            UNIT_ASSERT(response.IsReady());
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                response.ExtractValue().GetError().GetCode());
            UNIT_ASSERT(closed.IsReady());
        }
    }

    Y_UNIT_TEST(CloseCallbackCanReleaseLastChannelOwner)
    {
        auto peer = std::make_shared<TTestPeers>();
        peer->WaitForConnect = true;
        const std::weak_ptr<TTestPeers> weakPeer = peer;
        const auto timer = std::make_shared<TTestTimer>();
        const auto scheduler = std::make_shared<TTestScheduler>();
        const auto completions = std::make_shared<TTestCompletionQueue>();
        auto channel = CreatePeerRequestChannel(
            std::move(peer),
            timer,
            scheduler,
            completions);
        UNIT_ASSERT(channel->Activate());
        const auto closed = channel->WhenClosed();
        auto response = channel->Request(
            std::make_unique<TEvService::TEvZeroBlocksRequest>(),
            TDuration::Seconds(1));
        response.Subscribe([&](const auto&) { channel.reset(); });

        bool peerHeldThroughEnqueue = false;
        completions->BeforeEnqueueReturns = [&]
        {
            peerHeldThroughEnqueue = !weakPeer.expired();
        };
        channel->Close();

        UNIT_ASSERT(!channel);
        UNIT_ASSERT(peerHeldThroughEnqueue);
        UNIT_ASSERT(weakPeer.expired());
        UNIT_ASSERT(closed.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            response.ExtractValue().GetError().GetCode());
    }

    Y_UNIT_TEST(CloseCallbackCanReleaseLastEndpointOwner)
    {
        for (bool waitForClose: {false, true}) {
            TFixture f;
            f.Durable.reset();
            f.Peers->WaitForConnect = true;
            auto response = f.Zero();
            NThreading::TFuture<void> closed;
            response.Subscribe(
                [&](const auto&)
                {
                    closed = f.Endpoint->RequestStop();
                    f.Endpoint.reset();
                });

            if (waitForClose) {
                f.Endpoint->Stop();
            } else {
                const auto stopped = f.Endpoint->RequestStop();
                UNIT_ASSERT(stopped.IsReady());
            }

            UNIT_ASSERT(!f.Endpoint);
            UNIT_ASSERT(closed.IsReady());
            UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Unregistered);
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                Wait(response).GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ClosedWaitsForWriteBeforeChannelAdmission)
    {
        class TCallbackTimer final: public ITimer
        {
            TTestTimer Timer;

        public:
            TCallback BeforeNow;

            TInstant Now() override
            {
                if (auto callback = std::exchange(BeforeNow, {})) {
                    callback();
                }
                return Timer.Now();
            }

            void Sleep(TDuration duration) override
            {
                Timer.Sleep(duration);
            }
        };

        const auto peers = std::make_shared<TTestPeers>();
        const auto timer = std::make_shared<TCallbackTimer>();
        const auto scheduler = std::make_shared<TTestScheduler>(timer->Now());
        auto [host, error] = CreateHostDataEndpoint(
            "host",
            1234,
            {},
            peers,
            timer,
            scheduler,
            CreateTaskQueueStub(),
            {});
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        NThreading::TFuture<void> closed;
        bool closedWhileSubmitting = true;
        timer->BeforeNow = [&]
        {
            closed = host.Endpoint->RequestStop();
            closedWhileSubmitting = closed.IsReady();
        };

        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        request->SetDiskId("disk");
        request->SetBlockSize(BlockSize);
        request->MutableHeaders()->SetRequestTimeout(100);
        request->MutableBlocks()->AddBuffers(TString(BlockSize, 'w'));
        auto response = host.Endpoint->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        UNIT_ASSERT(!closedWhileSubmitting);
        UNIT_ASSERT(peers->Channel->Requests.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(response).GetError().GetCode());
        UNIT_ASSERT(closed.IsReady());
    }

    Y_UNIT_TEST(ClosedWaitsForReadyChannelRequestToReturn)
    {
        NThreading::TFuture<void> closed;
        bool closedInsideRequest = true;
        TFixture f;
        f.Peers->Channel->BeforeRequestReturns = [&]
        {
            closed = f.Endpoint->RequestStop();
            closedInsideRequest = closed.IsReady();
        };

        auto response = f.Zero();
        f.Peers->Channel->BeforeRequestReturns = {};

        UNIT_ASSERT(!closedInsideRequest);
        UNIT_ASSERT(closed.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(response).GetError().GetCode());
    }

    Y_UNIT_TEST(ClosedWaitsForEnqueueToReturn)
    {
        for (bool expire: {false, true}) {
            NThreading::TFuture<void> closed;
            bool closedBeforeEnqueueReturned = true;
            TFixture f;
            f.Peers->WaitForConnect = true;
            auto response = f.Zero();
            f.Completions->BeforeEnqueueReturns = [&]
            {
                closed = f.Endpoint->RequestStop();
                closedBeforeEnqueueReturned = closed.IsReady();
            };

            if (expire) {
                f.Timer->AdvanceTime(TDuration::MilliSeconds(100));
                f.Scheduler->RunAllScheduledTasks();
            } else {
                f.Endpoint->RequestStop();
            }
            f.Completions->BeforeEnqueueReturns = {};

            UNIT_ASSERT(!closedBeforeEnqueueReturned);
            UNIT_ASSERT(closed.IsReady());
            UNIT_ASSERT_VALUES_EQUAL(
                expire ? E_TIMEOUT : E_REJECTED,
                Wait(response).GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ClosedWaitsForResultCallback)
    {
        TFixture f;
        f.Completions->Paused = true;
        auto response = f.Zero();
        NThreading::TFuture<void> closed;
        bool closedInsideCallback = true;
        response.Subscribe(
            [&](const auto&)
            {
                closed = f.Endpoint->RequestStop();
                closedInsideCallback = closed.IsReady();
            });
        f.Peers->Channel->Respond(0);

        UNIT_ASSERT(response.IsReady());
        UNIT_ASSERT(!HasError(Wait(response)));
        UNIT_ASSERT(!closedInsideCallback);
        UNIT_ASSERT(closed.IsReady());
    }

    Y_UNIT_TEST(DurableClientRetriesOnNewBackendWhenOldEndpointCloses)
    {
        for (bool connecting: {false, true}) {
            TFixture source;
            TFixture target;
            source.Peers->WaitForConnect = connecting;
            const auto switchable = CreateSwitchableClient(
                CreateLoggingService("console"),
                {},
                "disk",
                source.Endpoint);
            const auto durable = source.MakeDurable(switchable);
            auto future = durable->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                TFixture::MakeZero());
            UNIT_ASSERT(!future.IsReady());

            switchable->BeforeSwitching();
            switchable->Switch(target.Endpoint, "new-disk", "new-session");
            switchable->AfterSwitching();
            source.Endpoint->Stop();
            UNIT_ASSERT(!future.IsReady());

            source.Retries->RunAllScheduledTasks();
            UNIT_ASSERT_VALUES_EQUAL(1, target.Peers->Channel->Requests.size());
            const auto& record =
                static_cast<const TEvService::TEvZeroBlocksRequest&>(
                    *target.Peers->Channel->Requests.front().Event)
                    .Record;
            UNIT_ASSERT_VALUES_EQUAL("new-disk", record.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL("new-session", record.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(1, record.GetHeaders().GetRetryNumber());
            target.Peers->Channel->Respond(0);
            UNIT_ASSERT(!HasError(Wait(future)));
            UNIT_ASSERT_VALUES_EQUAL(1, source.Peers->Calls);
            UNIT_ASSERT_VALUES_EQUAL(
                connecting ? 0 : 1,
                source.Peers->Channel->Requests.size());
        }
    }

    Y_UNIT_TEST(DurableClientDoesNotRetryWhenCallerClosesReadBuffer)
    {
        TFixture f;
        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->SetDiskId("disk");
        request->SetBlocksCount(1);
        request->SetBlockSize(BlockSize);
        request->Sglist =
            TGuardedSgList({TBlockDataRef(f.Data.data(), f.Data.size())});
        request->Sglist.Close();
        auto response = f.Durable->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        UNIT_ASSERT(response.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            Wait(response).GetError().GetCode());
        f.Retries->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(0, f.Peers->Calls);
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

    Y_UNIT_TEST(RequestStopRejectsNewRequests)
    {
        TFixture f;
        f.Endpoint->RequestStop();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(f.Zero()).GetError().GetCode());
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
            E_REJECTED,
            Wait(f.Zero()).GetError().GetCode());
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
