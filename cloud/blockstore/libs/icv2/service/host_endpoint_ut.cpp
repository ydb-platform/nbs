#include "host_endpoint.h"

#include "peer_request_sender.h"

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

#include <util/generic/scope.h>

#include <array>
#include <future>
#include <optional>
#include <utility>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IPeer;
using NCloud::NIcV2::IPeerManager;
using NCloud::NIcV2::IPeerManagerPtr;
using NCloud::NIcV2::IPeerPtr;
using NCloud::NIcV2::IRequestChannel;
using NCloud::NIcV2::IRequestChannelPtr;
using NCloud::NIcV2::TPeerStatus;
using NCloud::NIcV2::TRequestChannelFuture;
using NCloud::NIcV2::TRequestChannelResult;
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
    std::optional<TDuration> LastRequestTimeout;
    bool Closing = false;

    bool Activate() override
    {
        return true;
    }

    TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) override
    {
        LastRequestTimeout = timeout;
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
            if (request.Promise.Initialized()) {
                request.Promise.TrySetValue(error);
            }
        }
    }

    NThreading::TFuture<NProto::TError> WhenClosed() const override
    {
        return NThreading::MakeFuture(NProto::TError{});
    }

    auto PrepareResponse(
        size_t index,
        const NProto::TError& error = {},
        char readData = 'r',
        size_t readBytes = BlockSize)
    {
        std::unique_ptr<NActors::IEventBase> event;
        switch (Requests.at(index).Event->Type()) {
            case TEvService::EvReadBlocksRequest: {
                auto response =
                    std::make_unique<TEvService::TEvReadBlocksResponse>();
                *response->Record.MutableError() = error;
                if (!HasError(error)) {
                    response->AddPayload(TRope(TString(readBytes, readData)));
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
        return [promise = std::move(Requests.at(index).Promise),
                handle = std::move(handle)]() mutable
        {
            promise.SetValue(std::move(handle));
        };
    }

    void
    Respond(size_t index, const NProto::TError& error = {}, char readData = 'r')
    {
        PrepareResponse(index, error, readData)();
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
    NThreading::TPromise<TRequestChannelResult> Connected =
        NThreading::NewPromise<TRequestChannelResult>();

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

    TRequestChannelFuture GetRequestChannel() override
    {
        ++Calls;
        if (Unregistered) {
            return NThreading::MakeFuture<TRequestChannelResult>(
                MakeError(E_REJECTED, "test peer is closed"));
        }
        if (WaitForConnect) {
            return Connected.GetFuture();
        }
        if (HasError(ConnectError)) {
            return NThreading::MakeFuture<TRequestChannelResult>(ConnectError);
        }
        return NThreading::MakeFuture<TRequestChannelResult>(
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
    const std::shared_ptr<TTestScheduler> Retries =
        std::make_shared<TTestScheduler>(Timer->Now());
    const std::shared_ptr<TTestCompletionQueue> Completions =
        std::make_shared<TTestCompletionQueue>();
    IDataEndpointPtr Endpoint;
    IBlockStorePtr Durable;
    TString Data = TString(BlockSize, 'w');

    explicit TFixture(TDataConfig dataConfig = {})
    {
        auto [host, error] =
            CreateHostDataEndpoint("host", 1234, {}, Peers, dataConfig);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Endpoint = host.GetEndpoint();
        Durable = MakeDurable(Endpoint);
    }

    ~TFixture()
    {
        Completions->Resume();
        if (Endpoint) {
            const auto stopped = Endpoint->RequestStop();
            Y_ABORT_UNLESS(
                stopped.Wait(TDuration::Seconds(5)),
                "IC V2 test endpoint did not stop");

            stopped.GetValue();
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
        auto assertRejected = [](IPeerManagerPtr peers, TDataConfig config)
        {
            auto [endpoint, error] = CreateHostDataEndpoint(
                "host",
                1234,
                {},
                std::move(peers),
                config);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT(!endpoint.GetEndpoint());
        };

        const auto peers = std::make_shared<TTestPeers>();
        TDataConfig config;
        config.MaxInflightRequests = 0;

        assertRejected(peers, config);
        assertRejected({}, {});

        UNIT_ASSERT_VALUES_EQUAL(0, peers->CreatePeerCalls);
    }

    Y_UNIT_TEST(SharesHostChannelAcrossDisks)
    {
        TFixture f;
        auto sendZero = [&](const TString& diskId)
        {
            auto request = TFixture::MakeZero();
            request->SetDiskId(diskId);
            return f.Endpoint->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));
        };
        auto first = sendZero("disk-a");
        auto second = sendZero("disk-b");
        const auto channel = f.Peers->Channel;

        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->CreatePeerCalls);
        UNIT_ASSERT_VALUES_EQUAL(2, channel->Requests.size());
        auto getDiskId = [&](size_t index) -> const TString&
        {
            return static_cast<const TEvService::TEvZeroBlocksRequest&>(
                       *channel->Requests.at(index).Event)
                .Record.GetDiskId();
        };
        UNIT_ASSERT_VALUES_EQUAL("disk-a", getDiskId(0));
        UNIT_ASSERT_VALUES_EQUAL("disk-b", getDiskId(1));

        channel->Respond(1);
        UNIT_ASSERT(!first.IsReady());
        UNIT_ASSERT(!HasError(Wait(second)));
        channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(first)));
    }

    Y_UNIT_TEST(ClassifiesConnectionAndChannelErrorsWithoutResending)
    {
        struct TErrorCase
        {
            ui32 Code;
            ui32 ConnectionCode;
            ui32 ChannelCode;
        };

        const TErrorCase cases[] = {
            {E_TRANSPORT_ERROR, E_REJECTED, E_REJECTED},
            {E_UNAVAILABLE, E_UNAVAILABLE, E_REJECTED},
            {E_GRPC_UNAVAILABLE, E_GRPC_UNAVAILABLE, E_REJECTED},
            {E_IO, E_REJECTED, E_REJECTED},
            {E_ABORTED, E_REJECTED, E_REJECTED},
            {E_FAIL, E_REJECTED, E_REJECTED},
            {MAKE_SYSTEM_ERROR(ECONNRESET), E_REJECTED, E_REJECTED},
            {E_BADMSG, E_REJECTED, E_REJECTED},
            {E_REJECTED, E_REJECTED, E_REJECTED},
            {E_ARGUMENT, E_REJECTED, E_ARGUMENT},
            {E_TIMEOUT, E_REJECTED, E_TIMEOUT},
            {E_CANCELLED, E_REJECTED, E_REJECTED},
        };
        for (bool connect: {false, true}) {
            for (const auto& [code, connectionCode, channelCode]: cases) {
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
                    connect ? connectionCode : channelCode,
                    response.GetError().GetCode());
                UNIT_ASSERT_VALUES_EQUAL(
                    error.GetMessage(),
                    response.GetError().GetMessage());
                UNIT_ASSERT_VALUES_EQUAL(17, response.GetError().GetFlags());
                UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Calls);
                UNIT_ASSERT_VALUES_EQUAL(
                    connect ? 0 : 1,
                    f.Peers->Channel->Requests.size());
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

    Y_UNIT_TEST(DurableClientUsesShortBackoffWhilePeerReconnects)
    {
        TFixture f;
        f.Peers->ConnectError =
            MakeError(E_UNAVAILABLE, "IC V2 peer is waiting to reconnect");
        auto future = f.Zero(true);
        UNIT_ASSERT(!future.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Calls);

        auto advanceRetries = [&](TDuration duration)
        {
            f.Timer->AdvanceTime(duration);
            f.Retries->AdvanceTime(duration);
            f.Retries->RunAllScheduledTasksUntilNow();
        };
        advanceRetries(TDuration::MilliSeconds(99));
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Calls);
        f.Peers->ConnectError.Clear();
        f.Peers->WaitForConnect = true;
        advanceRetries(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2, f.Peers->Calls);
        UNIT_ASSERT(f.Peers->Channel->Requests.empty());

        f.Peers->Connected.SetValue(IRequestChannelPtr(f.Peers->Channel));
        UNIT_ASSERT(f.Peers->Channel->Requests.empty());
        UNIT_ASSERT(!future.IsReady());
        advanceRetries(TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(3, f.Peers->Calls);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());
        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(future)));
    }

    Y_UNIT_TEST(ReportsPeerClosureAsRejected)
    {
        for (bool closedBeforeRequest: {false, true}) {
            TFixture f;
            if (closedBeforeRequest) {
                f.Peers->Close();
            }
            auto future = f.Zero();
            if (!closedBeforeRequest) {
                UNIT_ASSERT(!future.IsReady());
                f.Peers->Close();
            }

            UNIT_ASSERT(future.IsReady());
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                Wait(future).GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Calls);
            UNIT_ASSERT_VALUES_EQUAL(
                closedBeforeRequest ? 0 : 1,
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

    Y_UNIT_TEST(DurableClientRetriesInvalidReadPayloadOnReplacementChannel)
    {
        TFixture f;
        auto request = std::make_shared<NProto::TReadBlocksRequest>();
        request->SetDiskId("disk");
        request->SetBlockSize(BlockSize);
        request->SetBlocksCount(2);
        request->MutableHeaders()->SetRequestTimeout(100);

        auto retryScheduled = f.Retries->WaitForTaskSchedule();
        auto future = f.Durable->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        const auto badChannel = f.Peers->Channel;
        const auto replacement = std::make_shared<TTestChannel>();
        f.Peers->Channel = replacement;
        badChannel->Respond(0);

        UNIT_ASSERT(badChannel->Closing);
        UNIT_ASSERT(!replacement->Closing);
        UNIT_ASSERT(retryScheduled.IsReady());
        UNIT_ASSERT(!future.IsReady());
        f.Retries->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(1, badChannel->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(1, replacement->Requests.size());
        replacement->PrepareResponse(0, {}, 'r', 2 * size_t{BlockSize})();
        UNIT_ASSERT(!HasError(Wait(future)));
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
        auto retryScheduled = f.Retries->WaitForTaskSchedule();
        auto future = f.Durable->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            f.MakeWrite());
        f.Timer->AdvanceTime(TDuration::MilliSeconds(90));
        f.Completions->ExecuteSimple(f.Peers->Channel->PrepareResponse(0));
        UNIT_ASSERT(!future.HasValue());

        f.Timer->AdvanceTime(TDuration::MilliSeconds(20));
        f.Completions->Resume();

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT(!HasError(Wait(future)));
        UNIT_ASSERT(!retryScheduled.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());
    }

    Y_UNIT_TEST(ReportsConnectionAttemptWithoutSendingRequest)
    {
        TFixture f;
        f.Peers->WaitForConnect = true;

        auto future = f.Zero();
        UNIT_ASSERT(future.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            E_UNAVAILABLE,
            future.GetValue().GetError().GetCode());

        f.Peers->Connected.SetValue(IRequestChannelPtr(f.Peers->Channel));
        UNIT_ASSERT(!f.Peers->Channel->LastRequestTimeout.has_value());
    }

    Y_UNIT_TEST(PassesRequestTimeoutToReadyChannel)
    {
        TFixture f;
        auto request = TFixture::MakeZero();
        const auto timeout =
            TDuration::MilliSeconds(request->GetHeaders().GetRequestTimeout());
        auto future = f.Endpoint->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            timeout,
            f.Peers->Channel->Requests.front().Timeout);
        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(future)));
    }

    Y_UNIT_TEST(DurableReadRetriesAfterChannelTimeout)
    {
        TFixture f;
        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->SetDiskId("disk");
        request->SetBlocksCount(1);
        request->SetBlockSize(BlockSize);
        request->Sglist =
            TGuardedSgList({TBlockDataRef(f.Data.Detach(), f.Data.size())});
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
        TFixture f;
        auto future = f.Zero();
        f.Endpoint->Stop();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, Wait(future).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(f.Zero()).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Calls);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());
        auto& promise = f.Peers->Channel->Requests.front().Promise;
        UNIT_ASSERT(!promise.TrySetValue(MakeError(E_TRANSPORT_ERROR, "late")));
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Unregistered);
    }

    Y_UNIT_TEST(DeliversAcceptedWriteResponseDuringShutdown)
    {
        TFixture f;
        auto response = f.Endpoint->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            f.MakeWrite());
        f.Completions->Paused = true;
        f.Completions->ExecuteSimple(f.Peers->Channel->PrepareResponse(0));

        const auto closed = f.Endpoint->RequestStop();
        UNIT_ASSERT(!response.IsReady());
        UNIT_ASSERT(!closed.IsReady());
        f.Completions->Resume();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, Wait(response).GetError().GetCode());
        UNIT_ASSERT(closed.IsReady());
    }

    Y_UNIT_TEST(DestroyingEndpointRejectsPendingRequests)
    {
        TFixture f;
        f.Durable.reset();
        auto response = f.Zero();

        f.Endpoint.reset();
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Unregistered);
        UNIT_ASSERT(response.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            response.GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(CloseCallbackCanReleaseLastSenderOwner)
    {
        auto peer = std::make_shared<TTestPeers>();
        const std::weak_ptr<TTestPeers> weakPeer = peer;
        auto sender = CreateDataRequestSender(std::move(peer));
        auto response = sender->Request(
            std::make_unique<TEvService::TEvZeroBlocksRequest>(),
            TDuration::Seconds(1));
        NThreading::TFuture<void> closedInsideCallback;
        bool closedBeforeCallbackReturned = true;
        response.Subscribe(
            [&](const TDataResponseFuture& future)
            {
                Y_UNUSED(future);
                closedInsideCallback = sender->Close();
                closedBeforeCallbackReturned = closedInsideCallback.IsReady();
                sender.reset();
            });

        const auto closed = sender->Close();

        UNIT_ASSERT(!sender);
        UNIT_ASSERT(weakPeer.expired());
        UNIT_ASSERT(!closedBeforeCallbackReturned);
        UNIT_ASSERT(closed.StateId() == closedInsideCallback.StateId());
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

    Y_UNIT_TEST(ClosedWaitsForConcurrentRequestToReturn)
    {
        TFixture f;
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        f.Peers->Channel->BeforeRequestReturns = [&]
        {
            entered.SetValue();
            release.GetFuture().GetValueSync();
        };
        auto submitting =
            std::async(std::launch::async, [&] { return f.Zero(); });
        Y_DEFER
        {
            release.TrySetValue();
        };

        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));
        const auto closed = f.Endpoint->RequestStop();
        UNIT_ASSERT(!closed.IsReady());

        release.SetValue();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(submitting.get()).GetError().GetCode());
        UNIT_ASSERT(closed.IsReady());
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
            TGuardedSgList({TBlockDataRef(f.Data.Detach(), f.Data.size())});
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
        const auto cases = std::to_array<std::pair<ui32, bool>>({
            {E_ARGUMENT, false},
            {E_ARGUMENT, true},
            {E_IO, true},
            {E_TRANSPORT_ERROR, true},
        });
        for (const auto& [code, fromStorage]: cases) {
            TFixture f;
            auto retryScheduled = f.Retries->WaitForTaskSchedule();
            auto future = f.Zero(true);
            const auto error = MakeError(code, "fatal reason");
            if (fromStorage) {
                f.Peers->Channel->Respond(0, error);
            } else {
                f.Peers->Channel->Fail(0, error);
            }
            UNIT_ASSERT(!retryScheduled.IsReady());
            const auto response = Wait(future);
            UNIT_ASSERT_VALUES_EQUAL(code, response.GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                error.GetMessage(),
                response.GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Calls);
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

    Y_UNIT_TEST(DurableClientRetriesAfterAdmissionOverload)
    {
        TDataConfig config;
        config.MaxInflightRequests = 1;
        TFixture f(config);
        auto first = f.Zero();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            Wait(f.Zero()).GetError().GetCode());

        auto second = f.Zero(true);
        UNIT_ASSERT(!second.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Peers->Channel->Requests.size());

        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(first)));

        f.Retries->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(2, f.Peers->Channel->Requests.size());
        f.Peers->Channel->Respond(1);
        UNIT_ASSERT(!HasError(Wait(second)));
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
