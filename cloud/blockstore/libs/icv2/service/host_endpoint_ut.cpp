#include "host_endpoint.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/icv2/data/protocol.h>
#include <cloud/blockstore/libs/icv2/transport/event_test.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NIcV2 {
namespace {

using TEvents = NStorage::TEvService;
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
        NThreading::TPromise<TResponseResult> Promise =
            NThreading::NewPromise<TResponseResult>();
    };

    TVector<TRequest> Requests;

    bool Activate() override
    {
        return true;
    }

    TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event, TDuration) override
    {
        auto& request = Requests.emplace_back(std::move(event));
        return request.Promise.GetFuture();
    }

    bool Reply(ui64, std::unique_ptr<NActors::IEventBase>) override
    {
        UNIT_FAIL("unexpected incoming request");
        return false;
    }

    void Close() override
    {}

    NThreading::TFuture<void> WhenClosed() const override
    {
        return NThreading::MakeFuture();
    }

    void Respond(
        size_t index, const NProto::TError& error = {}, char readData = 'r')
    {
        std::unique_ptr<NActors::IEventBase> event;
        switch (Requests.at(index).Event->Type()) {
            case TEvents::EvReadBlocksRequest: {
                auto response =
                    std::make_unique<TEvents::TEvReadBlocksResponse>();
                *response->Record.MutableError() = error;
                if (!HasError(error)) {
                    response->AddPayload(TRope(TString(BlockSize, readData)));
                }
                event = std::move(response);
                break;
            }
            case TEvents::EvWriteBlocksRequest:
                event =
                    std::make_unique<TEvents::TEvWriteBlocksResponse>(error);
                break;
            case TEvents::EvZeroBlocksRequest:
                event = std::make_unique<TEvents::TEvZeroBlocksResponse>(error);
                break;
            default:
                UNIT_FAIL("unexpected request type");
        }
        auto handle = std::make_shared<NActors::IEventHandle>(
            NActors::TActorId(), NActors::TActorId(), event.release());
        NTests::PreserializeEvent(handle);
        auto promise = Requests.at(index).Promise;
        promise.SetValue(std::move(handle));
    }

    void Fail(size_t index, const NProto::TError& error)
    {
        auto promise = Requests.at(index).Promise;
        promise.SetValue(error);
    }
};

struct TTestPeers final: IPeerConnections
{
    const std::shared_ptr<TTestChannel> Channel =
        std::make_shared<TTestChannel>();
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

    TChannelFuture GetChannel(const TString&) override
    {
        UNIT_FAIL("unexpected diagnostic channel");
        return {};
    }

    NProto::TError RegisterHost(
        const TString&,
        ui64,
        TVector<NInterconnect::TAddress>) override
    {
        return {};
    }

    TChannelFuture GetHostChannel(const TString&, ui64) override
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

    THostChannelStatus GetHostStatus(const TString&, ui64) override
    {
        return {};
    }

    void UnregisterHost(const TString&, ui64) override
    {
        ++Unregistered;
    }
};

struct TFixture
{
    const std::shared_ptr<TTestPeers> Peers = std::make_shared<TTestPeers>();
    const std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    const std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>();
    // Drive retry backoff independently of the per-attempt deadline timer.
    const std::shared_ptr<TTestScheduler> Retries =
        std::make_shared<TTestScheduler>();
    IDataEndpointPtr Endpoint;
    IBlockStorePtr Durable;
    TString Data = TString(BlockSize, 'w');

    explicit TFixture(TDataConfig dataConfig = {})
    {
        auto [host, error] = CreateHostDataEndpoint(
            "cell-2",
            1,
            {}, Peers, Timer, Scheduler, CreateTaskQueueStub(), dataConfig);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Endpoint = std::move(host.Endpoint);
        auto config = std::make_shared<NClient::TClientAppConfig>();
        Durable = NClient::CreateDurableClient(
            config,
            Endpoint,
            NClient::CreateRetryPolicy(config, NProto::STORAGE_MEDIA_SSD),
            CreateLoggingService("console"),
            Timer, Retries, CreateRequestStatsStub(), CreateVolumeStatsStub());
    }

    ~TFixture()
    {
        Endpoint->Stop();
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
    Y_UNIT_TEST(ClassifiesConnectionAndChannelErrorsWithoutResending)
    {
        for (bool connect: {false, true}) {
            for (ui32 code: TVector<ui32>{
                     E_TRANSPORT_ERROR,
                     E_IO,
                     static_cast<ui32>(MAKE_SYSTEM_ERROR(ECONNRESET)),
                     E_ARGUMENT, E_BADMSG, E_TIMEOUT, E_REJECTED, E_CANCELLED})
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
                const bool network =
                    code == E_TRANSPORT_ERROR || code == E_IO ||
                    FACILITY_FROM_CODE(code) == FACILITY_SYSTEM;
                UNIT_ASSERT_VALUES_EQUAL(
                    response.GetError().GetCode(),
                    network ? E_UNAVAILABLE : code);
                UNIT_ASSERT_VALUES_EQUAL(
                    response.GetError().GetMessage(), error.GetMessage());
                UNIT_ASSERT_VALUES_EQUAL(response.GetError().GetFlags(), 17);
                UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
                UNIT_ASSERT_VALUES_EQUAL(
                    f.Peers->Channel->Requests.size(), connect ? 0 : 1);
            }
        }
    }

    Y_UNIT_TEST(PreservesStorageErrorCodeMessageAndFlags)
    {
        for (ui32 code: TVector<ui32>{
                 E_TRANSPORT_ERROR,
                 E_IO,
                 E_UNAVAILABLE, E_BS_INVALID_SESSION, E_BADMSG, E_CANCELLED})
        {
            TFixture f;
            auto future = f.Zero();
            auto error = MakeError(code, "storage reason");
            error.SetFlags(17);
            f.Peers->Channel->Respond(0, error);
            const auto response = Wait(future);
            UNIT_ASSERT_VALUES_EQUAL(response.GetError().GetCode(), code);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetError().GetMessage(), error.GetMessage());
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
        const auto& record = static_cast<TEvents::TEvZeroBlocksRequest*>(
                                 f.Peers->Channel->Requests.at(0).Event.get())
                                 ->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetHeaders().GetRequestId(), 123);
        UNIT_ASSERT_VALUES_EQUAL(record.GetHeaders().GetRetryNumber(), 1);
        f.Peers->Channel->Respond(0);
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
            const auto* event = static_cast<TEvents::TEvWriteBlocksRequest*>(
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

    Y_UNIT_TEST(DurableReadRetryIgnoresLateResponseFromTimedOutAttempt)
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
        f.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT(!future.HasValue());
        f.Retries->RunAllScheduledTasks();
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Channel->Requests.size(), 2);
        const auto& record = static_cast<TEvents::TEvReadBlocksRequest*>(
                                 f.Peers->Channel->Requests.at(1).Event.get())
                                 ->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetHeaders().GetRequestId(), 789);
        UNIT_ASSERT_VALUES_EQUAL(record.GetHeaders().GetRetryNumber(), 1);
        UNIT_ASSERT(record.GetHeaders().GetRequestTimeout() > 100);
        f.Peers->Channel->Respond(1, {}, 'b');
        UNIT_ASSERT(!HasError(Wait(future)));
        f.Peers->Channel->Respond(0, {}, 'a');
        UNIT_ASSERT_VALUES_EQUAL(f.Data, TString(BlockSize, 'b'));
    }

    Y_UNIT_TEST(OwnerStopCancelsDurableRequestWithoutRetry)
    {
        for (bool connecting: {false, true}) {
            TFixture f;
            f.Peers->WaitForConnect = connecting;
            auto future = f.Zero(true);
            f.Endpoint->Stop();
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(future).GetError().GetCode(), E_CANCELLED);
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(f.Zero(true)).GetError().GetCode(), E_CANCELLED);
            f.Retries->RunAllScheduledTasks();
            if (connecting) {
                f.Peers->Connected.SetValue(
                    IRequestChannelPtr(f.Peers->Channel));
            } else {
                f.Peers->Channel->Fail(0, MakeError(E_TRANSPORT_ERROR, "late"));
            }
            UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
            UNIT_ASSERT_VALUES_EQUAL(
                f.Peers->Channel->Requests.size(), connecting ? 0 : 1);
            UNIT_ASSERT_VALUES_EQUAL(f.Peers->Unregistered, 1);
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
            Wait(future).GetError().GetCode(), E_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Peers->Channel->Requests.size(), 1);
    }

    Y_UNIT_TEST(DurableClientDoesNotRetryFatalProtocolOrStorageErrors)
    {
        for (bool storage: {false, true}) {
            for (ui32 code: {E_ARGUMENT, E_BADMSG, E_IO, E_TRANSPORT_ERROR}) {
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
                    response.GetError().GetMessage(), error.GetMessage());
                f.Retries->RunAllScheduledTasks();
                UNIT_ASSERT_VALUES_EQUAL(f.Peers->Calls, 1);
            }
        }
    }

    Y_UNIT_TEST(AdmissionOverloadRemainsRetriable)
    {
        TDataConfig config;
        config.MaxInflightRequests = 1;
        TFixture f(config);
        auto first = f.Zero();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Zero()).GetError().GetCode(), E_REJECTED);
        f.Peers->Channel->Respond(0);
        UNIT_ASSERT(!HasError(Wait(first)));
        f.Endpoint->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Zero()).GetError().GetCode(), E_CANCELLED);
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
