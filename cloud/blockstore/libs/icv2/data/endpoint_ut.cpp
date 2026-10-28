#include "data.h"
#include "protocol.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/icv2/transport/event_test.h>
#include <cloud/blockstore/libs/icv2/transport/handshake.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/icv2/tcp/testlib/socket_test.h>

#include <contrib/ydb/library/actors/interconnect/interconnect_common.h>
#include <contrib/ydb/library/actors/interconnect/interconnect_uring_engine.h>
#include <contrib/ydb/library/actors/interconnect/uring_context.h>

#include <library/cpp/testing/unittest/registar.h>

#include <future>
#include <mutex>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TEvents = NStorage::TEvService;
using namespace std::chrono_literals;
constexpr ui32 BlockSize = 4096;
constexpr ui32 BlockCount = 8;

template <typename T>
T Wait(const NThreading::TFuture<T>& future)
{
    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
    return future.GetValue();
}

class TManualExecutor final: public ITaskQueue
{
    std::mutex Mutex;
    TVector<ITaskPtr> Tasks;

public:
    void Start() override
    {}

    void Stop() override
    {}

    void Enqueue(ITaskPtr task) override
    {
        std::lock_guard guard(Mutex);
        Tasks.push_back(std::move(task));
    }

    void Run()
    {
        for (;;) {
            TVector<ITaskPtr> tasks;
            {
                std::lock_guard guard(Mutex);
                tasks.swap(Tasks);
            }
            if (tasks.empty()) {
                return;
            }
            for (auto& task: tasks) {
                task->Execute();
            }
        }
    }
};

struct TTestChannel final: IRequestChannel
{
    struct TRequest
    {
        std::unique_ptr<NActors::IEventBase> Event;
        TDuration Timeout;
        NThreading::TPromise<TResponseResult> Promise =
            NThreading::NewPromise<TResponseResult>();
        bool Completed = false;
    };

    std::mutex Mutex;
    TVector<TRequest> Requests;
    bool Closing = false;
    NThreading::TPromise<void> Closed = NThreading::NewPromise<void>();

    bool Activate() override
    {
        return true;
    }

    TResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) override
    {
        std::lock_guard guard(Mutex);
        if (Closing) {
            return NThreading::MakeFuture<TResponseResult>(
                MakeError(E_REJECTED, "closed"));
        }
        auto& request = Requests.emplace_back(std::move(event), timeout);
        return request.Promise.GetFuture();
    }

    bool Reply(ui64 cookie, std::unique_ptr<NActors::IEventBase> event) override
    {
        NThreading::TPromise<TResponseResult> promise;
        {
            std::lock_guard guard(Mutex);
            if (Closing || !cookie || cookie > Requests.size()) {
                return false;
            }
            auto& request = Requests[cookie - 1];
            if (request.Completed) {
                return true;
            }
            request.Completed = true;
            promise = request.Promise;
        }
        auto handle = std::make_shared<NActors::IEventHandle>(
            NActors::TActorId(),
            NActors::TActorId(),
            event.release(),
            0,
            cookie);
        NTests::PreserializeEvent(handle);
        promise.SetValue(TResponseResult(std::move(handle)));
        return true;
    }

    void Fail(size_t index, ui32 code)
    {
        NThreading::TPromise<TResponseResult> promise;
        {
            std::lock_guard guard(Mutex);
            auto& request = Requests.at(index);
            if (request.Completed) {
                return;
            }
            request.Completed = true;
            promise = request.Promise;
        }
        promise.SetValue(MakeError(code, "test error"));
    }

    void Close() override
    {
        size_t count;
        {
            std::lock_guard guard(Mutex);
            if (Closing) {
                return;
            }
            Closing = true;
            count = Requests.size();
        }
        for (size_t i = 0; i < count; ++i) {
            Fail(i, E_REJECTED);
        }
        Closed.SetValue();
    }

    NThreading::TFuture<void> WhenClosed() const override
    {
        return Closed.GetFuture();
    }

    std::unique_ptr<NActors::IEventHandle> Take(size_t index)
    {
        std::lock_guard guard(Mutex);
        auto handle = std::make_unique<NActors::IEventHandle>(
            NActors::TActorId(),
            NActors::TActorId(),
            Requests.at(index).Event.release(),
            0,
            index + 1);
        NTests::PreserializeEvent(handle);
        return handle;
    }
};

template <typename T>
std::shared_ptr<T> MakeRequest()
{
    auto request = std::make_shared<T>();
    request->SetDiskId("disk");
    request->SetSessionId("session");
    request->SetStartIndex(1);
    request->MutableHeaders()->SetClientId("client");
    request->MutableHeaders()->SetRequestId(123);
    request->MutableHeaders()->SetRequestTimeout(1000);
    return request;
}

auto MakeRead(TString& buffer)
{
    auto request = MakeRequest<NProto::TReadBlocksLocalRequest>();
    request->SetBlockSize(BlockSize);
    request->SetBlocksCount(buffer.size() / BlockSize);
    request->Sglist = TGuardedSgList({{buffer.data(), buffer.size()}});
    return request;
}

auto MakeWrite(const TString& buffer)
{
    auto request = MakeRequest<NProto::TWriteBlocksLocalRequest>();
    request->SetBlockSize(BlockSize);
    request->BlocksCount = buffer.size() / BlockSize;
    request->Sglist = TGuardedSgList(
        {{buffer.data(), 17}, {buffer.data() + 17, buffer.size() - 17}});
    return request;
}

auto MakeZero()
{
    auto request = MakeRequest<NProto::TZeroBlocksRequest>();
    request->SetBlocksCount(2);
    return request;
}

struct TFixture
{
    std::shared_ptr<TTestChannel> Channel = std::make_shared<TTestChannel>();
    std::shared_ptr<TManualExecutor> Executor =
        std::make_shared<TManualExecutor>();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestService> Service = std::make_shared<TTestService>();
    IDataEndpointPtr Endpoint;
    IDataTargetPtr Target;
    TString Data = TString(BlockSize * BlockCount, 'x');
    size_t Reads = 0;
    size_t Writes = 0;
    size_t Zeroes = 0;
    NThreading::TPromise<NProto::TReadBlocksLocalResponse> ReadCompletion =
        NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
    NThreading::TPromise<NProto::TWriteBlocksLocalResponse> WriteCompletion =
        NThreading::NewPromise<NProto::TWriteBlocksLocalResponse>();
    std::shared_ptr<NProto::TReadBlocksLocalRequest> DelayedRead;
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> DelayedWrite;

    explicit TFixture(
        TDataConfig endpointConfig = {},
        TDataConfig targetConfig = {})
    {
        auto [endpoint, endpointError] =
            CreateDataEndpoint(Channel, Timer, endpointConfig);
        UNIT_ASSERT_C(!HasError(endpointError), FormatError(endpointError));
        Endpoint = std::move(endpoint);
        auto [target, targetError] =
            CreateDataTarget(Service, Executor, targetConfig);
        UNIT_ASSERT_C(!HasError(targetError), FormatError(targetError));
        Target = std::move(target);
        Service->ReadBlocksLocalHandler = [&](auto request)
        {
            ++Reads;
            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            SgListCopy(
                {Data.data() + request->GetStartIndex() * BlockSize,
                 static_cast<size_t>(request->GetBlocksCount()) * BlockSize},
                guard.Get());
            return NThreading::MakeFuture<NProto::TReadBlocksLocalResponse>();
        };
        Service->WriteBlocksLocalHandler = [&](auto request)
        {
            ++Writes;
            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            SgListCopy(
                guard.Get(),
                {Data.Detach() + request->GetStartIndex() * BlockSize,
                 static_cast<size_t>(request->BlocksCount) * BlockSize});
            return NThreading::MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };
        Service->ZeroBlocksHandler = [&](auto request)
        {
            ++Zeroes;
            memset(
                Data.Detach() + request->GetStartIndex() * BlockSize,
                0,
                request->GetBlocksCount() * BlockSize);
            return NThreading::MakeFuture<NProto::TZeroBlocksResponse>();
        };
    }

    ~TFixture()
    {
        Endpoint->RequestStop();
        Target->RequestStop();
        if (!ReadCompletion.GetFuture().HasValue()) {
            ReadCompletion.SetValue(TErrorResponse(E_REJECTED, "fixture stop"));
        }
        if (!WriteCompletion.GetFuture().HasValue()) {
            WriteCompletion.SetValue(
                TErrorResponse(E_REJECTED, "fixture stop"));
        }
        Executor->Run();
        Target->Stop();
        Endpoint->Stop();
    }

    void Run(size_t index)
    {
        Target->HandleRequest(Channel, Channel->Take(index));
        Executor->Run();
    }

    void DelayRead()
    {
        Service->ReadBlocksLocalHandler = [&](auto request)
        {
            ++Reads;
            DelayedRead = std::move(request);
            {
                auto guard = DelayedRead->Sglist.Acquire();
                for (const auto& block: guard.Get()) {
                    memset(const_cast<char*>(block.Data()), 'r', block.Size());
                }
            }
            return ReadCompletion.GetFuture();
        };
    }

    void DelayWrite()
    {
        Service->WriteBlocksLocalHandler = [&](auto request)
        {
            ++Writes;
            DelayedWrite = std::move(request);
            return WriteCompletion.GetFuture();
        };
    }
};

struct TNetworkFixture
{
    std::array<TFixture, 2> Peers;
    std::array<ITransportPtr, 2> Transports;
    std::array<IRequestChannelPtr, 2> Channels;
    std::array<IDataEndpointPtr, 2> Endpoints;
    ITaskQueuePtr Completions = CreateThreadPool("ICV2DataTest", 2);

    explicit TNetworkFixture(bool checksums, bool xdc = false)
    {
        Completions->Start();
        NCloud::NIcV2::NTests::TTestListener listener;
        listener.Listener->Start();
        auto connector = NCloud::NIcV2::NTests::CreateTestConnector();
        connector->Start();
        std::array<TSocketPtr, 2> sockets{
            NCloud::NIcV2::NTests::WaitSocket(connector->Connect(
                listener.Listener->GetAddress(),
                TDuration::Seconds(5))),
            listener.Accept()};
        connector->Stop();
        std::array<IHandshakeProviderPtr, 2> hellos;
        for (size_t i = 0; i < hellos.size(); ++i) {
            auto [hello, error] =
                CreateHandshakeProvider(THandshakeConfig{checksums, xdc});
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            hellos[i] = std::move(hello);
            hellos[i]->Start();
        }
        const auto outgoing =
            hellos[0]->Connect(std::move(sockets[0]), TDuration::Seconds(5));
        const auto incoming =
            hellos[1]->Accept(std::move(sockets[1]), TDuration::Seconds(5));
        if (xdc) {
            auto auxiliary = Wait(
                hellos[1]->Accept(listener.Accept(), TDuration::Seconds(5)));
            UNIT_ASSERT_C(
                !HasError(auxiliary.GetError()),
                FormatError(auxiliary.GetError()));
            UNIT_ASSERT(
                std::holds_alternative<THandshakeIncomplete>(
                    auxiliary.GetResult()));
        }
        const auto outgoingResult = Wait(outgoing);
        const auto incomingResult = Wait(incoming);
        UNIT_ASSERT_C(
            !HasError(outgoingResult.GetError()),
            FormatError(outgoingResult.GetError()));
        UNIT_ASSERT_C(
            !HasError(incomingResult.GetError()),
            FormatError(incomingResult.GetError()));
        const std::array<THandshakeReady, 2> connected{
            outgoingResult.GetResult(),
            std::get<THandshakeReady>(incomingResult.GetResult())};
        for (size_t i = 0; i < Channels.size(); ++i) {
            const auto& connection = connected[i];
            hellos[i]->Stop();
            auto common = MakeIntrusive<NActors::TInterconnectProxyCommon>();
            common->MonCounters =
                MakeIntrusive<NMonitoring::TDynamicCounters>();
            common->Settings.V2.Threads = 1;
            common->Settings.V2.EnableSQPOLL = false;
            common->Settings.V2.ChecksumEvents = checksums;
            Transports[i] = NCloud::NIcV2::CreateTransport(
                NActors::CreateUringEngine(std::move(common)),
                0,
                {});
            UNIT_ASSERT(Transports[i]);
            auto weak = std::make_shared<std::weak_ptr<IRequestChannel>>();
            TRequestChannelConfig config;
            config.EventTypes.assign(
                DataEventTypes.begin(),
                DataEventTypes.end());
            auto [channel, error] = CreateRequestChannel(
                Transports[i],
                connection.Socket,
                connection.XdcSocket,
                config,
                Peers[i].Timer,
                std::make_shared<TTestScheduler>(),
                Completions,
                [target = Peers[i].Target, weak](auto request)
                {
                    if (auto channel = weak->lock()) {
                        target->HandleRequest(channel, std::move(request));
                    }
                });
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            Channels[i] = std::move(channel);
            *weak = Channels[i];
            auto [endpoint, endpointError] =
                CreateDataEndpoint(Channels[i], Peers[i].Timer, {});
            UNIT_ASSERT_C(!HasError(endpointError), FormatError(endpointError));
            Endpoints[i] = std::move(endpoint);
            Peers[i].Data.resize(8 * 1024 * 1024, 'x');
        }
        for (auto& channel: Channels) {
            UNIT_ASSERT(channel->Activate());
        }
    }

    ~TNetworkFixture()
    {
        for (auto& endpoint: Endpoints) {
            if (endpoint) {
                endpoint->RequestStop();
            }
        }
        for (auto& transport: Transports) {
            if (transport) {
                transport->Stop();
            }
        }
        for (auto& peer: Peers) {
            peer.Executor->Run();
            peer.Target->Stop();
        }
        for (auto& endpoint: Endpoints) {
            if (endpoint) {
                endpoint->Stop();
            }
        }
        Completions->Stop();
    }

    template <typename T>
    T Pump(const NThreading::TFuture<T>& future)
    {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (!future.HasValue() &&
               std::chrono::steady_clock::now() < deadline)
        {
            for (auto& peer: Peers) {
                peer.Executor->Run();
            }
            future.Wait(TDuration::MilliSeconds(1));
        }
        return Wait(future);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2DataEndpointTest)
{
    Y_UNIT_TEST(WriteReadAndZeroPreserveDataAndRequestMetadata)
    {
        TFixture f;
        TString data(2 * BlockSize, 'w');
        data[0] = '\0';
        auto write = f.Endpoint->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeWrite(data));
        const auto* sent = static_cast<TEvents::TEvWriteBlocksRequest*>(
            f.Channel->Requests[0].Event.get());
        UNIT_ASSERT_VALUES_EQUAL(sent->Record.GetBlocks().BuffersSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sent->GetPayloadCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sent->GetPayload(0).ConvertToString(), data);
        f.Run(0);
        UNIT_ASSERT(!HasError(Wait(write)));
        TString buffer(data.size(), '?');
        auto read = f.Endpoint->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeRead(buffer));
        f.Run(1);
        UNIT_ASSERT(!HasError(Wait(read)));
        UNIT_ASSERT_VALUES_EQUAL(buffer, data);
        auto zero =
            f.Endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), MakeZero());
        f.Run(2);
        UNIT_ASSERT(!HasError(Wait(zero)));
        auto again = f.Endpoint->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeRead(buffer));
        f.Run(3);
        UNIT_ASSERT(!HasError(Wait(again)));
        UNIT_ASSERT_VALUES_EQUAL(buffer, TString(data.size(), '\0'));
        UNIT_ASSERT_VALUES_EQUAL(f.Reads, 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Writes, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Zeroes, 1);
    }

    Y_UNIT_TEST(WriteSnapshotOutlivesCallerAndTargetClosesStorageSglist)
    {
        TFixture f;
        f.DelayWrite();
        TString data(2 * BlockSize, 'a');
        auto request = MakeWrite(data);
        auto future = f.Endpoint->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            request);
        request->Sglist.Close();
        data = TString(data.size(), 'b');
        request.reset();
        f.Run(0);
        auto captured = f.DelayedWrite;
        TString snapshot(2 * BlockSize, '?');
        {
            auto guard = captured->Sglist.Acquire();
            UNIT_ASSERT(guard);
            SgListCopy(guard.Get(), {snapshot.Detach(), snapshot.size()});
        }
        const bool metadata =
            captured->GetSessionId() == "session" &&
            captured->GetHeaders().GetClientId() == "client" &&
            captured->GetHeaders().GetRequestId() == 123 &&
            captured->BlocksCount == 2;
        f.WriteCompletion.SetValue({});
        f.Executor->Run();
        UNIT_ASSERT(!HasError(Wait(future)));
        UNIT_ASSERT(metadata);
        UNIT_ASSERT_VALUES_EQUAL(snapshot, TString(snapshot.size(), 'a'));
        UNIT_ASSERT(!captured->Sglist.Acquire());
    }

    Y_UNIT_TEST(LateReadAfterTimeoutDoesNotTouchCallerBuffer)
    {
        TFixture f;
        f.DelayRead();
        TString buffer(2 * BlockSize, '?');
        auto request = MakeRead(buffer);
        auto future =
            f.Endpoint->ReadBlocksLocal(MakeIntrusive<TCallContext>(), request);
        f.Run(0);
        f.Channel->Fail(0, E_TIMEOUT);
        request->Sglist.Close();
        f.ReadCompletion.SetValue({});
        f.Executor->Run();
        UNIT_ASSERT_VALUES_EQUAL(Wait(future).GetError().GetCode(), E_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(buffer, TString(buffer.size(), '?'));
        UNIT_ASSERT(!f.DelayedRead->Sglist.Acquire());
    }

    Y_UNIT_TEST(ClosedCallerSglistRejectsReadCopy)
    {
        TFixture f;
        f.DelayRead();
        TString buffer(2 * BlockSize, '?');
        auto request = MakeRead(buffer);
        auto future =
            f.Endpoint->ReadBlocksLocal(MakeIntrusive<TCallContext>(), request);
        f.Run(0);
        request->Sglist.Close();
        f.ReadCompletion.SetValue({});
        f.Executor->Run();
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(future).GetError().GetCode(),
            E_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(buffer, TString(buffer.size(), '?'));
    }

    Y_UNIT_TEST(TargetStopWaitsForActualStorageCompletionAfterDisconnect)
    {
        TFixture f;
        f.DelayRead();
        TString buffer(2 * BlockSize, '?');
        auto future = f.Endpoint->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeRead(buffer));
        f.Run(0);
        f.Endpoint->RequestStop();
        auto stopping =
            std::async(std::launch::async, [&] { f.Target->Stop(); });
        const bool waits =
            stopping.wait_for(30ms) == std::future_status::timeout;
        const bool alive = static_cast<bool>(f.DelayedRead->Sglist.Acquire());
        f.ReadCompletion.SetValue({});
        f.Executor->Run();
        stopping.get();
        UNIT_ASSERT(waits);
        UNIT_ASSERT(alive);
        UNIT_ASSERT(!f.DelayedRead->Sglist.Acquire());
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(future).GetError().GetCode(),
            E_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(buffer, TString(buffer.size(), '?'));
    }

    Y_UNIT_TEST(EndpointAndTargetApplySharedAdmissionLimits)
    {
        for (bool targetLimit: {false, true}) {
            TDataConfig config;
            config.MaxInflightRequests = 1;
            TFixture f(
                targetLimit ? TDataConfig{} : config,
                targetLimit ? config : TDataConfig{});
            f.DelayRead();
            TString buffer(2 * BlockSize, '?');
            auto read = f.Endpoint->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                MakeRead(buffer));
            f.Run(0);
            auto zero = f.Endpoint->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                MakeZero());
            if (targetLimit) {
                f.Run(1);
            }
            const auto code = Wait(zero).GetError().GetCode();
            f.ReadCompletion.SetValue({});
            f.Executor->Run();
            UNIT_ASSERT(!HasError(Wait(read)));
            UNIT_ASSERT_VALUES_EQUAL(code, E_REJECTED);
            UNIT_ASSERT_VALUES_EQUAL(f.Zeroes, 0);
            auto next = f.Endpoint->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                MakeZero());
            f.Run(targetLimit ? 2 : 1);
            UNIT_ASSERT(!HasError(Wait(next)));
        }
    }

    Y_UNIT_TEST(TargetReadReservationPreventsUnboundedAllocation)
    {
        TDataConfig config;
        config.MaxInflightBytes = BlockSize;
        TFixture f({}, config);
        TString buffer(2 * BlockSize, '?');
        auto read = f.Endpoint->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeRead(buffer));
        f.Run(0);
        UNIT_ASSERT_VALUES_EQUAL(Wait(read).GetError().GetCode(), E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(f.Reads, 0);
        auto zero =
            f.Endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), MakeZero());
        f.Run(1);
        UNIT_ASSERT(!HasError(Wait(zero)));
    }

    Y_UNIT_TEST(EndpointRejectsPayloadOverMemoryBudgetBeforeSending)
    {
        TDataConfig config;
        config.MaxInflightBytes = BlockSize;
        TFixture f(config);
        TString buffer(2 * BlockSize, '?');
        auto future = f.Endpoint->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeRead(buffer));
        UNIT_ASSERT_VALUES_EQUAL(Wait(future).GetError().GetCode(), E_REJECTED);
        UNIT_ASSERT(f.Channel->Requests.empty());
    }

    Y_UNIT_TEST(TargetCountsPayloadInSerializedRequestLimit)
    {
        TDataConfig config;
        config.MaxRequestBytes = BlockSize;
        TFixture f({}, config);
        TString data(2 * BlockSize, 'w');
        auto write = f.Endpoint->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeWrite(data));
        f.Run(0);
        UNIT_ASSERT_VALUES_EQUAL(Wait(write).GetError().GetCode(), E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(f.Writes, 0);
    }

    Y_UNIT_TEST(StorageErrorsAndExceptionsAreReturnedWithoutRetry)
    {
        for (ui32 code: {E_BS_INVALID_SESSION, E_BADMSG, E_FAIL}) {
            TFixture f;
            f.Service->ZeroBlocksHandler =
                [&](auto) -> NThreading::TFuture<NProto::TZeroBlocksResponse>
            {
                ++f.Zeroes;
                if (code == E_FAIL) {
                    ythrow yexception() << "storage failure";
                }
                return NThreading::MakeFuture<NProto::TZeroBlocksResponse>(
                    TErrorResponse(code, "storage error"));
            };
            auto future = f.Endpoint->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                MakeZero());
            f.Run(0);
            UNIT_ASSERT_VALUES_EQUAL(Wait(future).GetError().GetCode(), code);
            UNIT_ASSERT_VALUES_EQUAL(f.Zeroes, 1);
            UNIT_ASSERT_VALUES_EQUAL(f.Channel->Requests.size(), 1);
            UNIT_ASSERT(!f.Channel->Closing);
        }
    }

    Y_UNIT_TEST(RejectsMalformedReadResponseBeforeCopyingAnyData)
    {
        for (size_t variant = 0; variant < 6; ++variant) {
            TFixture f;
            TString buffer(2 * BlockSize, '?');
            auto future = f.Endpoint->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                MakeRead(buffer));
            auto response = std::make_unique<TEvents::TEvReadBlocksResponse>();
            switch (variant) {
                case 0:   // Missing payload.
                    break;
                case 1:   // Short payload.
                    response->AddPayload(TRope(TString(BlockSize, 'x')));
                    break;
                case 2:   // Oversized payload.
                    response->AddPayload(TRope(TString(3 * BlockSize, 'x')));
                    break;
                case 3:   // Multiple payloads.
                    response->AddPayload(TRope(TString(BlockSize, 'x')));
                    response->AddPayload(TRope(TString(BlockSize, 'y')));
                    break;
                case 4:   // Mixed protobuf blocks and payload.
                    response->Record.MutableBlocks()->AddBuffers(
                        TString(BlockSize, 'x'));
                    response->AddPayload(TRope(TString(2 * BlockSize, 'x')));
                    break;
                case 5:   // Error responses carry no data.
                    *response->Record.MutableError() =
                        MakeError(E_FAIL, "storage failure");
                    response->AddPayload(TRope(TString(2 * BlockSize, 'x')));
                    break;
            }
            f.Channel->Reply(1, std::move(response));
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(future).GetError().GetCode(),
                E_BADMSG);
            UNIT_ASSERT(f.Channel->Closing);
            UNIT_ASSERT_VALUES_EQUAL(buffer, TString(buffer.size(), '?'));
        }
    }

    Y_UNIT_TEST(CopiesFragmentedReadPayloadIntoFragmentedCallerBuffers)
    {
        TFixture f;
        TString buffer(2 * BlockSize, '?');
        auto request = MakeRead(buffer);
        request->Sglist = TGuardedSgList(
            {{buffer.data(), 123}, {buffer.data() + 123, buffer.size() - 123}});
        auto future =
            f.Endpoint->ReadBlocksLocal(MakeIntrusive<TCallContext>(), request);
        auto response = std::make_unique<TEvents::TEvReadBlocksResponse>();
        TRope payload(TString(17, 'a'));
        payload.Insert(payload.End(), TRope(TString(buffer.size() - 17, 'b')));
        response->AddPayload(std::move(payload));
        f.Channel->Reply(1, std::move(response));
        UNIT_ASSERT(!HasError(Wait(future)));
        UNIT_ASSERT_VALUES_EQUAL(
            buffer,
            TString(17, 'a') + TString(buffer.size() - 17, 'b'));
    }

    Y_UNIT_TEST(StorageReadErrorHasNoPayloadAndDoesNotCopyData)
    {
        TFixture f;
        f.Service->ReadBlocksLocalHandler = [](auto)
        {
            return NThreading::MakeFuture<NProto::TReadBlocksLocalResponse>(
                TErrorResponse(E_IO, "storage failure"));
        };
        TString buffer(2 * BlockSize, '?');
        auto read = f.Endpoint->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeRead(buffer));
        f.Run(0);
        UNIT_ASSERT_VALUES_EQUAL(Wait(read).GetError().GetCode(), E_IO);
        UNIT_ASSERT(!f.Channel->Closing);
        UNIT_ASSERT_VALUES_EQUAL(buffer, TString(buffer.size(), '?'));
    }

    Y_UNIT_TEST(FragmentedWritePayloadOutlivesReceivedEvent)
    {
        TFixture f;
        f.DelayWrite();
        auto write = std::make_unique<TEvents::TEvWriteBlocksRequest>();
        write->Record.CopyFrom(*MakeRequest<NProto::TWriteBlocksRequest>());
        write->Record.SetBlockSize(BlockSize);
        TRope payload(TString(17, 'a'));
        payload.Insert(payload.End(), TRope(TString(2 * BlockSize - 17, 'b')));
        write->AddPayload(std::move(payload));
        auto future =
            f.Channel->Request(std::move(write), TDuration::Seconds(1));
        f.Run(0);
        TString snapshot(2 * BlockSize, '?');
        {
            auto guard = f.DelayedWrite->Sglist.Acquire();
            UNIT_ASSERT(guard);
            UNIT_ASSERT_VALUES_EQUAL(guard.Get().size(), 2);
            SgListCopy(guard.Get(), {snapshot.Detach(), snapshot.size()});
        }
        UNIT_ASSERT_VALUES_EQUAL(
            snapshot,
            TString(17, 'a') + TString(snapshot.size() - 17, 'b'));
        f.WriteCompletion.SetValue({});
        f.Executor->Run();
        UNIT_ASSERT(!HasError(Wait(future).GetError()));
        UNIT_ASSERT(!f.DelayedWrite->Sglist.Acquire());
    }

    Y_UNIT_TEST(RejectsMalformedWritePayloadBeforeCallingStorage)
    {
        for (size_t variant = 0; variant < 5; ++variant) {
            TFixture f;
            auto write = std::make_unique<TEvents::TEvWriteBlocksRequest>();
            write->Record.CopyFrom(*MakeRequest<NProto::TWriteBlocksRequest>());
            write->Record.SetBlockSize(BlockSize);
            switch (variant) {
                case 0:   // Missing payload.
                    break;
                case 1:   // Empty payload.
                    write->AddPayload(TRope());
                    break;
                case 2:   // Unaligned payload.
                    write->AddPayload(TRope(TString(17, 'x')));
                    break;
                case 3:   // Multiple payloads.
                    write->AddPayload(TRope(TString(BlockSize, 'x')));
                    write->AddPayload(TRope(TString(BlockSize, 'y')));
                    break;
                case 4:   // Mixed protobuf blocks and payload.
                    write->Record.MutableBlocks()->AddBuffers(
                        TString(BlockSize, 'x'));
                    write->AddPayload(TRope(TString(BlockSize, 'y')));
                    break;
            }
            auto future =
                f.Channel->Request(std::move(write), TDuration::Seconds(1));
            f.Run(0);
            auto result = Wait(future);
            UNIT_ASSERT(!HasError(result.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                result.GetResult()
                    ->Get<TEvents::TEvWriteBlocksResponse>()
                    ->Record.GetError()
                    .GetCode(),
                E_ARGUMENT);
            UNIT_ASSERT_VALUES_EQUAL(f.Writes, 0);
        }
    }

    Y_UNIT_TEST(RejectsInvalidOutgoingRangeSizesAndUnsupportedControlRequests)
    {
        TFixture f;
        TString buffer(2 * BlockSize, '?');
        for (ui32 size: {ui32(0), ui32(1024 * 1024 * 1024)}) {
            auto request = MakeRead(buffer);
            request->SetBlockSize(size);
            auto future = f.Endpoint->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                request);
            UNIT_ASSERT_VALUES_EQUAL(
                Wait(future).GetError().GetCode(),
                E_ARGUMENT);
        }
        auto request = MakeZero();
        request->SetStartIndex(Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), request))
                .GetError()
                .GetCode(),
            E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(f.Endpoint->MountVolume(
                     MakeIntrusive<TCallContext>(),
                     std::make_shared<NProto::TMountVolumeRequest>()))
                .GetError()
                .GetCode(),
            E_NOT_IMPLEMENTED);
        UNIT_ASSERT(f.Channel->Requests.empty());
    }

    Y_UNIT_TEST(ChecksDeadlineAgainBeforeCopyingQueuedReadResult)
    {
        TFixture f;
        TString buffer(2 * BlockSize, '?');
        auto future = f.Endpoint->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            MakeRead(buffer));
        f.Timer->AdvanceTime(TDuration::Seconds(1));
        f.Run(0);
        UNIT_ASSERT_VALUES_EQUAL(Wait(future).GetError().GetCode(), E_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(buffer, TString(buffer.size(), '?'));
    }

    Y_UNIT_TEST(TargetRejectsInvalidReadAndWriteBeforeCallingStorage)
    {
        TFixture f;
        auto read = std::make_unique<TEvents::TEvReadBlocksRequest>();
        read->Record.SetDiskId("disk");
        read->Record.SetBlocksCount(Max<ui32>());
        read->Record.SetBlockSize(Max<ui32>());
        auto readResult =
            f.Channel->Request(std::move(read), TDuration::Seconds(1));
        f.Run(0);
        auto readResponse = Wait(readResult);
        UNIT_ASSERT(!HasError(readResponse.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            readResponse.GetResult()
                ->Get<TEvents::TEvReadBlocksResponse>()
                ->Record.GetError()
                .GetCode(),
            E_ARGUMENT);
        auto write = std::make_unique<TEvents::TEvWriteBlocksRequest>();
        write->Record.SetDiskId("disk");
        write->Record.SetBlockSize(BlockSize);
        write->Record.MutableBlocks()->AddBuffers("unaligned");
        auto writeResult =
            f.Channel->Request(std::move(write), TDuration::Seconds(1));
        f.Run(1);
        auto writeResponse = Wait(writeResult);
        UNIT_ASSERT(!HasError(writeResponse.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            writeResponse.GetResult()
                ->Get<TEvents::TEvWriteBlocksResponse>()
                ->Record.GetError()
                .GetCode(),
            E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(f.Reads, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Writes, 0);
    }

    Y_UNIT_TEST(
        EndpointStopWaitsForResultContinuationAndRequestStopIsCallbackSafe)
    {
        TFixture f;
        auto future =
            f.Endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), MakeZero());
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        future.Subscribe(
            [&](const auto&)
            {
                f.Endpoint->RequestStop();
                entered.SetValue();
                release.GetFuture().GetValueSync();
            });
        auto delivery = std::async(std::launch::async, [&] { f.Run(0); });
        const bool started = entered.GetFuture().Wait(TDuration::Seconds(5));
        auto stopping =
            std::async(std::launch::async, [&] { f.Endpoint->Stop(); });
        const bool waits =
            stopping.wait_for(30ms) == std::future_status::timeout;
        release.SetValue();
        delivery.get();
        stopping.get();
        UNIT_ASSERT(started);
        UNIT_ASSERT(waits);
    }

    Y_UNIT_TEST(TargetRequestStopFromStorageCallbackRejectsFurtherRequests)
    {
        TFixture f;
        f.Service->ZeroBlocksHandler = [&](auto)
        {
            ++f.Zeroes;
            f.Target->RequestStop();
            return NThreading::MakeFuture<NProto::TZeroBlocksResponse>();
        };
        auto first =
            f.Endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), MakeZero());
        f.Run(0);
        UNIT_ASSERT(!HasError(Wait(first)));
        auto second =
            f.Endpoint->ZeroBlocks(MakeIntrusive<TCallContext>(), MakeZero());
        f.Run(1);
        UNIT_ASSERT_VALUES_EQUAL(
            Wait(second).GetError().GetCode(),
            E_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(f.Zeroes, 1);
    }

    Y_UNIT_TEST(RealTcpHelloAndIcV2ExchangeBlockDataInBothDirections)
    {
        if (!NActors::TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        for (const auto [checksums, xdc]:
             std::array<std::pair<bool, bool>, 4>{
                 {{false, false}, {false, true}, {true, false}, {true, true}}})
        {
            TNetworkFixture f(checksums, xdc);
            constexpr size_t Count = 16;
            constexpr size_t PayloadBytes = 256 * 1024;
            std::array<TString, Count> payloads;
            std::array<
                std::array<
                    NThreading::TFuture<NProto::TWriteBlocksResponse>,
                    Count>,
                2>
                writes;
            for (size_t i = 0; i < Count; ++i) {
                payloads[i] = TString(PayloadBytes, 'a' + i);
                payloads[i][0] = '\0';
                for (size_t peer = 0; peer < writes.size(); ++peer) {
                    auto request = MakeWrite(payloads[i]);
                    request->SetStartIndex(1 + i * PayloadBytes / BlockSize);
                    writes[peer][i] = f.Endpoints[peer]->WriteBlocksLocal(
                        MakeIntrusive<TCallContext>(),
                        request);
                }
            }
            for (auto& peer: writes) {
                for (const auto& future: peer) {
                    UNIT_ASSERT(!HasError(f.Pump(future)));
                }
            }
            std::array<std::array<TString, Count>, 2> buffers;
            std::array<
                std::array<
                    NThreading::TFuture<NProto::TReadBlocksLocalResponse>,
                    Count>,
                2>
                reads;
            for (size_t i = 0; i < Count; ++i) {
                for (size_t peer = 0; peer < reads.size(); ++peer) {
                    buffers[peer][i] = TString(PayloadBytes, '?');
                    auto request = MakeRead(buffers[peer][i]);
                    request->SetStartIndex(1 + i * PayloadBytes / BlockSize);
                    reads[peer][i] = f.Endpoints[peer]->ReadBlocksLocal(
                        MakeIntrusive<TCallContext>(),
                        request);
                }
            }
            for (size_t peer = 0; peer < reads.size(); ++peer) {
                for (size_t i = 0; i < Count; ++i) {
                    UNIT_ASSERT(!HasError(f.Pump(reads[peer][i])));
                    UNIT_ASSERT_VALUES_EQUAL(buffers[peer][i], payloads[i]);
                }
                auto request = MakeZero();
                request->SetBlocksCount(PayloadBytes / BlockSize);
                UNIT_ASSERT(!HasError(f.Pump(f.Endpoints[peer]->ZeroBlocks(
                    MakeIntrusive<TCallContext>(),
                    request))));
                TString buffer(PayloadBytes, '?');
                UNIT_ASSERT(!HasError(f.Pump(f.Endpoints[peer]->ReadBlocksLocal(
                    MakeIntrusive<TCallContext>(),
                    MakeRead(buffer)))));
                UNIT_ASSERT_VALUES_EQUAL(buffer, TString(PayloadBytes, '\0'));
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
