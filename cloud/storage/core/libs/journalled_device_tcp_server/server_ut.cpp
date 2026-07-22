#include "server.h"

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/vector.h>

#include <functional>
#include <mutex>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestBackend: public IServerBackend
{
    using TAcquireDevicesFunc =
        std::function<TFuture<NProto::TAcquireDevicesResponse>(
            NProto::TAcquireDevicesRequest)>;

    using TReleaseDevicesFunc =
        std::function<TFuture<NProto::TReleaseDevicesResponse>(
            NProto::TReleaseDevicesRequest)>;

    using TReadPagesFunc = std::function<TFuture<NProto::TReadPagesResponse>(
        NProto::TReadPagesRequest)>;

    using TWriteLogRecordFunc =
        std::function<TFuture<NProto::TWriteLogRecordResponse>(
            NProto::TWriteLogRecordRequest)>;

    TPromise<TAcquireDevicesFunc> AcquireDevicesImpl =
        NewPromise<TAcquireDevicesFunc>();
    TPromise<TReleaseDevicesFunc> ReleaseDevicesImpl =
        NewPromise<TReleaseDevicesFunc>();
    TPromise<TReadPagesFunc> ReadPagesImpl = NewPromise<TReadPagesFunc>();
    TPromise<TWriteLogRecordFunc> WriteLogRecordImpl =
        NewPromise<TWriteLogRecordFunc>();

    [[nodiscard]] auto AcquireDevices(
        TInstant now,
        NProto::TAcquireDevicesRequest request)
        -> TFuture<NProto::TAcquireDevicesResponse> final
    {
        Y_UNUSED(now);

        return AcquireDevicesImpl.GetFuture().Apply(
            [request](const auto& future)
            { return future.GetValue()(request); });
    }

    [[nodiscard]] auto ReleaseDevices(
        TInstant now,
        NProto::TReleaseDevicesRequest request)
        -> TFuture<NProto::TReleaseDevicesResponse> final
    {
        Y_UNUSED(now);

        return ReleaseDevicesImpl.GetFuture().Apply(
            [request](const auto& future)
            { return future.GetValue()(request); });
    }

    [[nodiscard]] auto ReadPages(
        TInstant now,
        NProto::TReadPagesRequest request)
        -> TFuture<NProto::TReadPagesResponse> final
    {
        Y_UNUSED(now);

        return ReadPagesImpl.GetFuture().Apply(
            [request](const auto& future)
            { return future.GetValue()(request); });
    }

    [[nodiscard]] auto WriteLogRecord(
        TInstant now,
        NProto::TWriteLogRecordRequest request)
        -> TFuture<NProto::TWriteLogRecordResponse> final
    {
        Y_UNUSED(now);

        return WriteLogRecordImpl.GetFuture().Apply(
            [request](const auto& future)
            { return future.GetValue()(request); });
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    ui16 Port = 0;
    std::shared_ptr<TTestBackend> Backend;
    TExecutorPtr Executor;
    ILoggingServicePtr Logging;
    std::shared_ptr<IStartable> Server;
    TPortManager PortManager;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        Port = PortManager.GetTcpPort();
        Backend = std::make_shared<TTestBackend>();
        Executor = TExecutor::Create("TestExecutor");
        Logging = CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_RESOURCES});
        Server =
            CreateServer(TNetworkAddress{Port}, Logging, Executor, Backend);

        Logging->Start();
        Executor->Start();
        Server->Start();
    }

    void TearDown(NUnitTest::TTestContext& /* testContext */) override
    {
        Server->Stop();
        Executor->Stop();
        Logging->Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestClient
{
private:
    TSocket Socket;

    TSocketInput In;
    TSocketOutput Out;

public:
    explicit TTestClient(ui16 port)
        : Socket(TNetworkAddress{port})
        , In(Socket)
        , Out(Socket)
    {
        Socket.SetNoDelay(true);
    }

    void Send(const NProto::TDeviceProtocolRequest& request)
    {
        TString payload;
        UNIT_ASSERT(request.SerializeToString(&payload));

        const ui32 wireSize = HostToInet(static_cast<ui32>(payload.size()));
        UNIT_ASSERT_GT(wireSize, 0);
        Out.Write(&wireSize, sizeof(wireSize));
        Out.Write(payload.data(), payload.size());
    }

    auto Receive() -> NProto::TDeviceProtocolResponse
    {
        NProto::TDeviceProtocolResponse response;
        ui32 wireSize = 0;
        In.LoadOrFail(&wireSize, sizeof(wireSize));

        const ui32 size = InetToHost(wireSize);

        TString payload;
        payload.resize(size);
        In.LoadOrFail(payload.Detach(), size);

        Y_ENSURE(response.ParseFromString(payload));

        return response;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceTCPServerTest)
{
    Y_UNIT_TEST_F(ShouldRejectBrokenRequest, TFixture)
    {
        const ui64 requestId = 42;

        TTestClient client{Port};

        {
            NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            client.Send(std::move(request));
        }

        {
            NProto::TDeviceProtocolResponse response = client.Receive();

            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kProtocolError,
                response.GetResponseCase());

            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                response.GetProtocolError().GetCode());
        }
    }

    Y_UNIT_TEST_F(ShouldDispatchRequestsToBackend, TFixture)
    {
        const ui64 expectedAcquireDevicesRequestId = 10;
        const ui64 expectedReleaseDevicesRequestId = 20;
        const ui64 expectedReadPagesRequestId = 30;
        const ui64 expectedWriteLogRecordRequestId = 40;

        const auto expectedAcquireDevicesRequest = []
        {
            NProto::TAcquireDevicesRequest proto;

            proto.MutableHeaders()->SetClientId("acquire");
            proto.MutableHeaders()->SetRequestTimeout(10'000);
            proto.MutableDeviceUUIDs()->Add("uuid-1");
            proto.MutableDeviceUUIDs()->Add("uuid-2");
            proto.MutableDeviceUUIDs()->Add("uuid-3");

            return proto;
        }();

        const auto expectedAcquireDevicesResponse = []
        {
            NProto::TAcquireDevicesResponse proto;

            proto.MutableError()->SetCode(E_PRECONDITION_FAILED);
            proto.MutableError()->SetMessage("expected-acquire-device-error");

            return proto;
        }();

        const auto expectedReleaseDevicesRequest = []
        {
            NProto::TReleaseDevicesRequest proto;

            proto.MutableHeaders()->SetClientId("release");
            proto.MutableHeaders()->SetRequestTimeout(1000);
            proto.MutableDeviceUUIDs()->Add("uuid-4");
            proto.MutableDeviceUUIDs()->Add("uuid-5");
            proto.MutableDeviceUUIDs()->Add("uuid-6");
            proto.MutableDeviceUUIDs()->Add("uuid-7");
            proto.MutableDeviceUUIDs()->Add("uuid-8");

            return proto;
        }();

        const auto expectedReleaseDevicesResponse = []
        {
            NProto::TReleaseDevicesResponse proto;

            proto.MutableError()->SetCode(E_INVALID_STATE);
            proto.MutableError()->SetMessage("expected-release-device-error");

            return proto;
        }();

        const auto expectedReadPagesRequest = []
        {
            NProto::TReadPagesRequest proto;

            proto.MutableHeaders()->SetClientId("read");
            proto.MutableHeaders()->SetRequestTimeout(200'000);
            proto.SetDeviceUUID("uuid-100");

            auto& refs = *proto.MutablePageGroupRefs();

            auto& ref1 = *refs.Add();
            ref1.SetFirstPageNo(0x8000);
            ref1.SetPageCount(1024);
            ref1.SetPageSize(32_KB);

            auto& ref2 = *refs.Add();
            ref2.SetFirstPageNo(0x1000);
            ref2.SetPageCount(32);
            ref2.SetPageSize(4_KB);

            return proto;
        }();

        const auto expectedReadPagesResponse = []
        {
            NProto::TReadPagesResponse proto;

            proto.MutableError()->SetCode(S_ALREADY);
            proto.MutableError()->SetMessage("expected-read-pages-error");

            auto& groups = *proto.MutablePageGroups();

            auto& group = *groups.Add();
            group.SetFirstPageNo(0x1000);
            auto& content = *group.MutableContent();
            content.Reserve(32);
            for (int i = 0; i != content.Capacity(); ++i) {
                content.Add()->resize(4_KB);
            }

            return proto;
        }();

        const auto expectedWriteLogRecordRequest = []
        {
            NProto::TWriteLogRecordRequest proto;

            proto.MutableHeaders()->SetClientId("write");
            proto.MutableHeaders()->SetRequestTimeout(17);
            proto.SetDeviceUUID("uuid-999");
            proto.SetLogSequenceNumber(10003);
            auto& groups = *proto.MutablePageGroups();

            {
                auto& group = *groups.Add();
                group.SetFirstPageNo(0x1000);
                auto& content = *group.MutableContent();
                content.Reserve(100);
                for (int i = 0; i != content.Capacity(); ++i) {
                    content.Add()->resize(4_KB);
                }
            }

            {
                auto& group = *groups.Add();
                group.SetFirstPageNo(0x2000);
                auto& content = *group.MutableContent();
                content.Reserve(100);
                for (int i = 0; i != content.Capacity(); ++i) {
                    content.Add()->resize(4_KB);
                }
            }

            {
                auto& group = *groups.Add();
                group.SetFirstPageNo(0x3000);
                auto& content = *group.MutableContent();
                content.Reserve(100);
                for (int i = 0; i != content.Capacity(); ++i) {
                    content.Add()->resize(4_KB);
                }
            }

            return proto;
        }();

        const auto expectedWriteLogRecordResponse = []
        {
            NProto::TWriteLogRecordResponse proto;

            proto.MutableError()->SetCode(E_PRECONDITION_FAILED);
            proto.MutableError()->SetMessage("expected-write-log-record-error");

            return proto;
        }();

        TTestClient client{Port};

        {
            NProto::TDeviceProtocolRequest request;
            request.SetRequestId(expectedAcquireDevicesRequestId);
            request.MutableAcquireDevices()->CopyFrom(
                expectedAcquireDevicesRequest);
            client.Send(request);
        }

        {
            NProto::TDeviceProtocolRequest request;
            request.SetRequestId(expectedReleaseDevicesRequestId);
            request.MutableReleaseDevices()->CopyFrom(
                expectedReleaseDevicesRequest);
            client.Send(request);
        }

        {
            NProto::TDeviceProtocolRequest request;
            request.SetRequestId(expectedReadPagesRequestId);
            request.MutableReadPages()->CopyFrom(expectedReadPagesRequest);
            client.Send(request);
        }

        {
            NProto::TDeviceProtocolRequest request;
            request.SetRequestId(expectedWriteLogRecordRequestId);
            request.MutableWriteLogRecord()->CopyFrom(
                expectedWriteLogRecordRequest);
            client.Send(request);
        }

        std::mutex mutex;
        std::optional<NProto::TAcquireDevicesRequest> acquireDevicesRequest;
        std::optional<NProto::TReleaseDevicesRequest> releaseDevicesRequest;
        std::optional<NProto::TReadPagesRequest> readPagesRequest;
        std::optional<NProto::TWriteLogRecordRequest> writeLogRecordRequest;

        Backend->AcquireDevicesImpl.SetValue(
            [&](auto request)
            {
                std::unique_lock lock(mutex);
                acquireDevicesRequest = std::move(request);
                return MakeFuture(expectedAcquireDevicesResponse);
            });

        Backend->ReleaseDevicesImpl.SetValue(
            [&](auto request)
            {
                std::unique_lock lock(mutex);
                releaseDevicesRequest = std::move(request);
                return MakeFuture(expectedReleaseDevicesResponse);
            });

        Backend->ReadPagesImpl.SetValue(
            [&](auto request)
            {
                std::unique_lock lock(mutex);
                readPagesRequest = std::move(request);
                return MakeFuture(expectedReadPagesResponse);
            });

        Backend->WriteLogRecordImpl.SetValue(
            [&](auto request)
            {
                std::unique_lock lock(mutex);
                writeLogRecordRequest = std::move(request);
                return MakeFuture(expectedWriteLogRecordResponse);
            });

        TVector<NProto::TDeviceProtocolResponse> responses;

        for (ui32 i = 0; i != 4; ++i) {
            responses.push_back(client.Receive());
        }

        SortBy(
            responses,
            [](const auto& proto) { return proto.GetRequestId(); });

        UNIT_ASSERT_VALUES_EQUAL(
            expectedAcquireDevicesRequestId,
            responses[0].GetRequestId());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedAcquireDevicesResponse.DebugString(),
            responses[0].GetAcquireDevices().DebugString());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedReleaseDevicesRequestId,
            responses[1].GetRequestId());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedReleaseDevicesResponse.DebugString(),
            responses[1].GetReleaseDevices().DebugString());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedReadPagesRequestId,
            responses[2].GetRequestId());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedReadPagesResponse.DebugString(),
            responses[2].GetReadPages().DebugString());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedWriteLogRecordRequestId,
            responses[3].GetRequestId());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedWriteLogRecordResponse.DebugString(),
            responses[3].GetWriteLogRecord().DebugString());

        UNIT_ASSERT(acquireDevicesRequest);
        UNIT_ASSERT(releaseDevicesRequest);
        UNIT_ASSERT(readPagesRequest);
        UNIT_ASSERT(writeLogRecordRequest);

        UNIT_ASSERT_VALUES_EQUAL(
            expectedAcquireDevicesRequest.DebugString(),
            acquireDevicesRequest->DebugString());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedReleaseDevicesRequest.DebugString(),
            releaseDevicesRequest->DebugString());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedReadPagesRequest.DebugString(),
            readPagesRequest->DebugString());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedWriteLogRecordRequest.DebugString(),
            writeLogRecordRequest->DebugString());
    }

    Y_UNIT_TEST_F(ShouldServeMultipleConnectionsConcurrently, TFixture)
    {
        const ui32 requestCount = 100;

        Backend->AcquireDevicesImpl.SetValue(
            [&](auto)
            { return MakeFuture(NProto::TAcquireDevicesResponse()); });

        TTestClient client1{Port};
        TTestClient client2{Port};

        auto send = [](TTestClient& client)
        {
            for (ui32 i = 0; i != requestCount; ++i) {
                NProto::TDeviceProtocolRequest request;
                request.SetRequestId(i);
                request.MutableAcquireDevices();
                client.Send(request);
            }
        };

        auto receive = [](TTestClient& client)
        {
            TVector<ui32> ids(requestCount);

            for (ui32 i = 0; i != requestCount; ++i) {
                ids[i] = client.Receive().GetRequestId();
            }

            Sort(ids);

            return ids;
        };

        TSimpleThreadPool queue;
        queue.Start(4);

        auto load1 = Async([&] { send(client1); }, queue);
        auto load2 = Async([&] { send(client2); }, queue);

        auto receive1 = Async([&] { return receive(client1); }, queue);
        auto receive2 = Async([&] { return receive(client2); }, queue);

        const auto& ids1 = receive1.GetValueSync();
        const auto& ids2 = receive2.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(requestCount, ids1.size());
        UNIT_ASSERT_VALUES_EQUAL(requestCount, ids2.size());

        const auto expectedIds = []
        {
            TVector<ui32> ids(requestCount);
            std::iota(ids.begin(), ids.end(), 0);
            return ids;
        }();

        UNIT_ASSERT_EQUAL(expectedIds, ids1);
        UNIT_ASSERT_EQUAL(expectedIds, ids2);

        queue.Stop();
    }
}

}   // namespace NCloud::NJournalled
