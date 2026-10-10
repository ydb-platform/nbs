#include "builder.h"
#include "server.h"
#include "service.h"

#include <cloud/fastshard/journal/iface/device.h>
#include <cloud/fastshard/journal/impl/memory_device.h>
#include <cloud/fastshard/protos/device.pb.h>

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/hash.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

#include <functional>
#include <mutex>
#include <optional>

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

    using TFormatDeviceFunc =
        std::function<TFuture<NProto::TFormatDeviceResponse>(
            NProto::TFormatDeviceRequest)>;

    using TReadPagesFunc = std::function<TFuture<NProto::TReadPagesResponse>(
        NProto::TReadPagesRequest)>;

    using TWriteLogRecordFunc =
        std::function<TFuture<NProto::TWriteLogRecordResponse>(
            NProto::TWriteLogRecordRequest)>;

    using TReadJournalTailFunc =
        std::function<TFuture<NProto::TReadJournalTailResponse>(
            NProto::TReadJournalTailRequest)>;

    using TAdvanceLsnLowWatermarkFunc =
        std::function<TFuture<NProto::TAdvanceLsnLowWatermarkResponse>(
            NProto::TAdvanceLsnLowWatermarkRequest)>;

    TAcquireDevicesFunc AcquireDevicesImpl;
    TReleaseDevicesFunc ReleaseDevicesImpl;
    TFormatDeviceFunc FormatDeviceImpl;
    TReadPagesFunc ReadPagesImpl;
    TWriteLogRecordFunc WriteLogRecordImpl;
    TReadJournalTailFunc ReadJournalTailImpl;
    TAdvanceLsnLowWatermarkFunc AdvanceLsnLowWatermarkImpl;

    TFuture<NProto::TError> Start() override
    {
        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> Stop() override
    {
        return MakeFuture<NProto::TError>();
    }

    [[nodiscard]] auto AcquireDevices(
        NProto::TAcquireDevicesRequest request)
        -> TFuture<NProto::TAcquireDevicesResponse> final
    {
        return AcquireDevicesImpl(std::move(request));
    }

    [[nodiscard]] auto ReleaseDevices(
        NProto::TReleaseDevicesRequest request)
        -> TFuture<NProto::TReleaseDevicesResponse> final
    {
        return ReleaseDevicesImpl(std::move(request));
    }

    [[nodiscard]] auto FormatDevice(
        NProto::TFormatDeviceRequest request)
        -> TFuture<NProto::TFormatDeviceResponse> final
    {
        return FormatDeviceImpl(std::move(request));
    }

    [[nodiscard]] auto ReadPages(
        NProto::TReadPagesRequest request)
        -> TFuture<NProto::TReadPagesResponse> final
    {
        return ReadPagesImpl(std::move(request));
    }

    [[nodiscard]] auto WriteLogRecord(
        NProto::TWriteLogRecordRequest request)
        -> TFuture<NProto::TWriteLogRecordResponse> final
    {
        return WriteLogRecordImpl(std::move(request));
    }

    [[nodiscard]] auto ReadJournalTail(
        NProto::TReadJournalTailRequest request)
        -> TFuture<NProto::TReadJournalTailResponse> final
    {
        return ReadJournalTailImpl(std::move(request));
    }

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NProto::TAdvanceLsnLowWatermarkResponse> final
    {
        return AdvanceLsnLowWatermarkImpl(std::move(request));
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

        std::mutex mutex;
        std::optional<NProto::TAcquireDevicesRequest> acquireDevicesRequest;
        std::optional<NProto::TReleaseDevicesRequest> releaseDevicesRequest;
        std::optional<NProto::TReadPagesRequest> readPagesRequest;
        std::optional<NProto::TWriteLogRecordRequest> writeLogRecordRequest;

        Backend->AcquireDevicesImpl = [&](auto request)
        {
            std::unique_lock lock(mutex);
            acquireDevicesRequest = std::move(request);
            return MakeFuture(expectedAcquireDevicesResponse);
        };

        Backend->ReleaseDevicesImpl = [&](auto request)
        {
            std::unique_lock lock(mutex);
            releaseDevicesRequest = std::move(request);
            return MakeFuture(expectedReleaseDevicesResponse);
        };

        Backend->ReadPagesImpl = [&](auto request)
        {
            std::unique_lock lock(mutex);
            readPagesRequest = std::move(request);
            return MakeFuture(expectedReadPagesResponse);
        };

        Backend->WriteLogRecordImpl = [&](auto request)
        {
            std::unique_lock lock(mutex);
            writeLogRecordRequest = std::move(request);
            return MakeFuture(expectedWriteLogRecordResponse);
        };

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

        Backend->AcquireDevicesImpl = [&](auto)
        {
            return MakeFuture(NProto::TAcquireDevicesResponse());
        };

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

    Y_UNIT_TEST_F(ShouldDispatchFormatDevice, TFixture)
    {
        const ui64 requestId = 50;

        const auto expectedRequest = []
        {
            NProto::TFormatDeviceRequest proto;
            proto.MutableHeaders()->SetClientId("format");
            proto.SetDeviceUUID("uuid-1");
            return proto;
        }();

        std::optional<NProto::TFormatDeviceRequest> formatDeviceRequest;

        Backend->FormatDeviceImpl = [&](auto request)
        {
            formatDeviceRequest = std::move(request);

            NProto::TFormatDeviceResponse response;
            *response.MutableError() = MakeError(E_REJECTED, "format-error");
            return MakeFuture(std::move(response));
        };

        TTestClient client{Port};

        {
            NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            request.MutableFormatDevice()->CopyFrom(expectedRequest);
            client.Send(request);
        }

        auto response = client.Receive();

        UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
        UNIT_ASSERT(response.HasFormatDevice());

        const auto& error = response.GetFormatDevice().GetError();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("format-error", error.GetMessage());

        UNIT_ASSERT(formatDeviceRequest);
        UNIT_ASSERT_VALUES_EQUAL(
            expectedRequest.DebugString(),
            formatDeviceRequest->DebugString());
    }

    Y_UNIT_TEST_F(ShouldHandleBackendExecptions, TFixture)
    {
        const ui64 requestId = 42;

        TTestClient client{Port};

        auto acquire = [&]
        {
            NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            request.MutableAcquireDevices();
            client.Send(request);

            auto response = client.Receive();

            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            return response.GetAcquireDevices().GetError();
        };

        auto release = [&]
        {
            NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            request.MutableReleaseDevices();
            client.Send(request);

            auto response = client.Receive();

            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            return response.GetReleaseDevices().GetError();
        };

        auto readPages = [&]
        {
            NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            request.MutableReadPages();
            client.Send(request);

            auto response = client.Receive();

            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            return response.GetReadPages().GetError();
        };

        auto writeLogRecord = [&]
        {
            NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            request.MutableWriteLogRecord();
            client.Send(request);

            auto response = client.Receive();

            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            return response.GetWriteLogRecord().GetError();
        };

        Backend->AcquireDevicesImpl =
            [](auto) -> TFuture<NProto::TAcquireDevicesResponse>
        {
            throw TServiceError(E_FAIL) << "acquire-inline-error";
        };

        Backend->ReleaseDevicesImpl =
            [](auto) -> TFuture<NProto::TReleaseDevicesResponse>
        {
            throw TServiceError(E_FAIL) << "release-inline-error";
        };

        Backend->ReadPagesImpl = [](auto) -> TFuture<NProto::TReadPagesResponse>
        {
            throw TServiceError(E_FAIL) << "readPages-inline-error";
        };
        Backend->WriteLogRecordImpl =
            [](auto) -> TFuture<NProto::TWriteLogRecordResponse>
        {
            throw TServiceError(E_FAIL) << "writeLogRecord-inline-error";
        };

        {
            auto error = acquire();
            UNIT_ASSERT_VALUES_EQUAL(E_FAIL, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                "acquire-inline-error",
                error.GetMessage());
        }

        {
            auto error = release();
            UNIT_ASSERT_VALUES_EQUAL(E_FAIL, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                "release-inline-error",
                error.GetMessage());
        }

        {
            auto error = readPages();
            UNIT_ASSERT_VALUES_EQUAL(E_FAIL, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                "readPages-inline-error",
                error.GetMessage());
        }

        {
            auto error = writeLogRecord();
            UNIT_ASSERT_VALUES_EQUAL(E_FAIL, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                "writeLogRecord-inline-error",
                error.GetMessage());
        }

        Backend->AcquireDevicesImpl = [](auto)
        {
            return MakeErrorFuture<NProto::TAcquireDevicesResponse>(
                std::make_exception_ptr(
                    TServiceError{E_ARGUMENT} << "acquire-async-error"));
        };

        Backend->ReleaseDevicesImpl = [](auto)
        {
            return MakeErrorFuture<NProto::TReleaseDevicesResponse>(
                std::make_exception_ptr(
                    TServiceError{E_ARGUMENT} << "release-async-error"));
        };

        Backend->ReadPagesImpl = [](auto)
        {
            return MakeErrorFuture<NProto::TReadPagesResponse>(
                std::make_exception_ptr(
                    TServiceError{E_ARGUMENT} << "readPages-async-error"));
        };

        Backend->WriteLogRecordImpl = [](auto)
        {
            return MakeErrorFuture<NProto::TWriteLogRecordResponse>(
                std::make_exception_ptr(
                    TServiceError{E_ARGUMENT} << "writeLogRecord-async-error"));
        };

        {
            auto error = acquire();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("acquire-async-error", error.GetMessage());
        }

        {
            auto error = release();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("release-async-error", error.GetMessage());
        }

        {
            auto error = readPages();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                "readPages-async-error",
                error.GetMessage());
        }

        {
            auto error = writeLogRecord();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                "writeLogRecord-async-error",
                error.GetMessage());
        }
    }
}

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

// Serves a region of a device, so the devices created for the regions of the
// same device share its pages.
class TRegionDevice final: public IDevice
{
private:
    const IDevicePtr Device;
    const TPageRangeRef Region;

public:
    TRegionDevice(IDevicePtr device, TPageRangeRef region)
        : Device(std::move(device))
        , Region(region)
    {}

    [[nodiscard]] auto ReadPages(TVector<TPageRangeRef> rangeRefs)
        -> TFuture<TResultOrError<TVector<TBuffer>>> final
    {
        for (auto& ref: rangeRefs) {
            ref.FirstPageNo += Region.FirstPageNo;
        }
        return Device->ReadPages(std::move(rangeRefs));
    }

    [[nodiscard]] auto WritePages(TVector<TPageRange> ranges)
        -> TFuture<NProto::TError> final
    {
        for (auto& range: ranges) {
            range.FirstPageNo += Region.FirstPageNo;
        }
        return Device->WritePages(std::move(ranges));
    }

    [[nodiscard]] auto ZeroPages(TVector<TPageRangeRef> rangeRefs)
        -> TFuture<NProto::TError> final
    {
        for (auto& ref: rangeRefs) {
            ref.FirstPageNo += Region.FirstPageNo;
        }
        return Device->ZeroPages(std::move(rangeRefs));
    }
};

////////////////////////////////////////////////////////////////////////////////

// Keeps the devices in memory and serves only the clients that have acquired
// them.
struct TInMemoryDeviceManager final: public IDeviceManager
{
    std::mutex Lock;
    THashMap<TString, IDevicePtr> Devices;
    THashMap<TString, TString> Owners;   // device UUID -> client id

    [[nodiscard]] auto AcquireDevices(NProto::TAcquireDevicesRequest request)
        -> TFuture<NProto::TAcquireDevicesResponse> final
    {
        std::lock_guard lock(Lock);

        for (const auto& uuid: request.GetDeviceUUIDs()) {
            Owners[uuid] = request.GetHeaders().GetClientId();
        }

        return MakeFuture<NProto::TAcquireDevicesResponse>();
    }

    [[nodiscard]] auto ReleaseDevices(NProto::TReleaseDevicesRequest request)
        -> TFuture<NProto::TReleaseDevicesResponse> final
    {
        std::lock_guard lock(Lock);

        for (const auto& uuid: request.GetDeviceUUIDs()) {
            Owners.erase(uuid);
        }

        return MakeFuture<NProto::TReleaseDevicesResponse>();
    }

    [[nodiscard]] NProto::TError AccessDevice(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode /*accessMode*/) final
    {
        std::lock_guard lock(Lock);

        const auto* owner = Owners.FindPtr(deviceUUID);
        if (!owner || *owner != clientId) {
            return MakeError(E_BS_INVALID_SESSION, "not acquired");
        }

        return {};
    }

    [[nodiscard]] IDevicePtr CreateDevice(
        const TString& deviceUUID,
        TPageRangeRef region,
        ui32 blockSize) final
    {
        std::lock_guard lock(Lock);

        auto& device = Devices[deviceUUID];
        if (!device) {
            device = CreateInMemoryDevice(blockSize);
        }

        return std::make_shared<TRegionDevice>(device, region);
    }
};

////////////////////////////////////////////////////////////////////////////////

// The server with the real service on top of the in-memory devices.
struct TServiceFixture: public NUnitTest::TBaseFixture
{
    const TString ClientId = "client-id";
    const TString DeviceUUID = "uuid-1";

    TPortManager PortManager;
    ui16 Port = 0;
    ILoggingServicePtr Logging;
    TExecutorPtr Executor;
    IStartablePtr Server;

    std::optional<TTestClient> Client;
    ui64 RequestId = 0;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        Port = PortManager.GetTcpPort();
        Logging = CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_RESOURCES});
        Executor = TExecutor::Create("TestExecutor");

        // No journal: the whole device holds the data.
        Server = TServerBuilder(
                     Logging,
                     Executor,
                     std::make_shared<TInMemoryDeviceManager>(),
                     TNetworkAddress{Port},
                     false,   // journalEnabled
                     {{.DeviceUUID = DeviceUUID,
                       .BlocksCount = 1024,
                       .BlockSize = DefaultBlockSize}})
                     .Build();

        Logging->Start();
        Executor->Start();
        Server->Start();

        Client.emplace(Port);
    }

    void TearDown(NUnitTest::TTestContext& /*testContext*/) override
    {
        Client.reset();

        Server->Stop();
        Executor->Stop();
        Logging->Stop();
    }

    NProto::TDeviceProtocolResponse Execute(
        NProto::TDeviceProtocolRequest request)
    {
        request.SetRequestId(++RequestId);
        Client->Send(request);

        auto response = Client->Receive();
        UNIT_ASSERT_VALUES_EQUAL(RequestId, response.GetRequestId());
        return response;
    }

    NProto::TError AcquireDevice(const TString& clientId)
    {
        NProto::TDeviceProtocolRequest request;
        auto& proto = *request.MutableAcquireDevices();
        proto.MutableHeaders()->SetClientId(clientId);
        *proto.MutableDeviceUUIDs()->Add() = DeviceUUID;

        auto response = Execute(std::move(request));
        UNIT_ASSERT(response.HasAcquireDevices());
        return response.GetAcquireDevices().GetError();
    }

    NProto::TError WritePage(
        const TString& clientId,
        ui64 pageNo,
        const TString& content)
    {
        NProto::TDeviceProtocolRequest request;
        auto& proto = *request.MutableWriteLogRecord();
        proto.MutableHeaders()->SetClientId(clientId);
        proto.SetDeviceUUID(DeviceUUID);
        proto.SetLogSequenceNumber(1);

        auto& group = *proto.MutablePageGroups()->Add();
        group.SetFirstPageNo(pageNo);
        *group.MutableContent()->Add() = content;

        auto response = Execute(std::move(request));
        UNIT_ASSERT(response.HasWriteLogRecord());
        return response.GetWriteLogRecord().GetError();
    }

    NProto::TReadPagesResponse ReadPage(const TString& clientId, ui64 pageNo)
    {
        NProto::TDeviceProtocolRequest request;
        auto& proto = *request.MutableReadPages();
        proto.MutableHeaders()->SetClientId(clientId);
        proto.SetDeviceUUID(DeviceUUID);

        auto& group = *proto.MutablePageGroupRefs()->Add();
        group.SetFirstPageNo(pageNo);
        group.SetPageSize(DefaultBlockSize);
        group.SetPageCount(1);

        auto response = Execute(std::move(request));
        UNIT_ASSERT(response.HasReadPages());
        return response.GetReadPages();
    }

    TString ReadPageContent(ui64 pageNo)
    {
        const auto response = ReadPage(ClientId, pageNo);
        const auto& error = response.GetError();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(1, response.PageGroupsSize());
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetPageGroups(0).ContentSize());
        return response.GetPageGroups(0).GetContent(0);
    }

    NProto::TError FormatDevice(bool wholeDevice)
    {
        NProto::TDeviceProtocolRequest request;
        auto& proto = *request.MutableFormatDevice();
        proto.MutableHeaders()->SetClientId(ClientId);
        proto.SetDeviceUUID(DeviceUUID);
        proto.SetWholeDevice(wholeDevice);

        auto response = Execute(std::move(request));
        UNIT_ASSERT(response.HasFormatDevice());
        return response.GetFormatDevice().GetError();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceTCPServerServiceTest)
{
    Y_UNIT_TEST_F(ShouldServeAcquiredDevice, TServiceFixture)
    {
        const ui64 pageNo = 0x10;
        const TString content(DefaultBlockSize, 'A');

        {
            const auto error = AcquireDevice(ClientId);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
        }

        // the requests reach the device and the data written comes back

        {
            const auto error = WritePage(ClientId, pageNo, content);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL(content, ReadPageContent(pageNo));

        // a client that has not acquired the device is rejected

        const TString otherClientId = "other-client-id";

        for (const auto& error:
             {WritePage(otherClientId, pageNo, content),
              ReadPage(otherClientId, pageNo).GetError()})
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_INVALID_SESSION,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldFormatWholeDevice, TServiceFixture)
    {
        const ui64 pageNo = 0x10;
        const TString content(DefaultBlockSize, 'A');

        {
            const auto error = AcquireDevice(ClientId);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
        }

        {
            const auto error = WritePage(ClientId, pageNo, content);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL(content, ReadPageContent(pageNo));

        // The device has no journal, so the default format leaves the data
        // untouched

        {
            const auto error = FormatDevice(false);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL(content, ReadPageContent(pageNo));

        // Formatting the whole device wipes the data

        {
            const auto error = FormatDevice(true);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, '\0'),
            ReadPageContent(pageNo));
    }
}

}   // namespace NCloud::NJournalled
