#include "server.h"

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <mutex>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDevice
{
    TString Id;
    TString ClientId;

    explicit TDevice(TString id)
        : Id(std::move(id))
    {}
};

class TTestBackend: public IServerBackend
{
private:
    mutable std::mutex Mutex;
    TVector<TDevice> Devices;

public:
    explicit TTestBackend(TVector<TDevice> devices)
        : Devices(std::move(devices))
    {}

    auto GetDevices() const -> TVector<TDevice>
    {
        std::unique_lock lock{Mutex};

        return Devices;
    }

    auto GetDeviceById(const TString& id) const -> std::optional<TDevice>
    {
        std::unique_lock lock{Mutex};

        auto it = std::ranges::find(Devices, id, &TDevice::Id);
        if (it == Devices.end()) {
            return std::nullopt;
        }

        return *it;
    }

    [[nodiscard]] auto AcquireDevices(
        TInstant now,
        NProto::TAcquireDevicesRequest request)
        -> TFuture<NProto::TAcquireDevicesResponse> final
    {
        Y_UNUSED(now);

        std::unique_lock lock{Mutex};

        NProto::TAcquireDevicesResponse response;
        *response.MutableError() = AcquireDevicesImpl(
            TVector<TString>(
                request.GetDeviceUUIDs().begin(),
                request.GetDeviceUUIDs().end()),
            request.GetHeaders().GetClientId());

        return MakeFuture(response);
    }

    [[nodiscard]] auto ReleaseDevices(
        TInstant now,
        NProto::TReleaseDevicesRequest request)
        -> TFuture<NProto::TReleaseDevicesResponse> final
    {
        Y_UNUSED(now);

        std::unique_lock lock{Mutex};

        NProto::TReleaseDevicesResponse response;
        *response.MutableError() = ReleaseDevicesImpl(
            TVector<TString>(
                request.GetDeviceUUIDs().begin(),
                request.GetDeviceUUIDs().end()),
            request.GetHeaders().GetClientId());

        return MakeFuture(response);
    }

    [[nodiscard]] virtual auto ReadPages(
        TInstant now,
        NProto::TReadPagesRequest request)
        -> TFuture<NProto::TReadPagesResponse> final
    {
        Y_UNUSED(now);

        NProto::TReadPagesResponse response;
        auto& pageGroups = *response.MutablePageGroups();
        pageGroups.Reserve(request.PageGroupRefsSize());

        for (const NProto::TDevicePageGroupRef& ref: request.GetPageGroupRefs())
        {
            NProto::TDevicePageGroup& group = *pageGroups.Add();

            auto& content = *group.MutableContent();

            content.Reserve(ref.GetPageCount());
            for (ui64 i = 0; i != ref.GetPageCount(); ++i) {
                content.Add()->ReserveAndResize(ref.GetPageSize());
            }
        }

        return MakeFuture(std::move(response));
    }

    [[nodiscard]] virtual auto WriteLogRecord(
        TInstant now,
        NProto::TWriteLogRecordRequest request)
        -> TFuture<NProto::TWriteLogRecordResponse> final
    {
        Y_UNUSED(now, request);

        return MakeFuture(NProto::TWriteLogRecordResponse());
    }

private:
    auto AcquireDevicesImpl(
        const TVector<TString>& uuids,
        const TString& clientId) -> NProto::TError
    {
        if (!clientId) {
            return MakeError(E_ARGUMENT, "empty client id");
        }

        for (const auto& uuid: uuids) {
            auto it = std::ranges::find(Devices, uuid, &TDevice::Id);
            if (it == Devices.end()) {
                return MakeError(
                    E_NOT_FOUND,
                    TStringBuilder()
                        << "Device " << uuid.Quote() << " not found");
            }
            if (it->ClientId && it->ClientId != clientId) {
                return MakeError(E_BS_MOUNT_CONFLICT);
            }
        }

        for (const auto& uuid: uuids) {
            auto it = std::ranges::find(Devices, uuid, &TDevice::Id);
            it->ClientId = clientId;
        }

        return {};
    }

    auto ReleaseDevicesImpl(
        const TVector<TString>& uuids,
        const TString& clientId) -> NProto::TError
    {
        if (!clientId) {
            return MakeError(E_ARGUMENT, "empty client id");
        }

        for (const auto& uuid: uuids) {
            auto it = std::ranges::find(Devices, uuid, &TDevice::Id);
            if (it == Devices.end()) {
                return MakeError(
                    E_NOT_FOUND,
                    TStringBuilder()
                        << "Device " << uuid.Quote() << " not found");
            }
            if (it->ClientId == clientId) {
                it->ClientId.clear();
            }
        }

        return {};
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

        Backend = std::make_shared<TTestBackend>(TVector{
            TDevice("uuid-1"),
            TDevice("uuid-2"),
            TDevice("uuid-3"),
        });

        Executor = TExecutor::Create("TestExecutor");

        Logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});

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
    Y_UNIT_TEST_F(ShouldAcquireDevices, TFixture)
    {
        const ui64 requestId = 42;
        const TString clientId = "client-id";
        const TString uuid = "uuid-2";

        TTestClient client{Port};

        UNIT_ASSERT_VALUES_EQUAL("", Backend->GetDeviceById(uuid)->ClientId);

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

        {
            NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            auto& acquireDevices = *request.MutableAcquireDevices();
            acquireDevices.MutableHeaders()->SetClientId(clientId);
            acquireDevices.AddDeviceUUIDs(uuid);

            client.Send(std::move(request));
        }

        {
            NProto::TDeviceProtocolResponse response = client.Receive();

            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kAcquireDevices,
                response.GetResponseCase());

            auto& acq = response.GetAcquireDevices();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, acq.GetError().GetCode());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            clientId,
            Backend->GetDeviceById(uuid)->ClientId);
    }
}

}   // namespace NCloud::NJournalled
