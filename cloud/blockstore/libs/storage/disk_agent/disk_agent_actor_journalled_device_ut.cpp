#include "disk_agent.h"

#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/disk_agent/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/folder/tempdir.h>
#include <util/random/random.h>

#include <chrono>
#include <optional>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NServer;
using namespace NThreading;
using namespace NDiskAgentTest;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    const TTempDir TempDir;
    const TFsPath DevPath = TempDir.Path() / "dev";

    std::optional<TTestBasicRuntime> Runtime;
    ui16 Port = 0;

    std::array<NProto::TFileDeviceArgs, 4> FileDevices;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        InitFileDevices();

        Runtime.emplace();
        Port = Runtime->GetPortManager().GetTcpPort();
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        Runtime.reset();

        for (const auto& device: FileDevices) {
            TFsPath(device.GetPath()).DeleteIfExists();
        }
    }

    void PrepareFile(const NProto::TFileDeviceArgs& device)
    {
        TFile file(device.GetPath(), EOpenModeFlag::CreateNew);
        file.Resize(device.GetFileSize());
    }

    void InitFileDevices()
    {
        DevPath.MkDirs();

        for (size_t i = 0; i != FileDevices.size(); ++i) {
            const TString uuid = "uuid-" + ToString(i + 1);

            NProto::TFileDeviceArgs& device = FileDevices[i];
            device.SetPath(DevPath / (uuid + ".bin"));
            device.SetBlockSize(4_KB);
            device.SetDeviceId(uuid);
            device.SetPoolName("journalled");
            device.SetFileSize(1_MB);

            PrepareFile(device);
        }
    }

    auto CreateDiskAgentConfig() const -> NProto::TDiskAgentConfig
    {
        NProto::TDiskAgentConfig config = CreateDefaultAgentConfig();
        config.SetEnabled(true);
        config.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
        config.SetAcquireRequired(true);
        config.SetJournalledDeviceTcpServerListenAddress(
            "localhost:" + ToString(Port));

        config.MutableFileDevices()->Assign(
            FileDevices.begin(),
            FileDevices.end());

        return config;
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

Y_UNIT_TEST_SUITE(TDiskAgentJournalledDeviceTest)
{
    Y_UNIT_TEST_F(ShouldAcquireDevices, TFixture)
    {
        const TString clientId = "client-id";
        const ui64 requestId = 42;

        auto env =
            TTestEnvBuilder(*Runtime).With(CreateDiskAgentConfig()).Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        TTestClient client{Port};

        {
            NCloud::NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            auto& proto = *request.MutableAcquireDevices();
            proto.MutableHeaders()->SetClientId(clientId);
            proto.MutableDeviceUUIDs()->Add("unknown");
            client.Send(request);
        }

        Runtime->DispatchEvents(TDispatchOptions(), 10ms);

        {
            auto response = client.Receive();
            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kAcquireDevices,
                response.GetResponseCase());

            const auto& error = response.GetAcquireDevices().GetError();

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());
        }

        {
            NCloud::NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            auto& proto = *request.MutableAcquireDevices();
            proto.MutableHeaders()->SetClientId(clientId);
            *proto.MutableDeviceUUIDs()->Add() = FileDevices[0].GetDeviceId();
            *proto.MutableDeviceUUIDs()->Add() = FileDevices[1].GetDeviceId();
            client.Send(request);
        }

        Runtime->DispatchEvents(TDispatchOptions(), 10ms);

        {
            auto response = client.Receive();
            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kAcquireDevices,
                response.GetResponseCase());

            const auto& error = response.GetAcquireDevices().GetError();

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        }

        {
            NCloud::NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            auto& proto = *request.MutableReleaseDevices();
            proto.MutableHeaders()->SetClientId(clientId);
            *proto.MutableDeviceUUIDs()->Add() = FileDevices[0].GetDeviceId();
            *proto.MutableDeviceUUIDs()->Add() = FileDevices[1].GetDeviceId();
            client.Send(request);
        }

        Runtime->DispatchEvents(TDispatchOptions(), 10ms);

        {
            auto response = client.Receive();
            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kReleaseDevices,
                response.GetResponseCase());

            const auto& error = response.GetReleaseDevices().GetError();

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        }
    }

    Y_UNIT_TEST_F(ShouldRouteRequestsToDevices, TFixture)
    {
        const TString clientId = "client-id";
        const TString uuid = FileDevices[0].GetDeviceId();
        const TString unknownUuid = "unknown";

        auto env =
            TTestEnvBuilder(*Runtime).With(CreateDiskAgentConfig()).Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        TTestClient client{Port};

        // Acquire device

        {
            NCloud::NProto::TDeviceProtocolRequest request;
            auto& proto = *request.MutableAcquireDevices();
            proto.MutableHeaders()->SetClientId(clientId);
            *proto.MutableDeviceUUIDs()->Add() = uuid;
            client.Send(request);
        }

        Runtime->DispatchEvents(TDispatchOptions(), 10ms);

        {
            auto response = client.Receive();
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kAcquireDevices,
                response.GetResponseCase());

            const auto& error = response.GetAcquireDevices().GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        ui64 requestId = 0;

        const auto writeLogRecord = [&](const TString& deviceUUID)
        {
            NCloud::NProto::TDeviceProtocolRequest request;
            request.SetRequestId(++requestId);

            auto& proto = *request.MutableWriteLogRecord();
            proto.MutableHeaders()->SetClientId(clientId);
            proto.SetDeviceUUID(deviceUUID);
            proto.SetLogSequenceNumber(1);

            auto& group = *proto.MutablePageGroups()->Add();
            group.SetFirstPageNo(0x10);
            group.MutableContent()->Add()->resize(DefaultBlockSize, 'A');

            client.Send(request);

            Runtime->DispatchEvents(TDispatchOptions(), 10ms);

            auto response = client.Receive();
            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kWriteLogRecord,
                response.GetResponseCase());

            return response.GetWriteLogRecord().GetError();
        };

        const auto readPages = [&](const TString& deviceUUID)
        {
            NCloud::NProto::TDeviceProtocolRequest request;
            request.SetRequestId(++requestId);

            auto& proto = *request.MutableReadPages();
            proto.MutableHeaders()->SetClientId(clientId);
            proto.SetDeviceUUID(deviceUUID);

            auto& group = *proto.MutablePageGroupRefs()->Add();
            group.SetFirstPageNo(0x10);
            group.SetPageSize(DefaultBlockSize);
            group.SetPageCount(1);

            client.Send(request);

            Runtime->DispatchEvents(TDispatchOptions(), 10ms);

            auto response = client.Receive();
            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kReadPages,
                response.GetResponseCase());

            return response.GetReadPages().GetError();
        };

        // the requests reach the device hosted by this agent

        {
            const auto error = writeLogRecord(uuid);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            const auto error = readPages(uuid);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        // an unknown device is rejected before the request is validated

        for (const auto& error: {
                 writeLogRecord(unknownUuid),
                 readPages(unknownUuid)})
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_FOUND,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "Device " + unknownUuid.Quote() + " not found");
        }

        // a request without a device is rejected as well

        for (const auto& error: {writeLogRecord({}), readPages({})}) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "empty device UUID");
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
