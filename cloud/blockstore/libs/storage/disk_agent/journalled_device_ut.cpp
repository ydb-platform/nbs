#include "disk_agent.h"
#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/disk_agent/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/folder/tempdir.h>

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

    Y_UNIT_TEST_F(ShouldWriteLogRecord, TFixture)
    {
        const TString clientId = "client-id";
        const ui64 requestId = 42;
        const TString uuid = FileDevices[0].GetDeviceId();

        auto env =
            TTestEnvBuilder(*Runtime).With(CreateDiskAgentConfig()).Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        TTestClient client{Port};

        const auto writeLogRecordRequest = [&]
        {
            NCloud::NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            auto& proto = *request.MutableWriteLogRecord();
            proto.MutableHeaders()->SetClientId(clientId);
            proto.SetDeviceUUID(uuid);
            proto.SetLogSequenceNumber(1);
            // proto.SetPageSize(4_KB); // ???
            auto& groups = *proto.MutablePageGroups();

            {
                auto& group = *groups.Add();
                group.SetFirstPageNo(0x10);
                group.MutableContent()->Add()->resize(8_KB, 'A');
                group.MutableContent()->Add()->resize(8_KB, 'B');
            }

            {
                auto& group = *groups.Add();
                group.SetFirstPageNo(0x20);
                group.MutableContent()->Add()->resize(16_KB, 'X');
                group.MutableContent()->Add()->resize(16_KB, 'Y');
            }
            return request;
        }();

        client.Send(writeLogRecordRequest);
        Runtime->DispatchEvents(TDispatchOptions(), 10ms);

        {
            auto response = client.Receive();
            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kWriteLogRecord,
                response.GetResponseCase());

            const auto& error = response.GetWriteLogRecord().GetError();

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_INVALID_SESSION,
                error.GetCode(),
                FormatError(error));
        }

        {
            NCloud::NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);
            auto& proto = *request.MutableAcquireDevices();
            proto.MutableHeaders()->SetClientId(clientId);
            *proto.MutableDeviceUUIDs()->Add() = uuid;
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

        client.Send(writeLogRecordRequest);
        Runtime->DispatchEvents(TDispatchOptions(), 10ms);

        {
            auto response = client.Receive();
            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kWriteLogRecord,
                response.GetResponseCase());

            const auto& error = response.GetWriteLogRecord().GetError();

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldReadPages, TFixture)
    {
        const TString clientId = "client-id";
        // const ui64 requestId = 42;

        auto env =
            TTestEnvBuilder(*Runtime).With(CreateDiskAgentConfig()).Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        TTestClient client{Port};

        {
            //
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
