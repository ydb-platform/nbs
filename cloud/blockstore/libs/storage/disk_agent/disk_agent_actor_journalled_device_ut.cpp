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

    Y_UNIT_TEST_F(ShouldValidateWriteLogRecordRequest, TFixture)
    {
        const TString clientId = "client-id";
        const TString uuid = "unknown";

        auto env =
            TTestEnvBuilder(*Runtime).With(CreateDiskAgentConfig()).Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        TTestClient client{Port};

        using TPrepareFunc =
            std::function<void(NCloud::NProto::TWriteLogRecordRequest&)>;

        const std::tuple<TPrepareFunc, NProto::TError> testCases[]{
            {[&](auto&) {}, MakeError(E_ARGUMENT, "empty device UUID")},
            {[&](auto& proto) { proto.SetDeviceUUID(uuid); },
             MakeError(E_ARGUMENT, "nothing to write")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(uuid);
                 proto.SetLogSequenceNumber(1);
                 proto.MutablePageGroups()->Add();
             },
             MakeError(E_ARGUMENT, "empty page group")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(uuid);
                 proto.SetLogSequenceNumber(1);
                 auto& groups = *proto.MutablePageGroups();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.MutableContent()->Add();   // an empty block
                 }
             },
             MakeError(
                 E_ARGUMENT,
                 "invalid page data: block must not be empty")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(uuid);
                 proto.SetLogSequenceNumber(1);
                 auto& groups = *proto.MutablePageGroups();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.MutableContent()->Add()->resize(4_KB, 'A');
                 }

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x20);
                     group.MutableContent()->Add()->resize(4_KB, 'A');
                     group.MutableContent()->Add();   // an empty block
                     group.MutableContent()->Add()->resize(4_KB, 'A');
                 }
             },
             MakeError(
                 E_ARGUMENT,
                 "invalid page data: block must not be empty")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(uuid);
                 proto.SetLogSequenceNumber(1);
                 auto& groups = *proto.MutablePageGroups();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.MutableContent()->Add()->resize(4_KB, 'A');
                     group.MutableContent()->Add()->resize(8_KB, 'B');
                 }
             },
             MakeError(E_ARGUMENT, "invalid page data: block size mismatch")},
            {[&](auto& proto)
             {
                 // the client id is checked after the device is found
                 proto.SetDeviceUUID(FileDevices[0].GetDeviceId());
                 proto.SetLogSequenceNumber(1);
                 auto& groups = *proto.MutablePageGroups();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.MutableContent()->Add()->resize(
                         DefaultBlockSize,
                         'A');
                     group.MutableContent()->Add()->resize(
                         DefaultBlockSize,
                         'B');
                 }
             },
             MakeError(E_ARGUMENT, "empty client id")},
            {[&](auto& proto)
             {
                 proto.MutableHeaders()->SetClientId(clientId);
                 proto.SetDeviceUUID(uuid);
                 proto.SetLogSequenceNumber(1);
                 auto& groups = *proto.MutablePageGroups();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.MutableContent()->Add()->resize(
                         DefaultBlockSize,
                         'A');
                     group.MutableContent()->Add()->resize(
                         DefaultBlockSize,
                         'B');
                 }
             },
             MakeError(E_NOT_FOUND, "Device " + uuid.Quote() + " not found")},
            {[&](auto& proto)
             {
                 proto.MutableHeaders()->SetClientId(clientId);
                 proto.SetDeviceUUID(FileDevices[0].GetDeviceId());
                 proto.SetLogSequenceNumber(1);

                 auto& groups = *proto.MutablePageGroups();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.MutableContent()->Add()->resize(
                         DefaultBlockSize,
                         'A');
                     group.MutableContent()->Add()->resize(
                         DefaultBlockSize,
                         'B');
                 }
             },
             MakeError(E_BS_INVALID_SESSION, "not acquired by client")},
            {[&](auto& proto)
             {
                 proto.MutableHeaders()->SetClientId(clientId);
                 proto.SetDeviceUUID(uuid);
                 // LogSequenceNumber is not set

                 auto& groups = *proto.MutablePageGroups();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.MutableContent()->Add()->resize(
                         DefaultBlockSize,
                         'A');
                 }
             },
             MakeError(E_ARGUMENT, "invalid lsn: 0")},
            {[&](auto& proto)
             {
                 proto.MutableHeaders()->SetClientId(clientId);
                 proto.SetDeviceUUID(uuid);
                 proto.SetLogSequenceNumber(10);
                 proto.SetPrevLogSequenceNumber(10);

                 auto& groups = *proto.MutablePageGroups();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.MutableContent()->Add()->resize(
                         DefaultBlockSize,
                         'A');
                 }
             },
             MakeError(
                 E_ARGUMENT,
                 "invalid lsn: 10, must be greater than the prev one: 10")},
        };

        for (ui64 i = 0; i != std::size(testCases); ++i) {
            const auto& [prepare, _] = testCases[i];

            NCloud::NProto::TDeviceProtocolRequest request;
            request.SetRequestId(i + 1);
            auto& proto = *request.MutableWriteLogRecord();
            prepare(proto);
            client.Send(request);
        }

        Runtime->DispatchEvents(TDispatchOptions(), 10ms);

        TVector<std::pair<ui64, NProto::TError>> errors(std::size(testCases));
        std::generate_n(
            errors.begin(),
            std::size(testCases),
            [&]
            {
                auto response = client.Receive();
                UNIT_ASSERT_EQUAL(
                    NProto::TDeviceProtocolResponse::ResponseCase::
                        kWriteLogRecord,
                    response.GetResponseCase());

                return std::make_pair(
                    response.GetRequestId(),
                    response.GetWriteLogRecord().GetError());
            });

        SortBy(errors, [](const auto& p) { return p.first; });

        for (size_t i = 0; i != errors.size(); ++i) {
            const auto& [requestId, error] = errors[i];
            UNIT_ASSERT_VALUES_EQUAL(i + 1, requestId);

            const auto& [_, expectedError] = testCases[i];
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedError.GetCode(),
                error.GetCode(),
                "#" << (i + 1) << ": " << FormatError(expectedError) << " !~ "
                    << FormatError(error));

            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                expectedError.GetMessage(),
                "#" << (i + 1) << ": " << FormatError(expectedError) << " !~ "
                    << FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldValidateReadPagesRequest, TFixture)
    {
        const TString clientId = "client-id";
        const TString uuid = "unknown";

        auto env =
            TTestEnvBuilder(*Runtime).With(CreateDiskAgentConfig()).Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        TTestClient client{Port};

        using TPrepareFunc =
            std::function<void(NCloud::NProto::TReadPagesRequest&)>;

        const std::tuple<TPrepareFunc, NProto::TError> testCases[]{
            {[&](auto&) {}, MakeError(E_ARGUMENT, "empty device UUID")},
            {[&](auto& proto) { proto.SetDeviceUUID(uuid); },
             MakeError(E_ARGUMENT, "nothing to read")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(uuid);
                 proto.MutablePageGroupRefs()->Add();
             },
             MakeError(
                 E_ARGUMENT,
                 "page group ref must contain at least one page")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(uuid);
                 auto& groups = *proto.MutablePageGroupRefs();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.SetPageSize(DefaultBlockSize);
                     group.SetPageCount(1);
                 }

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x20);
                     group.SetPageSize(DefaultBlockSize);
                     group.SetPageCount(0);
                 }
             },
             MakeError(
                 E_ARGUMENT,
                 "page group ref must contain at least one page")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(uuid);
                 auto& groups = *proto.MutablePageGroupRefs();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.SetPageSize(DefaultBlockSize);
                     group.SetPageCount(1);
                 }

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x20);
                     group.SetPageSize(0);
                     group.SetPageCount(1);
                 }
             },
             MakeError(E_ARGUMENT, "page size must be greater than zero")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(uuid);
                 auto& groups = *proto.MutablePageGroupRefs();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.SetPageSize(DefaultBlockSize);
                     group.SetPageCount(1);
                 }

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x20);
                     group.SetPageSize(DefaultBlockSize);
                     group.SetPageCount(1);
                 }
             },
             MakeError(E_ARGUMENT, "empty client id")},
            {[&](auto& proto)
             {
                 proto.MutableHeaders()->SetClientId(clientId);
                 proto.SetDeviceUUID(uuid);

                 auto& groups = *proto.MutablePageGroupRefs();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.SetPageSize(DefaultBlockSize);
                     group.SetPageCount(1);
                 }

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x20);
                     group.SetPageSize(DefaultBlockSize);
                     group.SetPageCount(1);
                 }
             },
             MakeError(E_NOT_FOUND, "Device " + uuid.Quote() + " not found")},
            {[&](auto& proto)
             {
                 proto.MutableHeaders()->SetClientId(clientId);
                 proto.SetDeviceUUID(FileDevices[0].GetDeviceId());

                 auto& groups = *proto.MutablePageGroupRefs();

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x10);
                     group.SetPageSize(DefaultBlockSize);
                     group.SetPageCount(1);
                 }

                 {
                     auto& group = *groups.Add();
                     group.SetFirstPageNo(0x20);
                     group.SetPageSize(DefaultBlockSize);
                     group.SetPageCount(1);
                 }
             },
             MakeError(E_BS_INVALID_SESSION, "not acquired by client")},
        };

        for (ui64 i = 0; i != std::size(testCases); ++i) {
            const auto& [prepare, _] = testCases[i];

            NCloud::NProto::TDeviceProtocolRequest request;
            request.SetRequestId(i + 1);
            auto& proto = *request.MutableReadPages();
            prepare(proto);
            client.Send(request);
        }

        Runtime->DispatchEvents(TDispatchOptions(), 10ms);

        TVector<std::pair<ui64, NProto::TError>> errors(std::size(testCases));
        std::generate_n(
            errors.begin(),
            std::size(testCases),
            [&]
            {
                auto response = client.Receive();
                UNIT_ASSERT_EQUAL(
                    NProto::TDeviceProtocolResponse::ResponseCase::kReadPages,
                    response.GetResponseCase());

                return std::make_pair(
                    response.GetRequestId(),
                    response.GetReadPages().GetError());
            });

        SortBy(errors, [](const auto& p) { return p.first; });

        for (size_t i = 0; i != errors.size(); ++i) {
            const auto& [requestId, error] = errors[i];
            UNIT_ASSERT_VALUES_EQUAL(i + 1, requestId);

            const auto& [_, expectedError] = testCases[i];
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedError.GetCode(),
                error.GetCode(),
                "#" << (i + 1) << ": " << FormatError(expectedError) << " !~ "
                    << FormatError(error));

            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                expectedError.GetMessage(),
                "#" << (i + 1) << ": " << FormatError(expectedError) << " !~ "
                    << FormatError(error));
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
            auto& groups = *proto.MutablePageGroups();

            {
                auto& group = *groups.Add();
                group.SetFirstPageNo(0x10);
                group.MutableContent()->Add()->resize(DefaultBlockSize, 'A');
                group.MutableContent()->Add()->resize(DefaultBlockSize, 'B');
            }

            {
                auto& group = *groups.Add();
                group.SetFirstPageNo(0x20);
                group.MutableContent()->Add()->resize(DefaultBlockSize, 'X');
                group.MutableContent()->Add()->resize(DefaultBlockSize, 'Y');
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

    Y_UNIT_TEST_F(ShouldValidateLogSequenceNumber, TFixture)
    {
        const TString clientId = "client-id";
        const TString uuid = FileDevices[0].GetDeviceId();

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

        const auto writeLogRecord = [&](ui64 lsn, ui64 prevLsn)
        {
            NCloud::NProto::TDeviceProtocolRequest request;
            request.SetRequestId(++requestId);

            auto& proto = *request.MutableWriteLogRecord();
            proto.MutableHeaders()->SetClientId(clientId);
            proto.SetDeviceUUID(uuid);
            proto.SetLogSequenceNumber(lsn);
            proto.SetPrevLogSequenceNumber(prevLsn);

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

        // the very first record is accepted with any prev lsn

        {
            const auto error = writeLogRecord(10, 5);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            const auto error = writeLogRecord(11, 10);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        // a gap in the log

        {
            const auto error = writeLogRecord(20, 15);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "Wrong lsn: 15, expected 11");
        }

        // an outdated record

        {
            const auto error = writeLogRecord(13, 5);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "Wrong lsn: 5, expected 11");
        }

        // the rejected records have not changed the state

        {
            const auto error = writeLogRecord(12, 11);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldReadPages, TFixture)
    {
        const TString clientId = "client-id";
        const auto& device = FileDevices[0];
        const ui64 blocksCount = device.GetFileSize() / device.GetBlockSize();
        const ui32 requestsCount = 100;

        auto env =
            TTestEnvBuilder(*Runtime).With(CreateDiskAgentConfig()).Build();

        TDiskAgentClient diskAgent(*Runtime);
        diskAgent.WaitReady();

        TTestClient client{Port};

        auto blockData = [](ui64 blockIndex)
        {
            return 'A' + blockIndex % 26;
        };

        // Prepare data
        {
            auto block = std::make_unique<char[]>(device.GetBlockSize());

            TFile file(device.GetPath(), EOpenModeFlag::OpenExisting);

            for (ui64 i = 0; i != blocksCount; ++i) {
                std::memset(block.get(), blockData(i), device.GetBlockSize());
                file.Write(block.get(), device.GetBlockSize());
            }
        }

        // Acquire device

        {
            NCloud::NProto::TDeviceProtocolRequest request;
            auto& proto = *request.MutableAcquireDevices();
            proto.MutableHeaders()->SetClientId(clientId);
            *proto.MutableDeviceUUIDs()->Add() = device.GetDeviceId();
            client.Send(request);
        }

        Runtime->DispatchEvents(TDispatchOptions(), 10ms);

        {
            auto response = client.Receive();
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kAcquireDevices,
                response.GetResponseCase());
            const auto& error = response.GetAcquireDevices().GetError();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        }

        for (ui32 i = 0; i != requestsCount; ++i) {
            // Send request

            const ui64 requestId = i + 1;

            NCloud::NProto::TDeviceProtocolRequest request;
            request.SetRequestId(requestId);

            auto& readPagesRequest = *request.MutableReadPages();

            readPagesRequest.MutableHeaders()->SetClientId(clientId);
            readPagesRequest.SetDeviceUUID(device.GetDeviceId());

            const ui64 groupsCount = 1 + RandomNumber<ui64>(8);

            auto& groupRefs = *readPagesRequest.MutablePageGroupRefs();
            for (ui64 j = 0; j != groupsCount; ++j) {
                auto& group = *groupRefs.Add();

                group.SetFirstPageNo(RandomNumber<ui64>(blocksCount));
                group.SetPageSize(device.GetBlockSize());
                group.SetPageCount(
                    1 +
                    RandomNumber<ui64>(blocksCount - group.GetFirstPageNo()));
            }

            client.Send(request);

            Runtime->DispatchEvents(TDispatchOptions(), 10ms);

            // Check response

            auto response = client.Receive();
            UNIT_ASSERT_VALUES_EQUAL(requestId, response.GetRequestId());
            UNIT_ASSERT_EQUAL(
                NProto::TDeviceProtocolResponse::ResponseCase::kReadPages,
                response.GetResponseCase());

            const auto& readPagesResponse = response.GetReadPages();
            const auto& error = readPagesResponse.GetError();
            const auto& groups = readPagesResponse.GetPageGroups();

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));

            UNIT_ASSERT_VALUES_EQUAL(
                groupsCount,
                readPagesResponse.PageGroupsSize());

            for (size_t i = 0; i != groupsCount; ++i) {
                const auto& groupRef = groupRefs[i];
                const auto& group = groups[i];

                UNIT_ASSERT_VALUES_EQUAL(
                    groupRef.GetFirstPageNo(),
                    group.GetFirstPageNo());

                UNIT_ASSERT_VALUES_EQUAL(
                    groupRef.GetPageCount(),
                    group.ContentSize());

                for (ui64 j = 0; j != group.ContentSize(); ++j) {
                    const ui64 blockIndex = group.GetFirstPageNo() + j;

                    TStringBuf block = group.GetContent(j);

                    UNIT_ASSERT_VALUES_EQUAL(
                        groupRef.GetPageSize(),
                        block.size());

                    UNIT_ASSERT_VALUES_EQUAL(
                        block.size(),
                        std::ranges::count(block, blockData(blockIndex)));
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
