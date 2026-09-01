#include "journalled_device.h"

#include <cloud/blockstore/libs/rdma_test/memory_test_storage.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>

#include <chrono>
#include <functional>
#include <ranges>

namespace NCloud::NBlockStore::NStorage {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4_KB;
constexpr ui64 DefaultBlockCount = 1_MB / DefaultBlockSize;

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    const TString ClientId = "client-id";
    const TString DeviceUUID = "uuid-1";
    const TInstant Now = TInstant::Seconds(1);

    ILoggingServicePtr Logging = CreateLoggingService("console");

    std::shared_ptr<TMemoryTestStorage> Storage;
    TStorageAdapterPtr StorageAdapter;
    TDeviceClientPtr DeviceClient;
    IJournalledDevicePtr Device;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Storage = std::make_shared<TMemoryTestStorage>(
            DefaultBlockCount * DefaultBlockSize);

        StorageAdapter = std::make_shared<TStorageAdapter>(
            Storage,
            DefaultBlockSize,
            false,                    // normalize
            TDuration::Seconds(1),    // maxRequestDuration
            TDuration::Seconds(1));   // shutdownTimeout

        DeviceClient = std::make_shared<TDeviceClient>(
            10s,   // releaseInactiveSessionsTimeout
            TVector<std::pair<TString, TStorageAdapterPtr>>{
                {DeviceUUID, StorageAdapter}},
            Logging->CreateLog("BLOCKSTORE_DISK_AGENT"),
            false   // kickOutOldClientsEnabled
        );

        // the device is not acquired here: some of the tests observe the
        // behaviour of an unacquired device
        Device = CreateJournalledDevice(DeviceUUID, DeviceClient);
    }

    void AcquireDevice()
    {
        const auto result = DeviceClient->AcquireDevices(
            {DeviceUUID},
            ClientId,
            Now,
            NProto::VOLUME_ACCESS_READ_WRITE,
            0,    // mountSeqNumber
            {},   // diskId
            0     // volumeGeneration
        );

        UNIT_ASSERT_C(
            !HasError(result.GetError()),
            FormatError(result.GetError()));
    }

    static char BlockData(ui64 blockIndex)
    {
        return static_cast<char>('A' + blockIndex % 26);
    }

    void FillDevice()
    {
        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        request->SetStartIndex(0);
        request->SetBlockSize(DefaultBlockSize);

        auto& buffers = *request->MutableBlocks()->MutableBuffers();
        for (ui64 i = 0; i != DefaultBlockCount; ++i) {
            buffers.Add()->resize(DefaultBlockSize, BlockData(i));
        }

        const auto response = StorageAdapter->WriteBlocks(
            Now,
            CreateCallContext(),
            std::move(request),
            DefaultBlockSize,
            TStringBuf()   // dataBuffer
        ).GetValueSync();

        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));
    }

    NProto::TError WriteLogRecord(
        NCloud::NProto::TWriteLogRecordRequest request)
    {
        return Device->WriteLogRecord(Now, std::move(request))
            .GetValueSync()
            .GetError();
    }

    NProto::TError WriteLogRecord(ui64 lsn, ui64 prevLsn)
    {
        NCloud::NProto::TWriteLogRecordRequest request;
        request.MutableHeaders()->SetClientId(ClientId);
        request.SetDeviceUUID(DeviceUUID);
        request.SetLogSequenceNumber(lsn);
        request.SetPrevLogSequenceNumber(prevLsn);

        auto& group = *request.MutablePageGroups()->Add();
        group.SetFirstPageNo(0x10);
        group.MutableContent()->Add()->resize(DefaultBlockSize, 'A');

        return WriteLogRecord(std::move(request));
    }

    auto ReadPages(NCloud::NProto::TReadPagesRequest request)
    {
        return Device->ReadPages(Now, std::move(request)).GetValueSync();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalledDeviceTest)
{
    Y_UNIT_TEST_F(ShouldValidateWriteLogRecordRequest, TFixture)
    {
        using TPrepareFunc =
            std::function<void(NCloud::NProto::TWriteLogRecordRequest&)>;

        const std::tuple<TPrepareFunc, NProto::TError> testCases[]{
            {[&](auto&) {}, MakeError(E_ARGUMENT, "empty device UUID")},
            {[&](auto& proto) { proto.SetDeviceUUID(DeviceUUID); },
             MakeError(E_ARGUMENT, "nothing to write")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(DeviceUUID);
                 proto.SetLogSequenceNumber(1);
                 proto.MutablePageGroups()->Add();
             },
             MakeError(E_ARGUMENT, "empty page group")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(DeviceUUID);
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
                 proto.SetDeviceUUID(DeviceUUID);
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
                 proto.SetDeviceUUID(DeviceUUID);
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
                 proto.SetDeviceUUID(DeviceUUID);
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
                 proto.MutableHeaders()->SetClientId(ClientId);
                 proto.SetDeviceUUID(DeviceUUID);
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
                 proto.MutableHeaders()->SetClientId(ClientId);
                 proto.SetDeviceUUID(DeviceUUID);
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
                 proto.MutableHeaders()->SetClientId(ClientId);
                 proto.SetDeviceUUID(DeviceUUID);
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

        for (size_t i = 0; i != std::size(testCases); ++i) {
            const auto& [prepare, expectedError] = testCases[i];

            NCloud::NProto::TWriteLogRecordRequest request;
            prepare(request);

            const auto error = WriteLogRecord(std::move(request));

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
        using TPrepareFunc =
            std::function<void(NCloud::NProto::TReadPagesRequest&)>;

        const std::tuple<TPrepareFunc, NProto::TError> testCases[]{
            {[&](auto&) {}, MakeError(E_ARGUMENT, "empty device UUID")},
            {[&](auto& proto) { proto.SetDeviceUUID(DeviceUUID); },
             MakeError(E_ARGUMENT, "nothing to read")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(DeviceUUID);
                 proto.MutablePageGroupRefs()->Add();
             },
             MakeError(
                 E_ARGUMENT,
                 "page group ref must contain at least one page")},
            {[&](auto& proto)
             {
                 proto.SetDeviceUUID(DeviceUUID);
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
                 proto.SetDeviceUUID(DeviceUUID);
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
                 proto.SetDeviceUUID(DeviceUUID);
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
                 proto.MutableHeaders()->SetClientId(ClientId);
                 proto.SetDeviceUUID(DeviceUUID);

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

        for (size_t i = 0; i != std::size(testCases); ++i) {
            const auto& [prepare, expectedError] = testCases[i];

            NCloud::NProto::TReadPagesRequest request;
            prepare(request);

            const auto error = ReadPages(std::move(request)).GetError();

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
        const auto makeRequest = [&]
        {
            NCloud::NProto::TWriteLogRecordRequest request;
            request.MutableHeaders()->SetClientId(ClientId);
            request.SetDeviceUUID(DeviceUUID);
            request.SetLogSequenceNumber(1);

            auto& groups = *request.MutablePageGroups();

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
        };

        // the device has not been acquired yet

        {
            const auto error = WriteLogRecord(makeRequest());
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_INVALID_SESSION,
                error.GetCode(),
                FormatError(error));
        }

        AcquireDevice();

        {
            const auto error = WriteLogRecord(makeRequest());
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldValidateLogSequenceNumber, TFixture)
    {
        AcquireDevice();

        // the very first record is accepted with any prev lsn

        {
            const auto error = WriteLogRecord(10, 5);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            const auto error = WriteLogRecord(11, 10);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        // a gap in the log

        {
            const auto error = WriteLogRecord(20, 15);
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
            const auto error = WriteLogRecord(13, 5);
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
            const auto error = WriteLogRecord(12, 11);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldReadPages, TFixture)
    {
        constexpr ui32 requestCount = 100;

        FillDevice();
        AcquireDevice();

        for (ui32 i = 0; i != requestCount; ++i) {
            NCloud::NProto::TReadPagesRequest request;
            request.MutableHeaders()->SetClientId(ClientId);
            request.SetDeviceUUID(DeviceUUID);

            const ui64 groupCount = 1 + RandomNumber<ui64>(8);

            auto& groupRefs = *request.MutablePageGroupRefs();
            for (ui64 j = 0; j != groupCount; ++j) {
                auto& group = *groupRefs.Add();

                group.SetFirstPageNo(RandomNumber<ui64>(DefaultBlockCount));
                group.SetPageSize(DefaultBlockSize);
                group.SetPageCount(
                    1 + RandomNumber<ui64>(
                            DefaultBlockCount - group.GetFirstPageNo()));
            }

            const auto response = ReadPages(request);

            const auto& error = response.GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));

            UNIT_ASSERT_VALUES_EQUAL(groupCount, response.PageGroupsSize());

            const auto& groups = response.GetPageGroups();

            for (size_t j = 0; j != groupCount; ++j) {
                const auto& groupRef = groupRefs[j];
                const auto& group = groups[j];

                UNIT_ASSERT_VALUES_EQUAL(
                    groupRef.GetFirstPageNo(),
                    group.GetFirstPageNo());

                UNIT_ASSERT_VALUES_EQUAL(
                    groupRef.GetPageCount(),
                    group.ContentSize());

                for (ui64 k = 0; k != group.ContentSize(); ++k) {
                    const ui64 blockIndex = group.GetFirstPageNo() + k;

                    TStringBuf block = group.GetContent(k);

                    UNIT_ASSERT_VALUES_EQUAL(
                        groupRef.GetPageSize(),
                        block.size());

                    UNIT_ASSERT_VALUES_EQUAL(
                        block.size(),
                        std::ranges::count(block, BlockData(blockIndex)));
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
