#include "journalled_device_v2.h"

#include "device.h"
#include "journal.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/system/event.h>
#include <util/system/mutex.h>

#include <functional>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultPageSize = 4096;
constexpr TStringBuf DefaultDeviceUUID = "uuid";
constexpr TStringBuf DefaultClientId = "test-client";
constexpr TStringBuf BackgroundClientId = "background-ops";

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TDevicePageGroup MakeGroup(
    ui64 firstPageNo,
    ui64 pageCount,
    TStringBuf prefix)
{
    NCloud::NProto::TDevicePageGroup group;
    group.SetFirstPageNo(firstPageNo);

    for (ui64 i = 0; i < pageCount; ++i) {
        *group.AddContent() = TStringBuilder() << prefix << firstPageNo + i;
    }

    return group;
}

NCloud::NProto::TReadPagesRequest MakeReadRequest(
    const TVector<std::pair<ui64 /*firstPageNo*/, ui64 /*pageCount*/>>& refs,
    TStringBuf deviceUUID = DefaultDeviceUUID)
{
    NCloud::NProto::TReadPagesRequest request;
    request.MutableHeaders()->SetClientId(TString{DefaultClientId});
    request.SetDeviceUUID(TString{deviceUUID});

    for (const auto& [firstPageNo, pageCount]: refs) {
        auto& ref = *request.AddPageGroupRefs();
        ref.SetFirstPageNo(firstPageNo);
        ref.SetPageCount(pageCount);
        ref.SetPageSize(DefaultPageSize);
    }

    return request;
}

NCloud::NProto::TWriteLogRecordRequest MakeWriteRequest(TStringBuf deviceUUID)
{
    NCloud::NProto::TWriteLogRecordRequest request;
    request.SetDeviceUUID(TString{deviceUUID});
    request.SetLogSequenceNumber(1);
    *request.AddPageGroups() = MakeGroup(10, 1, "W");

    return request;
}

NCloud::NProto::TReadJournalTailRequest MakeTailRequest(TStringBuf deviceUUID)
{
    NCloud::NProto::TReadJournalTailRequest request;
    request.SetDeviceUUID(TString{deviceUUID});

    return request;
}

NCloud::NProto::TAdvanceLsnLowWatermarkRequest MakeAdvanceRequest(
    TStringBuf deviceUUID)
{
    NCloud::NProto::TAdvanceLsnLowWatermarkRequest request;
    request.SetDeviceUUID(TString{deviceUUID});
    request.SetLsnLowWatermark(1);

    return request;
}

NCloud::NProto::TReadPagesResponse MakeReadResponse(
    TVector<NCloud::NProto::TDevicePageGroup> groups,
    ui64 lastAckedLsn = 0)
{
    NCloud::NProto::TReadPagesResponse response;
    response.SetLastAckedLogSequenceNumber(lastAckedLsn);

    for (auto& group: groups) {
        response.AddPageGroups()->Swap(&group);
    }

    return response;
}

NCloud::NProto::TJournalRecord MakeRecord(
    ui64 lsn,
    ui64 firstPageNo,
    ui64 pageCount)
{
    NCloud::NProto::TJournalRecord record;
    record.SetLogSequenceNumber(lsn);
    record.SetPrevLogSequenceNumber(lsn - 1);
    *record.AddPageGroups() = MakeGroup(firstPageNo, pageCount, "J");

    return record;
}

// Serves every requested page group ref with pages named after the device.
NCloud::NProto::TReadPagesResponse ServePagesFromDevice(
    const NCloud::NProto::TReadPagesRequest& request)
{
    NCloud::NProto::TReadPagesResponse response;

    for (const auto& ref: request.GetPageGroupRefs()) {
        *response.AddPageGroups() =
            MakeGroup(ref.GetFirstPageNo(), ref.GetPageCount(), "D");
    }

    return response;
}

// "10+2@4096, 15+5@4096"
TString DescribeRefs(const NCloud::NProto::TReadPagesRequest& request)
{
    TVector<TString> refs;

    for (const auto& ref: request.GetPageGroupRefs()) {
        refs.push_back(TStringBuilder()
            << ref.GetFirstPageNo() << "+" << ref.GetPageCount()
            << "@" << ref.GetPageSize());
    }

    return JoinSeq(", ", refs);
}

// "10:[D10,D11,J12] 20:[J20]"
template <typename T>
TString DescribeGroups(const T& source)
{
    TVector<TString> groups;

    for (const auto& group: source.GetPageGroups()) {
        groups.push_back(TStringBuilder()
            << group.GetFirstPageNo()
            << ":[" << JoinSeq(",", group.GetContent()) << "]");
    }

    return JoinSeq(" ", groups);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestJournal final: public IJournal
{
    using TReadHandler = std::function<NCloud::NProto::TReadPagesResponse(
        const NCloud::NProto::TReadPagesRequest&)>;

    TResultOrError<ui64> RestoreResponse = 0;

    TReadHandler ReadHandler = [] (const auto& request) {
        Y_UNUSED(request);
        return NCloud::NProto::TReadPagesResponse();
    };

    TManualEvent AllRecordsFlushed;
    mutable TManualEvent FlushCycleCompleted;

    mutable TMutex Mutex;
    mutable TVector<NCloud::NProto::TReadPagesRequest> ReadRequests;
    mutable TDeque<NCloud::NProto::TJournalRecord> RecordsToFlush;
    TVector<ui64> FlushedLsns;
    ui32 RestoreCount = 0;
    ui32 CleanupCount = 0;

    // IJournal

    TFuture<TResultOrError<ui64>> Restore() override
    {
        with_lock (Mutex) {
            ++RestoreCount;
        }

        return MakeFuture(RestoreResponse);
    }

    TFuture<NCloud::NProto::TWriteLogRecordResponse> Write(
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        Y_UNUSED(request);

        return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>();
    }

    TFuture<NCloud::NProto::TReadPagesResponse> Read(
        NCloud::NProto::TReadPagesRequest request) const override
    {
        with_lock (Mutex) {
            ReadRequests.push_back(request);
        }

        return MakeFuture(ReadHandler(request));
    }

    auto ReadTail(NCloud::NProto::TReadJournalTailRequest request) const
        -> TFuture<NCloud::NProto::TReadJournalTailResponse> override
    {
        Y_UNUSED(request);

        return MakeFuture<NCloud::NProto::TReadJournalTailResponse>();
    }

    auto AdvanceLastAckedLsn(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> override
    {
        Y_UNUSED(request);

        return MakeFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse>();
    }

    auto GetRecordToFlush(ui64 maxAllowedLsn) const
        -> TFuture<TResultOrError<NCloud::NProto::TJournalRecord>> override
    {
        NCloud::NProto::TJournalRecord record;

        with_lock (Mutex) {
            if (!RecordsToFlush.empty() &&
                RecordsToFlush.front().GetLogSequenceNumber() <= maxAllowedLsn)
            {
                // the record is kept until it gets acked
                record = RecordsToFlush.front();
            }
        }

        return MakeFuture<TResultOrError<NCloud::NProto::TJournalRecord>>(
            std::move(record));
    }

    void MarkRecordAsFlushed(ui64 lsn) override
    {
        with_lock (Mutex) {
            if (!RecordsToFlush.empty() &&
                RecordsToFlush.front().GetLogSequenceNumber() == lsn)
            {
                RecordsToFlush.pop_front();
            }

            FlushedLsns.push_back(lsn);
        }
    }

    TFuture<NCloud::NProto::TError> CleanupFlushedRecords() override
    {
        with_lock (Mutex) {
            ++CleanupCount;

            if (RecordsToFlush.empty()) {
                // the flush cycle has drained the journal and cleaned up
                AllRecordsFlushed.Signal();
            }
        }

        FlushCycleCompleted.Signal();

        return MakeFuture(NCloud::NProto::TError());
    }

    // helpers

    void AddRecordToFlush(NCloud::NProto::TJournalRecord record)
    {
        with_lock (Mutex) {
            RecordsToFlush.push_back(std::move(record));
        }
    }

    TVector<ui64> GetFlushedLsns() const
    {
        with_lock (Mutex) {
            return FlushedLsns;
        }
    }

    ui32 GetCleanupCount() const
    {
        with_lock (Mutex) {
            return CleanupCount;
        }
    }

    ui32 GetRestoreCount() const
    {
        with_lock (Mutex) {
            return RestoreCount;
        }
    }

    TVector<NCloud::NProto::TReadPagesRequest> GetReadRequests() const
    {
        with_lock (Mutex) {
            return ReadRequests;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestDevice final: public IDevice
{
    using TReadHandler = std::function<NCloud::NProto::TReadPagesResponse(
        const NCloud::NProto::TReadPagesRequest&)>;
    using TWriteHandler =
        std::function<NCloud::NProto::TWriteLogRecordResponse(
            const NCloud::NProto::TWriteLogRecordRequest&)>;

    TReadHandler ReadHandler = ServePagesFromDevice;

    TWriteHandler WriteHandler = [] (const auto& request) {
        Y_UNUSED(request);
        return NCloud::NProto::TWriteLogRecordResponse();
    };

    mutable TMutex Mutex;
    TVector<NCloud::NProto::TReadPagesRequest> ReadRequests;
    TVector<NCloud::NProto::TWriteLogRecordRequest> WriteRequests;

    // IDevice

    TFuture<NCloud::NProto::TReadPagesResponse> ReadPages(
        NCloud::NProto::TReadPagesRequest request) override
    {
        with_lock (Mutex) {
            ReadRequests.push_back(request);
        }

        return MakeFuture(ReadHandler(request));
    }

    TFuture<NCloud::NProto::TWriteLogRecordResponse> WritePages(
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        with_lock (Mutex) {
            WriteRequests.push_back(request);
        }

        return MakeFuture(WriteHandler(request));
    }

    // helpers

    TVector<NCloud::NProto::TReadPagesRequest> GetReadRequests() const
    {
        with_lock (Mutex) {
            return ReadRequests;
        }
    }

    TVector<NCloud::NProto::TWriteLogRecordRequest> GetWriteRequests() const
    {
        with_lock (Mutex) {
            return WriteRequests;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging;
    TExecutorPtr Executor;

    std::shared_ptr<TTestJournal> Journal;
    std::shared_ptr<TTestDevice> DataStore;

    IJournalledDevicePtr Device;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Logging = CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_RESOURCES});
        Logging->Start();

        Executor = TExecutor::Create("TestExecutor");
        Executor->Start();

        Journal = std::make_shared<TTestJournal>();
        DataStore = std::make_shared<TTestDevice>();

        Device = CreateJournalledDeviceV2(
            Logging,
            Executor,
            Journal,
            DataStore,
            TString{DefaultDeviceUUID},
            TString{BackgroundClientId});
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        Device->Stop();
        Executor->Stop();
        Logging->Stop();
    }

    NCloud::NProto::TReadPagesResponse ReadPages(
        NCloud::NProto::TReadPagesRequest request)
    {
        return Device->ReadPages(std::move(request)).GetValueSync();
    }

    void WaitForAllRecordsToBeFlushed()
    {
        UNIT_ASSERT(Journal->AllRecordsFlushed.WaitT(TDuration::Seconds(30)));
        Journal->AllRecordsFlushed.Reset();
    }

    // Waits until a flush cycle runs to completion, i.e. until it stops finding
    // records it is allowed to flush.
    void WaitForFlushCycle()
    {
        UNIT_ASSERT(Journal->FlushCycleCompleted.WaitT(TDuration::Seconds(30)));
        Journal->FlushCycleCompleted.Reset();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalledDeviceV2Test)
{
    Y_UNIT_TEST_F(ShouldReadAllPagesFromTheJournal, TFixture)
    {
        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return MakeReadResponse({MakeGroup(10, 4, "J")}, 42);
        };

        const auto response = ReadPages(MakeReadRequest({{10, 4}}));

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[J10,J11,J12,J13]",
            DescribeGroups(response));
        UNIT_ASSERT_VALUES_EQUAL(42, response.GetLastAckedLogSequenceNumber());

        // the data store has not been touched at all

        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetReadRequests().size());
    }

    Y_UNIT_TEST_F(ShouldReadAllPagesFromTheDataStore, TFixture)
    {
        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return MakeReadResponse({}, 7);
        };

        const auto response = ReadPages(MakeReadRequest({{10, 4}}));

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        // the whole request has been forwarded to the data store as is

        const auto deviceRequests = DataStore->GetReadRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, deviceRequests.size());
        UNIT_ASSERT_VALUES_EQUAL("10+4@4096", DescribeRefs(deviceRequests[0]));
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultDeviceUUID,
            deviceRequests[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultClientId,
            deviceRequests[0].GetHeaders().GetClientId());

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[D10,D11,D12,D13]",
            DescribeGroups(response));

        // the lsn is taken from the journal response

        UNIT_ASSERT_VALUES_EQUAL(7, response.GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST_F(ShouldRequestOnlyMissingPagesFromTheDataStore, TFixture)
    {
        // the journal covers the middle of the requested range

        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return MakeReadResponse({MakeGroup(12, 3, "J")}, 100);
        };

        const auto response = ReadPages(MakeReadRequest({{10, 10}}));

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        const auto deviceRequests = DataStore->GetReadRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, deviceRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "10+2@4096, 15+5@4096",
            DescribeRefs(deviceRequests[0]));

        // the response is shaped after the request, not after the sources

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[D10,D11,J12,J13,J14,D15,D16,D17,D18,D19]",
            DescribeGroups(response));
        UNIT_ASSERT_VALUES_EQUAL(100, response.GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST_F(ShouldRequestMissingPagesForEveryPageGroupRef, TFixture)
    {
        // the first journal group covers the tail of the first ref and goes
        // beyond it, the second one covers the head of the second ref

        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return MakeReadResponse(
                {MakeGroup(2, 4, "J"), MakeGroup(100, 2, "J")});
        };

        const auto response = ReadPages(MakeReadRequest({{0, 4}, {100, 4}}));

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        const auto deviceRequests = DataStore->GetReadRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, deviceRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "0+2@4096, 102+2@4096",
            DescribeRefs(deviceRequests[0]));

        UNIT_ASSERT_VALUES_EQUAL(
            "0:[D0,D1,J2,J3] 100:[J100,J101,D102,D103]",
            DescribeGroups(response));
    }

    Y_UNIT_TEST_F(ShouldIgnoreEmptyJournalPageGroups, TFixture)
    {
        // a page group without content covers nothing

        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return MakeReadResponse(
                {MakeGroup(10, 0, "J"), MakeGroup(20, 2, "J")});
        };

        const auto response = ReadPages(MakeReadRequest({{10, 4}, {20, 2}}));

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        const auto deviceRequests = DataStore->GetReadRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, deviceRequests.size());
        UNIT_ASSERT_VALUES_EQUAL("10+4@4096", DescribeRefs(deviceRequests[0]));

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[D10,D11,D12,D13] 20:[J20,J21]",
            DescribeGroups(response));
    }

    Y_UNIT_TEST_F(ShouldSkipEmptyPageGroupRefs, TFixture)
    {
        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return NCloud::NProto::TReadPagesResponse();
        };

        const auto response = ReadPages(MakeReadRequest({{10, 0}, {20, 2}}));

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        const auto deviceRequests = DataStore->GetReadRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, deviceRequests.size());
        UNIT_ASSERT_VALUES_EQUAL("20+2@4096", DescribeRefs(deviceRequests[0]));

        UNIT_ASSERT_VALUES_EQUAL("20:[D20,D21]", DescribeGroups(response));
    }

    Y_UNIT_TEST_F(ShouldShapeTheResponseAfterTheRequest, TFixture)
    {
        // the journal holds a wider page group than the requested one

        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return MakeReadResponse({MakeGroup(8, 8, "J")}, 42);
        };

        const auto response = ReadPages(MakeReadRequest({{10, 4}}));

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        // only the requested pages are returned

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[J10,J11,J12,J13]",
            DescribeGroups(response));
        UNIT_ASSERT_VALUES_EQUAL(42, response.GetLastAckedLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetReadRequests().size());
    }

    Y_UNIT_TEST_F(ShouldPreferJournalPagesOverDataStorePages, TFixture)
    {
        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return MakeReadResponse({MakeGroup(12, 2, "J")});
        };

        // the data store returns more pages than it has been asked for

        DataStore->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return MakeReadResponse({MakeGroup(10, 4, "D")});
        };

        const auto response = ReadPages(MakeReadRequest({{10, 4}}));

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[D10,D11,J12,J13]",
            DescribeGroups(response));
    }

    Y_UNIT_TEST_F(ShouldFailIfAPageIsMissingInBothResponses, TFixture)
    {
        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return MakeReadResponse({MakeGroup(10, 1, "J")});
        };

        // the data store does not return the missing page

        DataStore->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return NCloud::NProto::TReadPagesResponse();
        };

        const auto response = ReadPages(MakeReadRequest({{10, 2}}));

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
        UNIT_ASSERT_STRING_CONTAINS(
            response.GetError().GetMessage(),
            "page 11 is missing");
    }

    Y_UNIT_TEST_F(ShouldHandleJournalReadError, TFixture)
    {
        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return NCloud::NProto::TReadPagesResponse(
                TErrorResponse(E_IO, "journal is broken"));
        };

        const auto response = ReadPages(MakeReadRequest({{10, 4}}));

        UNIT_ASSERT_VALUES_EQUAL(E_IO, response.GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            response.GetError().GetMessage(),
            "journal is broken");

        // the data store has not been touched

        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetReadRequests().size());
    }

    Y_UNIT_TEST_F(ShouldHandleDataStoreReadError, TFixture)
    {
        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return NCloud::NProto::TReadPagesResponse();
        };

        DataStore->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return NCloud::NProto::TReadPagesResponse(
                TErrorResponse(E_IO, "device is broken"));
        };

        const auto response = ReadPages(MakeReadRequest({{10, 4}}));

        UNIT_ASSERT_VALUES_EQUAL(E_IO, response.GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            response.GetError().GetMessage(),
            "device is broken");
    }

    Y_UNIT_TEST_F(ShouldRejectARequestForAnotherDevice, TFixture)
    {
        constexpr TStringBuf otherUUID = "another-device";

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ReadPages(MakeReadRequest({{10, 4}}, otherUUID))
                .GetError()
                .GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            Device->WriteLogRecord(MakeWriteRequest(otherUUID))
                .GetValueSync()
                .GetError()
                .GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            Device->ReadJournalTail(MakeTailRequest(otherUUID))
                .GetValueSync()
                .GetError()
                .GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            Device->AdvanceLsnLowWatermark(MakeAdvanceRequest(otherUUID))
                .GetValueSync()
                .GetError()
                .GetCode());

        // none of them got anywhere near the journal or the device

        UNIT_ASSERT_VALUES_EQUAL(0, Journal->GetReadRequests().size());
        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetReadRequests().size());
        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetWriteRequests().size());
    }

    Y_UNIT_TEST_F(ShouldRejectARequestWithNoDeviceUUID, TFixture)
    {
        auto response = ReadPages(MakeReadRequest({{10, 4}}, ""));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            response.GetError().GetMessage(),
            "empty device UUID");

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            Device->WriteLogRecord(MakeWriteRequest(""))
                .GetValueSync()
                .GetError()
                .GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            Device->ReadJournalTail(MakeTailRequest(""))
                .GetValueSync()
                .GetError()
                .GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            Device->AdvanceLsnLowWatermark(MakeAdvanceRequest(""))
                .GetValueSync()
                .GetError()
                .GetCode());

        UNIT_ASSERT_VALUES_EQUAL(0, Journal->GetReadRequests().size());
    }

    Y_UNIT_TEST_F(ShouldServeARequestForThisDevice, TFixture)
    {
        Journal->ReadHandler = [] (const auto& request) {
            Y_UNUSED(request);
            return MakeReadResponse({MakeGroup(10, 1, "J")}, 1);
        };

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ReadPages(MakeReadRequest({{10, 1}})).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            Device->WriteLogRecord(MakeWriteRequest(DefaultDeviceUUID))
                .GetValueSync()
                .GetError()
                .GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            Device->ReadJournalTail(MakeTailRequest(DefaultDeviceUUID))
                .GetValueSync()
                .GetError()
                .GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            Device
                ->AdvanceLsnLowWatermark(MakeAdvanceRequest(DefaultDeviceUUID))
                .GetValueSync()
                .GetError()
                .GetCode());
    }

    Y_UNIT_TEST_F(ShouldFlushJournalRecordsToTheDataStore, TFixture)
    {
        Journal->RestoreResponse = 3;
        Journal->AddRecordToFlush(MakeRecord(1, 10, 2));
        Journal->AddRecordToFlush(MakeRecord(2, 20, 1));
        Journal->AddRecordToFlush(MakeRecord(3, 30, 3));

        Device->Start();
        UNIT_ASSERT_VALUES_EQUAL(1, Journal->GetRestoreCount());

        WaitForAllRecordsToBeFlushed();
        Device->Stop();

        const auto writes = DataStore->GetWriteRequests();
        UNIT_ASSERT_VALUES_EQUAL(3, writes.size());
        UNIT_ASSERT_VALUES_EQUAL("10:[J10,J11]", DescribeGroups(writes[0]));
        UNIT_ASSERT_VALUES_EQUAL("20:[J20]", DescribeGroups(writes[1]));
        UNIT_ASSERT_VALUES_EQUAL("30:[J30,J31,J32]", DescribeGroups(writes[2]));

        // the flush has no client request behind it, so it goes out under the
        // identity of the device itself - a real device refuses a write
        // without one

        for (size_t i = 0; i < writes.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(DefaultDeviceUUID, writes[i].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                BackgroundClientId,
                writes[i].GetHeaders().GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(i + 1, writes[i].GetLogSequenceNumber());
            UNIT_ASSERT_VALUES_EQUAL(i, writes[i].GetPrevLogSequenceNumber());
        }

        // every flushed record has been acked in the journal

        const auto flushedLsns = Journal->GetFlushedLsns();
        UNIT_ASSERT_VALUES_EQUAL(3, flushedLsns.size());
        UNIT_ASSERT_VALUES_EQUAL(1, flushedLsns[0]);
        UNIT_ASSERT_VALUES_EQUAL(2, flushedLsns[1]);
        UNIT_ASSERT_VALUES_EQUAL(3, flushedLsns[2]);

        UNIT_ASSERT_GE(Journal->GetCleanupCount(), 1);
    }

    Y_UNIT_TEST_F(ShouldRetryFlushAfterDataStoreWriteError, TFixture)
    {
        std::atomic<ui32> writeCount = 0;

        DataStore->WriteHandler = [&] (const auto& request)
            -> NCloud::NProto::TWriteLogRecordResponse
        {
            Y_UNUSED(request);

            if (++writeCount == 1) {
                return TErrorResponse(E_IO, "device is busy");
            }

            return {};
        };

        Journal->RestoreResponse = 1;
        Journal->AddRecordToFlush(MakeRecord(1, 10, 2));

        Device->Start();
        WaitForAllRecordsToBeFlushed();
        Device->Stop();

        // the record has been written twice: the failed attempt and the retry

        const auto writes = DataStore->GetWriteRequests();
        UNIT_ASSERT_VALUES_EQUAL(2, writes.size());
        UNIT_ASSERT_VALUES_EQUAL("10:[J10,J11]", DescribeGroups(writes[0]));
        UNIT_ASSERT_VALUES_EQUAL("10:[J10,J11]", DescribeGroups(writes[1]));

        // the record has been acked only once, after the successful write

        const auto flushedLsns = Journal->GetFlushedLsns();
        UNIT_ASSERT_VALUES_EQUAL(1, flushedLsns.size());
        UNIT_ASSERT_VALUES_EQUAL(1, flushedLsns[0]);
    }

    Y_UNIT_TEST_F(ShouldNotFlushRecordsAboveTheRestoredLsn, TFixture)
    {
        // Restore reports lsn 2, so the record with lsn 3 is not indexed yet
        // and must stay in the journal until some writer advances the horizon.

        Journal->RestoreResponse = 2;
        Journal->AddRecordToFlush(MakeRecord(1, 10, 2));
        Journal->AddRecordToFlush(MakeRecord(2, 20, 1));
        Journal->AddRecordToFlush(MakeRecord(3, 30, 3));

        Device->Start();

        // the first cycle flushes lsn 1 and 2 and stops at lsn 3, the second
        // one finds nothing to flush at all

        WaitForFlushCycle();
        WaitForFlushCycle();
        Device->Stop();

        const auto writes = DataStore->GetWriteRequests();
        UNIT_ASSERT_VALUES_EQUAL(2, writes.size());
        UNIT_ASSERT_VALUES_EQUAL("10:[J10,J11]", DescribeGroups(writes[0]));
        UNIT_ASSERT_VALUES_EQUAL("20:[J20]", DescribeGroups(writes[1]));

        const auto flushedLsns = Journal->GetFlushedLsns();
        UNIT_ASSERT_VALUES_EQUAL(2, flushedLsns.size());
        UNIT_ASSERT_VALUES_EQUAL(1, flushedLsns[0]);
        UNIT_ASSERT_VALUES_EQUAL(2, flushedLsns[1]);
    }

    Y_UNIT_TEST_F(ShouldNotStartWhenJournalRestoreFails, TFixture)
    {
        Journal->RestoreResponse = MakeError(E_IO, "journal is broken");
        Journal->AddRecordToFlush(MakeRecord(1, 10, 2));

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Device->Start(),
            yexception,
            "journal is broken");

        // the flush cycle has not been started

        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetWriteRequests().size());
        UNIT_ASSERT_VALUES_EQUAL(0, Journal->GetCleanupCount());
    }

    Y_UNIT_TEST_F(ShouldStopFlushCycle, TFixture)
    {
        Device->Start();
        WaitForAllRecordsToBeFlushed();
        Device->Stop();

        const auto cleanupCount = Journal->GetCleanupCount();

        // nothing happens after the device has been stopped

        Journal->AddRecordToFlush(MakeRecord(1, 10, 2));

        UNIT_ASSERT_VALUES_EQUAL(cleanupCount, Journal->GetCleanupCount());
        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetWriteRequests().size());
    }
}

}   // namespace NCloud::NJournalled
