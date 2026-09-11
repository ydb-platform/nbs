#include "journalled_device_v2.h"

#include "device.h"
#include "device_page_store.h"
#include "journal.h"
#include "key_buffer_store.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/system/mutex.h>

#include <functional>
#include <utility>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// The page size of the journal and of the data device - a page holds exactly
// one tagged content string, see MakeContent.
constexpr ui32 DataPageSize = 4;
constexpr ui64 DefaultJournalPageCount = 32;

// The metadata store keeps one entry per in-flight record plus the journal
// metadata, and its pages carry a 48 byte header.
constexpr ui32 MetaPageSize = 256;
constexpr ui64 MetaPageCount = 64;

constexpr TStringBuf DeviceUUID = "test-device";
constexpr TStringBuf BackgroundClientId = "background-ops";

constexpr auto WaitTimeout = TDuration::Seconds(30);
constexpr auto PollInterval = TDuration::MilliSeconds(10);

// "<firstPageNo>x<pageCount>" pairs
using TGroups = TVector<std::pair<ui64 /*firstPageNo*/, ui64 /*pageCount*/>>;

////////////////////////////////////////////////////////////////////////////////

// Exactly DataPageSize bytes - the tag says which record wrote the page,
// e.g. "A010".
TString MakeContent(char tag, ui64 pageNo)
{
    return Sprintf("%c%03u", tag, static_cast<ui32>(pageNo % 1000));
}

NCloud::NProto::TDevicePageGroup
MakeGroup(ui64 firstPageNo, ui64 pageCount, char tag)
{
    NCloud::NProto::TDevicePageGroup group;
    group.SetFirstPageNo(firstPageNo);

    for (ui64 i = 0; i < pageCount; ++i) {
        *group.AddContent() = MakeContent(tag, firstPageNo + i);
    }

    return group;
}

NCloud::NProto::TWriteLogRecordRequest
MakeWriteRequest(ui64 lsn, ui64 prevLsn, char tag, const TGroups& groups)
{
    NCloud::NProto::TWriteLogRecordRequest request;
    request.SetDeviceUUID(TString{DeviceUUID});
    request.SetLogSequenceNumber(lsn);
    request.SetPrevLogSequenceNumber(prevLsn);

    for (const auto& [firstPageNo, pageCount]: groups) {
        *request.AddPageGroups() = MakeGroup(firstPageNo, pageCount, tag);
    }

    return request;
}

NCloud::NProto::TReadPagesRequest MakeReadRequest(const TGroups& refs)
{
    NCloud::NProto::TReadPagesRequest request;
    request.SetDeviceUUID(TString{DeviceUUID});

    for (const auto& [firstPageNo, pageCount]: refs) {
        auto& ref = *request.AddPageGroupRefs();
        ref.SetFirstPageNo(firstPageNo);
        ref.SetPageCount(pageCount);
        ref.SetPageSize(DataPageSize);
    }

    return request;
}

NCloud::NProto::TReadJournalTailRequest MakeReadTailRequest(
    ui64 afterLsn,
    ui32 maxRecordCount)
{
    NCloud::NProto::TReadJournalTailRequest request;
    request.SetDeviceUUID(TString{DeviceUUID});
    request.SetAfterLogSequenceNumber(afterLsn);
    request.SetMaxRecordCount(maxRecordCount);

    return request;
}

NCloud::NProto::TAdvanceLsnLowWatermarkRequest MakeAdvanceRequest(ui64 lsn)
{
    NCloud::NProto::TAdvanceLsnLowWatermarkRequest request;
    request.SetDeviceUUID(TString{DeviceUUID});
    request.SetLsnLowWatermark(lsn);

    return request;
}

// A page that has never been written reads back as a zeroed one.
TString DescribeContent(TStringBuf content)
{
    if (AllOf(content, [](char byte) { return byte == 0; })) {
        return "----";
    }

    return TString{content};
}

// "10:[A010,A011] 20:[A020]"
template <typename T>
TString DescribeGroups(const T& source)
{
    TVector<TString> groups;

    for (const auto& group: source.GetPageGroups()) {
        TVector<TString> contents;
        for (const auto& content: group.GetContent()) {
            contents.push_back(DescribeContent(content));
        }

        groups.push_back(
            TStringBuilder()
            << group.GetFirstPageNo() << ":[" << JoinSeq(",", contents) << "]");
    }

    return JoinSeq(" ", groups);
}

// "1<-0 10:[A010]; 2<-1 20:[B020]"
TString DescribeRecords(
    const NCloud::NProto::TReadJournalTailResponse& response)
{
    TVector<TString> records;

    for (const auto& record: response.GetRecords()) {
        TStringBuilder sb;
        sb << record.GetLogSequenceNumber() << "<-"
           << record.GetPrevLogSequenceNumber();

        if (const auto groups = DescribeGroups(record)) {
            sb << " " << groups;
        }

        records.push_back(sb);
    }

    return JoinSeq("; ", records);
}

////////////////////////////////////////////////////////////////////////////////

// An in-memory device that keeps the write requests it was given, so a test
// can look at the shape of the ones the flush cycle makes up on its own.
struct TRecordingDevice final: public IDevice
{
    const IDevicePtr Impl = CreateInMemoryDevice();

    mutable TMutex Mutex;
    TVector<NCloud::NProto::TWriteLogRecordRequest> Writes;

    TFuture<NCloud::NProto::TReadPagesResponse> ReadPages(
        NCloud::NProto::TReadPagesRequest request) override
    {
        return Impl->ReadPages(std::move(request));
    }

    TFuture<NCloud::NProto::TWriteLogRecordResponse> WritePages(
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        with_lock (Mutex) {
            Writes.push_back(request);
        }

        return Impl->WritePages(std::move(request));
    }

    TVector<NCloud::NProto::TWriteLogRecordRequest> GetWrites() const
    {
        with_lock (Mutex) {
            return Writes;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// The whole stack over three in-memory devices: the journal metadata store
// lives on one, the journal data on another, and the third one is the data
// device the records are eventually flushed to.
struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging;
    TExecutorPtr Executor;

    IDevicePtr LogMetaDevice;
    IDevicePtr LogDataDevice;
    std::shared_ptr<TRecordingDevice> DataDevice;

    // the number of journal pages the next stack is given
    ui64 JournalPageCount = DefaultJournalPageCount;

    IKeyBufferStorePtr MetaStore;
    IDevicePageStorePtr PageStore;
    IJournalPtr Journal;
    IJournalledDevicePtr Device;

    bool Started = false;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Logging = CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_RESOURCES});
        Logging->Start();

        Executor = TExecutor::Create("TestExecutor");
        Executor->Start();

        LogMetaDevice = CreateInMemoryDevice();
        LogDataDevice = CreateInMemoryDevice();
        DataDevice = std::make_shared<TRecordingDevice>();

        Restart();
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        Stop();

        Executor->Stop();
        Logging->Stop();
    }

    // Builds the stack anew over the very same devices and starts it - what a
    // restart does. Everything above the devices is rebuilt from scratch and
    // restored from what they hold.
    void Restart()
    {
        Stop();

        MetaStore = CreateDeviceKeyBufferStore(
            LogMetaDevice,
            MetaPageCount,
            MetaPageSize);

        // the journal is the only caller and hands the store back the pages
        // it has allocated itself, and the ranges of a client request are
        // validated by the device before the journal ever looks them up
        PageStore = CreateDevicePageStore(
            LogDataDevice,
            JournalPageCount,
            DataPageSize,
            EDevicePageStoreMode::Trusted);
        Journal = CreateJournal(Logging, Executor, MetaStore, PageStore);
        Device = CreateJournalledDeviceV2(
            Logging,
            Executor,
            Journal,
            DataDevice,
            TString{DeviceUUID},
            TString{BackgroundClientId});

        Device->Start();
        Started = true;
    }

    void Stop()
    {
        if (Started) {
            Device->Stop();
            Started = false;
        }
    }

    ui32 WriteRecord(ui64 lsn, ui64 prevLsn, char tag, const TGroups& groups)
    {
        return Device
            ->WriteLogRecord(MakeWriteRequest(lsn, prevLsn, tag, groups))
            .GetValueSync()
            .GetError()
            .GetCode();
    }

    void
    WriteRecordOrFail(ui64 lsn, ui64 prevLsn, char tag, const TGroups& groups)
    {
        const auto response =
            Device->WriteLogRecord(MakeWriteRequest(lsn, prevLsn, tag, groups))
                .GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    NCloud::NProto::TReadPagesResponse ReadPages(const TGroups& refs)
    {
        return Device->ReadPages(MakeReadRequest(refs)).GetValueSync();
    }

    TString ReadPagesOrFail(const TGroups& refs)
    {
        const auto response = ReadPages(refs);

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        return DescribeGroups(response);
    }

    NCloud::NProto::TReadJournalTailResponse ReadTail(
        ui64 afterLsn = 0,
        ui32 maxRecordCount = 0)
    {
        return Device
            ->ReadJournalTail(MakeReadTailRequest(afterLsn, maxRecordCount))
            .GetValueSync();
    }

    ui32 AdvanceLsnLowWatermark(ui64 lsn)
    {
        return Device->AdvanceLsnLowWatermark(MakeAdvanceRequest(lsn))
            .GetValueSync()
            .GetError()
            .GetCode();
    }

    // Goes straight to the data device, bypassing the journal.
    TString ReadDataDevice(const TGroups& refs)
    {
        const auto response =
            DataDevice->ReadPages(MakeReadRequest(refs)).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        return DescribeGroups(response);
    }

    // Writes the pages the device held before the journal ever saw it.
    void FillDataDevice(const TGroups& groups, char tag)
    {
        NCloud::NProto::TWriteLogRecordRequest request;
        for (const auto& [firstPageNo, pageCount]: groups) {
            *request.AddPageGroups() = MakeGroup(firstPageNo, pageCount, tag);
        }

        const auto response =
            DataDevice->WritePages(std::move(request)).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    // The flush cycle is a background loop, so the tests wait for what it
    // does instead of driving it.
    void WaitUntil(std::function<bool()> predicate, TStringBuf what)
    {
        const auto deadline = TInstant::Now() + WaitTimeout;

        while (TInstant::Now() < deadline) {
            if (predicate()) {
                return;
            }

            Sleep(PollInterval);
        }

        UNIT_FAIL("timed out waiting for " << what);
    }

    void WaitForDataDevice(const TGroups& refs, TStringBuf expected)
    {
        WaitUntil(
            [&] { return ReadDataDevice(refs) == expected; },
            TStringBuilder() << "the data device to hold " << expected);
    }
};

////////////////////////////////////////////////////////////////////////////////

// A journal with room for eight pages only.
struct TTinyJournalFixture: public TFixture
{
    TTinyJournalFixture()
    {
        JournalPageCount = 8;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalledDeviceV2StackTest)
{
    Y_UNIT_TEST_F(ShouldServeReadsFromTheJournal, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 2}});

        UNIT_ASSERT_VALUES_EQUAL("10:[A010,A011]", ReadPagesOrFail({{10, 2}}));

        // nothing has reached the data device - the watermark has not moved

        UNIT_ASSERT_VALUES_EQUAL("10:[----,----]", ReadDataDevice({{10, 2}}));
    }

    Y_UNIT_TEST_F(ShouldOverwriteThePagesOfThePreviousRecords, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 4}});
        WriteRecordOrFail(2, 1, 'B', {{11, 2}});

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,B011,B012,A013]",
            ReadPagesOrFail({{10, 4}}));
    }

    Y_UNIT_TEST_F(ShouldMergeTheJournalAndTheDataDevice, TFixture)
    {
        // the pages the device held before the journal was given anything
        FillDataDevice({{10, 10}}, 'D');

        WriteRecordOrFail(1, 0, 'A', {{12, 3}});

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[D010,D011,A012,A013,A014,D015,D016,D017,D018,D019]",
            ReadPagesOrFail({{10, 10}}));

        // the response is shaped after the request, not after the sources

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[D010,D011] 14:[A014,D015]",
            ReadPagesOrFail({{10, 2}, {14, 2}}));
    }

    Y_UNIT_TEST_F(ShouldReadTheJournalTail, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 1}});
        WriteRecordOrFail(2, 1, 'B', {{20, 2}});
        WriteRecordOrFail(3, 2, 'C', {{30, 1}});

        const auto response = ReadTail();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020,B021]; 3<-2 30:[C030]",
            DescribeRecords(response));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetLastAckedLogSequenceNumber());

        // the tail starts after the requested lsn

        UNIT_ASSERT_VALUES_EQUAL(
            "3<-2 30:[C030]",
            DescribeRecords(ReadTail(2)));

        // and holds no more records than asked for

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020,B021]",
            DescribeRecords(ReadTail(0, 2)));
    }

    Y_UNIT_TEST_F(ShouldFlushTheAckedRecordsToTheDataDevice, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 2}});
        WriteRecordOrFail(2, 1, 'B', {{20, 2}});
        WriteRecordOrFail(3, 2, 'C', {{30, 2}});

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));

        WaitForDataDevice({{10, 2}, {20, 2}}, "10:[A010,A011] 20:[B020,B021]");

        // the record above the watermark stays in the journal

        UNIT_ASSERT_VALUES_EQUAL("30:[----,----]", ReadDataDevice({{30, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "3<-2 30:[C030,C031]",
            DescribeRecords(ReadTail()));

        // and the reads see the same pages no matter where they come from

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011] 20:[B020,B021] 30:[C030,C031]",
            ReadPagesOrFail({{10, 2}, {20, 2}, {30, 2}}));
    }

    Y_UNIT_TEST_F(
        ShouldFreeTheJournalPagesOfTheFlushedRecords,
        TTinyJournalFixture)
    {
        // the two records fill the journal up

        WriteRecordOrFail(1, 0, 'A', {{10, 4}});
        WriteRecordOrFail(2, 1, 'B', {{20, 4}});

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, WriteRecord(3, 2, 'C', {{30, 1}}));

        // the pages come back once the records they hold are flushed

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));

        WaitUntil(
            [&] { return WriteRecord(3, 2, 'C', {{30, 1}}) == S_OK; },
            "the journal pages to be freed");

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011,A012,A013] 30:[C030]",
            ReadPagesOrFail({{10, 4}, {30, 1}}));
    }

    Y_UNIT_TEST_F(ShouldRestoreTheUnflushedRecords, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 2}});
        WriteRecordOrFail(2, 1, 'B', {{20, 2}});

        Restart();

        // nothing has been acked, so the whole journal is still there

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010,A011]; 2<-1 20:[B020,B021]",
            DescribeRecords(ReadTail()));
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011] 20:[B020,B021]",
            ReadPagesOrFail({{10, 2}, {20, 2}}));
        UNIT_ASSERT_VALUES_EQUAL("10:[----,----]", ReadDataDevice({{10, 2}}));

        // and the journal goes on from where it stopped

        WriteRecordOrFail(3, 2, 'C', {{30, 1}});

        UNIT_ASSERT_VALUES_EQUAL("30:[C030]", ReadPagesOrFail({{30, 1}}));
    }

    Y_UNIT_TEST_F(ShouldRestoreTheFlushedWatermark, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 2}});
        WriteRecordOrFail(2, 1, 'B', {{20, 2}});
        WriteRecordOrFail(3, 2, 'C', {{30, 2}});

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));

        WaitForDataDevice({{10, 2}, {20, 2}}, "10:[A010,A011] 20:[B020,B021]");

        Restart();

        // the acked records have been dropped from the journal, the pages
        // they wrote are served by the data device now

        UNIT_ASSERT_VALUES_EQUAL(
            "3<-2 30:[C030,C031]",
            DescribeRecords(ReadTail()));
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011] 20:[B020,B021] 30:[C030,C031]",
            ReadPagesOrFail({{10, 2}, {20, 2}, {30, 2}}));

        // the watermark has survived the restart

        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, AdvanceLsnLowWatermark(2));
    }

    Y_UNIT_TEST_F(ShouldRestoreAFullyFlushedJournal, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 2}});
        WriteRecordOrFail(2, 1, 'B', {{20, 2}});

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));

        WaitForDataDevice({{10, 2}, {20, 2}}, "10:[A010,A011] 20:[B020,B021]");

        Restart();

        UNIT_ASSERT_VALUES_EQUAL("", DescribeRecords(ReadTail()));
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011] 20:[B020,B021]",
            ReadPagesOrFail({{10, 2}, {20, 2}}));

        // the next record chains from the restored watermark

        WriteRecordOrFail(3, 2, 'C', {{10, 1}});

        UNIT_ASSERT_VALUES_EQUAL("10:[C010,A011]", ReadPagesOrFail({{10, 2}}));
    }

    Y_UNIT_TEST_F(ShouldRejectARecordThatIsAlreadyIndexed, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 1}});
        WriteRecordOrFail(2, 1, 'B', {{10, 1}});

        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, WriteRecord(2, 1, 'B', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, WriteRecord(1, 0, 'A', {{10, 1}}));

        UNIT_ASSERT_VALUES_EQUAL("10:[B010]", ReadPagesOrFail({{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldRejectAWatermarkAboveTheIndexedLsn, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 1}});

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, AdvanceLsnLowWatermark(2));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(1));
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, AdvanceLsnLowWatermark(1));
    }

    Y_UNIT_TEST_F(ShouldAckAnOutOfOrderRecordOnceTheChainCloses, TFixture)
    {
        // the record arrives before the one it chains from
        auto second =
            Device->WriteLogRecord(MakeWriteRequest(2, 1, 'B', {{20, 1}}));

        Sleep(TDuration::MilliSeconds(200));

        // it is durable, but it cannot be acked - and cannot be read - until
        // the gap in the chain is closed
        UNIT_ASSERT(!second.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("20:[----]", ReadPagesOrFail({{20, 1}}));
        UNIT_ASSERT_VALUES_EQUAL("", DescribeRecords(ReadTail()));

        WriteRecordOrFail(1, 0, 'A', {{10, 1}});

        const auto response = second.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]",
            DescribeRecords(ReadTail()));
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 20:[B020]",
            ReadPagesOrFail({{10, 1}, {20, 1}}));
    }

    Y_UNIT_TEST_F(ShouldRejectARecordForkingTheChain, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 1}});
        WriteRecordOrFail(2, 1, 'B', {{10, 1}});

        // another record chaining from the lsn record 2 already continues
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            WriteRecord(3, 1, 'C', {{10, 1}}));

        UNIT_ASSERT_VALUES_EQUAL("10:[B010]", ReadPagesOrFail({{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldRejectAnInvalidLsn, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, WriteRecord(1, 1, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, WriteRecord(1, 5, 'A', {{10, 1}}));

        // the highest lsn is reserved for the journal metadata
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(Max<ui64>(), 0, 'A', {{10, 1}}));

        UNIT_ASSERT_VALUES_EQUAL("10:[----]", ReadPagesOrFail({{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldAcceptARecordWithNoPages, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 1}});

        // a record with no page groups at all, and one whose only group
        // carries no pages - both are chain links and nothing more
        WriteRecordOrFail(2, 1, 'B', {});
        WriteRecordOrFail(3, 2, 'C', {{20, 0}});

        WriteRecordOrFail(4, 3, 'D', {{30, 1}});

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1; 3<-2; 4<-3 30:[D030]",
            DescribeRecords(ReadTail()));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(4));

        // the records are flushed in lsn order, so the last one landing on
        // the device means the two in the middle have been dealt with
        WaitForDataDevice({{30, 1}}, "30:[D030]");

        const auto writes = DataDevice->GetWrites();
        UNIT_ASSERT_VALUES_EQUAL(2, writes.size());
        UNIT_ASSERT_VALUES_EQUAL(1, writes[0].GetLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(4, writes[1].GetLogSequenceNumber());

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 30:[D030]",
            ReadPagesOrFail({{10, 1}, {30, 1}}));
    }

    Y_UNIT_TEST_F(ShouldFlushUnderTheDeviceIdentity, TFixture)
    {
        WriteRecordOrFail(1, 0, 'A', {{10, 2}});

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(1));
        WaitForDataDevice({{10, 2}}, "10:[A010,A011]");

        // the flush has no client request behind it - a real device refuses
        // a write with no uuid and no client id

        const auto writes = DataDevice->GetWrites();
        UNIT_ASSERT_VALUES_EQUAL(1, writes.size());
        UNIT_ASSERT_VALUES_EQUAL(DeviceUUID, writes[0].GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            BackgroundClientId,
            writes[0].GetHeaders().GetClientId());
        UNIT_ASSERT_VALUES_EQUAL(1, writes[0].GetLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(0, writes[0].GetPrevLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL("10:[A010,A011]", DescribeGroups(writes[0]));
    }

    Y_UNIT_TEST_F(ShouldRejectAReadWithIntersectingRefs, TFixture)
    {
        FillDataDevice({{10, 10}}, 'D');
        WriteRecordOrFail(1, 0, 'A', {{12, 1}});

        // the same pages asked for twice - the journal would look them up
        // twice and the merged response would take their content twice
        auto response = ReadPages({{10, 4}, {10, 4}});
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            response.GetError().GetMessage(),
            "10x4 and 10x4 of a single request intersect");

        // two refs that merely touch the same page
        response = ReadPages({{10, 3}, {12, 2}});
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());

        // nothing the journal holds is served by refs the device covers
        response = ReadPages({{10, 2}, {11, 2}});
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());

        // the refs the request is allowed to hold still work, empty ones
        // among them
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[D010,D011] 12:[A012] 19:[D019]",
            ReadPagesOrFail({{10, 2}, {12, 1}, {15, 0}, {19, 1}}));
    }

    Y_UNIT_TEST_F(ShouldRejectARecordWithIntersectingGroups, TFixture)
    {
        // which group wins would come down to the order they are in
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(1, 0, 'A', {{10, 4}, {12, 4}}));

        // the rejected record has left nothing behind
        UNIT_ASSERT_VALUES_EQUAL("", DescribeRecords(ReadTail()));
        UNIT_ASSERT_VALUES_EQUAL("10:[----,----]", ReadPagesOrFail({{10, 2}}));

        WriteRecordOrFail(1, 0, 'A', {{10, 2}, {20, 2}});

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011] 20:[A020,A021]",
            ReadPagesOrFail({{10, 2}, {20, 2}}));
    }
}

}   // namespace NCloud::NJournalled
