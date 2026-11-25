#include "journalled_device_v2.h"

#include "journal.h"

#include <cloud/fastshard/libs/journal/iface/device.h>
#include <cloud/fastshard/libs/journal/iface/journalled_device.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/deque.h>
#include <util/generic/set.h>
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

constexpr TStringBuf DefaultDeviceUUID = "uuid";

////////////////////////////////////////////////////////////////////////////////

using TPageRefs = TVector<std::pair<ui64 /*firstPageNo*/, ui64 /*pageCount*/>>;

TVector<TPageRange> MakePageRanges(const TPageRefs& refs = {{10, 2}, {20, 1}})
{
    TVector<TPageRange> ranges;

    for (const auto& [firstPageNo, pageCount]: refs) {
        auto& range = ranges.emplace_back();
        range.FirstPageNo = firstPageNo;

        for (ui64 i = 0; i < pageCount; ++i) {
            const TString page = TStringBuilder() << "P" << firstPageNo + i;
            range.Pages.emplace_back(page.data(), page.size());
        }
    }

    return ranges;
}

TVector<TBuffer> MakePages(ui64 firstPageNo, ui64 pageCount, TStringBuf prefix)
{
    TVector<TBuffer> pages;

    for (ui64 i = 0; i < pageCount; ++i) {
        const TString page = TStringBuilder() << prefix << firstPageNo + i;
        pages.emplace_back(page.data(), page.size());
    }

    return pages;
}

TJournalRecord MakeRecord(
    ui64 lsn,
    ui64 firstPageNo,
    ui64 pageCount,
    TStringBuf prefix = "J")
{
    TJournalRecord record{.Lsn = lsn, .PrevLsn = lsn - 1};
    record.Ranges.push_back(
        {.FirstPageNo = firstPageNo,
         .Pages = MakePages(firstPageNo, pageCount, prefix)});

    return record;
}

ui64 CountPages(const TJournalRecord& record)
{
    ui64 pageCount = 0;
    for (const auto& range: record.Ranges) {
        pageCount += range.Pages.size();
    }

    return pageCount;
}

// Serves every requested range ref with pages named after the device.
TVector<TBuffer> ServePagesFromDevice(const TVector<TPageRangeRef>& rangeRefs)
{
    TVector<TBuffer> pages;

    for (const auto& ref: rangeRefs) {
        for (ui64 i = 0; i < ref.PageCount; ++i) {
            const TString page = TStringBuilder() << "D" << ref.FirstPageNo + i;
            pages.emplace_back(page.data(), page.size());
        }
    }

    return pages;
}

// "10+2, 15+5"
TString DescribeRefs(const TVector<TPageRangeRef>& rangeRefs)
{
    TVector<TString> refs;

    for (const auto& ref: rangeRefs) {
        refs.push_back(TStringBuilder()
            << ref.FirstPageNo << "+" << ref.PageCount);
    }

    return JoinSeq(", ", refs);
}

// "10:[J10,J11] 20:[J20]"
TString DescribeRanges(const TVector<TPageRange>& ranges)
{
    TVector<TString> groups;

    for (const auto& range: ranges) {
        TVector<TStringBuf> pages;
        for (const auto& page: range.Pages) {
            pages.emplace_back(page.Data(), page.Size());
        }

        groups.push_back(TStringBuilder()
            << range.FirstPageNo << ":[" << JoinSeq(",", pages) << "]");
    }

    return JoinSeq(" ", groups);
}

// "D10,D11,J12,J20"
TString DescribePages(const TResultOrError<TReadPagesResult>& result)
{
    TVector<TStringBuf> pages;

    for (const auto& page: result.GetResult().Pages) {
        pages.emplace_back(page.Data(), page.Size());
    }

    return JoinSeq(",", pages);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestJournal final: public IJournal
{
    TResultOrError<ui64> RestoreResponse = 0;

    // The pages that the journal holds. A read takes all the other pages as
    // the missing ones.
    TVector<TPageRangeRef> JournalRanges;
    ui64 LastAckedLsn = 0;

    // When set, a read fails with this error
    NProto::TError ReadError;

    // The future that Read returns is set only when this one is set
    TFuture<void> ReadBarrier = MakeFuture();

    TManualEvent AllRecordsFlushed;
    mutable TManualEvent FlushCycleCompleted;

    mutable TMutex Mutex;
    mutable TVector<TVector<TPageRangeRef>> ReadRequests;
    mutable TDeque<TJournalRecord> RecordsToFlush;
    TVector<ui64> FlushedLsns;
    TVector<TVector<TPageRangeRef>> FlushedRanges;
    ui32 RestoreCount = 0;
    ui32 CleanupCount = 0;

    // IJournal

    TResultOrError<ui64> Restore() override
    {
        with_lock (Mutex) {
            ++RestoreCount;
        }

        return RestoreResponse;
    }

    TFuture<NProto::TError> Write(
        TVector<TPageRange> ranges,
        ui64 lsn,
        ui64 prevLsn) override
    {
        Y_UNUSED(ranges, lsn, prevLsn);

        return MakeFuture<NProto::TError>();
    }

    // Answers with the pages of the request that JournalRanges hold, named
    // after the journal, and takes the other pages of the request as the
    // missing ones.
    TResultOrError<TJournalReadResult> Read(
        TVector<TPageRangeRef> rangeRefs) const override
    {
        with_lock (Mutex) {
            ReadRequests.push_back(rangeRefs);
        }

        if (HasError(ReadError)) {
            return ReadError;
        }

        TSet<ui64> journalled;
        for (const auto& range: JournalRanges) {
            for (ui64 i = 0; i < range.PageCount; ++i) {
                journalled.insert(range.FirstPageNo + i);
            }
        }

        TJournalReadResult result;
        TReadPagesResult pages{.LastAckedLsn = LastAckedLsn};

        auto& missing = result.MissingRanges;
        for (const auto& ref: rangeRefs) {
            bool extend = false;

            for (ui64 i = 0; i < ref.PageCount; ++i) {
                const ui64 pageNo = ref.FirstPageNo + i;
                if (journalled.contains(pageNo)) {
                    const TString page = TStringBuilder() << "J" << pageNo;
                    pages.Pages.emplace_back(page.data(), page.size());
                    extend = false;
                    continue;
                }

                if (extend) {
                    ++missing.back().PageCount;
                } else {
                    missing.push_back({.FirstPageNo = pageNo, .PageCount = 1});
                    extend = true;
                }
            }
        }

        result.Future = ReadBarrier.Apply(
            [pages = std::move(pages)](
                const auto& future) -> TResultOrError<TReadPagesResult>
            {
                Y_UNUSED(future);
                return pages;
            });

        return result;
    }

    TResultOrError<TJournalTail> ReadTail(
        ui64 afterLsn,
        ui32 maxRecordCount) const override
    {
        Y_UNUSED(afterLsn, maxRecordCount);

        return TJournalTail();
    }

    NProto::TError AdvanceLsnLowWatermark(ui64 lsnLowWatermark) override
    {
        Y_UNUSED(lsnLowWatermark);

        return {};
    }

    // Hands out the records the way the journal does: from the front, up to
    // the lsn and the page count asked for - but at least one record.
    TResultOrError<TJournalRecords> GetRecordsToFlush(
        ui64 maxAllowedLsn,
        ui64 maxPageCount) const override
    {
        UNIT_ASSERT(maxPageCount > 0);

        TJournalRecords records;
        ui64 pageCount = 0;

        with_lock (Mutex) {
            // the records are kept until they get acked
            for (const auto& record: RecordsToFlush) {
                if (record.Lsn > maxAllowedLsn) {
                    break;
                }

                const ui64 recordPageCount = CountPages(record);
                if (!records.empty() &&
                    pageCount + recordPageCount > maxPageCount)
                {
                    break;
                }

                records.push_back(record);
                pageCount += recordPageCount;
            }
        }

        return records;
    }

    void MarkRecordsAsFlushed(
        ui64 lsn,
        const TVector<TPageRangeRef>& ranges) override
    {
        with_lock (Mutex) {
            while (!RecordsToFlush.empty() && RecordsToFlush.front().Lsn <= lsn)
            {
                RecordsToFlush.pop_front();
            }

            FlushedLsns.push_back(lsn);
            FlushedRanges.push_back(ranges);
        }
    }

    NProto::TError CleanupFlushedRecords() override
    {
        with_lock (Mutex) {
            ++CleanupCount;

            if (RecordsToFlush.empty()) {
                // the flush cycle has drained the journal and cleaned up
                AllRecordsFlushed.Signal();
            }
        }

        FlushCycleCompleted.Signal();

        return {};
    }

    // helpers

    void AddRecordToFlush(TJournalRecord record)
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

    // "<first page>+<count>" of every range, one string per flushed batch
    TVector<TString> GetFlushedRanges() const
    {
        TVector<TString> result;

        with_lock (Mutex) {
            for (const auto& ranges: FlushedRanges) {
                TStringBuilder sb;
                for (const auto& [firstPageNo, pageCount]: ranges) {
                    if (sb) {
                        sb << " ";
                    }
                    sb << firstPageNo << "+" << pageCount;
                }
                result.push_back(sb);
            }
        }

        return result;
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

    TVector<TVector<TPageRangeRef>> GetReadRequests() const
    {
        with_lock (Mutex) {
            return ReadRequests;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestDevice final: public IDevice
{
    using TReadHandler = std::function<TResultOrError<TVector<TBuffer>>(
        const TVector<TPageRangeRef>&)>;
    using TWriteHandler =
        std::function<NProto::TError(const TVector<TPageRange>&)>;

    TReadHandler ReadHandler = ServePagesFromDevice;

    TWriteHandler WriteHandler = [] (const auto& ranges) {
        Y_UNUSED(ranges);
        return NProto::TError();
    };

    // The first write stays in flight until this promise is set.
    TPromise<NProto::TError> FirstWriteResult;
    TManualEvent FirstWriteStarted;

    mutable TMutex Mutex;
    TVector<TVector<TPageRangeRef>> ReadRequests;
    TVector<TVector<TPageRange>> WriteRequests;
    TVector<TVector<TPageRangeRef>> ZeroRequests;

    // IDevice

    TFuture<TResultOrError<TVector<TBuffer>>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) override
    {
        with_lock (Mutex) {
            ReadRequests.push_back(rangeRefs);
        }

        return MakeFuture(ReadHandler(rangeRefs));
    }

    TFuture<NProto::TError> WritePages(TVector<TPageRange> ranges) override
    {
        bool isFirst = false;
        with_lock (Mutex) {
            isFirst = WriteRequests.empty();
            WriteRequests.push_back(ranges);
        }

        if (isFirst && FirstWriteResult.Initialized()) {
            FirstWriteStarted.Signal();
            return FirstWriteResult.GetFuture();
        }

        return MakeFuture(WriteHandler(ranges));
    }

    TFuture<NProto::TError> ZeroPages(TVector<TPageRangeRef> ranges) override
    {
        with_lock (Mutex) {
            ZeroRequests.push_back(ranges);
        }

        return MakeFuture<NProto::TError>();
    }

    // helpers

    TVector<TVector<TPageRangeRef>> GetReadRequests() const
    {
        with_lock (Mutex) {
            return ReadRequests;
        }
    }

    TVector<TVector<TPageRange>> GetWriteRequests() const
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
            TString{DefaultDeviceUUID});
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        Device->Stop().Wait();
        Executor->Stop();
        Logging->Stop();
    }

    NProto::TError StartDevice()
    {
        return Device->Start().GetValueSync();
    }

    void StartDeviceAndCheck()
    {
        const auto error = StartDevice();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
    }

    void StopDevice()
    {
        Device->Stop().Wait();
    }

    TResultOrError<TReadPagesResult> ReadPages(const TPageRefs& refs)
    {
        TVector<TPageRangeRef> rangeRefs;
        for (const auto& [firstPageNo, pageCount]: refs) {
            rangeRefs.push_back(
                {.FirstPageNo = firstPageNo, .PageCount = pageCount});
        }

        return Device->ReadPages(std::move(rangeRefs)).GetValueSync();
    }

    NProto::TError WriteLogRecord()
    {
        const TString page = "W10";

        TVector<TPageRange> ranges;
        auto& range = ranges.emplace_back();
        range.FirstPageNo = 10;
        range.Pages.emplace_back(page.data(), page.size());

        return Device->WriteLogRecord(std::move(ranges), 1, 0).GetValueSync();
    }

    NProto::TError ReadJournalTail()
    {
        return Device->ReadJournalTail(0, 0).GetValueSync().GetError();
    }

    NProto::TError AdvanceWatermark()
    {
        return Device->AdvanceWatermark(1).GetValueSync();
    }

    NProto::TError WritePages(const TPageRefs& refs = {{10, 2}, {20, 1}})
    {
        return Device->WritePages(MakePageRanges(refs)).GetValueSync();
    }

    void HoldFirstDataStoreWrite()
    {
        DataStore->FirstWriteResult = NewPromise<NProto::TError>();
    }

    void WaitForFirstDataStoreWrite()
    {
        UNIT_ASSERT(
            DataStore->FirstWriteStarted.WaitT(TDuration::Seconds(30)));
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
    Y_UNIT_TEST_F(ShouldReadAllPagesFromJournal, TFixture)
    {
        Journal->JournalRanges = {{10, 4}};
        Journal->LastAckedLsn = 42;

        const auto response = ReadPages({{10, 4}});

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
        UNIT_ASSERT_VALUES_EQUAL("J10,J11,J12,J13", DescribePages(response));
        UNIT_ASSERT_VALUES_EQUAL(42, response.GetResult().LastAckedLsn);

        // the data store has not been touched at all

        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetReadRequests().size());
    }

    Y_UNIT_TEST_F(ShouldReadAllPagesFromDataStore, TFixture)
    {
        Journal->JournalRanges = {};
        Journal->LastAckedLsn = 7;

        const auto response = ReadPages({{10, 4}});

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        // the whole request has been forwarded to the data store as is

        const auto deviceRequests = DataStore->GetReadRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, deviceRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "10+4",
            DescribeRefs(deviceRequests[0]));

        UNIT_ASSERT_VALUES_EQUAL("D10,D11,D12,D13", DescribePages(response));

        // the lsn is taken from the journal response

        UNIT_ASSERT_VALUES_EQUAL(7, response.GetResult().LastAckedLsn);
    }

    Y_UNIT_TEST_F(ShouldRequestOnlyMissingPagesFromDataStore, TFixture)
    {
        // the journal covers the middle of the requested range

        Journal->JournalRanges = {{12, 3}};
        Journal->LastAckedLsn = 100;

        const auto response = ReadPages({{10, 10}});

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        const auto deviceRequests = DataStore->GetReadRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, deviceRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "10+2, 15+5",
            DescribeRefs(deviceRequests[0]));

        // the response is shaped after the request, not after the sources

        UNIT_ASSERT_VALUES_EQUAL(
            "D10,D11,J12,J13,J14,D15,D16,D17,D18,D19",
            DescribePages(response));
        UNIT_ASSERT_VALUES_EQUAL(100, response.GetResult().LastAckedLsn);
    }

    Y_UNIT_TEST_F(ShouldRequestMissingPagesForEveryPageGroupRef, TFixture)
    {
        // the first journal group covers the tail of the first ref and goes
        // beyond it, the second one covers the head of the second ref

        Journal->JournalRanges = {{2, 4}, {100, 2}};

        const auto response = ReadPages({{0, 4}, {100, 4}});

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        const auto deviceRequests = DataStore->GetReadRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, deviceRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "0+2, 102+2",
            DescribeRefs(deviceRequests[0]));

        UNIT_ASSERT_VALUES_EQUAL(
            "D0,D1,J2,J3,J100,J101,D102,D103",
            DescribePages(response));
    }

    Y_UNIT_TEST_F(ShouldIgnoreEmptyJournalPageGroups, TFixture)
    {
        // a page group without content covers nothing

        Journal->JournalRanges = {{10, 0}, {20, 2}};

        const auto response = ReadPages({{10, 4}, {20, 2}});

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        const auto deviceRequests = DataStore->GetReadRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, deviceRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "10+4",
            DescribeRefs(deviceRequests[0]));

        UNIT_ASSERT_VALUES_EQUAL(
            "D10,D11,D12,D13,J20,J21",
            DescribePages(response));
    }

    Y_UNIT_TEST_F(ShouldSkipEmptyPageGroupRefs, TFixture)
    {
        const auto response = ReadPages({{10, 0}, {20, 2}});

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        const auto deviceRequests = DataStore->GetReadRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, deviceRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "20+2",
            DescribeRefs(deviceRequests[0]));

        UNIT_ASSERT_VALUES_EQUAL("D20,D21", DescribePages(response));
    }

    Y_UNIT_TEST_F(ShouldShapeResponseAfterRequest, TFixture)
    {
        // the journal holds a wider page group than the requested one

        Journal->JournalRanges = {{8, 8}};
        Journal->LastAckedLsn = 42;

        const auto response = ReadPages({{10, 4}});

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        // only the requested pages are returned

        UNIT_ASSERT_VALUES_EQUAL("J10,J11,J12,J13", DescribePages(response));
        UNIT_ASSERT_VALUES_EQUAL(42, response.GetResult().LastAckedLsn);
        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetReadRequests().size());
    }

    Y_UNIT_TEST_F(ShouldFailIfDataStoreReturnsMorePagesThanAsked, TFixture)
    {
        Journal->JournalRanges = {{12, 2}};

        // the data store returns the pages the journal covers as well

        DataStore->ReadHandler = [] (const auto& rangeRefs) {
            Y_UNUSED(rangeRefs);
            return ServePagesFromDevice({{.FirstPageNo = 10, .PageCount = 4}});
        };

        const auto response = ReadPages({{10, 4}});

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    Y_UNIT_TEST_F(ShouldFailIfDataStoreMissesPage, TFixture)
    {
        Journal->JournalRanges = {{10, 1}};

        // the data store does not return the missing page

        DataStore->ReadHandler = [] (const auto& rangeRefs) {
            Y_UNUSED(rangeRefs);
            return TVector<TBuffer>();
        };

        const auto response = ReadPages({{10, 2}});

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    Y_UNIT_TEST_F(ShouldReadFromJournalAndDataStoreInParallel, TFixture)
    {
        Journal->JournalRanges = {{12, 2}};
        Journal->LastAckedLsn = 5;

        // the journal does not answer until the test lets it

        auto journalReadBarrier = NewPromise<void>();
        Journal->ReadBarrier = journalReadBarrier.GetFuture();

        TManualEvent dataStoreReadStarted;
        DataStore->ReadHandler = [&] (const auto& rangeRefs) {
            dataStoreReadStarted.Signal();
            return ServePagesFromDevice(rangeRefs);
        };

        auto future = Device->ReadPages({{.FirstPageNo = 10, .PageCount = 4}});

        // the data store is read while the journal read is still running

        UNIT_ASSERT(dataStoreReadStarted.WaitT(TDuration::Seconds(30)));
        UNIT_ASSERT(!future.HasValue());

        journalReadBarrier.SetValue();

        const auto response = future.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        const auto deviceRequests = DataStore->GetReadRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, deviceRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "10+2",
            DescribeRefs(deviceRequests[0]));

        UNIT_ASSERT_VALUES_EQUAL("D10,D11,J12,J13", DescribePages(response));
        UNIT_ASSERT_VALUES_EQUAL(5, response.GetResult().LastAckedLsn);
    }

    Y_UNIT_TEST_F(ShouldHandleJournalReadError, TFixture)
    {
        Journal->ReadError = MakeError(E_IO, "journal is broken");

        const auto response = ReadPages({{10, 4}});

        UNIT_ASSERT_VALUES_EQUAL(E_IO, response.GetError().GetCode());

        // the data store has not been touched

        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetReadRequests().size());
    }

    Y_UNIT_TEST_F(ShouldHandleDataStoreReadError, TFixture)
    {
        DataStore->ReadHandler = [] (const auto& rangeRefs)
            -> TResultOrError<TVector<TBuffer>>
        {
            Y_UNUSED(rangeRefs);
            return MakeError(E_IO, "device is broken");
        };

        const auto response = ReadPages({{10, 4}});

        UNIT_ASSERT_VALUES_EQUAL(E_IO, response.GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldServeRequests, TFixture)
    {
        Journal->JournalRanges = {{10, 1}};
        Journal->LastAckedLsn = 1;

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ReadPages({{10, 1}}).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteLogRecord().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, ReadJournalTail().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceWatermark().GetCode());
    }

    Y_UNIT_TEST_F(ShouldWritePagesToDataStorePastJournal, TFixture)
    {
        const auto error = WritePages();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        // the pages went to the data store as they are

        const auto writeRequests = DataStore->GetWriteRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, writeRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[P10,P11] 20:[P20]",
            DescribeRanges(writeRequests[0]));

        // the journal was not asked for anything

        UNIT_ASSERT_VALUES_EQUAL(0, Journal->GetReadRequests().size());
        UNIT_ASSERT_VALUES_EQUAL(0, Journal->GetFlushedLsns().size());
    }

    Y_UNIT_TEST_F(ShouldHandleDataStoreWritePagesError, TFixture)
    {
        DataStore->WriteHandler = [] (const auto& ranges) {
            Y_UNUSED(ranges);
            return MakeError(E_IO, "device is broken");
        };

        UNIT_ASSERT_VALUES_EQUAL(E_IO, WritePages().GetCode());
    }

    Y_UNIT_TEST_F(ShouldRejectWritePagesOverlappingWritePagesInFlight, TFixture)
    {
        HoldFirstDataStoreWrite();

        // pages 10, 11 and 20 stay in flight
        auto first = Device->WritePages(MakePageRanges());
        WaitForFirstDataStoreWrite();

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, WritePages({{11, 3}}).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, WritePages({{20, 1}}).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, DataStore->GetWriteRequests().size());

        // the pages between the busy ranges are free

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WritePages({{12, 8}}).GetCode());

        DataStore->FirstWriteResult.SetValue({});
        UNIT_ASSERT_VALUES_EQUAL(S_OK, first.GetValueSync().GetCode());

        // the pages are free again

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WritePages({{11, 3}}).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WritePages({{20, 1}}).GetCode());
    }

    Y_UNIT_TEST_F(ShouldReleasePagesAfterDataStoreWritePagesError, TFixture)
    {
        HoldFirstDataStoreWrite();

        auto first = Device->WritePages(MakePageRanges());
        WaitForFirstDataStoreWrite();

        DataStore->FirstWriteResult.SetValue(
            MakeError(E_IO, "device is broken"));
        UNIT_ASSERT_VALUES_EQUAL(E_IO, first.GetValueSync().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WritePages({{10, 2}}).GetCode());
    }

    Y_UNIT_TEST_F(ShouldAcceptWritePagesWithOverlappingRanges, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            WritePages({{10, 4}, {12, 4}, {11, 1}}).GetCode());

        // all the ranges have been released

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WritePages({{10, 6}}).GetCode());
    }

    Y_UNIT_TEST_F(ShouldPostponeFlushOverlappingWritePagesInFlight, TFixture)
    {
        HoldFirstDataStoreWrite();

        // pages 10, 11 and 20 stay in flight
        auto first = Device->WritePages(MakePageRanges());
        WaitForFirstDataStoreWrite();

        Journal->RestoreResponse = 1;
        Journal->AddRecordToFlush(MakeRecord(1, 11, 2));

        StartDeviceAndCheck();
        WaitForFlushCycle();

        // the flush cycle has left the record in the journal

        UNIT_ASSERT_VALUES_EQUAL(1, DataStore->GetWriteRequests().size());
        UNIT_ASSERT_VALUES_EQUAL(0, Journal->GetFlushedLsns().size());

        DataStore->FirstWriteResult.SetValue({});
        UNIT_ASSERT_VALUES_EQUAL(S_OK, first.GetValueSync().GetCode());

        WaitForAllRecordsToBeFlushed();
        StopDevice();

        const auto writes = DataStore->GetWriteRequests();
        UNIT_ASSERT_VALUES_EQUAL(2, writes.size());
        UNIT_ASSERT_VALUES_EQUAL("11:[J11,J12]", DescribeRanges(writes[1]));

        const auto flushedLsns = Journal->GetFlushedLsns();
        UNIT_ASSERT_VALUES_EQUAL(1, flushedLsns.size());
        UNIT_ASSERT_VALUES_EQUAL(1, flushedLsns[0]);
    }

    Y_UNIT_TEST_F(ShouldRejectWritePagesOverlappingFlushInFlight, TFixture)
    {
        HoldFirstDataStoreWrite();

        Journal->RestoreResponse = 1;
        Journal->AddRecordToFlush(MakeRecord(1, 10, 2));

        // the flush of pages 10 and 11 stays in flight
        StartDeviceAndCheck();
        WaitForFirstDataStoreWrite();

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, WritePages({{11, 1}}).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WritePages({{12, 1}}).GetCode());

        DataStore->FirstWriteResult.SetValue({});
        WaitForAllRecordsToBeFlushed();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WritePages({{11, 1}}).GetCode());
    }

    Y_UNIT_TEST_F(ShouldFlushJournalRecordsToDataStore, TFixture)
    {
        Journal->RestoreResponse = 3;
        Journal->AddRecordToFlush(MakeRecord(1, 10, 2));
        Journal->AddRecordToFlush(MakeRecord(2, 20, 1));
        Journal->AddRecordToFlush(MakeRecord(3, 30, 3));

        StartDeviceAndCheck();
        UNIT_ASSERT_VALUES_EQUAL(1, Journal->GetRestoreCount());

        WaitForAllRecordsToBeFlushed();
        StopDevice();

        // the records that are flushable at once go to the data store in a
        // single write

        const auto writes = DataStore->GetWriteRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, writes.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[J10,J11] 20:[J20] 30:[J30,J31,J32]",
            DescribeRanges(writes[0]));

        // and are acked in the journal as one batch, by the last lsn

        const auto flushedLsns = Journal->GetFlushedLsns();
        UNIT_ASSERT_VALUES_EQUAL(1, flushedLsns.size());
        UNIT_ASSERT_VALUES_EQUAL(3, flushedLsns[0]);

        // together with the pages that went to the data store

        const auto flushedRanges = Journal->GetFlushedRanges();
        UNIT_ASSERT_VALUES_EQUAL(1, flushedRanges.size());
        UNIT_ASSERT_VALUES_EQUAL("10+2 20+1 30+3", flushedRanges[0]);

        UNIT_ASSERT_GE(Journal->GetCleanupCount(), 1);
    }

    Y_UNIT_TEST_F(ShouldWriteLaterRecordOverEarlierOneWithinBatch, TFixture)
    {
        Journal->RestoreResponse = 3;
        Journal->AddRecordToFlush(MakeRecord(1, 10, 3, "J"));
        Journal->AddRecordToFlush(MakeRecord(2, 11, 1, "K"));
        Journal->AddRecordToFlush(MakeRecord(3, 13, 1, "L"));

        StartDeviceAndCheck();
        WaitForAllRecordsToBeFlushed();
        StopDevice();

        // a page two records of the batch write goes to the data store once,
        // with the content of the later record - and the pages next to each
        // other go in one range

        const auto writes = DataStore->GetWriteRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, writes.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[J10,K11,J12,L13]",
            DescribeRanges(writes[0]));

        // the journal is told about the merged range

        const auto flushedRanges = Journal->GetFlushedRanges();
        UNIT_ASSERT_VALUES_EQUAL(1, flushedRanges.size());
        UNIT_ASSERT_VALUES_EQUAL("10+4", flushedRanges[0]);
    }

    Y_UNIT_TEST_F(ShouldRetryFlushAfterDataStoreWriteError, TFixture)
    {
        std::atomic<ui32> writeCount = 0;

        DataStore->WriteHandler = [&](const auto& ranges) -> NProto::TError
        {
            Y_UNUSED(ranges);

            if (++writeCount == 1) {
                return MakeError(E_IO, "device is busy");
            }

            return {};
        };

        Journal->RestoreResponse = 1;
        Journal->AddRecordToFlush(MakeRecord(1, 10, 2));

        StartDeviceAndCheck();
        WaitForAllRecordsToBeFlushed();
        StopDevice();

        // the record has been written twice: the failed attempt and the retry

        const auto writes = DataStore->GetWriteRequests();
        UNIT_ASSERT_VALUES_EQUAL(2, writes.size());
        UNIT_ASSERT_VALUES_EQUAL("10:[J10,J11]", DescribeRanges(writes[0]));
        UNIT_ASSERT_VALUES_EQUAL("10:[J10,J11]", DescribeRanges(writes[1]));

        // the record has been acked only once, after the successful write

        const auto flushedLsns = Journal->GetFlushedLsns();
        UNIT_ASSERT_VALUES_EQUAL(1, flushedLsns.size());
        UNIT_ASSERT_VALUES_EQUAL(1, flushedLsns[0]);
    }

    Y_UNIT_TEST_F(ShouldNotFlushRecordsAboveRestoredLsn, TFixture)
    {
        // Restore reports lsn 2, so the record with lsn 3 is not indexed yet
        // and must stay in the journal until some writer advances the horizon.

        Journal->RestoreResponse = 2;
        Journal->AddRecordToFlush(MakeRecord(1, 10, 2));
        Journal->AddRecordToFlush(MakeRecord(2, 20, 1));
        Journal->AddRecordToFlush(MakeRecord(3, 30, 3));

        StartDeviceAndCheck();

        // the first cycle flushes lsn 1 and 2 and stops at lsn 3, the second
        // one finds nothing to flush at all

        WaitForFlushCycle();
        WaitForFlushCycle();
        StopDevice();

        const auto writes = DataStore->GetWriteRequests();
        UNIT_ASSERT_VALUES_EQUAL(1, writes.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[J10,J11] 20:[J20]",
            DescribeRanges(writes[0]));

        const auto flushedLsns = Journal->GetFlushedLsns();
        UNIT_ASSERT_VALUES_EQUAL(1, flushedLsns.size());
        UNIT_ASSERT_VALUES_EQUAL(2, flushedLsns[0]);
    }

    Y_UNIT_TEST_F(ShouldRejectRequestsWhenJournalRestoreFails, TFixture)
    {
        Journal->RestoreResponse = MakeError(E_IO, "journal is broken");
        Journal->AddRecordToFlush(MakeRecord(1, 10, 2));

        const auto startError = StartDevice();
        UNIT_ASSERT_VALUES_EQUAL(E_IO, startError.GetCode());

        const auto readResponse = ReadPages({{10, 4}});
        UNIT_ASSERT_VALUES_EQUAL(E_IO, readResponse.GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(E_IO, WriteLogRecord().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(E_IO, ReadJournalTail().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(E_IO, AdvanceWatermark().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(E_IO, WritePages().GetCode());

        StopDevice();

        // neither the requests nor the flush cycle got to the journal or the
        // data store

        UNIT_ASSERT_VALUES_EQUAL(0, Journal->GetReadRequests().size());
        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetReadRequests().size());
        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetWriteRequests().size());
        UNIT_ASSERT_VALUES_EQUAL(0, Journal->GetCleanupCount());
    }

    Y_UNIT_TEST_F(ShouldStopFlushCycle, TFixture)
    {
        StartDeviceAndCheck();
        WaitForAllRecordsToBeFlushed();
        StopDevice();

        const auto cleanupCount = Journal->GetCleanupCount();

        // nothing happens after the device has been stopped

        Journal->AddRecordToFlush(MakeRecord(1, 10, 2));

        UNIT_ASSERT_VALUES_EQUAL(cleanupCount, Journal->GetCleanupCount());
        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetWriteRequests().size());
    }

    Y_UNIT_TEST_F(ShouldWaitForRequestsInFlightOnStop, TFixture)
    {
        StartDeviceAndCheck();
        HoldFirstDataStoreWrite();

        auto write = Device->WritePages(MakePageRanges());
        WaitForFirstDataStoreWrite();

        auto stopped = Device->Stop();

        // the stop waits for the write

        UNIT_ASSERT(!stopped.Wait(TDuration::MilliSeconds(100)));
        UNIT_ASSERT(!write.HasValue());

        DataStore->FirstWriteResult.SetValue({});

        UNIT_ASSERT(stopped.Wait(TDuration::Seconds(30)));
        UNIT_ASSERT(write.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, write.GetValue().GetCode());
    }

    Y_UNIT_TEST_F(ShouldRejectRequestsAfterStop, TFixture)
    {
        StartDeviceAndCheck();
        StopDevice();

        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            ReadPages({{10, 1}}).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, WriteLogRecord().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, ReadJournalTail().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, AdvanceWatermark().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, WritePages({{10, 1}}).GetCode());

        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->GetWriteRequests().size());
    }
}

}   // namespace NCloud::NJournalled
