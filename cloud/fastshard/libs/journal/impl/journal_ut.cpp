#include "journal.h"

#include "device_page_store.h"
#include "key_buffer_store.h"
#include "log_record.h"
#include "memory_device.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/fastshard/libs/journal/iface/device.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/buffer.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/system/event.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>
#include <type_traits>
#include <utility>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultPageSize = 4;
constexpr ui64 DefaultPageCount = 64;
constexpr ui64 DevicePageCount = 1024;

// "<firstPageNo>x<pageCount>" pairs
using TGroups = TVector<std::pair<ui64 /*firstPageNo*/, ui64 /*pageCount*/>>;

////////////////////////////////////////////////////////////////////////////////

// Exactly DefaultPageSize bytes, the only size the page store accepts - the
// tag says which record wrote the page, e.g. "A010".
TString MakeContent(char tag, ui64 pageNo)
{
    return Sprintf("%c%03u", tag, static_cast<ui32>(pageNo % 1000));
}

TJournalRecord MakeRecord(
    ui64 lsn,
    ui64 prevLsn,
    char tag,
    const TGroups& groups)
{
    TJournalRecord record{.Lsn = lsn, .PrevLsn = prevLsn};

    for (const auto& [firstPageNo, pageCount]: groups) {
        auto& range = record.Ranges.emplace_back();
        range.FirstPageNo = firstPageNo;

        for (ui64 i = 0; i < pageCount; ++i) {
            const auto content = MakeContent(tag, firstPageNo + i);
            range.Pages.emplace_back(content.data(), content.size());
        }
    }

    return record;
}

TVector<TPageRangeRef> MakeRangeRefs(const TGroups& refs)
{
    TVector<TPageRangeRef> rangeRefs;

    for (const auto& [firstPageNo, pageCount]: refs) {
        rangeRefs.push_back(
            {.FirstPageNo = firstPageNo, .PageCount = pageCount});
    }

    return rangeRefs;
}

TPageRangeRef Range(ui64 firstPageNo, ui64 pageCount)
{
    return {.FirstPageNo = firstPageNo, .PageCount = pageCount};
}

// "10:[A010,A011] 20:[A020]"
TString DescribeRanges(const TVector<TPageRange>& ranges)
{
    TVector<TString> groups;

    for (const auto& range: ranges) {
        TVector<TStringBuf> pages;
        for (const auto& page: range.Pages) {
            pages.emplace_back(page.Data(), page.Size());
        }

        groups.push_back(
            TStringBuilder()
            << range.FirstPageNo << ":[" << JoinSeq(",", pages) << "]");
    }

    return JoinSeq(" ", groups);
}

// "2<-1 20:[B020]"
TString DescribeRecord(const TJournalRecord& record)
{
    TStringBuilder sb;
    sb << record.Lsn << "<-" << record.PrevLsn;

    if (const auto ranges = DescribeRanges(record.Ranges)) {
        sb << " " << ranges;
    }

    return sb;
}

// "1<-0 10:[A010]; 2<-1 20:[B020]"
TString DescribeRecords(const TResultOrError<TJournalTail>& tail)
{
    UNIT_ASSERT_C(!HasError(tail), FormatError(tail.GetError()));

    TVector<TString> records;

    for (const auto& record: tail.GetResult().Records) {
        records.push_back(DescribeRecord(record));
    }

    return JoinSeq("; ", records);
}

// What a journal read gives. The journal returns its pages as a plain list,
// so the page numbers are worked out from the request and the missing ranges.
struct TReadResponse
{
    NProto::TError Error;
    ui64 LastAckedLsn = 0;

    // "10:[A010,A011] 20:[A020]" - each run of pages that follow one another
    // in one range ref of the request
    TString Pages;

    const NProto::TError& GetError() const
    {
        return Error;
    }
};

TReadResponse DescribeRead(
    const TVector<TPageRangeRef>& rangeRefs,
    const TVector<TPageRangeRef>& missing,
    const TResultOrError<TReadPagesResult>& result)
{
    if (HasError(result)) {
        return {.Error = result.GetError()};
    }

    const auto& pages = result.GetResult().Pages;

    TVector<TPageRange> ranges;
    size_t pageIndex = 0;
    size_t missingIndex = 0;

    for (const auto& ref: rangeRefs) {
        bool extend = false;

        for (ui64 i = 0; i != ref.PageCount; ++i) {
            const ui64 pageNo = ref.FirstPageNo + i;

            while (missingIndex < missing.size() &&
                   missing[missingIndex].FirstPageNo +
                           missing[missingIndex].PageCount <=
                       pageNo)
            {
                ++missingIndex;
            }

            if (missingIndex < missing.size() &&
                missing[missingIndex].FirstPageNo <= pageNo)
            {
                extend = false;
                continue;
            }

            UNIT_ASSERT(pageIndex < pages.size());

            if (!extend) {
                ranges.push_back({.FirstPageNo = pageNo});
                extend = true;
            }

            const auto& page = pages[pageIndex++];
            ranges.back().Pages.emplace_back(page.Data(), page.Size());
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(pages.size(), pageIndex);

    return {
        .LastAckedLsn = result.GetResult().LastAckedLsn,
        .Pages = DescribeRanges(ranges)};
}

////////////////////////////////////////////////////////////////////////////////

// An in-memory device that can be told to fail its requests or to hold the
// reads or the writes until the test lets them through.
struct TTestDevice final: public IDevice
{
    const IDevicePtr Impl = CreateInMemoryDevice(DefaultPageSize);

    std::atomic<bool> FailReads = false;
    std::atomic<bool> FailWrites = false;

    struct TBlockedRead
    {
        TVector<TPageRangeRef> RangeRefs;
        TPromise<TResultOrError<TVector<TBuffer>>> Response;
    };

    struct TBlockedWrite
    {
        TVector<TPageRangeView> Ranges;
        TPromise<NProto::TError> Error;
    };

    TMutex Mutex;
    bool BlockReads = false;
    TManualEvent ReadBlocked;
    TVector<TBlockedRead> BlockedReads;
    bool BlockWrites = false;
    TManualEvent WriteBlocked;
    TVector<TBlockedWrite> BlockedWrites;

    TFuture<TResultOrError<TVector<TBuffer>>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) override
    {
        if (FailReads.load()) {
            return MakeFuture<TResultOrError<TVector<TBuffer>>>(
                MakeError(E_IO, "read failed"));
        }

        with_lock (Mutex) {
            if (BlockReads) {
                auto response =
                    NewPromise<TResultOrError<TVector<TBuffer>>>();
                BlockedReads.push_back(
                    {.RangeRefs = std::move(rangeRefs), .Response = response});
                ReadBlocked.Signal();
                return response.GetFuture();
            }
        }

        return Impl->ReadPages(std::move(rangeRefs));
    }

    TFuture<NProto::TError> WritePages(TVector<TPageRangeView> ranges) override
    {
        if (FailWrites.load()) {
            return MakeFuture(MakeError(E_IO, "write failed"));
        }

        with_lock (Mutex) {
            if (BlockWrites) {
                auto error = NewPromise<NProto::TError>();
                BlockedWrites.push_back(
                    {.Ranges = std::move(ranges), .Error = error});
                WriteBlocked.Signal();
                return error.GetFuture();
            }
        }

        return Impl->WritePages(std::move(ranges));
    }

    TFuture<NProto::TError> ZeroPages(TVector<TPageRangeRef> ranges) override
    {
        if (FailWrites.load()) {
            return MakeFuture(MakeError(E_IO, "zero failed"));
        }

        return Impl->ZeroPages(std::move(ranges));
    }

    void BlockReadsUntilReleased()
    {
        with_lock (Mutex) {
            BlockReads = true;
        }
    }

    void ReleaseReads()
    {
        TVector<TBlockedRead> blocked;

        with_lock (Mutex) {
            BlockReads = false;
            blocked.swap(BlockedReads);
            ReadBlocked.Reset();
        }

        for (auto& read: blocked) {
            read.Response.SetValue(
                Impl->ReadPages(std::move(read.RangeRefs)).GetValueSync());
        }
    }

    void BlockWritesUntilReleased()
    {
        with_lock (Mutex) {
            BlockWrites = true;
        }
    }

    void ReleaseWrites()
    {
        TVector<TBlockedWrite> blocked;

        with_lock (Mutex) {
            BlockWrites = false;
            blocked.swap(BlockedWrites);
            WriteBlocked.Reset();
        }

        for (auto& write: blocked) {
            write.Error.SetValue(
                Impl->WritePages(std::move(write.Ranges)).GetValueSync());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// The same for the meta store, which can also hold the writes until the test
// lets them through - the underlying store is reachable through |Impl| so
// that a test can look at what the journal has persisted and inject what a
// restart should find.
struct TTestKeyBufferStore final: public IKeyBufferStore
{
    const IKeyBufferStorePtr Impl = CreateInMemoryKeyBufferStore();

    std::atomic<bool> FailRestore = false;
    std::atomic<bool> FailWrites = false;

    struct TBlockedWrite
    {
        std::optional<ui64> Key;   // none for a metadata write
        TBuffer Buffer;
        TPromise<NProto::TError> Error;
    };

    TMutex Mutex;
    bool BlockWrites = false;
    TManualEvent WriteBlocked;
    TVector<TBlockedWrite> BlockedWrites;

    TFuture<TRestoreResult> Restore() override
    {
        if (FailRestore.load()) {
            return MakeFuture<TRestoreResult>(
                MakeError(E_IO, "restore failed"));
        }

        auto response = Impl->Restore().GetValueSync();
        if (HasError(response)) {
            return MakeFuture<TRestoreResult>(response.GetError());
        }

        // the interface promises no particular order, so hand the buffers
        // back reversed - restoring must not depend on how they arrive
        auto restored = response.ExtractResult();
        Reverse(restored.KeyBuffers.begin(), restored.KeyBuffers.end());

        return MakeFuture<TRestoreResult>(std::move(restored));
    }

    TResultOrError<TFuture<NProto::TError>> Write(
        ui64 key,
        TBuffer buffer) override
    {
        // a failing store rejects the record right away, as the device store
        // does when it runs out of pages
        if (FailWrites.load()) {
            return MakeError(E_IO, "meta write failed");
        }

        return WriteOrHold(key, std::move(buffer));
    }

    TFuture<NProto::TError> WriteMeta(TBuffer buffer) override
    {
        return WriteOrHold(std::nullopt, std::move(buffer));
    }

    TFuture<NProto::TError> EraseBelow(ui64 key) override
    {
        return Impl->EraseBelow(key);
    }

    void BlockWritesUntilReleased()
    {
        with_lock (Mutex) {
            BlockWrites = true;
        }
    }

private:
    TFuture<NProto::TError> WriteOrHold(std::optional<ui64> key, TBuffer buffer)
    {
        if (FailWrites.load()) {
            return MakeFuture(MakeError(E_IO, "meta write failed"));
        }

        with_lock (Mutex) {
            if (BlockWrites) {
                auto error = NewPromise<NProto::TError>();
                BlockedWrites.push_back(
                    {.Key = key, .Buffer = std::move(buffer), .Error = error});
                WriteBlocked.Signal();
                return error.GetFuture();
            }
        }

        return WriteThrough(key, std::move(buffer));
    }

    TFuture<NProto::TError> WriteThrough(
        std::optional<ui64> key,
        TBuffer buffer)
    {
        if (key) {
            auto futureOrError = Impl->Write(*key, std::move(buffer));
            if (HasError(futureOrError)) {
                return MakeFuture(futureOrError.GetError());
            }
            return futureOrError.ExtractResult();
        }
        return Impl->WriteMeta(std::move(buffer));
    }

public:

    void ReleaseWrites()
    {
        TVector<TBlockedWrite> blocked;

        with_lock (Mutex) {
            BlockWrites = false;
            blocked.swap(BlockedWrites);
            WriteBlocked.Reset();
        }

        for (auto& write: blocked) {
            write.Error.SetValue(
                WriteThrough(write.Key, std::move(write.Buffer))
                    .GetValueSync());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging;
    TExecutorPtr Executor;
    NMonitoring::TDynamicCountersPtr Counters;

    std::shared_ptr<TTestDevice> Device;
    std::shared_ptr<TTestKeyBufferStore> MetaStore;

    // the number of journal pages the next journal is given
    ui64 PageCount = DefaultPageCount;

    // the memory the next journal may use to keep the pages of its records,
    // enough for all of them unless a test says otherwise
    ui64 MaxCachedPageBytes = Max<ui64>();

    IDevicePageStorePtr DataStore;
    IJournalPtr Journal;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Logging = CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_RESOURCES});
        Logging->Start();

        Executor = TExecutor::Create("TestExecutor");
        Executor->Start();

        Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(Counters);

        Device = std::make_shared<TTestDevice>();
        MetaStore = std::make_shared<TTestKeyBufferStore>();

        RecreateJournal();
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        Executor->Stop();
        Logging->Stop();
    }

    // Builds a journal over the device and the meta store the previous one
    // used - what a restart does. The page store is rebuilt from scratch, its
    // allocation state is restored from the log records.
    void RecreateJournal()
    {
        DataStore = CreateDevicePageStore(
            Device,
            PageCount,
            DefaultPageSize,
            EDevicePageStoreMode::Checked);
        Journal = CreateJournal(
            Logging,
            Executor,
            MetaStore,
            DataStore,
            "test-device",
            DevicePageCount,
            MaxCachedPageBytes);
    }

    // The number of times the journal has reported the given critical event
    i64 CriticalEventCount(const TString& name) const
    {
        return Counters->GetCounter("AppCriticalEvents/" + name, true)->Val();
    }

    // Runs |func| on the executor thread - the journal waits on the futures
    // of its stores through the executor, so its methods belong there.
    template <typename F>
    auto Run(F func) -> std::invoke_result_t<F>
    {
        std::optional<std::invoke_result_t<F>> result;
        Executor->Execute([&] { result.emplace(func()); }).GetValueSync();
        return std::move(*result);
    }

    TResultOrError<ui64> Restore()
    {
        return Run([&] { return Journal->Restore(); });
    }

    ui64 RestoreOrFail()
    {
        auto result = Restore();
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
        return result.ExtractResult();
    }

    // The response is ready only once the record has made it into the page
    // index, which a record that does not continue the chain yet has not.
    // The journal takes a view of the record, so the returned future keeps
    // the record until the write is done.
    TFuture<NProto::TError> WriteAsync(TJournalRecord record)
    {
        auto holder = std::make_shared<TJournalRecord>(std::move(record));

        auto future = Run(
            [&]
            {
                return Journal->Write(
                    MakePageRangeViews(holder->Ranges),
                    holder->Lsn,
                    holder->PrevLsn);
            });

        return future.Apply([holder](const auto& future)
                            { return future.GetValue(); });
    }

    // Returns once the write is on the executor - for a write the test
    // holds up at a store.
    TFuture<NProto::TError> StartWrite(TJournalRecord record)
    {
        auto holder = std::make_shared<TJournalRecord>(std::move(record));

        auto future = Executor->Execute(
            [this, holder]
            {
                return Journal->Write(
                    MakePageRangeViews(holder->Ranges),
                    holder->Lsn,
                    holder->PrevLsn);
            });

        return future.Apply([holder](const auto& future)
                            { return future.GetValue(); });
    }

    ui32 Write(TJournalRecord record)
    {
        return WriteAsync(std::move(record)).GetValueSync().GetCode();
    }

    ui32 WriteRecord(ui64 lsn, ui64 prevLsn, char tag, const TGroups& groups)
    {
        return Write(MakeRecord(lsn, prevLsn, tag, groups));
    }

    TResultOrError<TJournalReadResult> StartRead(const TGroups& refs)
    {
        return Run([&] { return Journal->Read(MakeRangeRefs(refs)); });
    }

    TReadResponse ReadPages(const TGroups& refs)
    {
        auto result = StartRead(refs);
        if (HasError(result)) {
            return {.Error = result.GetError()};
        }

        const auto& [missing, future] = result.GetResult();
        return DescribeRead(
            MakeRangeRefs(refs),
            missing,
            future.GetValueSync());
    }

    // "5+2, 20+1" - the page ranges the reader has to take from the device
    TString MissingRanges(const TGroups& refs)
    {
        auto result = StartRead(refs);
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));

        TVector<TString> ranges;
        for (const auto& range: result.GetResult().MissingRanges) {
            ranges.push_back(
                TStringBuilder() << range.FirstPageNo << "+" << range.PageCount);
        }

        return JoinSeq(", ", ranges);
    }

    // The journal read does not wait for the device, so the future is not
    // set while the device holds the read.
    TFuture<TReadResponse> ReadPagesAsync(const TGroups& refs)
    {
        auto result = StartRead(refs);
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));

        const auto& [missing, future] = result.GetResult();
        return future.Apply(
            [rangeRefs = MakeRangeRefs(refs),
             missing = missing](const auto& future)
            { return DescribeRead(rangeRefs, missing, future.GetValue()); });
    }

    TResultOrError<TJournalTail> ReadTail(
        ui64 afterLsn,
        ui32 maxRecordCount = 0)
    {
        return Run([&] { return Journal->ReadTail(afterLsn, maxRecordCount); });
    }

    ui32 AdvanceLsnLowWatermark(ui64 lsn)
    {
        return Run([&] { return Journal->AdvanceLsnLowWatermark(lsn); })
            .GetCode();
    }

    // No limit on the lsn or the pages of the batch unless given one.
    TResultOrError<TJournalRecords> GetRecordsToFlush(
        ui64 maxAllowedLsn = Max<ui64>(),
        ui64 maxPageCount = Max<ui64>())
    {
        return Run(
            [&]
            { return Journal->GetRecordsToFlush(maxAllowedLsn, maxPageCount); });
    }

    // "1<-0 10:[A010]; 2<-1 20:[B020]", empty when there is nothing to flush
    TString GetRecordsToFlushDescription(
        ui64 maxAllowedLsn = Max<ui64>(),
        ui64 maxPageCount = Max<ui64>())
    {
        auto result = GetRecordsToFlush(maxAllowedLsn, maxPageCount);
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));

        TVector<TString> records;
        for (const auto& record: result.GetResult()) {
            records.push_back(DescribeRecord(record));
        }

        return JoinSeq("; ", records);
    }

    ui32 CleanupFlushedRecords()
    {
        return Run([&] { return Journal->CleanupFlushedRecords(); }).GetCode();
    }

    // Marks the records up to |lsn| as flushed. The ranges are the whole
    // device: these tests do not track which pages a batch has written.
    void MarkRecordAsFlushed(ui64 lsn)
    {
        Journal->MarkRecordAsFlushed(
            lsn,
            {Range(0, DevicePageCount)});
    }

    // Takes the given record through the whole flush cycle.
    void FlushUpTo(ui64 lsn)
    {
        MarkRecordAsFlushed(lsn);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, CleanupFlushedRecords());
    }

    // Meta store access, to look at what a restart would find and to plant
    // what it should choke on.

    void PutBuffer(ui64 key, TBuffer buffer)
    {
        auto futureOrError = MetaStore->Impl->Write(key, std::move(buffer));
        UNIT_ASSERT_C(
            !HasError(futureOrError),
            FormatError(futureOrError.GetError()));

        const auto error = futureOrError.ExtractResult().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
    }

    void PutGarbage(ui64 key, TStringBuf data)
    {
        PutBuffer(key, TBuffer(data.data(), data.size()));
    }

    // The crc of what the device holds at the given locations
    ui32 DevicePagesCrc(const TVector<TPageMapping>& pageMappings)
    {
        TVector<TPageRangeRef> locations;
        for (const auto& mapping: pageMappings) {
            locations.push_back(mapping.Location);
        }

        auto response = Device->Impl->ReadPages(locations).GetValueSync();
        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));
        return CalculatePagesCrc(response.GetResult());
    }

    // Stores a record under its lsn with the crc of its pages on the device
    void PutRecord(
        ui64 lsn,
        ui64 prevLsn,
        TVector<TPageMapping> pageMappings = {})
    {
        PutRecordUnderKey(lsn, lsn, prevLsn, std::move(pageMappings));
    }

    // Stores a record whose crc does not match its pages
    void PutTornRecord(ui64 lsn, ui64 prevLsn, TVector<TPageMapping> mappings)
    {
        const ui32 crc = ~DevicePagesCrc(mappings);
        PutRecordUnderKey(lsn, lsn, prevLsn, std::move(mappings), crc);
    }

    void PutRecordUnderKey(
        ui64 key,
        ui64 lsn,
        ui64 prevLsn,
        TVector<TPageMapping> pageMappings = {},
        std::optional<ui32> pagesCrc = std::nullopt)
    {
        TLogRecord record;
        record.Lsn = lsn;
        record.PrevLsn = prevLsn;
        record.PagesCrc = pagesCrc ? *pagesCrc : DevicePagesCrc(pageMappings);
        record.PageMappings = std::move(pageMappings);

        PutBuffer(key, SerializeRecord(record));
    }

    void PutMetaBuffer(TBuffer buffer)
    {
        const auto error =
            MetaStore->Impl->WriteMeta(std::move(buffer)).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
    }

    void PutMetadata(ui64 lsnLowWatermark)
    {
        PutMetaBuffer(SerializeMetadata(
            {.Version = CurrentFormatVersion,
             .LsnLowWatermark = lsnLowWatermark}));
    }

    TRestoredBuffers StoredBuffers()
    {
        auto response = MetaStore->Impl->Restore().GetValueSync();
        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));
        return response.ExtractResult();
    }

    // "1,2,meta" - the keys the meta store holds, a record under its lsn
    TString StoredKeys()
    {
        auto buffers = StoredBuffers().KeyBuffers;
        SortBy(buffers, [](const auto& keyBuffer) { return keyBuffer.Key; });

        TVector<TString> keys;
        for (const auto& [key, buffer]: buffers) {
            keys.push_back(ToString(key));
        }

        return JoinSeq(",", keys);
    }

    // "lsn=2" - the lsn low watermark the meta store holds in its metadata,
    // "none" before the first advance
    TString StoredMetadata()
    {
        auto buffer = StoredBuffers().MetaBuffer;
        if (buffer.Empty()) {
            return "none";
        }

        auto metadata = DeserializeMetadata(buffer);
        UNIT_ASSERT_C(metadata, "the stored metadata does not deserialize");
        return TStringBuilder() << "lsn=" << metadata->LsnLowWatermark;
    }

    // Writes three single page records, one page each, and returns with the
    // journal holding all of them.
    void WriteThreeRecords()
    {
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'C', {{30, 1}}));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalTest)
{
    //
    // Restoring
    //

    Y_UNIT_TEST_F(ShouldRestoreEmptyJournal, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        // and be usable right away
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreWhenMetaStoreFails, TFixture)
    {
        MetaStore->FailRestore.store(true);

        UNIT_ASSERT_VALUES_EQUAL(E_IO, Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldRestoreRecordsAndTheirContents, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{11, 1}}));

        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(2, RestoreOrFail());

        // the newest content of every journalled page survives the restart
        auto response = ReadPages({{10, 2}});
        UNIT_ASSERT_VALUES_EQUAL("10:[A010,B011]", response.Pages);
        UNIT_ASSERT_VALUES_EQUAL(2, response.LastAckedLsn);
    }

    Y_UNIT_TEST_F(ShouldRestoreLsnLowWatermark, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));

        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(3, RestoreOrFail());

        auto response = ReadTail(0);
        UNIT_ASSERT_VALUES_EQUAL(2, response.GetResult().LsnLowWatermark);
        UNIT_ASSERT_VALUES_EQUAL("3<-2 30:[C030]", DescribeRecords(response));

        // and it is not silently moved back
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, AdvanceLsnLowWatermark(2));
    }

    Y_UNIT_TEST_F(ShouldRestoreLogThatDoesNotStartAtFirstLsn, TFixture)
    {
        // what is left after the head of the log has been flushed away
        PutMetadata(6);
        PutRecord(5, 4);
        PutRecord(6, 5);

        UNIT_ASSERT_VALUES_EQUAL(6, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(7, 6, 'A', {{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldFlushRestoredLogThatDoesNotStartAtFirstLsn, TFixture)
    {
        PutMetadata(5);
        PutRecord(5, 4, {{.PageNo = 10, .Location = Range(0, 1)}});
        PutRecord(6, 5, {{.PageNo = 20, .Location = Range(1, 1)}});

        UNIT_ASSERT_VALUES_EQUAL(6, RestoreOrFail());

        // the flusher starts where the restored log starts, not at lsn 0 -
        // the pages were never written, so only the record is looked at
        UNIT_ASSERT_STRING_CONTAINS(
            GetRecordsToFlushDescription(),
            "5<-4 10:[");

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(6));
        MarkRecordAsFlushed(5);
        UNIT_ASSERT_STRING_CONTAINS(
            GetRecordsToFlushDescription(),
            "6<-5 20:[");
    }

    Y_UNIT_TEST_F(ShouldDropPendingRecordBehindGapOnRestore, TFixture)
    {
        // everything acked has been flushed and cleaned up, lsn 5 <- 4 was
        // written before lsns 3 and 4 made it: the chain still starts at 2
        PutMetadata(2);
        PutRecord(5, 4, {{.PageNo = 50, .Location = Range(0, 1)}});

        // the record past the gap was never acked, the restart drops it
        UNIT_ASSERT_VALUES_EQUAL(2, RestoreOrFail());
        UNIT_ASSERT_VALUES_EQUAL("", DescribeRecords(ReadTail(2)));

        // filling the gap does not bring it back, it has to be written again
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'C', {{30, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(4, 3, 'D', {{40, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "3<-2 30:[C030]; 4<-3 40:[D040]",
            DescribeRecords(ReadTail(2)));

        // its page is free: the rewrite may take it
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(5, 4, 'E', {{50, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "3<-2 30:[C030]; 4<-3 40:[D040]; 5<-4 50:[E050]",
            DescribeRecords(ReadTail(2)));
    }

    Y_UNIT_TEST_F(ShouldRestoreAllocationStateOfRecords, TFixture)
    {
        PageCount = 4;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));

        RecreateJournal();
        UNIT_ASSERT_VALUES_EQUAL(1, RestoreOrFail());

        // the restored record still holds every page of the store
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            WriteRecord(2, 1, 'B', {{20, 1}}));

        // and its contents are where the mappings say they are
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011,A012,A013]",
            ReadPages({{10, 4}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreCorruptedMetadata, TFixture)
    {
        constexpr TStringBuf garbage = "not a metadata buffer";
        PutMetaBuffer(TBuffer(garbage.data(), garbage.size()));

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreCorruptedRecord, TFixture)
    {
        PutRecord(1, 0);
        PutGarbage(2, "not a log record");

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreRecordStoredUnderAnotherKey, TFixture)
    {
        PutRecordUnderKey(1, 7, 0);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreRecordWithBrokenChain, TFixture)
    {
        // a record cannot follow itself
        PutRecord(5, 5);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreRecordChainingFromAboveItself, TFixture)
    {
        // the watermark points at the record, so the walk back from it would
        // never end
        PutMetadata(5);
        PutRecord(5, 5);

        auto result = Restore();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, result.GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldRestoreJournalHoldingStrandedRecord, TFixture)
    {
        // a journal of three pages, all of them taken before the restart
        PageCount = 3;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        // lsn 3 chains from lsn 2, but lsn 4 continues from lsn 1: once lsn 1
        // arrives the chain is 0 -> 1 -> 4 and lsn 3 is stranded, durable and
        // still holding its page
        auto third = WriteAsync(MakeRecord(3, 2, 'C', {{30, 1}}));
        auto fourth = WriteAsync(MakeRecord(4, 1, 'D', {{40, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fourth.GetValueSync().GetCode());
        UNIT_ASSERT(!third.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("1,3,4", StoredKeys());

        RecreateJournal();

        // the restart drops the stranded record along with everything else
        // past the chain and restores the chain - a restore does not tell a
        // stranded record from one waiting behind a gap, so no critical event
        UNIT_ASSERT_VALUES_EQUAL(4, RestoreOrFail());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            CriticalEventCount("JournalStrandedRecordDetectedError"));
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 40:[D040]",
            ReadPages({{10, 1}, {30, 1}, {40, 1}}).Pages);

        // its page is free again - the journal fits one more record
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(5, 4, 'E', {{50, 1}}));

        // and its meta store entry goes with the first cleanup past its key -
        // the erased records are all flushed, none of them is stranded
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(5));
        FlushUpTo(5);
        UNIT_ASSERT_VALUES_EQUAL("", StoredKeys());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            CriticalEventCount("JournalStrandedRecordDetectedError"));
    }

    Y_UNIT_TEST_F(ShouldDropStrandedRecordsAroundWatermark, TFixture)
    {
        // the chain 0 -> 1 -> 4 -> 6 is acked up to 4; lsn 3 <- 2 sits below
        // the watermark with a gap before it, lsn 5 <- 3 chains from below
        // the watermark and ends past it: neither can ever join
        PutMetadata(4);
        PutRecord(1, 0);
        PutRecord(4, 1);
        PutRecord(3, 2);
        PutRecord(6, 4);
        PutRecord(5, 3);

        UNIT_ASSERT_VALUES_EQUAL(6, RestoreOrFail());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            CriticalEventCount("JournalStrandedRecordDetectedError"));

        // the unacked tail is kept and can be continued
        UNIT_ASSERT_VALUES_EQUAL("6<-4", DescribeRecords(ReadTail(4)));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(7, 6, 'G', {{70, 1}}));
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreRecordsSharingSamePages, TFixture)
    {
        PutRecord(1, 0, {{.PageNo = 10, .Location = Range(0, 2)}});
        PutRecord(2, 1, {{.PageNo = 20, .Location = Range(1, 2)}});

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(
        ShouldFailToRestoreAckedRecordWithSelfIntersectingPages,
        TFixture)
    {
        // a single record whose own mappings share a journal page - there is
        // no telling which of the two the page belongs to
        PutMetadata(1);
        PutRecord(
            1,
            0,
            {{.PageNo = 10, .Location = Range(0, 2)},
             {.PageNo = 20, .Location = Range(1, 2)}});

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldDropUnackedRecordWithSelfIntersectingPages, TFixture)
    {
        // the same record is dropped while unacked: it cannot be checked
        PutRecord(
            1,
            0,
            {{.PageNo = 10, .Location = Range(0, 2)},
             {.PageNo = 20, .Location = Range(1, 2)}});

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());
        UNIT_ASSERT_VALUES_EQUAL("", DescribeRecords(ReadTail(0)));
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreRecordWithEmptyPageRange, TFixture)
    {
        // a write never produces an empty mapping, and the page index would
        // choke on it at the next write of the page
        PutRecord(1, 0, {{.PageNo = 10, .Location = Range(0, 0)}});

        auto result = Restore();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, result.GetError().GetCode());
    }

    Y_UNIT_TEST_F(
        ShouldFailToRestoreRecordWithIntersectingDevicePages,
        TFixture)
    {
        // a write with these page groups is rejected, so is the record
        PutRecord(
            1,
            0,
            {{.PageNo = 10, .Location = Range(0, 4)},
             {.PageNo = 12, .Location = Range(4, 4)}});

        auto result = Restore();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, result.GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreRecordStartingPastDevice, TFixture)
    {
        PutRecord(
            1,
            0,
            {{.PageNo = DevicePageCount, .Location = Range(0, 1)}});

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreRecordRunningPastDevice, TFixture)
    {
        PutRecord(1, 0, {{.PageNo = 10, .Location = Range(0, 1)}});
        PutRecord(
            2,
            1,
            {{.PageNo = DevicePageCount - 1, .Location = Range(1, 2)}});

        auto result = Restore();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, result.GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreOverflowingRecord, TFixture)
    {
        // a record left behind by a writer that did not bound page numbers;
        // indexing it would wrap around and abort
        const ui64 lastPageNo = Max<ui64>();
        PutRecord(
            1,
            0,
            {{.PageNo = lastPageNo, .Location = Range(0, 1)},
             {.PageNo = lastPageNo, .Location = Range(1, 1)}});

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldRestoreRecordEndingAtLastDevicePage, TFixture)
    {
        PutRecord(
            1,
            0,
            {{.PageNo = DevicePageCount - 2, .Location = Range(0, 2)}});

        UNIT_ASSERT_VALUES_EQUAL(1, RestoreOrFail());
    }

    Y_UNIT_TEST_F(ShouldDropUnackedRecordWhosePagesCannotBeRead, TFixture)
    {
        PageCount = 1;
        RecreateJournal();

        PutRecord(1, 0, {{.PageNo = 10, .Location = Range(0, 1)}});

        // an unacked record whose pages cannot be read is dropped as torn
        Device->FailReads.store(true);
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        // its page is free again
        Device->FailReads.store(false);
        UNIT_ASSERT_VALUES_EQUAL("", DescribeRecords(ReadTail(0)));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldDropTornRecordOnRestore, TFixture)
    {
        PageCount = 1;
        RecreateJournal();

        // lsn 2 made it to the meta store but its page did not, lsn 3 did
        PutMetadata(1);
        PutRecord(1, 0);
        PutTornRecord(2, 1, {{.PageNo = 20, .Location = Range(0, 1)}});
        PutRecord(3, 2);

        // the chain stops before the torn record, lsn 3 is dropped with it
        UNIT_ASSERT_VALUES_EQUAL(1, RestoreOrFail());
        UNIT_ASSERT_VALUES_EQUAL("", DescribeRecords(ReadTail(1)));
        UNIT_ASSERT_VALUES_EQUAL("", ReadPages({{20, 1}}).Pages);

        // and its page is free: the rewrite of lsn 2 fits, lsn 3 is written
        // again after it
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "2<-1 20:[B020]",
            DescribeRecords(ReadTail(1)));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'C', {}));
        UNIT_ASSERT_VALUES_EQUAL(
            "2<-1 20:[B020]; 3<-2",
            DescribeRecords(ReadTail(1)));
    }

    Y_UNIT_TEST_F(ShouldNotReadPagesOfAckedRecordsOnRestore, TFixture)
    {
        // an acked record has its pages written, there is nothing to check
        PutMetadata(2);
        PutRecord(1, 0, {{.PageNo = 10, .Location = Range(0, 1)}});
        PutRecord(2, 1, {{.PageNo = 20, .Location = Range(1, 1)}});

        Device->FailReads.store(true);

        UNIT_ASSERT_VALUES_EQUAL(2, RestoreOrFail());
    }

    Y_UNIT_TEST_F(ShouldDropRecordOfFailedPageWriteOnRestore, TFixture)
    {
        PageCount = 1;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        // the record of lsn 2 lands, its page does not: the page is freed
        Device->FailWrites.store(true);
        UNIT_ASSERT_VALUES_EQUAL(E_IO, WriteRecord(2, 1, 'B', {{20, 1}}));
        UNIT_ASSERT_VALUES_EQUAL("2", StoredKeys());

        // the page goes to lsn 1 next
        Device->FailWrites.store(false);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL("1,2", StoredKeys());

        RecreateJournal();

        // the restart tells the records sharing the page apart by crc
        UNIT_ASSERT_VALUES_EQUAL(1, RestoreOrFail());
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010]",
            ReadPages({{10, 1}, {20, 1}}).Pages);
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]",
            DescribeRecords(ReadTail(0)));
    }

    //
    // Writing
    //

    Y_UNIT_TEST_F(ShouldWriteAndReadBackPageGroup, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 3}}));

        auto response = ReadPages({{10, 3}});
        UNIT_ASSERT_VALUES_EQUAL("10:[A010,A011,A012]", response.Pages);
        UNIT_ASSERT_VALUES_EQUAL(1, response.LastAckedLsn);
    }

    Y_UNIT_TEST_F(ShouldAnswerRetryOfIndexedRecord, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        // a retry is answered by the record the chain still holds
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'X', {{30, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'X', {{10, 1}}));

        // and the retry has changed nothing
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 30:[C030]",
            ReadPages({{10, 1}, {30, 1}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldRejectRecordThatFollowsItself, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, WriteRecord(5, 5, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, WriteRecord(5, 6, 'A', {{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldRejectRecordThatContradictsChain, TFixture)
    {
        // held until lsn 2 shows up
        auto third = WriteAsync(MakeRecord(3, 2, 'C', {{30, 1}}));
        UNIT_ASSERT(!third.HasValue());

        // lsn 4 following lsn 1 leaves no room for lsn 2, so lsn 3 can never
        // be chained - but the chain only finds that out once the run passes
        // lsn 2 by
        auto fourth = WriteAsync(MakeRecord(4, 1, 'D', {{40, 1}}));
        UNIT_ASSERT(!fourth.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, fourth.GetValueSync().GetCode());
        UNIT_ASSERT(!third.HasValue());

        // erasing the run strands lsn 3 for good
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(4));
        FlushUpTo(4);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            third.GetValueSync().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            CriticalEventCount("JournalStrandedRecordDetectedError"));
        UNIT_ASSERT_VALUES_EQUAL("", ReadPages({{30, 1}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldIndexOutOfOrderWritesOnceGapIsFilled, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        auto second = WriteAsync(MakeRecord(2, 1, 'B', {{20, 1}}));
        auto third = WriteAsync(MakeRecord(3, 2, 'C', {{30, 1}}));

        // neither is acked and neither is visible while lsn 1 is missing
        UNIT_ASSERT(!second.HasValue());
        UNIT_ASSERT(!third.HasValue());

        auto response = ReadPages({{20, 1}, {30, 1}});
        UNIT_ASSERT_VALUES_EQUAL("", response.Pages);
        UNIT_ASSERT_VALUES_EQUAL(0, response.LastAckedLsn);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));

        // the whole run is applied at once
        UNIT_ASSERT_VALUES_EQUAL(S_OK, second.GetValueSync().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, third.GetValueSync().GetCode());

        response = ReadPages({{10, 1}, {20, 1}, {30, 1}});
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 20:[B020] 30:[C030]",
            response.Pages);
        UNIT_ASSERT_VALUES_EQUAL(3, response.LastAckedLsn);
    }

    Y_UNIT_TEST_F(ShouldDeduplicateRepeatedRecord, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        auto first = WriteAsync(MakeRecord(2, 1, 'B', {{20, 1}}));
        auto retry = WriteAsync(MakeRecord(2, 1, 'B', {{20, 1}}));

        UNIT_ASSERT(!first.HasValue());
        UNIT_ASSERT(!retry.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));

        // both callers are answered by the record that was inserted
        UNIT_ASSERT_VALUES_EQUAL(S_OK, first.GetValueSync().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, retry.GetValueSync().GetCode());

        // and the retry took no pages of its own
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]",
            DescribeRecords(ReadTail(0)));
    }

    Y_UNIT_TEST_F(ShouldWriteRecordWithoutPages, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {}));

        // it takes no pages but still moves the chain forward
        UNIT_ASSERT_VALUES_EQUAL("1<-0", DescribeRecords(ReadTail(0)));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));

        auto response = ReadPages({{20, 1}});
        UNIT_ASSERT_VALUES_EQUAL("20:[B020]", response.Pages);
        UNIT_ASSERT_VALUES_EQUAL(2, response.LastAckedLsn);
    }

    Y_UNIT_TEST_F(ShouldRejectWriteThatDoesNotFitJournal, TFixture)
    {
        PageCount = 4;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            WriteRecord(1, 0, 'A', {{10, 5}}));

        // the rejected record has taken nothing, the journal still fits four
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));
    }

    Y_UNIT_TEST_F(ShouldReleasePagesOfFailedDataWrite, TFixture)
    {
        PageCount = 4;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        Device->FailWrites.store(true);
        UNIT_ASSERT_VALUES_EQUAL(E_IO, WriteRecord(1, 0, 'A', {{10, 4}}));

        Device->FailWrites.store(false);

        // the failed record has left neither its pages nor its lsn behind
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011,A012,A013]",
            ReadPages({{10, 4}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldReleasePagesOfFailedMetaWrite, TFixture)
    {
        PageCount = 4;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        MetaStore->FailWrites.store(true);
        UNIT_ASSERT_VALUES_EQUAL(E_IO, WriteRecord(1, 0, 'A', {{10, 4}}));

        MetaStore->FailWrites.store(false);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));
        UNIT_ASSERT_VALUES_EQUAL("1", StoredKeys());
    }

    Y_UNIT_TEST_F(ShouldWritePagesAndRecordInParallel, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        Device->BlockWritesUntilReleased();
        auto write = StartWrite(MakeRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT(Device->WriteBlocked.WaitT(TDuration::Seconds(30)));

        // the record is in the meta store while its page is still in flight
        UNIT_ASSERT_VALUES_EQUAL("1", StoredKeys());
        UNIT_ASSERT(!write.HasValue());

        Device->ReleaseWrites();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, write.GetValueSync().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("10:[A010]", ReadPages({{10, 1}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldNotFreePagesBeforeFailedWriteIsOver, TFixture)
    {
        PageCount = 1;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        // the page write fails at once, the record write is still in flight
        MetaStore->BlockWritesUntilReleased();
        Device->FailWrites.store(true);
        auto write = StartWrite(MakeRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT(MetaStore->WriteBlocked.WaitT(TDuration::Seconds(30)));

        // the failure waits for the record write: the page is still taken
        UNIT_ASSERT(!write.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, WriteRecord(2, 1, 'B', {{20, 1}}));

        MetaStore->ReleaseWrites();
        UNIT_ASSERT_VALUES_EQUAL(E_IO, write.GetValueSync().GetCode());

        // and free once it is over
        Device->FailWrites.store(false);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldNotWritePagesOfRecordRejectedByMetaStore, TFixture)
    {
        PageCount = 1;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        // a page write, if there were one, would be held here
        Device->BlockWritesUntilReleased();
        MetaStore->FailWrites.store(true);
        auto write = StartWrite(MakeRecord(1, 0, 'A', {{10, 1}}));

        // the record write fails at once, so the write is over without it
        UNIT_ASSERT(write.Wait(TDuration::Seconds(30)));
        UNIT_ASSERT_VALUES_EQUAL(E_IO, write.GetValue().GetCode());
        with_lock (Device->Mutex) {
            UNIT_ASSERT(Device->BlockedWrites.empty());
        }

        // and its page is free again
        Device->ReleaseWrites();
        MetaStore->FailWrites.store(false);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldSpreadRecordOverFragmentedPages, TFixture)
    {
        PageCount = 5;
        RecreateJournal();

        // pages 0-1 go to lsn 1, pages 2-3 to lsn 2, page 4 stays free
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 2}}));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));
        FlushUpTo(1);

        // the three pages of lsn 3 land in two runs - 0-1 and 4
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'C', {{30, 3}}));

        UNIT_ASSERT_VALUES_EQUAL(
            "30:[C030,C031,C032]",
            ReadPages({{30, 3}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldPackSeveralPageGroupsIntoOneRun, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            WriteRecord(1, 0, 'A', {{10, 1}, {20, 2}}));

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 20:[A020,A021]",
            ReadPages({{10, 1}, {20, 2}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldRejectRecordWithIntersectingPageGroups, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        // there is no telling which group owns pages 12 and 13, and the tail
        // would hand both of them to whoever replays it
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(1, 0, 'A', {{10, 4}, {12, 4}}));

        // the rejected record has taken nothing: no chain entry, no pages
        UNIT_ASSERT_VALUES_EQUAL("", DescribeRecords(ReadTail(0)));
        UNIT_ASSERT_VALUES_EQUAL("", StoredKeys());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            WriteRecord(1, 0, 'A', {{10, 4}, {20, 4}}));

        // a group that covers no pages carries nothing to write
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(2, 1, 'B', {{20, 1}, {30, 0}}));
    }

    //
    // Reading
    //

    Y_UNIT_TEST_F(ShouldReturnOnlyJournalledPages, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 2}}));

        // pages 5 and 20 have never been journalled
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011]",
            ReadPages({{5, 2}, {10, 2}, {20, 1}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldClipMappingsToRequestedRange, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));

        UNIT_ASSERT_VALUES_EQUAL("11:[A011,A012]", ReadPages({{11, 2}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldReturnNewestContentOfPage, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 3}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{11, 1}}));

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,B011,A012]",
            ReadPages({{10, 3}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldStopReturningFlushedPages, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));

        MarkRecordAsFlushed(1);

        // lsn 1 is on the device now, the reader is expected to go there
        auto response = ReadPages({{10, 1}, {20, 1}});
        UNIT_ASSERT_VALUES_EQUAL("20:[B020]", response.Pages);
        UNIT_ASSERT_VALUES_EQUAL(2, response.LastAckedLsn);
    }

    Y_UNIT_TEST_F(ShouldReturnMissingRanges, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        // nothing is journalled yet, so every page is missing
        UNIT_ASSERT_VALUES_EQUAL(
            "5+2, 20+1",
            MissingRanges({{5, 2}, {20, 1}}));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{16, 1}}));

        // fully journalled ranges
        UNIT_ASSERT_VALUES_EQUAL("", MissingRanges({{10, 4}, {16, 1}}));
        UNIT_ASSERT_VALUES_EQUAL("", MissingRanges({{11, 2}}));

        // gaps before, between and after the journalled pages
        UNIT_ASSERT_VALUES_EQUAL(
            "8+2, 14+2, 17+3",
            MissingRanges({{8, 12}}));

        // every range gets its own gaps, in the order of the request
        UNIT_ASSERT_VALUES_EQUAL(
            "17+1, 30+2, 9+1, 14+1",
            MissingRanges({{16, 2}, {30, 2}, {9, 6}}));
    }

    Y_UNIT_TEST_F(ShouldReturnFlushedPagesAsMissing, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));

        UNIT_ASSERT_VALUES_EQUAL("", MissingRanges({{10, 1}, {20, 1}}));

        MarkRecordAsFlushed(1);

        // lsn 1 is on the device now, the same pages Read stops returning
        UNIT_ASSERT_VALUES_EQUAL("10+1", MissingRanges({{10, 1}, {20, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "20:[B020]",
            ReadPages({{10, 1}, {20, 1}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldReturnMissingRangesBeforeJournalIsRead, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 2}}));

        Device->BlockReadsUntilReleased();

        auto result = StartRead({{9, 4}});
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));

        // the missing ranges are known while the journal pages are still
        // being read
        const auto& [missing, future] = result.GetResult();
        UNIT_ASSERT_VALUES_EQUAL(2, missing.size());
        UNIT_ASSERT(Device->ReadBlocked.WaitT(TDuration::Seconds(30)));
        UNIT_ASSERT(!future.HasValue());

        Device->ReleaseReads();
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011]",
            DescribeRead(
                MakeRangeRefs({{9, 4}}),
                missing,
                future.GetValueSync())
                .Pages);
    }

    Y_UNIT_TEST_F(ShouldFailReadWhenDeviceFails, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));

        Device->FailReads.store(true);

        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            ReadPages({{10, 1}}).GetError().GetCode());

        // a request that touches no journalled page needs no device read
        Device->FailReads.store(false);
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ReadPages({{50, 1}}).GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldRejectRecordOutsideDevice, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        const ui64 last = DevicePageCount - 1;

        // starts past the end
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(1, 0, 'A', {{last + 1, 1}}));

        // starts inside, runs past the end
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(1, 0, 'A', {{last, 2}}));

        // a good group does not excuse a bad one
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(1, 0, 'A', {{10, 1}, {last, 2}}));

        // the end of these wraps around ui64, which used to hide them from
        // the intersection check and abort the page index
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(1, 0, 'A', {{Max<ui64>(), 1}, {Max<ui64>(), 1}}));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(1, 0, 'A', {{Max<ui64>() - 1, 3}}));

        // the rejected records have taken nothing: no chain entry, no pages
        UNIT_ASSERT_VALUES_EQUAL("", StoredKeys());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldWriteAndReadBackLastDevicePages, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        const ui64 last = DevicePageCount - 1;

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{last - 1, 2}}));

        auto response = ReadPages({{last - 1, 2}});
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("1022:[A022,A023]", response.Pages);

        // a ref that covers no pages asks for nothing, wherever it points
        response = ReadPages({{last + 1, 0}, {last - 1, 2}});
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldRejectReadOutsideDevice, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));

        const ui64 last = DevicePageCount - 1;

        auto code = [&](const TGroups& refs)
        {
            return ReadPages(refs).GetError().GetCode();
        };

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, code({{last + 1, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, code({{10, 1}, {last, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, code({{Max<ui64>(), 1}}));

        auto response = ReadPages({{last, 2}});
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldRejectReadWithIntersectingPageGroupRefs, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));

        auto response = ReadPages({{10, 2}, {11, 2}});
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());

        // the refs a request is allowed to hold still work
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 12:[A012,A013]",
            ReadPages({{10, 1}, {12, 2}}).Pages);
    }

    // Reading the tail

    Y_UNIT_TEST_F(ShouldReadWholeTail, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        auto response = ReadTail(0);
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]; 3<-2 30:[C030]",
            DescribeRecords(response));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetResult().LsnLowWatermark);
    }

    Y_UNIT_TEST_F(ShouldReadTailAfterGivenLsn, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        UNIT_ASSERT_VALUES_EQUAL(
            "3<-2 30:[C030]",
            DescribeRecords(ReadTail(2)));
        UNIT_ASSERT_VALUES_EQUAL("", DescribeRecords(ReadTail(3)));
    }

    Y_UNIT_TEST_F(ShouldLimitNumberOfTailRecords, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]",
            DescribeRecords(ReadTail(0, 2)));
    }

    Y_UNIT_TEST_F(ShouldStopTailAtGap, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));

        auto third = WriteAsync(MakeRecord(3, 2, 'C', {{30, 1}}));

        // lsn 3 is written and durable but the reader must not skip lsn 2
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]",
            DescribeRecords(ReadTail(0)));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, third.GetValueSync().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]; 3<-2 30:[C030]",
            DescribeRecords(ReadTail(0)));
    }

    Y_UNIT_TEST_F(ShouldNotReturnAckedRecordsInTail, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));

        // the acked records are of no use to the reader, even when it asks
        // from the very beginning
        auto response = ReadTail(0);
        UNIT_ASSERT_VALUES_EQUAL("3<-2 30:[C030]", DescribeRecords(response));
        UNIT_ASSERT_VALUES_EQUAL(2, response.GetResult().LsnLowWatermark);
    }

    Y_UNIT_TEST_F(ShouldFailTailReadWhenDeviceFails, TFixture)
    {
        // the pages are not kept in memory, so the tail is read from the
        // device
        MaxCachedPageBytes = 0;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));

        Device->FailReads.store(true);

        UNIT_ASSERT_VALUES_EQUAL(E_IO, ReadTail(0).GetError().GetCode());
    }

    // Advancing the lsn low watermark

    Y_UNIT_TEST_F(ShouldAdvanceLsnLowWatermark, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));
        UNIT_ASSERT_VALUES_EQUAL("1,2,3", StoredKeys());
        UNIT_ASSERT_VALUES_EQUAL("lsn=2", StoredMetadata());

        // it only moves forward
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, AdvanceLsnLowWatermark(2));
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, AdvanceLsnLowWatermark(1));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(3));

        UNIT_ASSERT_VALUES_EQUAL(3, ReadTail(0).GetResult().LsnLowWatermark);
    }

    Y_UNIT_TEST_F(ShouldRejectUnindexedLsnLowWatermark, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, AdvanceLsnLowWatermark(4));
        UNIT_ASSERT_VALUES_EQUAL(0, ReadTail(0).GetResult().LsnLowWatermark);
    }

    Y_UNIT_TEST_F(ShouldNotAdvanceLsnLowWatermarkWhenMetaStoreFails, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        MetaStore->FailWrites.store(true);
        UNIT_ASSERT_VALUES_EQUAL(E_IO, AdvanceLsnLowWatermark(2));

        MetaStore->FailWrites.store(false);
        UNIT_ASSERT_VALUES_EQUAL(0, ReadTail(0).GetResult().LsnLowWatermark);

        // and the failure has not blocked the next attempt
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));
    }

    Y_UNIT_TEST_F(ShouldRejectConcurrentAdvanceOfLsnLowWatermark, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        MetaStore->BlockWritesUntilReleased();

        auto first = Executor->Execute(
            [&] { return Journal->AdvanceLsnLowWatermark(2); });

        UNIT_ASSERT(MetaStore->WriteBlocked.WaitT(TDuration::Seconds(30)));

        // only one advance may be in flight at a time
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, AdvanceLsnLowWatermark(3));

        MetaStore->ReleaseWrites();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, first.GetValueSync().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(3));
    }

    // Flushing

    Y_UNIT_TEST_F(ShouldNotOfferUnackedRecordsToFlusher, TFixture)
    {
        WriteThreeRecords();

        // nothing may leave the journal before the writer has acked it
        UNIT_ASSERT_VALUES_EQUAL("", GetRecordsToFlushDescription());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]",
            GetRecordsToFlushDescription());

        // and the caller may hold it back further
        UNIT_ASSERT_VALUES_EQUAL("", GetRecordsToFlushDescription(0));
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]",
            GetRecordsToFlushDescription(1));
    }

    Y_UNIT_TEST_F(ShouldWalkRecordsToFlushInOrder, TFixture)
    {
        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]",
            GetRecordsToFlushDescription());

        // the batch starts right after the flushed lsn

        MarkRecordAsFlushed(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "2<-1 20:[B020]",
            GetRecordsToFlushDescription());

        MarkRecordAsFlushed(2);
        UNIT_ASSERT_VALUES_EQUAL("", GetRecordsToFlushDescription());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(3));
        UNIT_ASSERT_VALUES_EQUAL(
            "3<-2 30:[C030]",
            GetRecordsToFlushDescription());
    }

    Y_UNIT_TEST_F(ShouldLimitFlushBatch, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'C', {{30, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(3));

        const ui64 anyLsn = Max<ui64>();
        const ui64 anyPageCount = Max<ui64>();

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020,B021]; 3<-2 30:[C030]",
            GetRecordsToFlushDescription(anyLsn, anyPageCount));

        // the batch stops before the record that takes it past the page
        // budget
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020,B021]",
            GetRecordsToFlushDescription(anyLsn, 3));
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]",
            GetRecordsToFlushDescription(anyLsn, 2));

        // a record bigger than the whole budget is handed out on its own
        MarkRecordAsFlushed(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "2<-1 20:[B020,B021]",
            GetRecordsToFlushDescription(anyLsn, 1));
    }

    Y_UNIT_TEST_F(ShouldNotOfferUnwrittenRecordToFlusher, TFixture)
    {
        auto second = WriteAsync(MakeRecord(2, 1, 'B', {{20, 1}}));

        // lsn 1 has not been written at all, so the chain starts at a gap
        UNIT_ASSERT_VALUES_EQUAL("", GetRecordsToFlushDescription());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, second.GetValueSync().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]",
            GetRecordsToFlushDescription());
    }

    Y_UNIT_TEST_F(ShouldFailToFetchRecordsToFlushWhenDeviceFails, TFixture)
    {
        // the pages are not kept in memory, so the flush reads them back
        // from the device
        MaxCachedPageBytes = 0;
        RecreateJournal();

        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(1));

        Device->FailReads.store(true);

        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            GetRecordsToFlush().GetError().GetCode());
    }

    // Keeping the pages in memory

    Y_UNIT_TEST_F(ShouldFlushRecordsWithoutReadingDevice, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'C', {}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(3));

        Device->FailReads.store(true);

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010,A011]; 2<-1 20:[B020]; 3<-2",
            GetRecordsToFlushDescription());
    }

    Y_UNIT_TEST_F(ShouldReadTailWithoutReadingDevice, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        Device->FailReads.store(true);

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]; 3<-2 30:[C030]",
            DescribeRecords(ReadTail(0)));
    }

    Y_UNIT_TEST_F(ShouldReadBackRecordsThatDoNotFitIntoMemory, TFixture)
    {
        // the second record does not fit, the third one does
        MaxCachedPageBytes = 2 * DefaultPageSize;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'C', {{30, 1}}));

        const TString records =
            "1<-0 10:[A010]; 2<-1 20:[B020,B021]; 3<-2 30:[C030]";

        // the tail holds only the records that are not acked yet
        UNIT_ASSERT_VALUES_EQUAL(records, DescribeRecords(ReadTail(0)));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(3));
        UNIT_ASSERT_VALUES_EQUAL(records, GetRecordsToFlushDescription());

        Device->FailReads.store(true);

        // the first record alone does not need the device
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]",
            GetRecordsToFlushDescription(1));
        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            GetRecordsToFlush().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldReuseMemoryOfCleanedUpRecords, TFixture)
    {
        MaxCachedPageBytes = DefaultPageSize;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(1));
        FlushUpTo(1);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));

        Device->FailReads.store(true);

        UNIT_ASSERT_VALUES_EQUAL(
            "2<-1 20:[B020]",
            GetRecordsToFlushDescription());
    }

    Y_UNIT_TEST_F(ShouldReuseMemoryOfFailedWrite, TFixture)
    {
        MaxCachedPageBytes = DefaultPageSize;
        RecreateJournal();

        Device->FailWrites.store(true);
        UNIT_ASSERT_VALUES_EQUAL(E_IO, WriteRecord(1, 0, 'A', {{10, 1}}));
        Device->FailWrites.store(false);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'B', {{20, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(1));

        Device->FailReads.store(true);

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 20:[B020]",
            GetRecordsToFlushDescription());
    }

    Y_UNIT_TEST_F(ShouldKeepPagesOfUnackedRecordsReadOnRestore, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(1));

        RecreateJournal();
        UNIT_ASSERT_VALUES_EQUAL(3, RestoreOrFail());

        Device->FailReads.store(true);

        // the restore has checked the pages of the records past the
        // watermark, so they are in memory
        UNIT_ASSERT_VALUES_EQUAL(
            "2<-1 20:[B020]; 3<-2 30:[C030]",
            DescribeRecords(ReadTail(1)));

        // the acked record is trusted without a read, so its pages are read
        // back when it is flushed
        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            GetRecordsToFlush().GetError().GetCode());
    }

    // Cleaning up

    Y_UNIT_TEST_F(ShouldCleanupFlushedRecords, TFixture)
    {
        PageCount = 4;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));
        UNIT_ASSERT_VALUES_EQUAL("1,2", StoredKeys());

        FlushUpTo(1);

        // the record is gone from the meta store, its pages are free again
        UNIT_ASSERT_VALUES_EQUAL("2", StoredKeys());
        UNIT_ASSERT_VALUES_EQUAL("", ReadPages({{10, 2}}).Pages);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'C', {{30, 2}}));

        // and lsn 2 is still served from the journal
        UNIT_ASSERT_VALUES_EQUAL(
            "20:[B020,B021] 30:[C030,C031]",
            ReadPages({{20, 2}, {30, 2}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldCleanupNothingWhenNothingHasBeenFlushed, TFixture)
    {
        WriteThreeRecords();

        // the watermark sits at lsn zero, so there is nothing below it
        UNIT_ASSERT_VALUES_EQUAL(S_OK, CleanupFlushedRecords());
        UNIT_ASSERT_VALUES_EQUAL("1,2,3", StoredKeys());
    }

    Y_UNIT_TEST_F(ShouldRestoreJournalThatHasBeenCleanedUp, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(2));
        FlushUpTo(2);

        UNIT_ASSERT_VALUES_EQUAL("3", StoredKeys());
        UNIT_ASSERT_VALUES_EQUAL("lsn=2", StoredMetadata());

        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(3, RestoreOrFail());
        UNIT_ASSERT_VALUES_EQUAL(
            "30:[C030]",
            ReadPages({{10, 1}, {20, 1}, {30, 1}}).Pages);
    }

    Y_UNIT_TEST_F(ShouldNotCleanupWhatReaderIsStillLookingAt, TFixture)
    {
        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLsnLowWatermark(3));

        Device->BlockReadsUntilReleased();
        auto read = ReadPagesAsync({{10, 1}});
        UNIT_ASSERT(Device->ReadBlocked.WaitT(TDuration::Seconds(30)));

        // the reader has decided to take lsn 1 from the journal, so its pages
        // must stay put even though the flusher is done with them
        MarkRecordAsFlushed(2);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, CleanupFlushedRecords());
        UNIT_ASSERT_VALUES_EQUAL("1,2,3", StoredKeys());

        Device->ReleaseReads();
        UNIT_ASSERT_VALUES_EQUAL("10:[A010]", read.GetValueSync().Pages);

        // once it is done the cleanup goes through
        UNIT_ASSERT_VALUES_EQUAL(S_OK, CleanupFlushedRecords());
        UNIT_ASSERT_VALUES_EQUAL("3", StoredKeys());
    }
}

}   // namespace NCloud::NJournalled
