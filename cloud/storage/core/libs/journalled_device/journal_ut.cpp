#include "journal.h"

#include "device.h"
#include "device_page_store.h"
#include "key_buffer_store.h"
#include "log_record.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

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

// the key the journal keeps its metadata under
constexpr ui64 MetadataKey = Max<ui64>();

// "<firstPageNo>x<pageCount>" pairs
using TGroups = TVector<std::pair<ui64 /*firstPageNo*/, ui64 /*pageCount*/>>;

////////////////////////////////////////////////////////////////////////////////

// Exactly DefaultPageSize bytes, the only size the page store accepts - the
// tag says which record wrote the page, e.g. "A010".
TString MakeContent(char tag, ui64 pageNo)
{
    return Sprintf("%c%03u", tag, static_cast<ui32>(pageNo % 1000));
}

NCloud::NProto::TWriteLogRecordRequest MakeWriteRequest(
    ui64 lsn,
    ui64 prevLsn,
    char tag,
    const TGroups& groups)
{
    NCloud::NProto::TWriteLogRecordRequest request;
    request.SetLogSequenceNumber(lsn);
    request.SetPrevLogSequenceNumber(prevLsn);

    for (const auto& [firstPageNo, pageCount]: groups) {
        auto& group = *request.AddPageGroups();
        group.SetFirstPageNo(firstPageNo);

        for (ui64 i = 0; i < pageCount; ++i) {
            *group.AddContent() = MakeContent(tag, firstPageNo + i);
        }
    }

    return request;
}

NCloud::NProto::TReadPagesRequest MakeReadRequest(const TGroups& refs)
{
    NCloud::NProto::TReadPagesRequest request;

    for (const auto& [firstPageNo, pageCount]: refs) {
        auto& ref = *request.AddPageGroupRefs();
        ref.SetFirstPageNo(firstPageNo);
        ref.SetPageCount(pageCount);
        ref.SetPageSize(DefaultPageSize);
    }

    return request;
}

NCloud::NProto::TReadJournalTailRequest MakeReadTailRequest(
    ui64 afterLsn,
    ui32 maxRecordCount = 0)
{
    NCloud::NProto::TReadJournalTailRequest request;
    request.SetAfterLogSequenceNumber(afterLsn);
    request.SetMaxRecordCount(maxRecordCount);

    return request;
}

NCloud::NProto::TAdvanceLsnLowWatermarkRequest MakeAdvanceRequest(ui64 lsn)
{
    NCloud::NProto::TAdvanceLsnLowWatermarkRequest request;
    request.SetLsnLowWatermark(lsn);

    return request;
}

TPageRange Range(ui64 firstPageNo, ui64 pageCount)
{
    return {.FirstPageNo = firstPageNo, .PageCount = pageCount};
}

// "10:[A010,A011] 20:[A020]"
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

// "2<-1 20:[B020]", empty for the empty record the journal hands back when
// there is nothing to flush
TString DescribeRecord(const NCloud::NProto::TJournalRecord& record)
{
    if (!record.GetLogSequenceNumber() && !record.PageGroupsSize()) {
        return {};
    }

    TStringBuilder sb;
    sb << record.GetLogSequenceNumber() << "<-"
       << record.GetPrevLogSequenceNumber();

    if (const auto groups = DescribeGroups(record)) {
        sb << " " << groups;
    }

    return sb;
}

// "1<-0 10:[A010]; 2<-1 20:[B020]"
TString DescribeRecords(
    const NCloud::NProto::TReadJournalTailResponse& response)
{
    TVector<TString> records;

    for (const auto& record: response.GetRecords()) {
        records.push_back(DescribeRecord(record));
    }

    return JoinSeq("; ", records);
}

////////////////////////////////////////////////////////////////////////////////

// An in-memory device that can be told to fail its requests or to hold the
// reads until the test lets them through.
struct TTestDevice final: public IDevice
{
    const IDevicePtr Impl = CreateInMemoryDevice();

    std::atomic<bool> FailReads = false;
    std::atomic<bool> FailWrites = false;

    struct TBlockedRead
    {
        NCloud::NProto::TReadPagesRequest Request;
        TPromise<NCloud::NProto::TReadPagesResponse> Response;
    };

    TMutex Mutex;
    bool BlockReads = false;
    TManualEvent ReadBlocked;
    TVector<TBlockedRead> BlockedReads;

    TFuture<NCloud::NProto::TReadPagesResponse> ReadPages(
        NCloud::NProto::TReadPagesRequest request) override
    {
        if (FailReads.load()) {
            return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                TErrorResponse(E_IO, "read failed"));
        }

        with_lock (Mutex) {
            if (BlockReads) {
                auto response =
                    NewPromise<NCloud::NProto::TReadPagesResponse>();
                BlockedReads.push_back(
                    {.Request = std::move(request), .Response = response});
                ReadBlocked.Signal();
                return response.GetFuture();
            }
        }

        return Impl->ReadPages(std::move(request));
    }

    TFuture<NCloud::NProto::TWriteLogRecordResponse> WritePages(
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        if (FailWrites.load()) {
            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(E_IO, "write failed"));
        }

        return Impl->WritePages(std::move(request));
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
                Impl->ReadPages(std::move(read.Request)).GetValueSync());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// The same for the meta store - the underlying store is reachable through
// |Impl| so that a test can look at what the journal has persisted and inject
// what a restart should find.
struct TTestKeyBufferStore final: public IKeyBufferStore
{
    const IKeyBufferStorePtr Impl = CreateInMemoryKeyBufferStore();

    std::atomic<bool> FailRestore = false;
    std::atomic<bool> FailWrites = false;

    struct TBlockedWrite
    {
        ui64 Key = 0;
        TBuffer Buffer;
        TPromise<NCloud::NProto::TError> Error;
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
            return MakeFuture<TRestoreResult>(
                response.GetError());
        }

        // the interface promises no particular order, so hand the buffers
        // back reversed - restoring must not depend on how they arrive
        auto buffers = response.ExtractResult();
        Reverse(buffers.begin(), buffers.end());

        return MakeFuture<TRestoreResult>(std::move(buffers));
    }

    TFuture<NCloud::NProto::TError> Write(ui64 key, TBuffer buffer) override
    {
        if (FailWrites.load()) {
            return MakeFuture(MakeError(E_IO, "meta write failed"));
        }

        with_lock (Mutex) {
            if (BlockWrites) {
                auto error = NewPromise<NCloud::NProto::TError>();
                BlockedWrites.push_back(
                    {.Key = key,
                     .Buffer = std::move(buffer),
                     .Error = error});
                WriteBlocked.Signal();
                return error.GetFuture();
            }
        }

        return Impl->Write(key, std::move(buffer));
    }

    TFuture<NCloud::NProto::TError> EraseBelow(ui64 key) override
    {
        return Impl->EraseBelow(key);
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
                Impl->Write(write.Key, std::move(write.Buffer)).GetValueSync());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging;
    TExecutorPtr Executor;

    std::shared_ptr<TTestDevice> Device;
    std::shared_ptr<TTestKeyBufferStore> MetaStore;

    // the number of journal pages the next journal is given
    ui64 PageCount = DefaultPageCount;

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
        DataStore =
            CreateDevicePageStore(Device, PageCount, DefaultPageSize);
        Journal = CreateJournal(Logging, Executor, MetaStore, DataStore);
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
        return Run([&] { return Journal->Restore(); }).GetValueSync();
    }

    ui64 RestoreOrFail()
    {
        auto result = Restore();
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
        return result.ExtractResult();
    }

    // The response is ready only once the record has made it into the page
    // index, which a record that does not continue the chain yet has not.
    TFuture<NCloud::NProto::TWriteLogRecordResponse> WriteAsync(
        NCloud::NProto::TWriteLogRecordRequest request)
    {
        return Run(
            [&] { return Journal->Write(std::move(request)); });
    }

    ui32 Write(NCloud::NProto::TWriteLogRecordRequest request)
    {
        return WriteAsync(std::move(request)).GetValueSync().GetError()
            .GetCode();
    }

    ui32 WriteRecord(ui64 lsn, ui64 prevLsn, char tag, const TGroups& groups)
    {
        return Write(MakeWriteRequest(lsn, prevLsn, tag, groups));
    }

    NCloud::NProto::TReadPagesResponse ReadPages(const TGroups& refs)
    {
        return Run(
                   [&] { return Journal->Read(MakeReadRequest(refs)); })
            .GetValueSync();
    }

    TFuture<NCloud::NProto::TReadPagesResponse> ReadPagesAsync(
        const TGroups& refs)
    {
        return Executor->Execute(
            [this, request = MakeReadRequest(refs)]() mutable
            { return Journal->Read(std::move(request)); });
    }

    NCloud::NProto::TReadJournalTailResponse ReadTail(
        ui64 afterLsn,
        ui32 maxRecordCount = 0)
    {
        return Run(
                   [&]
                   {
                       return Journal->ReadTail(
                           MakeReadTailRequest(afterLsn, maxRecordCount));
                   })
            .GetValueSync();
    }

    ui32 AdvanceLastAckedLsn(ui64 lsn)
    {
        return Run(
                   [&] {
                       return Journal->AdvanceLastAckedLsn(
                           MakeAdvanceRequest(lsn));
                   })
            .GetValueSync()
            .GetError()
            .GetCode();
    }

    TResultOrError<NCloud::NProto::TJournalRecord> GetRecordToFlush(
        ui64 maxAllowedLsn = Max<ui64>())
    {
        return Run([&] { return Journal->GetRecordToFlush(maxAllowedLsn); })
            .GetValueSync();
    }

    TString GetRecordToFlushDescription(ui64 maxAllowedLsn = Max<ui64>())
    {
        auto result = GetRecordToFlush(maxAllowedLsn);
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
        return DescribeRecord(result.GetResult());
    }

    ui32 CleanupFlushedRecords()
    {
        return Run([&] { return Journal->CleanupFlushedRecords(); })
            .GetValueSync()
            .GetCode();
    }

    // Takes the given record through the whole flush cycle.
    void FlushUpTo(ui64 lsn)
    {
        Journal->MarkRecordAsFlushed(lsn);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, CleanupFlushedRecords());
    }

    // Meta store access, to look at what a restart would find and to plant
    // what it should choke on.

    void PutBuffer(ui64 key, TBuffer buffer)
    {
        const auto error =
            MetaStore->Impl->Write(key, std::move(buffer)).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
    }

    void PutGarbage(ui64 key, TStringBuf data)
    {
        PutBuffer(key, TBuffer(data.data(), data.size()));
    }

    void PutRecord(
        ui64 key,
        ui64 lsn,
        ui64 prevLsn,
        TVector<TPageMapping> pageMappings = {})
    {
        TLogRecord record;
        record.Lsn = lsn;
        record.PrevLsn = prevLsn;
        record.PageMappings = std::move(pageMappings);

        PutBuffer(key, SerializeRecord(record));
    }

    void PutMetadata(ui64 lastAckedLsn)
    {
        PutBuffer(
            MetadataKey,
            SerializeMetadata(
                {.Version = CurrentFormatVersion,
                 .LastAckedLsn = lastAckedLsn}));
    }

    // "0,1,meta" - the keys the meta store holds, a record under its prev lsn
    TString StoredKeys()
    {
        auto response = MetaStore->Impl->Restore().GetValueSync();
        UNIT_ASSERT_C(
            !HasError(response),
            FormatError(response.GetError()));

        auto buffers = response.ExtractResult();
        SortBy(buffers, [] (const auto& keyBuffer) { return keyBuffer.first; });

        TVector<TString> keys;
        for (const auto& [key, buffer]: buffers) {
            keys.push_back(key == MetadataKey ? "meta" : ToString(key));
        }

        return JoinSeq(",", keys);
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
    // Restoring

    Y_UNIT_TEST_F(ShouldRestoreAnEmptyJournal, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        // and be usable right away
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreWhenTheMetaStoreFails, TFixture)
    {
        MetaStore->FailRestore.store(true);

        UNIT_ASSERT_VALUES_EQUAL(E_IO, Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldRestoreTheRecordsAndTheirContents, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{11, 1}}));

        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(2, RestoreOrFail());

        // the newest content of every journalled page survives the restart
        auto response = ReadPages({{10, 2}});
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 11:[B011]",
            DescribeGroups(response));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            response.GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST_F(ShouldRestoreTheLastAckedLsn, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(2));

        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(3, RestoreOrFail());

        auto response = ReadTail(0);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            response.GetLastAckedLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(
            "3<-2 30:[C030]",
            DescribeRecords(response));

        // and it is not silently moved back
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, AdvanceLastAckedLsn(2));
    }

    Y_UNIT_TEST_F(ShouldRestoreALogThatDoesNotStartAtTheFirstLsn, TFixture)
    {
        // what is left after the head of the log has been flushed away
        PutMetadata(6);
        PutRecord(4, 5, 4);
        PutRecord(5, 6, 5);

        UNIT_ASSERT_VALUES_EQUAL(6, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(7, 6, 'A', {{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldRestoreTheAllocationStateOfTheRecords, TFixture)
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
            DescribeGroups(ReadPages({{10, 4}})));
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreCorruptedMetadata, TFixture)
    {
        PutGarbage(MetadataKey, "not a metadata buffer");

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreACorruptedRecord, TFixture)
    {
        PutRecord(0, 1, 0);
        PutGarbage(2, "not a log record");

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreARecordStoredUnderAnotherKey, TFixture)
    {
        PutRecord(1, 7, 0);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreARecordWithABrokenChain, TFixture)
    {
        // a record cannot follow itself
        PutRecord(5, 5, 5);

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreRecordsSharingTheSamePages, TFixture)
    {
        PutRecord(0, 1, 0, {{.PageNo = 10, .Location = Range(0, 2)}});
        PutRecord(1, 2, 1, {{.PageNo = 20, .Location = Range(1, 2)}});

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreARecordWithSelfIntersectingPages, TFixture)
    {
        // a single record whose own mappings share a journal page - there is
        // no telling which of the two the page belongs to
        PutRecord(
            0,
            1,
            0,
            {{.PageNo = 10, .Location = Range(0, 2)},
             {.PageNo = 20, .Location = Range(1, 2)}});

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            Restore().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToRestoreALogEndingBelowTheLastAckedLsn, TFixture)
    {
        PutMetadata(3);
        PutRecord(0, 1, 0);
        // lsn 2 is gone, so the log cannot be indexed up to lsn 3
        PutRecord(2, 3, 2);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            Restore().GetError().GetCode());
    }

    // Writing

    Y_UNIT_TEST_F(ShouldWriteAndReadBackAPageGroup, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 3}}));

        auto response = ReadPages({{10, 3}});
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011,A012]",
            DescribeGroups(response));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            response.GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST_F(ShouldRejectTheLsnReservedForTheMetadata, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(MetadataKey, 1, 'A', {{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldReportAnAlreadyIndexedLsn, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            WriteRecord(3, 2, 'X', {{30, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            WriteRecord(1, 0, 'X', {{10, 1}}));

        // and the retry has changed nothing
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 30:[C030]",
            DescribeGroups(ReadPages({{10, 1}, {30, 1}})));
    }

    Y_UNIT_TEST_F(ShouldRejectARecordThatFollowsItself, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(5, 5, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(5, 6, 'A', {{10, 1}}));
    }

    Y_UNIT_TEST_F(ShouldRejectARecordThatContradictsTheChain, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        // held until lsn 2 shows up
        auto third = WriteAsync(MakeWriteRequest(3, 2, 'C', {{30, 1}}));
        UNIT_ASSERT(!third.HasValue());

        // lsn 4 following lsn 1 leaves no room for lsn 2, so lsn 3 can never
        // be chained - but the chain only finds that out once the run passes
        // lsn 2 by
        auto fourth = WriteAsync(MakeWriteRequest(4, 1, 'D', {{40, 1}}));
        UNIT_ASSERT(!fourth.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            fourth.GetValueSync().GetError().GetCode());
        UNIT_ASSERT(!third.HasValue());

        // erasing the run strands lsn 3 for good
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(4));
        FlushUpTo(4);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            third.GetValueSync().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("", DescribeGroups(ReadPages({{30, 1}})));
    }

    Y_UNIT_TEST_F(ShouldIndexOutOfOrderWritesOnceTheGapIsFilled, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        auto second = WriteAsync(MakeWriteRequest(2, 1, 'B', {{20, 1}}));
        auto third = WriteAsync(MakeWriteRequest(3, 2, 'C', {{30, 1}}));

        // neither is acked and neither is visible while lsn 1 is missing
        UNIT_ASSERT(!second.HasValue());
        UNIT_ASSERT(!third.HasValue());

        auto response = ReadPages({{20, 1}, {30, 1}});
        UNIT_ASSERT_VALUES_EQUAL("", DescribeGroups(response));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            response.GetLastAckedLogSequenceNumber());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));

        // the whole run is applied at once
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            second.GetValueSync().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            third.GetValueSync().GetError().GetCode());

        response = ReadPages({{10, 1}, {20, 1}, {30, 1}});
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 20:[B020] 30:[C030]",
            DescribeGroups(response));
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            response.GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST_F(ShouldDeduplicateARepeatedRecord, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        auto first = WriteAsync(MakeWriteRequest(2, 1, 'B', {{20, 1}}));
        auto retry = WriteAsync(MakeWriteRequest(2, 1, 'B', {{20, 1}}));

        UNIT_ASSERT(!first.HasValue());
        UNIT_ASSERT(!retry.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));

        // both callers are answered by the record that was inserted
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            first.GetValueSync().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            retry.GetValueSync().GetError().GetCode());

        // and the retry took no pages of its own
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]",
            DescribeRecords(ReadTail(0)));
    }

    Y_UNIT_TEST_F(ShouldWriteARecordWithoutPages, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {}));

        // it takes no pages but still moves the chain forward
        UNIT_ASSERT_VALUES_EQUAL("1<-0", DescribeRecords(ReadTail(0)));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));

        auto response = ReadPages({{20, 1}});
        UNIT_ASSERT_VALUES_EQUAL("20:[B020]", DescribeGroups(response));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            response.GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST_F(ShouldRejectAWriteThatDoesNotFitTheJournal, TFixture)
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

    Y_UNIT_TEST_F(ShouldReleaseThePagesOfAFailedDataWrite, TFixture)
    {
        PageCount = 4;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        Device->FailWrites.store(true);
        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            WriteRecord(1, 0, 'A', {{10, 4}}));

        Device->FailWrites.store(false);

        // the failed record has left neither its pages nor its lsn behind
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011,A012,A013]",
            DescribeGroups(ReadPages({{10, 4}})));
    }

    Y_UNIT_TEST_F(ShouldReleaseThePagesOfAFailedMetaWrite, TFixture)
    {
        PageCount = 4;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        MetaStore->FailWrites.store(true);
        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            WriteRecord(1, 0, 'A', {{10, 4}}));

        MetaStore->FailWrites.store(false);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));
        UNIT_ASSERT_VALUES_EQUAL("0", StoredKeys());
    }

    Y_UNIT_TEST_F(ShouldSpreadARecordOverFragmentedPages, TFixture)
    {
        PageCount = 5;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        // pages 0-1 go to lsn 1, pages 2-3 to lsn 2, page 4 stays free
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 2}}));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(2));
        FlushUpTo(1);

        // the three pages of lsn 3 land in two runs - 0-1 and 4
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'C', {{30, 3}}));

        UNIT_ASSERT_VALUES_EQUAL(
            "30:[C030,C031] 32:[C032]",
            DescribeGroups(ReadPages({{30, 3}})));
    }

    Y_UNIT_TEST_F(ShouldPackSeveralPageGroupsIntoOneRun, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            WriteRecord(1, 0, 'A', {{10, 1}, {20, 2}}));

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 20:[A020,A021]",
            DescribeGroups(ReadPages({{10, 1}, {20, 2}})));
    }

    Y_UNIT_TEST_F(ShouldRejectARecordWithIntersectingPageGroups, TFixture)
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

        // a group that covers no pages cannot intersect anything
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            WriteRecord(2, 1, 'B', {{10, 1}, {10, 0}}));
    }

    // Reading

    Y_UNIT_TEST_F(ShouldReturnOnlyTheJournalledPages, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 2}}));

        // pages 5 and 20 have never been journalled
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010,A011]",
            DescribeGroups(ReadPages({{5, 2}, {10, 2}, {20, 1}})));
    }

    Y_UNIT_TEST_F(ShouldClipTheMappingsToTheRequestedRange, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));

        UNIT_ASSERT_VALUES_EQUAL(
            "11:[A011,A012]",
            DescribeGroups(ReadPages({{11, 2}})));
    }

    Y_UNIT_TEST_F(ShouldReturnTheNewestContentOfAPage, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 3}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{11, 1}}));

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 11:[B011] 12:[A012]",
            DescribeGroups(ReadPages({{10, 3}})));
    }

    Y_UNIT_TEST_F(ShouldStopReturningTheFlushedPages, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));

        Journal->MarkRecordAsFlushed(1);

        // lsn 1 is on the device now, the reader is expected to go there
        auto response = ReadPages({{10, 1}, {20, 1}});
        UNIT_ASSERT_VALUES_EQUAL("20:[B020]", DescribeGroups(response));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            response.GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST_F(ShouldFailAReadWhenTheDeviceFails, TFixture)
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

    Y_UNIT_TEST_F(ShouldRejectAReadWithIntersectingPageGroupRefs, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 4}}));

        auto response = ReadPages({{10, 2}, {11, 2}});
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            response.GetError().GetMessage(),
            "10x2 and 11x2 of a single request intersect");

        // the refs a request is allowed to hold still work - a ref covering
        // no pages cannot intersect anything, and the journal answers it
        // with a group holding nothing
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010] 11:[] 12:[A012,A013]",
            DescribeGroups(ReadPages({{10, 1}, {11, 0}, {12, 2}})));
    }

    // Reading the tail

    Y_UNIT_TEST_F(ShouldReadTheWholeTail, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        auto response = ReadTail(0);
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]; 3<-2 30:[C030]",
            DescribeRecords(response));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            response.GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST_F(ShouldReadTheTailAfterTheGivenLsn, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        UNIT_ASSERT_VALUES_EQUAL(
            "3<-2 30:[C030]",
            DescribeRecords(ReadTail(2)));
        UNIT_ASSERT_VALUES_EQUAL("", DescribeRecords(ReadTail(3)));
    }

    Y_UNIT_TEST_F(ShouldLimitTheNumberOfTailRecords, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]",
            DescribeRecords(ReadTail(0, 2)));
    }

    Y_UNIT_TEST_F(ShouldStopTheTailAtAGap, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));

        auto third = WriteAsync(MakeWriteRequest(3, 2, 'C', {{30, 1}}));

        // lsn 3 is written and durable but the reader must not skip lsn 2
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]",
            DescribeRecords(ReadTail(0)));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            third.GetValueSync().GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]; 2<-1 20:[B020]; 3<-2 30:[C030]",
            DescribeRecords(ReadTail(0)));
    }

    Y_UNIT_TEST_F(ShouldNotReturnAckedRecordsInTheTail, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(2));

        // the acked records are of no use to the reader, even when it asks
        // from the very beginning
        auto response = ReadTail(0);
        UNIT_ASSERT_VALUES_EQUAL(
            "3<-2 30:[C030]",
            DescribeRecords(response));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            response.GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST_F(ShouldFailATailReadWhenTheDeviceFails, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));

        Device->FailReads.store(true);

        UNIT_ASSERT_VALUES_EQUAL(E_IO, ReadTail(0).GetError().GetCode());
    }

    // Advancing the last acked lsn

    Y_UNIT_TEST_F(ShouldAdvanceTheLastAckedLsn, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(2));
        UNIT_ASSERT_VALUES_EQUAL("0,1,2,meta", StoredKeys());

        // it only moves forward
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, AdvanceLastAckedLsn(2));
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, AdvanceLastAckedLsn(1));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(3));

        UNIT_ASSERT_VALUES_EQUAL(
            3,
            ReadTail(0).GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST_F(ShouldRejectAnUnindexedLastAckedLsn, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, AdvanceLastAckedLsn(4));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            ReadTail(0).GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST_F(ShouldNotAdvanceTheLastAckedLsnWhenTheMetaStoreFails,
        TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        MetaStore->FailWrites.store(true);
        UNIT_ASSERT_VALUES_EQUAL(E_IO, AdvanceLastAckedLsn(2));

        MetaStore->FailWrites.store(false);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            ReadTail(0).GetLastAckedLogSequenceNumber());

        // and the failure has not blocked the next attempt
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(2));
    }

    Y_UNIT_TEST_F(ShouldRejectAConcurrentAdvanceOfTheLastAckedLsn, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        MetaStore->BlockWritesUntilReleased();

        auto first = Executor->Execute(
            [&] { return Journal->AdvanceLastAckedLsn(MakeAdvanceRequest(2)); });

        UNIT_ASSERT(MetaStore->WriteBlocked.WaitT(TDuration::Seconds(30)));

        // only one advance may be in flight at a time
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, AdvanceLastAckedLsn(3));

        MetaStore->ReleaseWrites();
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            first.GetValueSync().GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(3));
    }

    // Flushing

    Y_UNIT_TEST_F(ShouldNotOfferUnackedRecordsToTheFlusher, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        // nothing may leave the journal before the writer has acked it
        UNIT_ASSERT_VALUES_EQUAL("", GetRecordToFlushDescription());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(2));
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]",
            GetRecordToFlushDescription());

        // and the caller may hold it back further
        UNIT_ASSERT_VALUES_EQUAL("", GetRecordToFlushDescription(0));
    }

    Y_UNIT_TEST_F(ShouldWalkTheRecordsToFlushInOrder, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(2));

        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]",
            GetRecordToFlushDescription());

        Journal->MarkRecordAsFlushed(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "2<-1 20:[B020]",
            GetRecordToFlushDescription());

        Journal->MarkRecordAsFlushed(2);
        UNIT_ASSERT_VALUES_EQUAL("", GetRecordToFlushDescription());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(3));
        UNIT_ASSERT_VALUES_EQUAL(
            "3<-2 30:[C030]",
            GetRecordToFlushDescription());
    }

    Y_UNIT_TEST_F(ShouldNotOfferAnUnwrittenRecordToTheFlusher, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        auto second = WriteAsync(MakeWriteRequest(2, 1, 'B', {{20, 1}}));

        // lsn 1 has not been written at all, so the chain starts at a gap
        UNIT_ASSERT_VALUES_EQUAL("", GetRecordToFlushDescription());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            second.GetValueSync().GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(2));
        UNIT_ASSERT_VALUES_EQUAL(
            "1<-0 10:[A010]",
            GetRecordToFlushDescription());
    }

    Y_UNIT_TEST_F(ShouldFailToFetchARecordToFlushWhenTheDeviceFails, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(1));

        Device->FailReads.store(true);

        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            GetRecordToFlush().GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldCleanupTheFlushedRecords, TFixture)
    {
        PageCount = 4;
        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(2));
        UNIT_ASSERT_VALUES_EQUAL("0,1,meta", StoredKeys());

        FlushUpTo(1);

        // the record is gone from the meta store, its pages are free again
        UNIT_ASSERT_VALUES_EQUAL("1,meta", StoredKeys());
        UNIT_ASSERT_VALUES_EQUAL("", DescribeGroups(ReadPages({{10, 2}})));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'C', {{30, 2}}));

        // and lsn 2 is still served from the journal
        UNIT_ASSERT_VALUES_EQUAL(
            "20:[B020,B021] 30:[C030,C031]",
            DescribeGroups(ReadPages({{20, 2}, {30, 2}})));
    }

    Y_UNIT_TEST_F(ShouldCleanupNothingWhenNothingHasBeenFlushed, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();

        // the watermark sits at lsn zero, so there is nothing below it
        UNIT_ASSERT_VALUES_EQUAL(S_OK, CleanupFlushedRecords());
        UNIT_ASSERT_VALUES_EQUAL("0,1,2", StoredKeys());
    }

    Y_UNIT_TEST_F(ShouldRestoreAJournalThatHasBeenCleanedUp, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(2));
        FlushUpTo(2);

        UNIT_ASSERT_VALUES_EQUAL("2,meta", StoredKeys());

        RecreateJournal();

        UNIT_ASSERT_VALUES_EQUAL(3, RestoreOrFail());
        UNIT_ASSERT_VALUES_EQUAL(
            "30:[C030]",
            DescribeGroups(ReadPages({{10, 1}, {20, 1}, {30, 1}})));
    }

    Y_UNIT_TEST_F(ShouldNotCleanupWhatAReaderIsStillLookingAt, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RestoreOrFail());

        WriteThreeRecords();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, AdvanceLastAckedLsn(3));

        Device->BlockReadsUntilReleased();
        auto read = ReadPagesAsync({{10, 1}});
        UNIT_ASSERT(Device->ReadBlocked.WaitT(TDuration::Seconds(30)));

        // the reader has decided to take lsn 1 from the journal, so its pages
        // must stay put even though the flusher is done with them
        Journal->MarkRecordAsFlushed(2);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, CleanupFlushedRecords());
        UNIT_ASSERT_VALUES_EQUAL("0,1,2,meta", StoredKeys());

        Device->ReleaseReads();
        UNIT_ASSERT_VALUES_EQUAL(
            "10:[A010]",
            DescribeGroups(read.GetValueSync()));

        // once it is done the cleanup goes through
        UNIT_ASSERT_VALUES_EQUAL(S_OK, CleanupFlushedRecords());
        UNIT_ASSERT_VALUES_EQUAL("2,meta", StoredKeys());
    }
}

}   // namespace NCloud::NJournalled
