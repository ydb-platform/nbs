#include "journal.h"

#include "device_page_store.h"
#include "key_buffer_store.h"
#include "log_chain.h"
#include "log_index.h"
#include "lsn_barrier.h"

#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/hash.h>
#include <util/generic/scope.h>
#include <util/generic/utility.h>
#include <util/string/builder.h>

#include <atomic>
#include <utility>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TDevicePageGroups =
    google::protobuf::RepeatedPtrField<NCloud::NProto::TDevicePageGroup>;

////////////////////////////////////////////////////////////////////////////////

ui64 PageCountOf(const NCloud::NProto::TDevicePageGroupRef& ref)
{
    return ref.GetPageCount();
}

ui64 PageCountOf(const NCloud::NProto::TDevicePageGroup& group)
{
    return group.ContentSize();
}

ui64 PageCountOf(const TPageMapping& mapping)
{
    return mapping.Location.PageCount;
}

ui64 FirstPageNoOf(const NCloud::NProto::TDevicePageGroupRef& ref)
{
    return ref.GetFirstPageNo();
}

ui64 FirstPageNoOf(const NCloud::NProto::TDevicePageGroup& group)
{
    return group.GetFirstPageNo();
}

ui64 FirstPageNoOf(const TPageMapping& mapping)
{
    return mapping.PageNo;
}

bool IsInsideDevice(ui64 firstPageNo, ui64 pageCount, ui64 devicePageCount)
{
    return firstPageNo < devicePageCount &&
           pageCount <= devicePageCount - firstPageNo;
}

template <typename TRanges>
NCloud::NProto::TError ValidatePageRanges(
    const TRanges& ranges,
    ui64 devicePageCount)
{
    const auto count = static_cast<ui64>(ranges.size());

    for (ui64 i = 0; i < count; ++i) {
        const ui64 begin = FirstPageNoOf(ranges[i]);
        const ui64 pageCount = PageCountOf(ranges[i]);

        //
        // Check that the range is not empty and lies inside the device
        //

        if (!pageCount) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "page range at page " << begin << " is empty");
        }

        if (!IsInsideDevice(begin, pageCount, devicePageCount)) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "page range " << begin << "x" << pageCount
                    << " is outside the device of " << devicePageCount
                    << " pages");
        }

        const ui64 end = begin + pageCount;

        //
        // Check that the range does not intersect the ones before it
        //

        for (ui64 j = 0; j < i; ++j) {
            const ui64 otherPageCount = PageCountOf(ranges[j]);
            const ui64 otherBegin = FirstPageNoOf(ranges[j]);
            const ui64 otherEnd = otherBegin + otherPageCount;

            if (begin < otherEnd && otherBegin < end) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "page ranges " << otherBegin << "x" << otherPageCount
                        << " and " << begin << "x" << pageCount
                        << " intersect");
            }
        }
    }

    return {};
}

TVector<TPageRangeRef> GetLocations(const TVector<TPageMapping>& mappings)
{
    TVector<TPageRangeRef> locations;
    locations.reserve(mappings.size());

    for (const auto& mapping: mappings) {
        locations.push_back(mapping.Location);
    }

    return locations;
}

ui64 GetPageCount(const TVector<TPageMapping>& mappings)
{
    ui64 pageCount = 0;
    for (const auto& mapping: mappings) {
        pageCount += mapping.Location.PageCount;
    }

    return pageCount;
}

// Fills the page groups of a record with the buffers read for its mappings -
// one buffer per page, in the order of the mappings - starting at
// |*bufferIndex|, which is moved past the buffers taken.
NProto::TError FillPageGroups(
    const TVector<TPageMapping>& mappings,
    const TVector<TBuffer>& buffers,
    size_t* bufferIndex,
    TDevicePageGroups* pageGroups)
{
    for (const auto& [pageNo, location]: mappings) {
        if (*bufferIndex + location.PageCount > buffers.size()) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "not enough buffers for page " << pageNo);
        }

        auto& pageGroup = *pageGroups->Add();
        pageGroup.SetFirstPageNo(pageNo);

        for (ui64 i = 0; i < location.PageCount; ++i) {
            const auto& buffer = buffers[(*bufferIndex)++];
            pageGroup.AddContent(TString(buffer.Data(), buffer.Size()));
        }
    }
    return {};
}

TVector<TPageRangeRef> GetPageRanges(
    const NCloud::NProto::TWriteLogRecordRequest& request)
{
    TVector<TPageRangeRef> ranges;
    ranges.reserve(request.PageGroupsSize());

    for (const auto& group: request.GetPageGroups()) {
        ranges.push_back(
            {.FirstPageNo = group.GetFirstPageNo(),
             .PageCount = group.ContentSize()});
    }

    return ranges;
}

// TODO(#6956): return TVector<TArrayRef> to avoid unnecessary copying
TVector<TBuffer> GetPages(const NCloud::NProto::TWriteLogRecordRequest& request)
{
    TVector<TBuffer> pages;

    for (const auto& group: request.GetPageGroups()) {
        for (const auto& content: group.GetContent()) {
            pages.emplace_back(content.data(), content.size());
        }
    }

    return pages;
}

TLogRecordPtr CreateRecord(
    const NCloud::NProto::TWriteLogRecordRequest& request)
{
    auto record = std::make_shared<TLogRecord>();
    record->Lsn = request.GetLogSequenceNumber();
    record->PrevLsn = request.GetPrevLogSequenceNumber();
    record->Promise = NewPromise<NCloud::NProto::TWriteLogRecordResponse>();

    return record;
}

// Maps the device pages of a write request onto the page store locations
// allocated for them. The request ranges and the locations are laid out one
// after another in the same page order, so the i-th page of the request lands
// in the i-th page of the locations.
TVector<TPageMapping> CreatePageMappings(
    const TVector<TPageRangeRef>& requestPageRanges,
    const TVector<TPageRangeRef>& locations)
{
    auto totalPageCount = [](const TVector<TPageRangeRef>& ranges)
    {
        ui64 total = 0;
        for (const auto& range: ranges) {
            total += range.PageCount;
        }
        return total;
    };

    const ui64 pageCount = totalPageCount(requestPageRanges);

    // Verify that the locations hold exactly as many pages as the request -
    // the page store allocates the page count it is asked for
    STORAGE_VERIFY(
        pageCount == totalPageCount(locations),
        "PageCount",
        pageCount);

    TVector<TPageMapping> mappings;

    size_t requestIndex = 0;
    size_t locationIndex = 0;
    ui64 requestOffset = 0;
    ui64 locationOffset = 0;

    //
    // Walk both lists, emitting a mapping per contiguous run
    //

    while (requestIndex < requestPageRanges.size() &&
           locationIndex < locations.size())
    {
        const auto& requestRange = requestPageRanges[requestIndex];
        const auto& location = locations[locationIndex];

        const ui64 requestLeft = requestRange.PageCount - requestOffset;
        if (!requestLeft) {
            ++requestIndex;
            requestOffset = 0;
            continue;
        }

        const ui64 locationLeft = location.PageCount - locationOffset;
        if (!locationLeft) {
            ++locationIndex;
            locationOffset = 0;
            continue;
        }

        const ui64 runPageCount = Min(requestLeft, locationLeft);

        mappings.push_back(
            TPageMapping{
                .PageNo = requestRange.FirstPageNo + requestOffset,
                .Location = TPageRangeRef{
                    .FirstPageNo = location.FirstPageNo + locationOffset,
                    .PageCount = runPageCount}});

        requestOffset += runPageCount;
        if (requestOffset == requestRange.PageCount) {
            ++requestIndex;
            requestOffset = 0;
        }

        locationOffset += runPageCount;
        if (locationOffset == location.PageCount) {
            ++locationIndex;
            locationOffset = 0;
        }
    }

    return mappings;
}

////////////////////////////////////////////////////////////////////////////////

class TJournal final
    : public IJournal
    , public std::enable_shared_from_this<TJournal>
{
private:
    const ILoggingServicePtr Logging;
    const TExecutorPtr Executor;
    const IKeyBufferStorePtr MetaStore;
    const IDevicePageStorePtr DataStore;
    const ui64 DevicePageCount;

    TLog Log;

    TLogRecordChain LogRecordChain;
    TLogPageIndex LogPageIndex;
    mutable TLsnBarrier FlushedLsnBarrier;

    std::atomic<bool> AdvancingLsnLowWatermark = false;
    std::atomic<ui64> LsnLowWatermark = 0;

public:
    TJournal(
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        IKeyBufferStorePtr metaStore,
        IDevicePageStorePtr dataStore,
        ui64 devicePageCount);

    // Restores the journal state and returns lsn of the last indexed record
    [[nodiscard]] TResultOrError<ui64> Restore() override;

    [[nodiscard]] auto Write(NCloud::NProto::TWriteLogRecordRequest request)
        -> TFuture<NCloud::NProto::TWriteLogRecordResponse> override;

    [[nodiscard]] auto Read(NCloud::NProto::TReadPagesRequest request) const
        -> TFuture<NCloud::NProto::TReadPagesResponse> override;

    [[nodiscard]] auto ReadTail(
        NCloud::NProto::TReadJournalTailRequest request) const
        -> TFuture<NCloud::NProto::TReadJournalTailResponse> override;

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> override;

    [[nodiscard]] auto GetRecordsToFlush(
        ui64 maxAllowedLsn,
        ui64 maxPageCount) const
        -> TFuture<TResultOrError<TJournalRecords>> override;

    void MarkRecordAsFlushed(ui64 lsn) override;

    [[nodiscard]] auto CleanupFlushedRecords()
        -> TFuture<NCloud::NProto::TError> override;

private:
    TVector<TLogRecordPtr> FilterStrandedRecords(
        const THashMap<ui64, TLogRecordPtr>& lsnToRecord,
        ui64 headLsn,
        ui64 ackedLsn);

    NProto::TError ValidateRecordPages(const TLogRecord& record, ui64 ackedLsn);

    TResultOrError<ui32> ReadPagesCrc(const TLogRecord& record);

    NCloud::NProto::TError ValidateWriteRequest(
        const NCloud::NProto::TWriteLogRecordRequest& request) const;

    NProto::TError WriteLogRecord(
        TLogRecord& record,
        const NCloud::NProto::TWriteLogRecordRequest& request);

    void IndexChainedRecords();

    NCloud::NProto::TError ReadPageGroups(
        const TVector<TPageMapping>& mappings,
        TDevicePageGroups* pageGroups) const;
};

////////////////////////////////////////////////////////////////////////////////

TJournal::TJournal(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IKeyBufferStorePtr metaStore,
    IDevicePageStorePtr dataStore,
    ui64 devicePageCount)
    : Logging(std::move(logging))
    , Executor(std::move(executor))
    , MetaStore(std::move(metaStore))
    , DataStore(std::move(dataStore))
    , DevicePageCount(devicePageCount)
    , Log(Logging->CreateLog("BLOCKSTORE_JOURNALLED_DEVICE"))
{}

TResultOrError<ui64> TJournal::Restore()
{
    auto future = MetaStore->Restore();

    auto response = Executor->ExtractResponse(std::move(future));
    if (HasError(response)) {
        return response.GetError();
    }

    auto restored = response.ExtractResult();

    //
    // Take the lsn low watermark from the metadata - a journal that has
    // never advanced it holds no metadata
    //

    if (const auto& buffer = restored.MetaBuffer; !buffer.Empty()) {
        auto metadata = DeserializeMetadata(buffer);
        if (!metadata) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "failed to deserialize journal metadata from "
                    << buffer.Size() << " bytes");
        }

        LsnLowWatermark.store(metadata->LsnLowWatermark);
    }

    ui64 lsnLowWatermark = LsnLowWatermark.load();

    //
    // Deserialize and validate the records
    //

    THashMap<ui64, TLogRecordPtr> lsnToRecord;

    for (const auto& [key, buffer]: restored.KeyBuffers) {
        auto record = DeserializeRecord(buffer);
        if (!record) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "failed to deserialize log record with key " << key
                    << " from " << buffer.Size() << " bytes");
        }

        if (record->Lsn != key) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "log record with key " << key
                                 << " has lsn " << record->Lsn);
        }

        if (record->PrevLsn >= record->Lsn) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "log record with key " << key << " has lsn "
                    << record->Lsn << ", not above its prevLsn");
        }

        auto rangeError =
            ValidatePageRanges(record->PageMappings, DevicePageCount);
        if (HasError(rangeError)) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "log record with key " << key << ": "
                                 << rangeError.GetMessage());
        }

        // the store holds one buffer per key and the key is the lsn
        lsnToRecord.emplace(record->Lsn, record);
    }

    //
    // Walk the acked chain back from the watermark to find where it starts
    //

    ui64 headLsn = lsnLowWatermark;
    for (;;) {
        auto it = lsnToRecord.find(headLsn);
        if (it == lsnToRecord.end()) {
            break;
        }

        headLsn = it->second->PrevLsn;
    }

    //
    // Take the chain from its head, dropping everything that is not on it
    //

    auto records = FilterStrandedRecords(lsnToRecord, headLsn, lsnLowWatermark);

    //
    // Start the chain, the page index and the flushed lsn at the head of the
    // chain
    //

    LogRecordChain.InitLastErasedLsn(headLsn);
    LogPageIndex.InitLastIndexedLsn(headLsn);
    FlushedLsnBarrier.Advance(headLsn);

    //
    // Rebuild the page store allocation and the chain
    //

    for (const auto& record: records) {
        auto error = DataStore->AllocateAt(GetLocations(record->PageMappings));
        if (HasError(error)) {
            return error;
        }

        auto insertResult = LogRecordChain.Insert(record);
        if (HasError(insertResult)) {
            return insertResult.GetError();
        }

        bool marked = LogRecordChain.MarkAsReady(record->PrevLsn);
        STORAGE_VERIFY(marked, "MarkAsReady", record->PrevLsn);
    }

    //
    // Index the restored records that follow each other unbroken - the log
    // must reach at least the watermark, or the acked records are lost
    //

    IndexChainedRecords();

    ui64 lastIndexedLsn = LogPageIndex.GetLastIndexedLsn();
    if (lastIndexedLsn < lsnLowWatermark) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "restored log ends at lsn " << lastIndexedLsn
                             << ", below the lsn low watermark "
                             << lsnLowWatermark);
    }

    return lastIndexedLsn;
}

TVector<TLogRecordPtr> TJournal::FilterStrandedRecords(
    const THashMap<ui64, TLogRecordPtr>& lsnToRecord,
    ui64 headLsn,
    ui64 ackedLsn)
{
    //
    // Index the records by prev lsn - the chain is walked forward by it
    //

    THashMap<ui64, TLogRecordPtr> prevLsnToRecord;
    for (const auto& [_, record]: lsnToRecord) {
        auto [it, inserted] = prevLsnToRecord.emplace(record->PrevLsn, record);
        if (!inserted && record->Lsn > it->second->Lsn) {
            STORAGE_INFO(
                "record with lsn " << it->second->Lsn << " is replaced by lsn "
                                   << record->Lsn << " chaining from lsn "
                                   << record->PrevLsn);
            it->second = record;
        }
    }

    //
    // Walk the chain forward, in order. The chain ends at a torn record or at
    // a gap. Whatever is past it was never acked - a record is acked only once
    // it is on the chain - so it is dropped: the writer writes it again
    //

    TVector<TLogRecordPtr> records;

    ui64 tailLsn = headLsn;
    for (;;) {
        auto it = prevLsnToRecord.find(tailLsn);
        if (it == prevLsnToRecord.end()) {
            break;
        }

        auto error = ValidateRecordPages(*it->second, ackedLsn);
        if (HasError(error)) {
            STORAGE_WARN(
                "dropping the log record with lsn "
                << it->second->Lsn << " chaining from lsn " << tailLsn << ": "
                << FormatError(error));
            prevLsnToRecord.erase(it);
            break;
        }

        tailLsn = it->second->Lsn;
        records.push_back(std::move(it->second));
        prevLsnToRecord.erase(it);
    }

    return records;
}

NProto::TError TJournal::ValidateRecordPages(
    const TLogRecord& record,
    ui64 ackedLsn)
{
    //
    // The pages and the record are written in parallel, so a crash or a
    // failed page write leaves a torn record behind. An acked record is never
    // torn, so the records up to the watermark are trusted without a read
    //

    if (record.Lsn <= ackedLsn) {
        return {};
    }

    auto crcOrError = ReadPagesCrc(record);
    if (HasError(crcOrError)) {
        const auto& error = crcOrError.GetError();
        return MakeError(
            error.GetCode(),
            TStringBuilder()
                << "unable to read the pages back: " << error.GetMessage());
    }

    const ui32 crc = crcOrError.GetResult();
    if (crc != record.PagesCrc) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "the pages have crc " << crc << ", expected "
                             << record.PagesCrc);
    }

    return {};
}

TResultOrError<ui32> TJournal::ReadPagesCrc(const TLogRecord& record)
{
    auto locations = GetLocations(record.PageMappings);

    auto future = DataStore->Read(locations);
    const auto& response = Executor->WaitFor(future);

    if (HasError(response)) {
        return response.GetError();
    }

    return CalculatePagesCrc(response.GetResult());
}

TFuture<NCloud::NProto::TWriteLogRecordResponse> TJournal::Write(
    NCloud::NProto::TWriteLogRecordRequest request)
{
    using TResponse = NCloud::NProto::TWriteLogRecordResponse;

    if (auto error = ValidateWriteRequest(request); HasError(error)) {
        return MakeFuture<TResponse>(TErrorResponse(std::move(error)));
    }

    auto record = CreateRecord(request);

    //
    // Add the record to the chain, which holds it under its prev lsn - answer
    // a record repeating a held one, a retry, with the held record and do not
    // write it again
    //

    auto [inserted, insertError] = LogRecordChain.Insert(record);
    if (HasError(insertError)) {
        return MakeFuture<TResponse>(TErrorResponse(std::move(insertError)));
    }

    if (inserted != record) {
        return inserted->Promise.GetFuture();
    }

    //
    // Write the record, then take a failed one out of the chain and report
    // the error through its promise - to the duplicates that have joined it
    // while it was being written as well - or mark a written one as ready
    //

    auto error = WriteLogRecord(*record, request);

    const bool completed = HasError(error)
        ? LogRecordChain.Remove(record->PrevLsn)
        : LogRecordChain.MarkAsReady(record->PrevLsn);

    // Verify that the record was still held in the chain and not ready yet
    STORAGE_VERIFY(completed, "PrevLsn", record->PrevLsn);

    if (HasError(error)) {
        record->Promise.SetValue(TErrorResponse(error));
        return record->Promise.GetFuture();
    }

    //
    // Index the record if every record before it is already in the page
    // index, together with the records that have been waiting for it -
    // otherwise the record that fills the gap indexes it later
    //

    IndexChainedRecords();

    return record->Promise.GetFuture();
}

NCloud::NProto::TError TJournal::ValidateWriteRequest(
    const NCloud::NProto::TWriteLogRecordRequest& request) const
{
    return ValidatePageRanges(request.GetPageGroups(), DevicePageCount);
}

NProto::TError TJournal::WriteLogRecord(
    TLogRecord& record,
    const NCloud::NProto::TWriteLogRecordRequest& request)
{
    //
    // Allocate free journal pages for the pages of the request - they may be
    // split into several runs, so map every page of the request to its
    // journal page
    //

    auto requestPageRanges = GetPageRanges(request);
    auto pages = GetPages(request);

    auto locations = DataStore->Allocate(pages.size());
    if (locations.empty() && !pages.empty()) {
        return MakeError(
            E_REJECTED,
            TStringBuilder() << "not enough free journal pages to write "
                             << pages.size() << " pages");
    }

    bool success = false;

    Y_DEFER
    {
        if (!success) {
            auto freeError = DataStore->Free(locations);
            if (HasError(freeError)) {
                STORAGE_ERROR(
                    "unable to free the pages of the failed record with lsn "
                    << record.Lsn << ": " << FormatError(freeError));
            }
        }
    };

    record.PageMappings = CreatePageMappings(requestPageRanges, locations);
    record.PagesCrc = CalculatePagesCrc(pages);

    //
    // Issue the record write first: the log metadata rejects a record it cannot
    // take right away. Nothing is in flight then, so skip the page write and
    // free the pages at once

    auto futureOrError = MetaStore->Write(record.Lsn, SerializeRecord(record));
    if (HasError(futureOrError)) {
        return futureOrError.GetError();
    }

    auto metaFuture = futureOrError.ExtractResult();

    //
    // Write the pages in parallel with the record - the crc tells a restore
    // whether the pages are there. Wait for both: the pages of a failed
    // record are freed, so nothing may be writing to them by then
    //

    auto dataFuture = DataStore->Write(locations, pages);

    const auto& metaError = Executor->WaitFor(metaFuture);
    const auto& dataError = Executor->WaitFor(dataFuture);

    if (HasError(metaError)) {
        return metaError;
    }

    if (HasError(dataError)) {
        return dataError;
    }

    success = true;
    return {};
}

void TJournal::IndexChainedRecords()
{
    //
    // Index the ready records that continue the page index and complete
    // their promises
    //

    while (auto r = LogRecordChain.GetNext(LogPageIndex.GetLastIndexedLsn())) {
        bool applied = LogPageIndex.TryApplyNext(*r);

        // Verify that the page index has taken the record - it is the ready
        // one chained to the last indexed lsn, so it continues the index
        STORAGE_VERIFY(applied, "Lsn", r->Lsn);

        r->Promise.SetValue(TErrorResponse(S_OK));
    }
}

TFuture<NCloud::NProto::TReadPagesResponse> TJournal::Read(
    NCloud::NProto::TReadPagesRequest request) const
{
    using TResponse = NCloud::NProto::TReadPagesResponse;

    auto err = ValidatePageRanges(request.GetPageGroupRefs(), DevicePageCount);
    if (HasError(err)) {
        return MakeFuture<TResponse>(TErrorResponse(std::move(err)));
    }

    //
    // Pin the flushed lsn until the read is done: the records past it stay in
    // the journal, and the ones up to it are on the device already
    //

    auto lsnBarrierGuard = FlushedLsnBarrier.Acquire();

    //
    // Look up the journalled pages of the requested ranges, skipping the
    // flushed records - the reader takes their pages from the device
    //

    TVector<TPageRangeRef> ranges;
    ranges.reserve(request.PageGroupRefsSize());
    for (const auto& ref: request.GetPageGroupRefs()) {
        ranges.push_back(
            {.FirstPageNo = ref.GetFirstPageNo(),
             .PageCount = ref.GetPageCount()});
    }

    auto lookup = LogPageIndex.Lookup(ranges, lsnBarrierGuard.GetLsn());

    //
    // Fill the response with the content of the journalled pages and the lsn
    // the page index was looked up at
    //

    TResponse response;
    response.SetLastAckedLogSequenceNumber(lookup.LastIndexedLsn);

    auto error = ReadPageGroups(lookup.Mappings, response.MutablePageGroups());
    if (HasError(error)) {
        return MakeFuture<TResponse>(TErrorResponse(error));
    }

    return MakeFuture(std::move(response));
}

TFuture<NCloud::NProto::TReadJournalTailResponse> TJournal::ReadTail(
    NCloud::NProto::TReadJournalTailRequest request) const
{
    using TResponse = NCloud::NProto::TReadJournalTailResponse;

    //
    // Pin the flushed lsn, at least at the lsn the reader asks from, so that
    // the records past it stay in the journal until the read is done
    //

    auto lsnBarrierGuard =
        FlushedLsnBarrier.AcquireAtLeast(request.GetAfterLogSequenceNumber());

    //
    // Walk the ready records that follow each other unbroken past the pinned
    // lsn and the lsn low watermark - the acked records are of no use to the
    // reader, and a gap must not be skipped
    //

    ui64 lsnLowWatermark = LsnLowWatermark.load(std::memory_order_acquire);
    ui64 afterLsn = Max(lsnBarrierGuard.GetLsn(), lsnLowWatermark);

    auto records =
        LogRecordChain.GetReadyRun(afterLsn, request.GetMaxRecordCount());

    //
    // Fill the response with the records and their page contents
    //

    TResponse response;
    response.SetLsnLowWatermark(lsnLowWatermark);

    for (const auto& record: records) {
        auto& journalRecord = *response.AddRecords();
        journalRecord.SetLogSequenceNumber(record->Lsn);
        journalRecord.SetPrevLogSequenceNumber(record->PrevLsn);

        auto error = ReadPageGroups(
            record->PageMappings,
            journalRecord.MutablePageGroups());

        if (HasError(error)) {
            return MakeFuture<TResponse>(TErrorResponse(error));
        }
    }

    return MakeFuture(std::move(response));
}

TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse>
TJournal::AdvanceLsnLowWatermark(
    NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
{
    using TResponse = NCloud::NProto::TAdvanceLsnLowWatermarkResponse;

    //
    // Reject a watermark past the last indexed lsn - the writer may only ack
    // what the journal has indexed
    //

    const ui64 lsnLowWatermark = request.GetLsnLowWatermark();
    const ui64 lastIndexedLsn = LogPageIndex.GetLastIndexedLsn();

    if (lsnLowWatermark > lastIndexedLsn) {
        return MakeFuture<TResponse>(TErrorResponse(
            E_ARGUMENT,
            TStringBuilder()
                << "lsn low watermark " << lsnLowWatermark
                << " reaches past the last indexed lsn " << lastIndexedLsn));
    }

    //
    // Let one advance in at a time - the metadata write below yields, and
    // two of them racing could persist the older watermark last
    //

    if (AdvancingLsnLowWatermark.exchange(true, std::memory_order_acquire)) {
        return MakeFuture<TResponse>(TErrorResponse(
            E_REJECTED,
            TStringBuilder() << "another advance to lsn low watermark "
                             << lsnLowWatermark << " is already in progress"));
    }
    Y_DEFER
    {
        AdvancingLsnLowWatermark.store(false, std::memory_order_release);
    };

    if (lsnLowWatermark <= LsnLowWatermark.load(std::memory_order_acquire)) {
        return MakeFuture<TResponse>(TErrorResponse(S_ALREADY));
    }

    //
    // Persist the watermark in the journal metadata before taking it: a
    // restart must not move the lsn low watermark back
    //

    TJournalMetadata metadata = {
        .Version = CurrentFormatVersion,
        .LsnLowWatermark = lsnLowWatermark,
    };

    auto future = MetaStore->WriteMeta(SerializeMetadata(metadata));
    if (const auto& error = Executor->WaitFor(future); HasError(error)) {
        return MakeFuture<TResponse>(TErrorResponse(error));
    }

    LsnLowWatermark.store(lsnLowWatermark, std::memory_order_release);
    return MakeFuture<TResponse>();
}

TFuture<TResultOrError<TJournalRecords>> TJournal::GetRecordsToFlush(
    ui64 maxAllowedLsn,
    ui64 maxPageCount) const
{
    using TResult = TResultOrError<TJournalRecords>;

    //
    // Pin the flushed lsn and take the ready records right after it, in
    // chain order - stop at the first one that has not been acked yet or is
    // past what the caller allows
    //

    auto lsnBarrierGuard = FlushedLsnBarrier.Acquire();

    const ui64 lsnLowWatermark =
        LsnLowWatermark.load(std::memory_order_acquire);
    const ui64 maxLsn = Min(maxAllowedLsn, lsnLowWatermark);

    auto run = LogRecordChain.GetReadyRun(lsnBarrierGuard.GetLsn(), 0);

    //
    // Cut the run to the page budget: a record that takes the batch past it
    // waits for the next one, unless it is the first - a record bigger than
    // the whole budget still has to be flushed
    //

    TVector<TLogRecordPtr> records;
    TVector<TPageRangeRef> locations;
    ui64 pageCount = 0;

    for (const auto& record: run) {
        if (record->Lsn > maxLsn) {
            break;
        }

        const ui64 recordPageCount = GetPageCount(record->PageMappings);
        if (!records.empty() && pageCount + recordPageCount > maxPageCount) {
            break;
        }

        for (const auto& mapping: record->PageMappings) {
            locations.push_back(mapping.Location);
        }

        records.push_back(record);
        pageCount += recordPageCount;
    }

    if (records.empty()) {
        return MakeFuture<TResult>(TJournalRecords());
    }

    //
    // Read the pages of the whole batch in one go - one request to the device
    // for all the records, not one per record
    //

    auto future = DataStore->Read(locations);
    const auto& response = Executor->WaitFor(future);
    if (HasError(response)) {
        return MakeFuture<TResult>(response.GetError());
    }

    const auto& buffers = response.GetResult();

    TJournalRecords result;
    result.reserve(records.size());

    size_t bufferIndex = 0;
    for (const auto& record: records) {
        auto& journalRecord = result.emplace_back();
        journalRecord.SetLogSequenceNumber(record->Lsn);
        journalRecord.SetPrevLogSequenceNumber(record->PrevLsn);

        auto error = FillPageGroups(
            record->PageMappings,
            buffers,
            &bufferIndex,
            journalRecord.MutablePageGroups());

        if (HasError(error)) {
            return MakeFuture<TResult>(std::move(error));
        }
    }

    return MakeFuture<TResult>(std::move(result));
}

void TJournal::MarkRecordAsFlushed(ui64 lsn)
{
    FlushedLsnBarrier.Advance(lsn);
}

TFuture<NCloud::NProto::TError> TJournal::CleanupFlushedRecords()
{
    //
    // Erase the records up to the barrier lsn - the flushed ones that no
    // reader is looking at any more - from the meta store first: a restart
    // must not find a record whose pages have been reused
    //

    ui64 eraseUpToLsn = FlushedLsnBarrier.GetBarrierLsn();

    // The meta store holds the records by their lsn
    auto future = MetaStore->EraseBelow(eraseUpToLsn + 1);
    if (const auto& error = Executor->WaitFor(future); HasError(error)) {
        return MakeFuture(error);
    }

    //
    // Erase them from the chain and the page index, then release their pages
    //

    // The chain holds the records by their lsn
    auto recordsOrError = LogRecordChain.EraseBelow(eraseUpToLsn + 1);

    // Verify that the erased run is chained: the barrier never moves past a
    // record the chain does not hold ready
    STORAGE_VERIFY_C(
        !HasError(recordsOrError),
        "Lsn",
        eraseUpToLsn,
        FormatError(recordsOrError.GetError()));

    auto records = recordsOrError.ExtractResult();

    // The index holds the records by their lsn
    LogPageIndex.EraseBelow(eraseUpToLsn + 1);

    auto result = MakeError(S_OK);
    for (const auto& record: records) {
        //
        // Fail the writer still waiting on a ready record stranded below the
        // erased watermark - a flushed record has its promise set already
        //

        const TString message = TStringBuilder()
            << "record with lsn " << record->Lsn << " chaining from lsn "
            << record->PrevLsn << " can no longer join the chain";

        const bool stranded = record->Promise.TrySetValue(
            TErrorResponse(E_INVALID_STATE, message));

        if (stranded) {
            ReportJournalStrandedRecordDetectedError(message);
        }

        auto error = DataStore->Free(GetLocations(record->PageMappings));
        if (HasError(error)) {
            STORAGE_ERROR(
                "unable to free the pages of the flushed record with lsn "
                << record->Lsn << ": " << FormatError(error));
            result = error;
        }
    }

    return MakeFuture(result);
}

NCloud::NProto::TError TJournal::ReadPageGroups(
    const TVector<TPageMapping>& mappings,
    TDevicePageGroups* pageGroups) const
{
    auto future = DataStore->Read(GetLocations(mappings));
    const auto& response = Executor->WaitFor(future);
    if (HasError(response)) {
        return response.GetError();
    }

    size_t bufferIndex = 0;
    return FillPageGroups(
        mappings,
        response.GetResult(),
        &bufferIndex,
        pageGroups);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalPtr CreateJournal(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IKeyBufferStorePtr metaStore,
    IDevicePageStorePtr dataStore,
    ui64 devicePageCount)
{
    return std::make_shared<TJournal>(
        std::move(logging),
        std::move(executor),
        std::move(metaStore),
        std::move(dataStore),
        devicePageCount);
}

}   // namespace NCloud::NJournalled
