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
#include <mutex>
#include <utility>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 PageCountOf(const TPageRangeRef& ref)
{
    return ref.PageCount;
}

ui64 PageCountOf(const TPageRange& range)
{
    return range.Pages.size();
}

ui64 PageCountOf(const TPageMapping& mapping)
{
    return mapping.Location.PageCount;
}

ui64 FirstPageNoOf(const TPageRangeRef& ref)
{
    return ref.FirstPageNo;
}

ui64 FirstPageNoOf(const TPageRange& range)
{
    return range.FirstPageNo;
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
NProto::TError ValidatePageRanges(const TRanges& ranges, ui64 devicePageCount)
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

// Fills the page ranges of a record with the buffers read for its mappings -
// one buffer per page, in the order of the mappings. The buffers are moved
// out.
NProto::TError FillPageRanges(
    const TVector<TPageMapping>& mappings,
    TVector<TBuffer>& buffers,
    size_t firstBufferIndex,
    size_t* bufferCount,
    TVector<TPageRange>* ranges)
{
    ranges->reserve(ranges->size() + mappings.size());

    size_t bufferIndex = firstBufferIndex;
    for (const auto& [pageNo, location]: mappings) {
        if (bufferIndex + location.PageCount > buffers.size()) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "not enough buffers for page " << pageNo);
        }

        auto& range = ranges->emplace_back();
        range.FirstPageNo = pageNo;
        range.Pages.reserve(location.PageCount);

        for (ui64 i = 0; i < location.PageCount; ++i) {
            range.Pages.push_back(std::move(buffers[bufferIndex++]));
        }
    }

    *bufferCount = bufferIndex - firstBufferIndex;
    return {};
}

TVector<TPageRangeRef> GetPageRangeRefs(const TVector<TPageRange>& ranges)
{
    TVector<TPageRangeRef> rangeRefs;
    rangeRefs.reserve(ranges.size());

    for (const auto& range: ranges) {
        rangeRefs.push_back(
            {.FirstPageNo = range.FirstPageNo,
             .PageCount = range.Pages.size()});
    }

    return rangeRefs;
}

// Moves the pages out of the ranges, in the order of the ranges.
TVector<TBuffer> ExtractPages(TVector<TPageRange> ranges)
{
    size_t pageCount = 0;
    for (const auto& range: ranges) {
        pageCount += range.Pages.size();
    }

    TVector<TBuffer> pages;
    pages.reserve(pageCount);

    for (auto& range: ranges) {
        for (auto& page: range.Pages) {
            pages.push_back(std::move(page));
        }
    }

    return pages;
}

TLogRecordPtr CreateRecord(ui64 lsn, ui64 prevLsn)
{
    auto record = std::make_shared<TLogRecord>();
    record->Lsn = lsn;
    record->PrevLsn = prevLsn;
    record->Promise = NewPromise<NProto::TError>();

    return record;
}

// Maps the device pages of a write request onto the page store locations
// allocated for them. The request ranges and the locations are laid out one
// after another in the same page order, so the i-th page of the request lands
// in the i-th page of the locations.
TVector<TPageMapping> CreatePageMappings(
    const TVector<TPageRangeRef>& requestPageRanges,
    const TVector<TPageRangeRef>& locations,
    const TString& deviceUUID,
    ui64 recordLsn)
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
    STORAGE_VERIFY_C(
        pageCount == totalPageCount(locations),
        "JournalledDevice",
        deviceUUID,
        "Lsn: " << recordLsn << ", pageCount: " << pageCount);

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
    const TString DeviceUUID;
    const ui64 DevicePageCount;

    TLog Log;

    // Lets one thread at a time move a ready record to the page index.
    std::mutex ChainLock;
    TLogRecordChain LogRecordChain;
    TLogPageIndex LogPageIndex;
    std::shared_ptr<TLsnBarrier> FlushedLsnBarrier =
        std::make_shared<TLsnBarrier>();

    std::atomic<bool> AdvancingLsnLowWatermark = false;
    std::atomic<ui64> LsnLowWatermark = 0;

public:
    TJournal(
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        IKeyBufferStorePtr metaStore,
        IDevicePageStorePtr dataStore,
        TString deviceUUID,
        ui64 devicePageCount);

    // Restores the journal state and returns lsn of the last indexed record
    [[nodiscard]] TResultOrError<ui64> Restore() override;

    [[nodiscard]] TFuture<NProto::TError> Write(
        TVector<TPageRange> ranges,
        ui64 lsn,
        ui64 prevLsn) override;

    [[nodiscard]] TResultOrError<TJournalReadResult> Read(
        TVector<TPageRangeRef> rangeRefs) const override;

    [[nodiscard]] TResultOrError<TJournalTail> ReadTail(
        ui64 afterLsn,
        ui32 maxRecordCount) const override;

    [[nodiscard]] NProto::TError AdvanceLsnLowWatermark(
        ui64 lsnLowWatermark) override;

    [[nodiscard]] TResultOrError<TJournalRecords> GetRecordsToFlush(
        ui64 maxAllowedLsn,
        ui64 maxPageCount) const override;

    void MarkRecordsAsFlushed(
        ui64 lastFlushedLsn,
        const TVector<TPageRangeRef>& ranges) override;

    [[nodiscard]] NProto::TError CleanupFlushedRecords() override;

private:
    TVector<TLogRecordPtr> FilterStrandedRecords(
        const THashMap<ui64, TLogRecordPtr>& lsnToRecord,
        ui64 headLsn,
        ui64 ackedLsn);

    NProto::TError ValidateRecordPages(const TLogRecord& record, ui64 ackedLsn);

    TResultOrError<ui32> ReadPagesCrc(const TLogRecord& record);

    TFuture<NProto::TError> WriteLogRecord(
        TLogRecord& record,
        TVector<TPageRange> ranges);

    void OnWriteLogRecord(NProto::TError error, TLogRecord& record);

    void FreePages(const TVector<TPageRangeRef>& locations, ui64 lsn);

    void IndexChainedRecords();

    static TResultOrError<TReadPagesResult> OnRead(
        const TLogPageIndex::TLookupResult& lookup,
        TResultOrError<TVector<TBuffer>> buffersOrError);

    NProto::TError ReadPageRanges(
        const TVector<TPageMapping>& mappings,
        TVector<TPageRange>* ranges) const;
};

////////////////////////////////////////////////////////////////////////////////

TJournal::TJournal(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IKeyBufferStorePtr metaStore,
    IDevicePageStorePtr dataStore,
    TString deviceUUID,
    ui64 devicePageCount)
    : Logging(std::move(logging))
    , Executor(std::move(executor))
    , MetaStore(std::move(metaStore))
    , DataStore(std::move(dataStore))
    , DeviceUUID(std::move(deviceUUID))
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
    FlushedLsnBarrier->Advance(headLsn);

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
        STORAGE_VERIFY_C(
            marked,
            "JournalledDevice",
            DeviceUUID,
            "Lsn: " << record->Lsn);
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

TFuture<NProto::TError> TJournal::Write(
    TVector<TPageRange> ranges,
    ui64 lsn,
    ui64 prevLsn)
{
    auto error = ValidatePageRanges(ranges, DevicePageCount);
    if (HasError(error)) {
        return MakeFuture(std::move(error));
    }

    auto record = CreateRecord(lsn, prevLsn);

    //
    // Add the record to the chain, which holds it under its prev lsn - answer
    // a record repeating a held one, a retry, with the held record and do not
    // write it again
    //

    auto [inserted, insertError] = LogRecordChain.Insert(record);
    if (HasError(insertError)) {
        return MakeFuture(std::move(insertError));
    }

    if (inserted != record) {
        return inserted->Promise.GetFuture();
    }

    //
    // Write the record, then take a failed one out of the chain and report
    // the error through its promise - to the duplicates that have joined it
    // while it was being written as well - or mark a written one as ready
    //

    auto future = WriteLogRecord(*record, std::move(ranges));
    return future.Apply(
        [record = std::move(record), weakSelf = weak_from_this()](auto future)
        {
            auto self = weakSelf.lock();
            if (!self) {
                record->Promise.SetValue(
                    MakeError(E_CANCELLED, "TJournal is destroyed"));
            } else {
                self->OnWriteLogRecord(future.ExtractValue(), *record);
            }

            return record->Promise.GetFuture();
        });
}

void TJournal::OnWriteLogRecord(NProto::TError error, TLogRecord& record)
{
    const bool completed = HasError(error)
        ? LogRecordChain.Remove(record.PrevLsn)
        : LogRecordChain.MarkAsReady(record.PrevLsn);

    // Verify that the record was still held in the chain and not ready yet
    STORAGE_VERIFY_C(
        completed,
        "JournalledDevice",
        DeviceUUID,
        "Lsn: " << record.Lsn);

    if (HasError(error)) {
        record.Promise.SetValue(std::move(error));
        return;
    }

    //
    // Index the record if every record before it is already in the page
    // index, together with the records that have been waiting for it -
    // otherwise the record that fills the gap indexes it later
    //

    IndexChainedRecords();
}

TFuture<NProto::TError> TJournal::WriteLogRecord(
    TLogRecord& record,
    TVector<TPageRange> ranges)
{
    //
    // Allocate free journal pages for the pages of the request - they may be
    // split into several runs, so map every page of the request to its
    // journal page
    //

    auto requestPageRanges = GetPageRangeRefs(ranges);
    auto pages = ExtractPages(std::move(ranges));

    auto locations = DataStore->Allocate(pages.size());
    if (locations.empty() && !pages.empty()) {
        return MakeFuture(MakeError(
            E_REJECTED,
            TStringBuilder() << "not enough free journal pages to write "
                             << pages.size() << " pages"));
    }

    record.PageMappings = CreatePageMappings(
        requestPageRanges,
        locations,
        DeviceUUID,
        record.Lsn);
    record.PagesCrc = CalculatePagesCrc(pages);

    //
    // Issue the record write first: the log metadata rejects a record it cannot
    // take right away. Nothing is in flight then, so skip the page write and
    // free the pages at once
    //

    auto futureOrError = MetaStore->Write(record.Lsn, SerializeRecord(record));
    if (HasError(futureOrError)) {
        FreePages(locations, record.Lsn);
        return MakeFuture(futureOrError.GetError());
    }

    auto metaFuture = futureOrError.ExtractResult();

    //
    // Write the pages in parallel with the record - the crc tells a restore
    // whether the pages are there. Wait for both: the pages of a failed
    // record are freed, so nothing may be writing to them by then
    //

    auto dataFuture = DataStore->Write(locations, pages);

    return WaitAll(metaFuture.IgnoreResult(), dataFuture.IgnoreResult())
        .Apply(
            [weakSelf = weak_from_this(),
             locations,
             lsn = record.Lsn,
             metaFuture,
             dataFuture](const auto&)
            {
                const auto& metaError = metaFuture.GetValue();
                const auto& dataError = dataFuture.GetValue();

                if (HasError(metaError) || HasError(dataError)) {
                    if (auto self = weakSelf.lock()) {
                        self->FreePages(locations, lsn);
                    }
                    return HasError(metaError) ? metaError : dataError;
                }

                return NProto::TError();
            });
}

void TJournal::FreePages(const TVector<TPageRangeRef>& locations, ui64 lsn)
{
    auto error = DataStore->Free(locations);
    if (HasError(error)) {
        STORAGE_ERROR(
            "unable to free the pages of the failed record with lsn "
            << lsn << ": " << FormatError(error));
    }
}

void TJournal::IndexChainedRecords()
{
    //
    // Index the ready records that continue the page index. Several threads
    // can do this at the same time
    //

    while (true) {
        TLogRecordPtr record;

        {
            //
            // Take the lock, or two threads could index the same record
            //

            std::lock_guard lock(ChainLock);

            auto lastIndexedLsn = LogPageIndex.GetLastIndexedLsn();
            record = LogRecordChain.GetNext(lastIndexedLsn);
            if (!record) {
                break;
            }

            bool applied = LogPageIndex.TryApplyNext(*record);

            // Verify that the page index has taken the record
            STORAGE_VERIFY_C(
                applied,
                "JournalledDevice",
                DeviceUUID,
                "Lsn: " << record->Lsn);
        }

        //
        // Complete the promise without the lock: its subscribers send the
        // response, and other threads must not wait for that
        //

        record->Promise.SetValue(NProto::TError());
    }
}

TResultOrError<TJournalReadResult> TJournal::Read(
    TVector<TPageRangeRef> rangeRefs) const
{
    auto err = ValidatePageRanges(rangeRefs, DevicePageCount);
    if (HasError(err)) {
        return err;
    }

    //
    // Pin the flushed lsn until the read is done: the records past it stay in
    // the journal, and the ones up to it are on the device already. The read
    // finishes after this method returns, so the guard is shared with the
    // callback below
    //

    auto lsnBarrierGuard =
        std::make_shared<TLsnBarrier::TGuard>(FlushedLsnBarrier->Acquire());

    //
    // Look up the journalled pages of the requested ranges, skipping the
    // flushed records - the reader takes their pages from the device
    //

    auto lookup = LogPageIndex.Lookup(rangeRefs, lsnBarrierGuard->GetLsn());

    TJournalReadResult result;
    result.MissingRanges = std::move(lookup.MissingRanges);

    //
    // Answer at once when no requested page is in the journal - there is
    // nothing to read from the data store
    //

    if (lookup.Mappings.empty()) {
        result.Future = MakeFuture<TResultOrError<TReadPagesResult>>(
            TReadPagesResult{.LastAckedLsn = lookup.LastIndexedLsn});
        return result;
    }

    //
    // Start reading the journalled pages and return without waiting, so that
    // the caller can read the missing pages from the device at the same time
    //

    auto future = DataStore->Read(GetLocations(lookup.Mappings));

    result.Future = future.Apply(
        [lsnBarrierGuard = std::move(lsnBarrierGuard),
         lookup = std::move(lookup)](auto future)
        {
            // Release the flushed lsn when the read is done and not when the
            // callback object is destroyed, which can be much later
            auto guard = std::move(*lsnBarrierGuard);

            return OnRead(lookup, future.ExtractValue());
        });

    return result;
}

TResultOrError<TReadPagesResult> TJournal::OnRead(
    const TLogPageIndex::TLookupResult& lookup,
    TResultOrError<TVector<TBuffer>> buffersOrError)
{
    if (HasError(buffersOrError)) {
        return buffersOrError.GetError();
    }

    //
    // Return the journalled pages as they are: they are in the order of the
    // mappings, and that is the order of the request
    //

    auto pages = buffersOrError.ExtractResult();

    const ui64 pageCount = GetPageCount(lookup.Mappings);
    if (pages.size() != pageCount) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "the journal read " << pages.size()
                             << " pages, expected " << pageCount);
    }

    return TReadPagesResult{
        .LastAckedLsn = lookup.LastIndexedLsn,
        .Pages = std::move(pages)};
}

TResultOrError<TJournalTail> TJournal::ReadTail(
    ui64 afterLsn,
    ui32 maxRecordCount) const
{
    //
    // Pin the flushed lsn, at least at the lsn the reader asks from, so that
    // the records past it stay in the journal until the read is done
    //

    auto lsnBarrierGuard = FlushedLsnBarrier->AcquireAtLeast(afterLsn);

    //
    // Walk the ready records that follow each other unbroken past the pinned
    // lsn and the lsn low watermark - the acked records are of no use to the
    // reader, and a gap must not be skipped
    //

    ui64 lsnLowWatermark = LsnLowWatermark.load(std::memory_order_acquire);
    ui64 firstLsn = Max(lsnBarrierGuard.GetLsn(), lsnLowWatermark);

    auto records = LogRecordChain.GetReadyRun(
        firstLsn,
        Max<ui64>() /* maxLsn */,
        maxRecordCount);

    //
    // Fill the tail with the records and their page contents
    //

    TJournalTail tail{.LsnLowWatermark = lsnLowWatermark};
    tail.Records.reserve(records.size());

    for (const auto& record: records) {
        auto& journalRecord = tail.Records.emplace_back();
        journalRecord.Lsn = record->Lsn;
        journalRecord.PrevLsn = record->PrevLsn;

        auto error =
            ReadPageRanges(record->PageMappings, &journalRecord.Ranges);

        if (HasError(error)) {
            return error;
        }
    }

    return tail;
}

NProto::TError TJournal::AdvanceLsnLowWatermark(ui64 lsnLowWatermark)
{
    //
    // Reject a watermark past the last indexed lsn - the writer may only ack
    // what the journal has indexed
    //

    const ui64 lastIndexedLsn = LogPageIndex.GetLastIndexedLsn();

    if (lsnLowWatermark > lastIndexedLsn) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "lsn low watermark " << lsnLowWatermark
                << " reaches past the last indexed lsn " << lastIndexedLsn);
    }

    //
    // Let one advance in at a time - the metadata write below yields, and
    // two of them racing could persist the older watermark last
    //

    if (AdvancingLsnLowWatermark.exchange(true, std::memory_order_acquire)) {
        return MakeError(
            E_REJECTED,
            TStringBuilder() << "another advance to lsn low watermark "
                             << lsnLowWatermark << " is already in progress");
    }
    Y_DEFER
    {
        AdvancingLsnLowWatermark.store(false, std::memory_order_release);
    };

    if (lsnLowWatermark <= LsnLowWatermark.load(std::memory_order_acquire)) {
        return MakeError(S_ALREADY);
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
        return error;
    }

    LsnLowWatermark.store(lsnLowWatermark, std::memory_order_release);
    return {};
}

TResultOrError<TJournalRecords> TJournal::GetRecordsToFlush(
    ui64 maxAllowedLsn,
    ui64 maxPageCount) const
{
    //
    // Take the ready records right after the flushed lsn, in chain order -
    // stop at the first one that has not been acked yet or is past what the
    // caller allows
    //

    const ui64 flushedLsn = FlushedLsnBarrier->Acquire().GetLsn();

    const ui64 lsnLowWatermark =
        LsnLowWatermark.load(std::memory_order_acquire);
    const ui64 maxLsn = Min(maxAllowedLsn, lsnLowWatermark);

    auto run = LogRecordChain.GetReadyRun(
        flushedLsn,
        maxLsn,
        0 /* maxRecordCount */);

    //
    // Cut the run to the page budget: a record that takes the batch past it
    // waits for the next one, unless it is the first - a record bigger than
    // the whole budget still has to be flushed
    //

    TVector<TLogRecordPtr> records;
    TVector<TPageRangeRef> locations;
    ui64 pageCount = 0;

    for (const auto& record: run) {
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
        return TJournalRecords();
    }

    //
    // Read the pages of the whole batch in one go - one request to the device
    // for all the records, not one per record
    //

    auto response = Executor->ExtractResponse(DataStore->Read(locations));
    if (HasError(response)) {
        return response.GetError();
    }

    auto buffers = response.ExtractResult();

    TJournalRecords result;
    result.reserve(records.size());

    size_t bufferIndex = 0;
    for (const auto& record: records) {
        auto& journalRecord = result.emplace_back();
        journalRecord.Lsn = record->Lsn;
        journalRecord.PrevLsn = record->PrevLsn;

        size_t bufferCount = 0;
        auto error = FillPageRanges(
            record->PageMappings,
            buffers,
            bufferIndex,
            &bufferCount,
            &journalRecord.Ranges);

        if (HasError(error)) {
            return error;
        }

        bufferIndex += bufferCount;
    }

    return result;
}

void TJournal::MarkRecordsAsFlushed(
    ui64 lastFlushedLsn,
    const TVector<TPageRangeRef>& ranges)
{
    FlushedLsnBarrier->Advance(lastFlushedLsn);

    //
    // Erase the flushed records from the page index right away, without
    // waiting for the readers: a lookup skips the flushed records anyway,
    // and the reader takes their pages from the device. The pages in the
    // journal stay until CleanupFlushedRecords, for the readers that have
    // looked them up already
    //

    LogPageIndex.EraseBelow(lastFlushedLsn + 1, ranges);
}

NProto::TError TJournal::CleanupFlushedRecords()
{
    //
    // Erase the records up to the barrier lsn - the flushed ones that no
    // reader is looking at any more - from the meta store first: a restart
    // must not find a record whose pages have been reused
    //

    ui64 eraseUpToLsn = FlushedLsnBarrier->GetBarrierLsn();

    // The meta store holds the records by their lsn
    auto future = MetaStore->EraseBelow(eraseUpToLsn + 1);
    if (const auto& error = Executor->WaitFor(future); HasError(error)) {
        return error;
    }

    //
    // Erase them from the chain, then release their pages.
    //

    // The chain holds the records by their lsn
    auto recordsOrError = LogRecordChain.EraseBelow(eraseUpToLsn + 1);

    // Verify that the erased run is chained: the barrier never moves past a
    // record the chain does not hold ready
    STORAGE_VERIFY_C(
        !HasError(recordsOrError),
        "JournalledDevice",
        DeviceUUID,
        "Lsn: " << eraseUpToLsn << ", error: "
                << FormatError(recordsOrError.GetError()));

    auto erased = recordsOrError.ExtractResult();

    //
    // Fail the writers of the stranded records: nobody else answers them.
    // The thread that indexed a chained record sets its promise
    //

    for (const auto& record: erased.Stranded) {
        const TString message = TStringBuilder()
            << "record with lsn " << record->Lsn << " chaining from lsn "
            << record->PrevLsn << " can no longer join the chain";

        record->Promise.SetValue(MakeError(E_INVALID_STATE, message));
        ReportJournalStrandedRecordDetectedError(message);
    }

    auto result = MakeError(S_OK);
    for (const auto* records: {&erased.Chained, &erased.Stranded}) {
        for (const auto& record: *records) {
            auto error = DataStore->Free(GetLocations(record->PageMappings));
            if (HasError(error)) {
                STORAGE_ERROR(
                    "unable to free the pages of the erased record with lsn "
                    << record->Lsn << ": " << FormatError(error));
                result = error;
            }
        }
    }

    return result;
}

NProto::TError TJournal::ReadPageRanges(
    const TVector<TPageMapping>& mappings,
    TVector<TPageRange>* ranges) const
{
    auto response =
        Executor->ExtractResponse(DataStore->Read(GetLocations(mappings)));
    if (HasError(response)) {
        return response.GetError();
    }

    auto buffers = response.ExtractResult();

    size_t bufferCount = 0;
    return FillPageRanges(
        mappings,
        buffers,
        0, /* firstBufferIndex */
        &bufferCount,
        ranges);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalPtr CreateJournal(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IKeyBufferStorePtr metaStore,
    IDevicePageStorePtr dataStore,
    TString deviceUUID,
    ui64 devicePageCount)
{
    return std::make_shared<TJournal>(
        std::move(logging),
        std::move(executor),
        std::move(metaStore),
        std::move(dataStore),
        std::move(deviceUUID),
        devicePageCount);
}

}   // namespace NCloud::NJournalled
