#include "journal.h"

#include "device_page_store.h"
#include "key_buffer_store.h"
#include "log_chain.h"
#include "log_index.h"

#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/scope.h>
#include <util/generic/utility.h>
#include <util/string/builder.h>

#include <atomic>
#include <utility>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 MetadataKey = Max<ui64>();

ui64 PageCountOf(const NCloud::NProto::TDevicePageGroupRef& ref)
{
    return ref.GetPageCount();
}

ui64 PageCountOf(const NCloud::NProto::TDevicePageGroup& group)
{
    return group.ContentSize();
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
    for (int i = 0; i < ranges.size(); ++i) {
        const ui64 pageCount = PageCountOf(ranges[i]);
        if (!pageCount) {
            continue;
        }

        //
        // Check that the range lies inside the device
        //

        const ui64 begin = ranges[i].GetFirstPageNo();
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

        for (int j = 0; j < i; ++j) {
            const ui64 otherPageCount = PageCountOf(ranges[j]);
            if (!otherPageCount) {
                continue;
            }

            const ui64 otherBegin = ranges[j].GetFirstPageNo();
            const ui64 otherEnd = otherBegin + otherPageCount;

            if (begin < otherEnd && otherBegin < end) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "page ranges " << otherBegin << "x" << otherPageCount
                        << " and " << begin << "x" << pageCount
                        << " of a single request intersect");
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

class TJournal final: public IJournal
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
    [[nodiscard]] TFuture<TResultOrError<ui64>> Restore() override;

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

    [[nodiscard]] auto GetRecordToFlush(ui64 maxAllowedLsn) const
        -> TFuture<TResultOrError<NCloud::NProto::TJournalRecord>> override;

    void MarkRecordAsFlushed(ui64 lsn) override;

    [[nodiscard]] auto CleanupFlushedRecords()
        -> TFuture<NCloud::NProto::TError> override;

private:
    NCloud::NProto::TError ValidateWriteRequest(
        const NCloud::NProto::TWriteLogRecordRequest& request) const;

    NProto::TError WriteLogRecord(
        TLogRecord& record,
        const NCloud::NProto::TWriteLogRecordRequest& request);

    void IndexChainedRecords();

    NCloud::NProto::TError FillPageGroups(
        const TVector<TPageMapping>& mappings,
        google::protobuf::RepeatedPtrField<NCloud::NProto::TDevicePageGroup>*
            pageGroups) const;
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
    , Log(Logging->CreateLog("JOURNAL"))
{}

TFuture<TResultOrError<ui64>> TJournal::Restore()
{
    return MakeFuture<TResultOrError<ui64>>(
        MakeError(E_NOT_IMPLEMENTED, "Restore"));
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
    auto err = ValidatePageRanges(request.GetPageGroups(), DevicePageCount);
    if (HasError(err)) {
        return err;
    }

    const ui64 lsn = request.GetLogSequenceNumber();
    if (lsn == MetadataKey) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "lsn " << lsn << " is reserved for journal metadata");
    }

    return {};
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

    //
    // Write the pages first and the record second: the record is what a
    // restore finds, so it may only point at pages that are already written
    //

    auto dataFuture = DataStore->Write(locations, pages);
    if (auto error = Executor->WaitFor(dataFuture); HasError(error)) {
        return error;
    }

    auto metaFuture = MetaStore->Write(record.PrevLsn, SerializeRecord(record));
    if (auto error = Executor->WaitFor(metaFuture); HasError(error)) {
        return error;
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

    ui64 lastFlushedLsn = 0; // TODO(#6956): implement with MarkRecordAsFlushed
    auto lookup = LogPageIndex.Lookup(ranges, lastFlushedLsn);

    //
    // Fill the response with the content of the journalled pages and the lsn
    // the page index was looked up at
    //

    TResponse response;
    response.SetLastAckedLogSequenceNumber(lookup.LastIndexedLsn);

    auto error = FillPageGroups(lookup.Mappings, response.MutablePageGroups());
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
    // Walk the ready records that follow each other unbroken past the lsn the
    // reader asks from and the last acked one - the acked records are of no
    // use to the reader, and a gap must not be skipped
    //

    auto lsnLowWatermark = LsnLowWatermark.load();
    auto afterLsn = Max(request.GetAfterLogSequenceNumber(), lsnLowWatermark);

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

        auto error = FillPageGroups(
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

    const auto lsnLowWatermark = request.GetLsnLowWatermark();
    const auto lastIndexedLsn = LogPageIndex.GetLastIndexedLsn();

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

    if (AdvancingLsnLowWatermark.exchange(true) == true) {
        return MakeFuture<TResponse>(TErrorResponse(
            E_REJECTED,
            TStringBuilder() << "another advance to lsn low watermark "
                             << lsnLowWatermark << " is already in progress"));
    }
    Y_DEFER
    {
        AdvancingLsnLowWatermark.store(false);
    };

    if (lsnLowWatermark <= LsnLowWatermark.load()) {
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

    auto future = MetaStore->Write(MetadataKey, SerializeMetadata(metadata));
    if (const auto& error = Executor->WaitFor(future); HasError(error)) {
        return MakeFuture<TResponse>(TErrorResponse(error));
    }

    LsnLowWatermark.store(lsnLowWatermark);
    return MakeFuture<TResponse>();
}

TFuture<TResultOrError<NCloud::NProto::TJournalRecord>>
TJournal::GetRecordToFlush(ui64 maxAllowedLsn) const
{
    Y_UNUSED(maxAllowedLsn);

    return MakeFuture<TResultOrError<NCloud::NProto::TJournalRecord>>(
        MakeError(E_NOT_IMPLEMENTED, "GetRecordToFlush"));
}

void TJournal::MarkRecordAsFlushed(ui64 lsn)
{
    Y_UNUSED(lsn);
}

TFuture<NCloud::NProto::TError> TJournal::CleanupFlushedRecords()
{
    return MakeFuture(MakeError(E_NOT_IMPLEMENTED, "CleanupFlushedRecords"));
}

NCloud::NProto::TError TJournal::FillPageGroups(
    const TVector<TPageMapping>& mappings,
    google::protobuf::RepeatedPtrField<NCloud::NProto::TDevicePageGroup>*
        pageGroups) const
{
    auto future = DataStore->Read(GetLocations(mappings));
    const auto& response = Executor->WaitFor(future);
    if (HasError(response)) {
        return response.GetError();
    }

    //
    // Fill the page groups with the buffers read - one buffer per page, in
    // the order the ranges were asked for
    //

    const auto& buffers = response.GetResult();

    size_t bufferIndex = 0;
    for (const auto& [pageNo, location]: mappings) {
        // Verify that the data store has returned a buffer for every page of
        // the mapping - it answers one buffer per requested page
        STORAGE_VERIFY(
            bufferIndex + location.PageCount <= buffers.size(),
            "PageNo",
            pageNo);

        auto& pageGroup = *pageGroups->Add();
        pageGroup.SetFirstPageNo(pageNo);

        for (ui64 i = 0; i < location.PageCount; ++i) {
            const auto& buffer = buffers[bufferIndex++];
            pageGroup.AddContent(TString(buffer.Data(), buffer.Size()));
        }
    }

    return {};
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
