#include "journal.h"

#include "device_page_store.h"
#include "key_buffer_store.h"
#include "log_chain.h"
#include "log_index.h"
#include "lsn_barrier.h"

#include <cloud/storage/core/libs/common/future_helper.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/algorithm.h>
#include <util/generic/scope.h>
#include <util/generic/utility.h>
#include <util/string/builder.h>

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

template <typename TRanges>
NCloud::NProto::TError ValidateNoIntersections(const TRanges& ranges)
{
    for (int i = 0; i < ranges.size(); ++i) {
        const ui64 pageCount = PageCountOf(ranges[i]);
        if (!pageCount) {
            continue;
        }

        const ui64 begin = ranges[i].GetFirstPageNo();
        const ui64 end = begin + pageCount;

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

TVector<TPageRange> GetLocations(const TVector<TPageMapping>& mappings)
{
    TVector<TPageRange> locations;
    locations.reserve(mappings.size());

    for (const auto& mapping: mappings) {
        locations.push_back(mapping.Location);
    }

    return locations;
}

TVector<TPageRange> GetPageRanges(
    const NCloud::NProto::TWriteLogRecordRequest& request)
{
    TVector<TPageRange> ranges;
    ranges.reserve(request.PageGroupsSize());

    for (const auto& group: request.GetPageGroups()) {
        ranges.push_back(
            {.FirstPageNo = group.GetFirstPageNo(),
             .PageCount = group.ContentSize()});
    }

    return ranges;
}

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

TVector<TPageMapping> CreatePageMappings(
    const TVector<TPageRange>& requestPageRanges,
    const TVector<TPageRange>& locations)
{
    auto totalPageCount = [](const TVector<TPageRange>& ranges)
    {
        ui64 total = 0;
        for (const auto& range: ranges) {
            total += range.PageCount;
        }
        return total;
    };

    const ui64 pageCount = totalPageCount(requestPageRanges);
    STORAGE_VERIFY(
        pageCount == totalPageCount(locations),
        "PageCount",
        pageCount);

    TVector<TPageMapping> mappings;

    size_t requestIndex = 0;
    size_t locationIndex = 0;
    ui64 requestOffset = 0;
    ui64 locationOffset = 0;

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
                .Location = TPageRange{
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

    TLog Log;

    TLogRecordChain LogRecordChain;
    TLogPageIndex LogPageIndex;
    mutable TLsnBarrier FlushedLsnBarrier;

    std::atomic<bool> AdvancingLastAckedLsn = false;
    std::atomic<ui64> LastAckedLsn = 0;

public:
    TJournal(
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        IKeyBufferStorePtr metaStore,
        IDevicePageStorePtr dataStore);

    // Restoring

    // Restores the journal state and returns lsn of the last indexed record
    [[nodiscard]] NThreading::TFuture<TResultOrError<ui64>> Restore() override;

    // Device API

    [[nodiscard]] auto Write(NCloud::NProto::TWriteLogRecordRequest request)
        -> NThreading::TFuture<
            NCloud::NProto::TWriteLogRecordResponse> override;

    [[nodiscard]] auto Read(NCloud::NProto::TReadPagesRequest request) const
        -> NThreading::TFuture<NCloud::NProto::TReadPagesResponse> override;

    [[nodiscard]] auto ReadTail(
        NCloud::NProto::TReadJournalTailRequest request) const
        -> NThreading::TFuture<
            NCloud::NProto::TReadJournalTailResponse> override;

    [[nodiscard]] auto AdvanceLastAckedLsn(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> NThreading::TFuture<
            NCloud::NProto::TAdvanceLsnLowWatermarkResponse> override;

    // Background cleanup

    [[nodiscard]] auto GetRecordToFlush(ui64 maxAllowedLsn) const
        -> NThreading::TFuture<
            TResultOrError<NCloud::NProto::TJournalRecord>> override;

    void MarkRecordAsFlushed(ui64 lsn) override;

    [[nodiscard]] auto CleanupFlushedRecords()
        -> NThreading::TFuture<NCloud::NProto::TError> override;

private:
    TResultOrError<ui64> RestoreFrom(TVector<std::pair<ui64, TBuffer>> buffers);

    NCloud::NProto::TError WriteChainedRecord(
        TLogRecord& record,
        NCloud::NProto::TWriteLogRecordRequest request);

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
    IDevicePageStorePtr dataStore)
    : Logging(std::move(logging))
    , Executor(std::move(executor))
    , MetaStore(std::move(metaStore))
    , DataStore(std::move(dataStore))
    , Log(Logging->CreateLog("JOURNAL"))
{}

TFuture<TResultOrError<ui64>> TJournal::Restore()
{
    return MetaStore->Restore().Apply(
        [self = shared_from_this()](const auto& future) -> TResultOrError<ui64>
        {
            auto response = UnsafeExtractValue(future);
            if (HasError(response)) {
                return response.GetError();
            }

            return self->RestoreFrom(response.ExtractResult());
        });
}

TResultOrError<ui64> TJournal::RestoreFrom(
    TVector<std::pair<ui64, TBuffer>> buffers)
{
    bool initialized = false;

    SortBy(buffers, [] (const auto& keyBuffer) { return keyBuffer.first; });

    if (!buffers.empty() && buffers.back().first == MetadataKey) {
        const auto& buffer = buffers.back().second;
        auto metadata = DeserializeMetadata(buffer);
        if (!metadata) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "failed to deserialize journal metadata from "
                    << buffer.Size() << " bytes");
        }

        LastAckedLsn.store(metadata->LastAckedLsn);
        buffers.pop_back();
    }

    for (const auto& [key, buffer]: buffers) {
        auto record = DeserializeRecord(buffer);
        if (!record) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "failed to deserialize log record with key " << key
                    << " from " << buffer.Size() << " bytes");
        }

        if (record->PrevLsn != key) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "log record with key " << key << " has prevLsn "
                                 << record->PrevLsn);
        }

        if (!initialized) {
            initialized = true;
            auto lsn = Min(record->PrevLsn, LastAckedLsn.load());
            LogRecordChain.InitLastErasedLsn(lsn);
            LogPageIndex.InitLastIndexedLsn(lsn);
            FlushedLsnBarrier.Advance(lsn);
        }

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

        if (LogPageIndex.TryApplyNext(*record)) {
            record->Promise.SetValue(TErrorResponse(S_OK));
        }
    }

    auto lastAckedLsn = LastAckedLsn.load();

    if (!initialized) {
        initialized = true;
        LogRecordChain.InitLastErasedLsn(lastAckedLsn);
        LogPageIndex.InitLastIndexedLsn(lastAckedLsn);
        FlushedLsnBarrier.Advance(lastAckedLsn);
    }

    ui64 lastIndexedLsn = LogPageIndex.GetLastIndexedLsn();
    if (lastIndexedLsn < lastAckedLsn) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "restored log ends at lsn " << lastIndexedLsn
                << ", below the last acked lsn " << lastAckedLsn);
    }

    return lastIndexedLsn;
}

TFuture<NCloud::NProto::TWriteLogRecordResponse> TJournal::Write(
    NCloud::NProto::TWriteLogRecordRequest request)
{
    using TResponse = NCloud::NProto::TWriteLogRecordResponse;

    if (auto error = ValidateNoIntersections(request.GetPageGroups());
        HasError(error))
    {
        return MakeFuture<TResponse>(TErrorResponse(std::move(error)));
    }

    ui64 lsn = request.GetLogSequenceNumber();
    if (lsn == MetadataKey) {
        return MakeFuture<TResponse>(TErrorResponse(
            E_ARGUMENT,
            TStringBuilder()
                << "lsn " << lsn << " is reserved for journal metadata"));
    }

    if (lsn <= LogPageIndex.GetLastIndexedLsn()) {
        return MakeFuture<TResponse>(TErrorResponse(S_ALREADY));
    }

    auto record = std::make_shared<TLogRecord>();
    record->Lsn = request.GetLogSequenceNumber();
    record->PrevLsn = request.GetPrevLogSequenceNumber();
    record->Promise = NewPromise<TResponse>();

    auto insertResult = LogRecordChain.Insert(record);
    if (HasError(insertResult)) {
        return MakeFuture<TResponse>(TErrorResponse(insertResult.GetError()));
    }

    if (auto inserted = insertResult.ExtractResult(); inserted != record) {
        // duplicated record
        return inserted->Promise.GetFuture();
    }

    auto error = WriteChainedRecord(*record, std::move(request));
    if (HasError(error)) {
        record->Promise.SetValue(TErrorResponse(error));
        return record->Promise.GetFuture();
    }

    auto recordIt = record;
    while (recordIt && LogPageIndex.TryApplyNext(*recordIt)) {
        recordIt->Promise.SetValue(TErrorResponse(S_OK));
        recordIt = LogRecordChain.GetNext(recordIt->Lsn);
    }

    return record->Promise.GetFuture();
}

NCloud::NProto::TError TJournal::WriteChainedRecord(
    TLogRecord& record,
    NCloud::NProto::TWriteLogRecordRequest request)
{
    bool success = false;

    Y_DEFER
    {
        if (!success) {
            bool removed = LogRecordChain.Remove(record.PrevLsn);
            STORAGE_VERIFY(removed, "Remove", record.PrevLsn);
        }
    };

    auto requestPageRanges = GetPageRanges(request);
    auto pages = GetPages(request);

    auto locations = DataStore->Allocate(pages.size());
    if (locations.empty() && !pages.empty()) {
        return MakeError(
            E_REJECTED,
            TStringBuilder() << "not enough free journal pages to write "
                             << pages.size() << " pages");
    }

    Y_DEFER
    {
        if (!success) {
            auto error = DataStore->Free(locations);
            if (HasError(error)) {
                STORAGE_ERROR(
                    "unable to free the pages of the failed record with lsn "
                    << record.Lsn << ": " << FormatError(error));
            }
        }
    };

    auto dataFuture = DataStore->Write(locations, pages);
    if (const auto& error = Executor->WaitFor(dataFuture); HasError(error)) {
        return error;
    }

    record.PageMappings = CreatePageMappings(requestPageRanges, locations);

    auto metaFuture = MetaStore->Write(record.PrevLsn, SerializeRecord(record));
    if (const auto& error = Executor->WaitFor(metaFuture); HasError(error)) {
        return error;
    }

    bool marked = LogRecordChain.MarkAsReady(record.PrevLsn);
    STORAGE_VERIFY(marked, "MarkAsReady", record.Lsn);

    success = true;
    return {};
}

TFuture<NCloud::NProto::TReadPagesResponse> TJournal::Read(
    NCloud::NProto::TReadPagesRequest request) const
{
    using TResponse = NCloud::NProto::TReadPagesResponse;

    if (auto error = ValidateNoIntersections(request.GetPageGroupRefs());
        HasError(error))
    {
        return MakeFuture<TResponse>(TErrorResponse(std::move(error)));
    }

    auto lsnBarrierGuard = FlushedLsnBarrier.Acquire();

    TVector<TPageRange> ranges;
    ranges.reserve(request.PageGroupRefsSize());
    for (const auto& ref: request.GetPageGroupRefs()) {
        ranges.push_back(
            {.FirstPageNo = ref.GetFirstPageNo(),
             .PageCount = ref.GetPageCount()});
    }

    auto lookup = LogPageIndex.Lookup(ranges, lsnBarrierGuard.GetLsn());

    TResponse response;

    // TODO: rename proto field into LastIndexedLogSequenceNumber
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

    auto lsnBarrierGuard =
        FlushedLsnBarrier.AcquireAtLeast(request.GetAfterLogSequenceNumber());

    auto lastAckedLsn = LastAckedLsn.load();
    auto afterLsn = Max(lsnBarrierGuard.GetLsn(), lastAckedLsn);

    auto records =
        LogRecordChain.GetReadyRun(afterLsn, request.GetMaxRecordCount());

    TResponse response;
    response.SetLastAckedLogSequenceNumber(lastAckedLsn);

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
TJournal::AdvanceLastAckedLsn(
    NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
{
    using TResponse = NCloud::NProto::TAdvanceLsnLowWatermarkResponse;

    const auto lastAckedLsn = request.GetLsnLowWatermark();
    const auto lastIndexedLsn = LogPageIndex.GetLastIndexedLsn();

    if (lastAckedLsn > lastIndexedLsn) {
        return MakeFuture<TResponse>(TErrorResponse(
            E_ARGUMENT,
            TStringBuilder() << "lsn low watermark " << lastAckedLsn
                             << " reaches past the last indexed lsn "
                             << lastIndexedLsn));
    }

    if (AdvancingLastAckedLsn.exchange(true) == true) {
        return MakeFuture<TResponse>(TErrorResponse(
            E_REJECTED,
            TStringBuilder() << "another advance to lsn low watermark "
                             << lastAckedLsn << " is already in progress"));
    }
    Y_DEFER
    {
        AdvancingLastAckedLsn.store(false);
    };

    if (lastAckedLsn <= LastAckedLsn.load()) {
        return MakeFuture<TResponse>(TErrorResponse(S_ALREADY));
    }

    TJournalMetadata metadata = {
        .Version = CurrentFormatVersion,
        .LastAckedLsn = lastAckedLsn,
    };

    auto future = MetaStore->Write(MetadataKey, SerializeMetadata(metadata));
    if (const auto& error = Executor->WaitFor(future); HasError(error)) {
        return MakeFuture<TResponse>(TErrorResponse(error));
    }

    LastAckedLsn.store(lastAckedLsn);
    return MakeFuture<TResponse>();
}

TFuture<TResultOrError<NCloud::NProto::TJournalRecord>>
TJournal::GetRecordToFlush(ui64 maxAllowedLsn) const
{
    using TResult = TResultOrError<NCloud::NProto::TJournalRecord>;

    auto lsnBarrierGuard = FlushedLsnBarrier.Acquire();

    NCloud::NProto::TJournalRecord response;

    auto record = LogRecordChain.GetNext(lsnBarrierGuard.GetLsn());
    if (!record || record->Lsn > Min(maxAllowedLsn, LastAckedLsn.load())) {
        return MakeFuture<TResult>(std::move(response));
    }

    response.SetLogSequenceNumber(record->Lsn);
    response.SetPrevLogSequenceNumber(record->PrevLsn);

    auto error =
        FillPageGroups(record->PageMappings, response.MutablePageGroups());

    if (HasError(error)) {
        return MakeFuture<TResult>(error);
    }

    return MakeFuture<TResult>(std::move(response));
}

void TJournal::MarkRecordAsFlushed(ui64 lsn)
{
    FlushedLsnBarrier.Advance(lsn);
}

TFuture<NCloud::NProto::TError> TJournal::CleanupFlushedRecords()
{
    auto eraseUpToLsn = FlushedLsnBarrier.GetBarrierLsn();

    auto future = MetaStore->EraseBelow(eraseUpToLsn);
    if (const auto& error = Executor->WaitFor(future); HasError(error)) {
        return MakeFuture(error);
    }

    auto recordsOrError = LogRecordChain.EraseUpTo(eraseUpToLsn);
    STORAGE_VERIFY_C(
        !HasError(recordsOrError),
        "Lsn",
        eraseUpToLsn,
        FormatError(recordsOrError.GetError()));

    auto records = recordsOrError.ExtractResult();
    LogPageIndex.EraseUpTo(eraseUpToLsn);

    auto result = MakeError(S_OK);
    for (const auto& record: records) {
        // a flushed record has its promise set, a ready record stranded
        // below the erased watermark does not - its writer is still waiting
        record->Promise.TrySetValue(TErrorResponse(
            E_INVALID_STATE,
            TStringBuilder()
                << "record with lsn " << record->Lsn << " chaining from lsn "
                << record->PrevLsn << " can no longer join the chain"));

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

    // one buffer per page, in the order the ranges were asked for
    const auto& buffers = response.GetResult();

    size_t bufferIndex = 0;
    for (const auto& [pageNo, location]: mappings) {
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

    return MakeError(S_OK);
}


}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalPtr CreateJournal(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IKeyBufferStorePtr metaStore,
    IDevicePageStorePtr dataStore)
{
    return std::make_shared<TJournal>(
        std::move(logging),
        std::move(executor),
        std::move(metaStore),
        std::move(dataStore));
}

}   // namespace NCloud::NJournalled
