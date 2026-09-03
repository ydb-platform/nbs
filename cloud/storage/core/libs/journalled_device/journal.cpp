#include "journal.h"

#include "lsn_barriers.h"
#include "log_index.h"
#include "log_chain.h"
#include "journal_store.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/scope.h>
#include <util/generic/utility.h>
#include <util/stream/buffer.h>
#include <util/string/builder.h>
#include <util/ysaveload.h>

#include <atomic>
#include <optional>
#include <utility>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 MetadataKey = Max<ui64>();

TVector<TPageGroupRef> ExtractRefs(
    const TVector<std::pair<ui64, TPageGroupRef>>& index)
{
    TVector<TPageGroupRef> refs;
    refs.reserve(index.size());

    for (const auto& [pageNo, groupRef]: index) {
        refs.push_back(groupRef);
    }

    return refs;
}

TVector<TPageGroupRef> GetPageGroupRefs(
    const NCloud::NProto::TWriteLogRecordRequest& request)
{
    TVector<TPageGroupRef> refs;
    refs.reserve(request.PageGroupsSize());

    for (const auto& group: request.GetPageGroups()) {
        refs.push_back({
            .FirstPageNo = group.GetFirstPageNo(),
            .PageCount = group.ContentSize()});
    }

    return refs;
}

TVector<std::pair<ui64, TPageGroupRef>> CreatePageGroupIndex(
    const TVector<TPageGroupRef>& requestPageGroupRefs,
    const TVector<TPageGroupRef>& storagePageGroupRefs)
{
    auto totalPageCount = [] (const TVector<TPageGroupRef>& refs)
    {
        ui64 total = 0;
        for (const auto& ref: refs) {
            total += ref.PageCount;
        }
        return total;
    };

    const ui64 pageCount = totalPageCount(requestPageGroupRefs);
    STORAGE_VERIFY(
        pageCount == totalPageCount(storagePageGroupRefs),
        "PageCount",
        pageCount);

    TVector<std::pair<ui64, TPageGroupRef>> index;

    size_t requestIndex = 0;
    size_t storageIndex = 0;
    ui64 requestOffset = 0;
    ui64 storageOffset = 0;

    while (requestIndex < requestPageGroupRefs.size() &&
           storageIndex < storagePageGroupRefs.size())
    {
        const auto& requestRef = requestPageGroupRefs[requestIndex];
        const auto& storageRef = storagePageGroupRefs[storageIndex];

        const ui64 requestLeft = requestRef.PageCount - requestOffset;
        if (!requestLeft) {
            ++requestIndex;
            requestOffset = 0;
            continue;
        }

        const ui64 storageLeft = storageRef.PageCount - storageOffset;
        if (!storageLeft) {
            ++storageIndex;
            storageOffset = 0;
            continue;
        }

        const ui64 runPageCount = Min(requestLeft, storageLeft);

        index.emplace_back(
            requestRef.FirstPageNo + requestOffset,
            TPageGroupRef{
                .FirstPageNo = storageRef.FirstPageNo + storageOffset,
                .PageCount = runPageCount});

        requestOffset += runPageCount;
        if (requestOffset == requestRef.PageCount) {
            ++requestIndex;
            requestOffset = 0;
        }

        storageOffset += runPageCount;
        if (storageOffset == storageRef.PageCount) {
            ++storageIndex;
            storageOffset = 0;
        }
    }

    return index;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TJournal::TImpl
{
private:
    const IKeyBufferStorePtr MetaStore;
    const IPageStorePtr DataStore;

    TLogRecordChain LogRecordChain;
    TLogPageMap LogPageMap;
    TWatermarkTracker FlushedLsnTracker;

    std::atomic<bool> AdvancingLastAckedLsn = false;
    std::atomic<ui64> LastAckedLsn = 0;

public:
    TImpl(IKeyBufferStorePtr logMetaStore, IPageStorePtr logDataStore)
        : MetaStore(std::move(logMetaStore))
        , DataStore(std::move(logDataStore))
    {}

    [[nodiscard]] TFuture<NCloud::NProto::TError> Restore();

    [[nodiscard]] TFuture<NCloud::NProto::TError> Write(
        const NCloud::NProto::TWriteLogRecordRequest& request);

    [[nodiscard]] auto Read(
        const NCloud::NProto::TReadPagesRequest& request) const
        -> TFuture<NCloud::NProto::TReadPagesResponse>;

    [[nodiscard]] auto ReadTail(ui64 afterLsn, ui64 maxRecordCnt) const
        -> TFuture<NCloud::NProto::TReadJournalTailResponse>;

    [[nodiscard]] auto AdvanceLastAckedLsn(ui64 lastAckedLsn)
        -> TFuture<NCloud::NProto::TError>;

    [[nodiscard]] auto GetFirstRecordToFlush() const
        -> TFutureResultOrError<NCloud::NProto::TJournalRecord>;

    void MarkRecordAsFlushed(ui64 lsn);

    [[nodiscard]] TFuture<NCloud::NProto::TError> CleanupFlushedRecords();

private:
    [[nodiscard]] NCloud::NProto::TError FillPageGroups(
        const TVector<std::pair<ui64, TPageGroupRef>>& index,
        google::protobuf::RepeatedPtrField<
            NCloud::NProto::TDevicePageGroup>* pageGroups) const;
};

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] TFuture<NCloud::NProto::TError> TJournal::TImpl::Restore()
{
    bool initialized = false;
    auto keys = MetaStore->GetKeys();

    if (auto it = keys.find(MetadataKey); it != keys.end()) {
        keys.erase(it);

        auto future = MetaStore->Read(MetadataKey);
        future.Wait();   // TODO:
        auto response = future.GetValue();
        if (HasError(response)) {
            return MakeFuture(response.GetError());
        }

        auto metadata = DeserializeMetadata(response.GetResult());
        if (!metadata) {
            return MakeFuture(MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "failed to deserialize journal metadata from "
                    << response.GetResult().Size() << " bytes"));
        }

        LastAckedLsn.store(metadata->LastAckedLsn);
    }

    for (auto key : keys) {
        auto future = MetaStore->Read(key);
        future.Wait();   // TODO:
        auto response = future.GetValue();
        if (HasError(response)) {
            return MakeFuture(response.GetError());
        }

        auto record = DeserializeRecord(response.GetResult());
        if (!record) {
            return MakeFuture(MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "failed to deserialize log record with key " << key
                    << " from " << response.GetResult().Size()
                    << " bytes"));
        }

        for (auto [_, pageGroupRef]: record->PageGroupIndex) {
            auto error = DataStore->MarkAsWritten({pageGroupRef});
            if (HasError(error)) {
                return MakeFuture(error);
            }
        }

        if (auto response = LogRecordChain.Insert(record); HasError(response)) {
            return MakeFuture(response.GetError());
        }

        if (!initialized) {
            initialized = true;
            auto lsn = Min(record->PrevLsn, LastAckedLsn.load());
            LogRecordChain.InitLastErasedLsn(lsn);
            LogPageMap.InitLastIndexedLsn(lsn);
            FlushedLsnTracker.AdvanceWatermark(lsn);
        }

        if (LogPageMap.AddNext(*record)) {
            record->Promise.SetValue(MakeError(S_OK));
        }
    }

    if (!initialized) {
        auto lsn = LastAckedLsn.load();
        LogRecordChain.InitLastErasedLsn(lsn);
        LogPageMap.InitLastIndexedLsn(lsn);
        FlushedLsnTracker.AdvanceWatermark(lsn);
    }

    if (LogPageMap.GetLastIndexedLsn() < LastAckedLsn.load()) {
        return MakeFuture(MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "restored log ends at lsn " << LogPageMap.GetLastIndexedLsn()
                << ", below the last acked lsn "
                << LastAckedLsn.load()));
    }

    return MakeFuture(MakeError(S_OK));
}

[[nodiscard]] TFuture<NCloud::NProto::TError> TJournal::TImpl::Write(
    const NCloud::NProto::TWriteLogRecordRequest& request)
{
    if (request.GetLogSequenceNumber() <= LogPageMap.GetLastIndexedLsn()) {
        return MakeFuture(MakeError(S_ALREADY));
    }

    auto record = std::make_shared<TLogRecord>();
    record->Lsn = request.GetLogSequenceNumber();
    record->PrevLsn = request.GetPrevLogSequenceNumber();
    record->Promise = NewPromise<NCloud::NProto::TError>();
    record->Ready.store(false);

    auto response = LogRecordChain.Insert(record);
    if (HasError(response)) {
        return MakeFuture(response.GetError());
    }

    auto insertedRecord = response.ExtractResult();
    if (insertedRecord != record) {
        // duplicated record
        return insertedRecord->Promise.GetFuture();
    }

    Y_DEFER {
        if (!record->Ready.load()) {
            if (LogRecordChain.Erase(record->Lsn) == nullptr) {
                // TODO: log error
            };
        }
    };

    auto requestPageGroupRefs = GetPageGroupRefs(request);
    auto dataFuture = DataStore->WritePageGroups(request);
    dataFuture.Wait();      // TODO:
    auto dataResponse = dataFuture.GetValue();
    if (HasError(dataResponse)) {
        record->Promise.SetValue(dataResponse.GetError());
        return record->Promise.GetFuture();
    }

    auto storagePageGroupRefs = dataResponse.ExtractResult();

    Y_DEFER {
        if (!record->Ready.load()) {
            auto error = DataStore->Free(storagePageGroupRefs);
            if (HasError(error)) {
                // TODO: log error
            }
        }
    };

    record->PageGroupIndex = CreatePageGroupIndex(
        requestPageGroupRefs,
        storagePageGroupRefs);

    auto metaFuture = MetaStore->Write(record->Lsn, SerializeRecord(*record));
    metaFuture.Wait();      // TODO:
    if (auto error = metaFuture.GetValue(); HasError(error)) {
        record->Promise.SetValue(error);
        return record->Promise.GetFuture();
    }

    record->Ready.store(true);

    auto nextRecord = record;

    while (nextRecord
        && nextRecord->Ready.load()
        && LogPageMap.AddNext(*nextRecord)) {

        nextRecord->Promise.SetValue(MakeError(S_OK));
        nextRecord = LogRecordChain.GetNext(nextRecord->Lsn);
    }

    return record->Promise.GetFuture();
}

NCloud::NProto::TError TJournal::TImpl::FillPageGroups(
    const TVector<std::pair<ui64, TPageGroupRef>>& index,
    google::protobuf::RepeatedPtrField<
        NCloud::NProto::TDevicePageGroup>* pageGroups) const
{
    auto future = DataStore->ReadPageGroups(ExtractRefs(index));
    future.Wait();   // TODO:
    auto response = future.GetValue();

    if (HasError(response)) {
        return response.GetError();
    }

    // one buffer per page, in the order the ranges were asked for
    auto buffers = response.ExtractResult();

    size_t bufferIndex = 0;
    for (const auto& [pageNo, groupRef]: index) {
        STORAGE_VERIFY(
            bufferIndex + groupRef.PageCount <= buffers.size(),
            "PageNo",
            pageNo);

        auto& pageGroup = *pageGroups->Add();
        pageGroup.SetFirstPageNo(pageNo);

        for (ui64 i = 0; i < groupRef.PageCount; ++i) {
            const auto& buffer = buffers[bufferIndex++];
            pageGroup.AddContent(TString(buffer.Data(), buffer.Size()));
        }
    }

    return MakeError(S_OK);
}

[[nodiscard]] auto TJournal::TImpl::Read(
    const NCloud::NProto::TReadPagesRequest& request) const
        -> TFuture<NCloud::NProto::TReadPagesResponse>
{
    auto pinnedLsn = FlushedLsnTracker.Acquire();
    Y_DEFER {
        FlushedLsnTracker.Release(pinnedLsn);
    };

    NCloud::NProto::TReadPagesResponse result;

    TVector<TPageGroupRef> pages;
    pages.reserve(request.PageGroupRefsSize());
    for (const auto& ref: request.GetPageGroupRefs()) {
        pages.push_back({
            .FirstPageNo = ref.GetFirstPageNo(),
            .PageCount = ref.GetPageCount()});
    }

    const auto& [lastIndexedLsn, index] = LogPageMap.GetIndex(pages, pinnedLsn);
    result.SetLastLogSequenceNumber(lastIndexedLsn);

    auto error = FillPageGroups(index, result.MutablePageGroups());

    if (HasError(error)) {
        return MakeFuture<NCloud::NProto::TReadPagesResponse>(
            TErrorResponse(error));
    }

    return MakeFuture(result);
}

[[nodiscard]] auto TJournal::TImpl::ReadTail(
    ui64 afterLsn,
    ui64 maxRecordCnt) const
        -> TFuture<NCloud::NProto::TReadJournalTailResponse>
{
    auto lastAckedLsn = LastAckedLsn.load();
    afterLsn = Max(afterLsn, lastAckedLsn);

    auto pinnedLsn = FlushedLsnTracker.AcquireFrom(afterLsn);
    Y_DEFER {
        FlushedLsnTracker.Release(pinnedLsn);
    };

    auto records = LogRecordChain.GetReadyTail(pinnedLsn, maxRecordCnt);

    NCloud::NProto::TReadJournalTailResponse response;
    response.SetLastAckedLogSequenceNumber(lastAckedLsn);

    for (const auto& record: records) {
        auto& journalRecord = *response.AddRecords();
        journalRecord.SetLogSequenceNumber(record->Lsn);
        journalRecord.SetPrevLogSequenceNumber(record->PrevLsn);

        auto error = FillPageGroups(
            record->PageGroupIndex,
            journalRecord.MutablePageGroups());

        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadJournalTailResponse>(
                TErrorResponse(error));
        }
    }

    return MakeFuture(response);
}

[[nodiscard]] auto TJournal::TImpl::AdvanceLastAckedLsn(ui64 lastAckedLsn)
    -> TFuture<NCloud::NProto::TError>
{
    if (lastAckedLsn > LogPageMap.GetLastIndexedLsn()) {
        return MakeFuture(MakeError(E_ARGUMENT));
    }

    if (AdvancingLastAckedLsn.exchange(true) == true) {
        return MakeFuture(MakeError(E_REJECTED));
    }
    Y_DEFER {
        AdvancingLastAckedLsn.store(false);
    };

    if (lastAckedLsn <= LastAckedLsn.load()) {
        return MakeFuture(MakeError(S_ALREADY));
    }

    TJournalMetadata metadata = {
        .Version = CurrentFormatVersion,
        .LastAckedLsn = lastAckedLsn,
    };

    auto future = MetaStore->Write(MetadataKey, SerializeMetadata(metadata));
    future.Wait();  // TODO
    auto error = future.GetValue();
    if (HasError(error)) {
        return MakeFuture(error);
    }

    LastAckedLsn.store(lastAckedLsn);
    return MakeFuture(MakeError(S_OK));
}

[[nodiscard]] auto TJournal::TImpl::GetFirstRecordToFlush() const
    -> TFutureResultOrError<NCloud::NProto::TJournalRecord>
{
    using TResult = TResultOrError<NCloud::NProto::TJournalRecord>;

    auto pinnedLsn = FlushedLsnTracker.Acquire();
    Y_DEFER {
        FlushedLsnTracker.Release(pinnedLsn);
    };

    NCloud::NProto::TJournalRecord response;

    auto record = LogRecordChain.GetNext(pinnedLsn);
    if (!record || !record->Ready.load() ||
        record->Lsn > LastAckedLsn.load())
    {
        return MakeFuture<TResult>(std::move(response));
    }

    response.SetLogSequenceNumber(record->Lsn);
    response.SetPrevLogSequenceNumber(record->PrevLsn);

    auto error = FillPageGroups(
        record->PageGroupIndex,
        response.MutablePageGroups());

    if (HasError(error)) {
        return MakeFuture<TResult>(error);
    }

    return MakeFuture<TResult>(std::move(response));
}

void TJournal::TImpl::MarkRecordAsFlushed(ui64 lsn) {
    FlushedLsnTracker.AdvanceWatermark(lsn);
}

TFuture<NCloud::NProto::TError> TJournal::TImpl::CleanupFlushedRecords() {
    auto minAcquired = FlushedLsnTracker.GetMinAcquired();

    auto future = MetaStore->EraseTo(minAcquired);
    future.Wait();   // TODO:
    if (auto error = future.GetValue(); HasError(error)) {
        return MakeFuture(error);
    }

    auto records = LogRecordChain.EraseTo(minAcquired);
    LogPageMap.EraseTo(minAcquired);

    auto result = MakeError(S_OK);
    for (const auto& record : records) {
        auto error = DataStore->Free(ExtractRefs(record->PageGroupIndex));
        if (HasError(error)) {
            // TODO: log error
            result = error;
        }
    }

    return MakeFuture(result);
}

////////////////////////////////////////////////////////////////////////////////

TJournal::TJournal(IKeyBufferStorePtr logMetaStore, IPageStorePtr logDataStore)
    : Impl(std::make_unique<TImpl>(
          std::move(logMetaStore),
          std::move(logDataStore)))
{}

TJournal::~TJournal() = default;

TFuture<NCloud::NProto::TError> TJournal::Restore()
{
    return Impl->Restore();
}

TFuture<NCloud::NProto::TError> TJournal::Write(
    const NCloud::NProto::TWriteLogRecordRequest& request)
{
    return Impl->Write(request);
}

auto TJournal::Read(
    const NCloud::NProto::TReadPagesRequest& request) const
    -> TFuture<NCloud::NProto::TReadPagesResponse>
{
    return Impl->Read(request);
}

auto TJournal::ReadTail(ui64 afterLsn, ui64 maxRecordCnt) const
    -> TFuture<NCloud::NProto::TReadJournalTailResponse>
{
    return Impl->ReadTail(afterLsn, maxRecordCnt);
}

TFuture<NCloud::NProto::TError> TJournal::AdvanceLastAckedLsn(ui64 lastAckedLsn)
{
    return Impl->AdvanceLastAckedLsn(lastAckedLsn);
}

auto TJournal::GetFirstRecordToFlush() const
    -> TFutureResultOrError<NCloud::NProto::TJournalRecord>
{
    return Impl->GetFirstRecordToFlush();
}

void TJournal::MarkRecordAsFlushed(ui64 lsn)
{
    Impl->MarkRecordAsFlushed(lsn);
}

TFuture<NCloud::NProto::TError> TJournal::CleanupFlushedRecords()
{
    return Impl->CleanupFlushedRecords();
}

}   // namespace NCloud::NJournalled
