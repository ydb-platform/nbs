#include "journalled_device_v2.h"

#include "device_helpers.h"
#include "journal.h"
#include "lsn_barrier.h"

#include <cloud/fastshard/libs/journal/iface/device.h>
#include <cloud/fastshard/libs/journal/iface/journalled_device.h>

#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/coroutine/engine/impl.h>
#include <library/cpp/threading/future/wait/wait.h>

#include <util/generic/algorithm.h>
#include <util/generic/buffer.h>
#include <util/generic/hash.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration IdleFlushDelay = TDuration::MilliSeconds(10);

// How much a flush batch may hold.
constexpr ui64 MaxFlushBatchPages = 1024;

////////////////////////////////////////////////////////////////////////////////

struct TPageContent
{
    ui64 PageNo = 0;
    const TString* Content = nullptr;
};

// Merges the pages of the records into page ranges. The records are in lsn
// order. If two records write the same page, the later record wins.
TVector<TPageRange> MergeRecordPages(const TJournalRecords& records)
{
    //
    // Skip the merge: one record never writes the same page twice
    //

    if (records.size() == 1) {
        return MakePageRanges(records[0]);
    }

    //
    // Sort the pages by number. The stable sort keeps equal pages in lsn order
    //

    size_t pageCount = 0;
    for (const auto& record: records) {
        for (const auto& group: record.GetPageGroups()) {
            pageCount += group.ContentSize();
        }
    }

    TVector<TPageContent> pages;
    pages.reserve(pageCount);

    for (const auto& record: records) {
        for (const auto& group: record.GetPageGroups()) {
            ui64 pageNo = group.GetFirstPageNo();
            for (const auto& content: group.GetContent()) {
                pages.push_back({.PageNo = pageNo++, .Content = &content});
            }
        }
    }

    StableSortBy(pages, [](const auto& page) { return page.PageNo; });

    TVector<TPageRange> ranges;

    for (size_t i = 0; i < pages.size(); ++i) {
        const auto& [pageNo, content] = pages[i];

        //
        // Skip the page if a later record writes it too
        //

        if (i + 1 < pages.size() && pages[i + 1].PageNo == pageNo) {
            continue;
        }

        if (ranges.empty() ||
            ranges.back().FirstPageNo + ranges.back().Pages.size() != pageNo)
        {
            ranges.push_back({.FirstPageNo = pageNo});
        }

        ranges.back().Pages.emplace_back(content->data(), content->size());
    }

    return ranges;
}

TVector<TPageRangeRef> MakeRangeRefs(const TVector<TPageRange>& ranges)
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

////////////////////////////////////////////////////////////////////////////////

NProto::TReadPagesResponse MergeResponses(
    const NProto::TReadPagesRequest& request,
    NProto::TReadPagesResponse* journalResp,
    NProto::TReadPagesResponse* deviceResp)
{
    THashMap<ui64, TString*> pages;

    auto index = [&](NProto::TReadPagesResponse* source)
    {
        if (!source) {
            return;
        }

        for (auto& group: *source->MutablePageGroups()) {
            ui64 pageNo = group.GetFirstPageNo();
            for (auto& content: *group.MutableContent()) {
                pages.emplace(pageNo++, &content);
            }
        }
    };

    index(journalResp);
    index(deviceResp);

    NProto::TReadPagesResponse response;

    for (const auto& ref: request.GetPageGroupRefs()) {
        if (!ref.GetPageCount()) {
            continue;
        }

        auto* group = response.AddPageGroups();
        group->SetFirstPageNo(ref.GetFirstPageNo());

        for (ui64 i = 0; i < ref.GetPageCount(); ++i) {
            const ui64 pageNo = ref.GetFirstPageNo() + i;

            auto it = pages.find(pageNo);
            if (it == pages.end()) {
                return ErrorResponse<NProto::TReadPagesResponse>(
                    E_INVALID_STATE,
                    TStringBuilder()
                        << "page " << pageNo
                        << " is missing"
                           " in both the journal and the device responses");
            }

            *group->AddContent() = std::move(*it->second);
        }
    }

    return response;
}

////////////////////////////////////////////////////////////////////////////////

class TPagesInFlight
{
private:
    ui64 LastId = 0;
    THashMap<ui64 /*id*/, TVector<TPageRangeRef>> Requests;

public:
    // Fails if any range overlaps a held one. Returns the id to pass to
    // Release.
    TResultOrError<ui64> TryAcquire(const TVector<TPageRange>& ranges)
    {
        TVector<TPageRangeRef> refs;

        for (const auto& range: ranges) {
            const ui64 begin = range.FirstPageNo;
            const ui64 end = begin + range.Pages.size();

            for (const auto& [id, heldRefs]: Requests) {
                for (const auto& ref: heldRefs) {
                    const ui64 refEnd = ref.FirstPageNo + ref.PageCount;
                    if (begin < refEnd && ref.FirstPageNo < end) {
                        return MakeError(
                            E_REJECTED,
                            TStringBuilder()
                                << "pages [" << begin << ", " << end
                                << ") overlap pages [" << ref.FirstPageNo
                                << ", " << refEnd
                                << ") that are being written");
                    }
                }
            }

            refs.push_back({.FirstPageNo = begin, .PageCount = end - begin});
        }

        Requests.emplace(++LastId, std::move(refs));
        return LastId;
    }

    void Release(ui64 id)
    {
        Requests.erase(id);
    }
};

////////////////////////////////////////////////////////////////////////////////

// Counts the requests that are running. The counter starts with one extra
// request that Stop takes away. So the counter reaches zero only after Stop,
// when the last request is done. Thread-safe.
class TRequestsInFlight
{
private:
    std::atomic<ui64> Count = 1;
    TPromise<void> DrainedPromise = NewPromise<void>();

public:
    // Fails when all the requests are done after Stop.
    bool TryAcquire()
    {
        ui64 count = Count.load();
        while (count != 0) {
            if (Count.compare_exchange_weak(count, count + 1)) {
                return true;
            }
        }
        return false;
    }

    void Release()
    {
        if (Count.fetch_sub(1) == 1) {
            DrainedPromise.SetValue();
        }
    }

    // Must be called once.
    void Stop()
    {
        Release();
    }

    // The future is set after Stop, when the last request is done.
    TFuture<void> GetDrainedFuture() const
    {
        return DrainedPromise.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceV2 final
    : public IJournalledDevice
    , public std::enable_shared_from_this<TJournalledDeviceV2>
{
private:
    const ILoggingServicePtr Logging;
    const TExecutorPtr Executor;
    const IJournalPtr Journal;
    const IDevicePtr DataStore;
    const TString DeviceUUID;

    TLog Log;

    std::shared_ptr<TLsnBarrier> IndexedLsnBarrier =
        std::make_shared<TLsnBarrier>();

    // Only WritePages and the flush cycle write directly to the data store.
    // Their page ranges are tracked here so that overlapping writes do not run
    // in parallel.
    TPagesInFlight PagesInFlight;

    // Stop waits for these requests.
    const std::shared_ptr<TRequestsInFlight> RequestsInFlight =
        std::make_shared<TRequestsInFlight>();

    std::atomic_bool ShouldStop = false;
    std::atomic_bool RestoreFailed = false;

    TPromise<void> FlushCycleStopped;

public:
    TJournalledDeviceV2(
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        IJournalPtr journal,
        IDevicePtr dataStore,
        TString deviceUUID);

    TFuture<NProto::TError> Start() noexcept override;

    TFuture<void> Stop() noexcept override;

    TFuture<NProto::TReadPagesResponse> ReadPages(
        NProto::TReadPagesRequest request) override;

    TFuture<NProto::TWriteLogRecordResponse> WriteLogRecord(
        NProto::TWriteLogRecordRequest request) override;

    TFuture<NProto::TReadJournalTailResponse> ReadJournalTail(
        NProto::TReadJournalTailRequest request) override;

    auto AdvanceLsnLowWatermark(NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NProto::TAdvanceLsnLowWatermarkResponse> override;

    TFuture<NProto::TWritePagesResponse> WritePages(
        NProto::TWritePagesRequest request) override;

private:
    template <typename TRequest>
    NProto::TError ValidateRequest(const TRequest& request) const;

    // Runs |func| as a request that Stop waits for.
    template <typename TRequest, typename F>
    std::invoke_result_t<F> RunRequest(const TRequest& request, F func);

    // Runs |func| in the executor.
    template <typename F>
    TFuture<std::invoke_result_t<F, TJournalledDeviceV2&>> Execute(F func);

    NProto::TError DoStart();

    TFuture<NProto::TReadPagesResponse> DoReadPages(
        NProto::TReadPagesRequest request);

    static NProto::TReadPagesResponse OnReadPages(
        NProto::TReadPagesRequest request,
        TVector<TPageRangeRef> missing,
        TFuture<NProto::TReadPagesResponse> journalFuture,
        TFuture<TResultOrError<TVector<TBuffer>>> deviceFuture);

    TFuture<NProto::TWriteLogRecordResponse> DoWriteLogRecord(
        NProto::TWriteLogRecordRequest request);

    NProto::TWriteLogRecordResponse OnWriteLogRecord(
        NProto::TWriteLogRecordResponse response,
        ui64 lsn);

    NProto::TWritePagesResponse DoWritePages(
        NProto::TWritePagesRequest request);

    void ScheduleFlushCycle();

    void RunFlushCycle();

    void CleanupFlushedRecords();
};

////////////////////////////////////////////////////////////////////////////////

TJournalledDeviceV2::TJournalledDeviceV2(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IJournalPtr journal,
    IDevicePtr dataStore,
    TString deviceUUID)
    : Logging(std::move(logging))
    , Executor(std::move(executor))
    , Journal(std::move(journal))
    , DataStore(std::move(dataStore))
    , DeviceUUID(std::move(deviceUUID))
    , Log(Logging->CreateLog("BLOCKSTORE_JOURNALLED_DEVICE"))
{}

TFuture<NProto::TError> TJournalledDeviceV2::Start() noexcept
{
    return Execute([](auto& self) { return self.DoStart(); });
}

NProto::TError TJournalledDeviceV2::DoStart()
{
    auto response = Journal->Restore();
    if (HasError(response)) {
        STORAGE_ERROR(
            "unable to restore the journal on "
            << DeviceUUID.Quote() << ": " << FormatError(response.GetError()));
        RestoreFailed.store(true);
        return response.GetError();
    }

    IndexedLsnBarrier->Advance(response.GetResult());

    FlushCycleStopped = NewPromise<void>();
    ScheduleFlushCycle();
    return {};
}

TFuture<void> TJournalledDeviceV2::Stop() noexcept
{
    //
    // Wait for all the requests that are running. New requests are rejected.
    //

    if (!ShouldStop.exchange(true)) {
        RequestsInFlight->Stop();
    }

    auto drained = RequestsInFlight->GetDrainedFuture();
    if (!FlushCycleStopped.Initialized()) {
        return drained;
    }

    return WaitAll(drained, FlushCycleStopped.GetFuture());
}

TFuture<NProto::TReadPagesResponse> TJournalledDeviceV2::ReadPages(
    NProto::TReadPagesRequest request)
{
    return RunRequest(
        request,
        [&]() { return DoReadPages(std::move(request)); });
}

TFuture<NProto::TReadPagesResponse> TJournalledDeviceV2::DoReadPages(
    NProto::TReadPagesRequest request)
{
    //
    // Pin the indexed lsn until the read is done: the flush cycle does
    // not flush records above the pinned lsn. Without the pin, a record
    // written after the journal lookup could reach the device while the
    // device read is running, and the response would mix pages of
    // different lsns
    //

    auto lsnBarrierGuard =
        std::make_shared<TLsnBarrier::TGuard>(IndexedLsnBarrier->Acquire());

    //
    // Start the journal read. It returns at once with the ranges that
    // the journal does not hold and a future for the pages that it does
    //

    auto journalResult = Journal->Read(request);
    if (HasError(journalResult)) {
        return MakeFuture<NProto::TReadPagesResponse>(
            TErrorResponse(journalResult.GetError()));
    }

    auto [missing, journalFuture] = journalResult.ExtractResult();

    //
    // Start the device read for the missing ranges before waiting for
    // the journal, so that the two reads run in parallel
    //

    TVector<TFuture<void>> futures;
    futures.push_back(journalFuture.IgnoreResult());

    TFuture<TResultOrError<TVector<TBuffer>>> deviceFuture;
    if (missing) {
        deviceFuture = DataStore->ReadPages(missing);
        futures.push_back(deviceFuture.IgnoreResult());
    }

    return WaitAll(std::move(futures))
        .Apply(
            [lsnBarrierGuard = std::move(lsnBarrierGuard),
             request = std::move(request),
             missing = std::move(missing),
             journalFuture = std::move(journalFuture),
             deviceFuture = std::move(deviceFuture)](const auto&)
            {
                // Release the indexed lsn when the read is done and not when
                // the callback object is destroyed, which can be much later
                auto guard = std::move(*lsnBarrierGuard);

                return OnReadPages(
                    std::move(request),
                    std::move(missing),
                    std::move(journalFuture),
                    std::move(deviceFuture));
            });
}

NProto::TReadPagesResponse TJournalledDeviceV2::OnReadPages(
    NProto::TReadPagesRequest request,
    TVector<TPageRangeRef> missing,
    TFuture<NProto::TReadPagesResponse> journalFuture,
    TFuture<TResultOrError<TVector<TBuffer>>> deviceFuture)
{
    auto journalResp = journalFuture.ExtractValue();
    if (HasError(journalResp)) {
        return journalResp;
    }

    ui64 lastAckedLsn = journalResp.GetLastAckedLogSequenceNumber();

    NProto::TReadPagesResponse deviceResp;

    if (missing) {
        auto result = deviceFuture.ExtractValue();
        if (HasError(result)) {
            return TErrorResponse(result.GetError());
        }

        deviceResp = MakeReadPagesResponse(missing, result.GetResult());
        if (HasError(deviceResp)) {
            return deviceResp;
        }
    }

    auto response = MergeResponses(request, &journalResp, &deviceResp);
    if (HasError(response)) {
        return response;
    }

    response.SetLastAckedLogSequenceNumber(lastAckedLsn);
    return response;
}

TFuture<NProto::TWriteLogRecordResponse> TJournalledDeviceV2::WriteLogRecord(
    NProto::TWriteLogRecordRequest request)
{
    return RunRequest(
        request,
        [&]() { return DoWriteLogRecord(std::move(request)); });
}

TFuture<NProto::TWriteLogRecordResponse> TJournalledDeviceV2::DoWriteLogRecord(
    NProto::TWriteLogRecordRequest request)
{
    const ui64 lsn = request.GetLogSequenceNumber();

    auto future = Journal->Write(std::move(request));
    return future.Apply(
        [lsn, weakSelf = weak_from_this()](
            auto future) -> NProto::TWriteLogRecordResponse
        {
            auto self = weakSelf.lock();
            if (!self) {
                return TErrorResponse(
                    E_CANCELLED,
                    "TJournalledDevice is destroyed");
            }

            return self->OnWriteLogRecord(future.GetValue(), lsn);
        });
}

NProto::TWriteLogRecordResponse TJournalledDeviceV2::OnWriteLogRecord(
    NProto::TWriteLogRecordResponse response,
    ui64 lsn)
{
    if (HasError(response)) {
        return response;
    }

    IndexedLsnBarrier->Advance(lsn);
    return response;
}

TFuture<NProto::TReadJournalTailResponse> TJournalledDeviceV2::ReadJournalTail(
    NProto::TReadJournalTailRequest request)
{
    return RunRequest(
        request,
        [&]()
        {
            return Execute(
                [request = std::move(request)](auto& self)
                { return self.Journal->ReadTail(std::move(request)); });
        });
}

auto TJournalledDeviceV2::AdvanceLsnLowWatermark(
    NProto::TAdvanceLsnLowWatermarkRequest request)
    -> TFuture<NProto::TAdvanceLsnLowWatermarkResponse>
{
    return RunRequest(
        request,
        [&]()
        {
            return Execute(
                [request = std::move(request)](auto& self)
                {
                    return self.Journal->AdvanceLsnLowWatermark(
                        std::move(request));
                });
        });
}

TFuture<NProto::TWritePagesResponse> TJournalledDeviceV2::WritePages(
    NProto::TWritePagesRequest request)
{
    return RunRequest(
        request,
        [&]()
        {
            return Execute([request = std::move(request)](auto& self)
                           { return self.DoWritePages(std::move(request)); });
        });
}

NProto::TWritePagesResponse TJournalledDeviceV2::DoWritePages(
    NProto::TWritePagesRequest request)
{
    auto ranges = MakePageRanges(request);

    auto acquired = PagesInFlight.TryAcquire(ranges);
    if (HasError(acquired)) {
        return TErrorResponse(acquired.GetError());
    }

    Y_DEFER
    {
        PagesInFlight.Release(acquired.GetResult());
    };

    auto future = DataStore->WritePages(std::move(ranges));
    return TErrorResponse(Executor->ExtractResponse(std::move(future)));
}

void TJournalledDeviceV2::ScheduleFlushCycle()
{
    Executor->Execute(
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->RunFlushCycle();
            }
        });
}

void TJournalledDeviceV2::RunFlushCycle()
{
    while (!ShouldStop.load()) {
        //
        // Take the next batch of records the journal lets go of. The
        // barrier is read anew every time: the records indexed while the
        // previous batch was being flushed join this one
        //

        auto response = Journal->GetRecordsToFlush(
            IndexedLsnBarrier->GetBarrierLsn(),
            MaxFlushBatchPages);

        if (HasError(response)) {
            STORAGE_ERROR(
                "unable to get the next records to flush: "
                << FormatError(response.GetError()));
            break;
        }

        auto records = response.ExtractResult();
        if (records.empty()) {
            // nothing to flush
            break;
        }

        const ui64 lsn = records.back().GetLogSequenceNumber();

        //
        // Write the pages of the whole batch in one request, the later
        // records over the earlier ones, and only then mark the batch as
        // flushed: a reader goes to the data store for the flushed
        // records, so every page of the batch must be there first
        //

        auto ranges = MergeRecordPages(records);
        const auto rangeRefs = MakeRangeRefs(ranges);

        if (!ranges.empty()) {
            auto acquired = PagesInFlight.TryAcquire(ranges);
            if (HasError(acquired)) {
                // WritePages holds the pages, the next cycle retries
                STORAGE_INFO(
                    "postpone the flush of "
                    << records.size() << " records up to lsn " << lsn
                    << ": " << FormatError(acquired.GetError()));
                break;
            }

            auto writeFuture = DataStore->WritePages(std::move(ranges));
            auto writeError = Executor->WaitFor(writeFuture);

            PagesInFlight.Release(acquired.GetResult());

            if (HasError(writeError)) {
                STORAGE_ERROR(
                    "unable to flush "
                    << records.size() << " records up to lsn " << lsn
                    << ": " << FormatError(writeError));
                break;
            }
        }

        Journal->MarkRecordsAsFlushed(lsn, rangeRefs);

        STORAGE_DEBUG(
            "flushed " << records.size() << " records up to lsn " << lsn);

        //
        // Free the journal pages of the batch right away and not when the
        // whole backlog is flushed: a journal that is catching up needs
        // the pages for the records coming in
        //

        CleanupFlushedRecords();
    }

    if (ShouldStop.load()) {
        FlushCycleStopped.SetValue();
        return;
    }

    CleanupFlushedRecords();

    //
    // Clean up the stranded records only here, when there is nothing to
    // flush: the journal walks all records that are not chained yet, and
    // a write waits for that
    //

    Journal->CleanupStrandedRecords();

    RunningCont()->SleepT(IdleFlushDelay);

    ScheduleFlushCycle();
}

void TJournalledDeviceV2::CleanupFlushedRecords()
{
    auto response = Journal->CleanupFlushedRecords();
    if (HasError(response)) {
        STORAGE_ERROR(
            "unable to cleanup flushed records: " << FormatError(response));
    }
}

template <typename TRequest>
NProto::TError TJournalledDeviceV2::ValidateRequest(
    const TRequest& request) const
{
    if (request.GetDeviceUUID().empty()) {
        return MakeError(E_ARGUMENT, "empty device UUID");
    }

    if (request.GetDeviceUUID() != DeviceUUID) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "the request is addressed to device "
                             << request.GetDeviceUUID().Quote() << ", this is "
                             << DeviceUUID.Quote());
    }

    if (RestoreFailed.load()) {
        return MakeError(
            E_IO,
            TStringBuilder() << "the journal on " << DeviceUUID.Quote()
                             << " failed to restore");
    }

    return {};
}

template <typename TRequest, typename F>
std::invoke_result_t<F> TJournalledDeviceV2::RunRequest(
    const TRequest& request,
    F func)
{
    using TResponse = typename std::invoke_result_t<F>::value_type;

    if (auto error = ValidateRequest(request); HasError(error)) {
        return MakeFuture<TResponse>(TErrorResponse(std::move(error)));
    }

    if (ShouldStop.load() || !RequestsInFlight->TryAcquire()) {
        return MakeFuture<TResponse>(TErrorResponse(
            E_REJECTED,
            TStringBuilder()
                << "the device " << DeviceUUID.Quote() << " is stopping"));
    }

    auto future = func();
    future.Subscribe([requests = RequestsInFlight](const auto&)
                     { requests->Release(); });
    return future;
}

template <typename F>
auto TJournalledDeviceV2::Execute(F func)
    -> TFuture<std::invoke_result_t<F, TJournalledDeviceV2&>>
{
    using TResponse = std::invoke_result_t<F, TJournalledDeviceV2&>;

    return Executor->Execute(
        [weakSelf = weak_from_this(), func = std::move(func)]() -> TResponse
        {
            auto self = weakSelf.lock();
            if (!self) {
                return TErrorResponse(
                    E_CANCELLED,
                    "TJournalledDevice is destroyed");
            }

            return func(*self);
        });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceV2(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IJournalPtr journal,
    IDevicePtr dataStore,
    TString deviceUUID)
{
    return std::make_shared<TJournalledDeviceV2>(
        std::move(logging),
        std::move(executor),
        std::move(journal),
        std::move(dataStore),
        std::move(deviceUUID));
}

}   // namespace NCloud::NJournalled
