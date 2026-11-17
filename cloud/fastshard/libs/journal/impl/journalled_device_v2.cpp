#include "journalled_device_v2.h"

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
    TBuffer* Content = nullptr;
};

// Merges the pages of the records into page ranges. The records are in lsn
// order. If two records write the same page, the later record wins. The pages
// are moved out of the records.
TVector<TPageRange> MergeRecordPages(TJournalRecords records)
{
    //
    // Skip the merge: one record never writes the same page twice
    //

    if (records.size() == 1) {
        return std::move(records[0].Ranges);
    }

    //
    // Sort the pages by number. The stable sort keeps equal pages in lsn order
    //

    size_t pageCount = 0;
    for (const auto& record: records) {
        for (const auto& range: record.Ranges) {
            pageCount += range.Pages.size();
        }
    }

    TVector<TPageContent> pages;
    pages.reserve(pageCount);

    for (auto& record: records) {
        for (auto& range: record.Ranges) {
            ui64 pageNo = range.FirstPageNo;
            for (auto& page: range.Pages) {
                pages.push_back({.PageNo = pageNo++, .Content = &page});
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

        ranges.back().Pages.push_back(std::move(*content));
    }

    return ranges;
}

////////////////////////////////////////////////////////////////////////////////

// Builds the pages of |rangeRefs| in the request order. The device holds the
// |missing| pages and the journal holds all the other ones. Both |missing|
// and the two page lists follow the request order.
TResultOrError<TVector<TBuffer>> MergePages(
    const TVector<TPageRangeRef>& rangeRefs,
    const TVector<TPageRangeRef>& missing,
    TVector<TBuffer> journalPages,
    TVector<TBuffer> devicePages)
{
    ui64 missingPageCount = 0;
    for (const auto& ref: missing) {
        missingPageCount += ref.PageCount;
    }

    if (devicePages.size() != missingPageCount) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "the device returned " << devicePages.size()
                             << " pages, expected " << missingPageCount);
    }

    ui64 pageCount = 0;
    for (const auto& ref: rangeRefs) {
        pageCount += ref.PageCount;
    }

    //
    // Skip the merge: one of the two sources holds the whole request
    //

    if (missing.empty() && journalPages.size() == pageCount) {
        return journalPages;
    }

    if (journalPages.empty() && devicePages.size() == pageCount) {
        return devicePages;
    }

    //
    // Walk the requested pages and take each one from the device if the
    // current missing range holds it, and from the journal if not
    //

    TVector<TBuffer> pages;
    pages.reserve(pageCount);

    size_t journalIndex = 0;
    size_t deviceIndex = 0;
    size_t missingIndex = 0;
    ui64 missingOffset = 0;

    auto skipEmptyMissing = [&]()
    {
        while (missingIndex < missing.size() &&
               missingOffset == missing[missingIndex].PageCount)
        {
            ++missingIndex;
            missingOffset = 0;
        }
    };

    skipEmptyMissing();

    for (const auto& ref: rangeRefs) {
        for (ui64 i = 0; i != ref.PageCount; ++i) {
            const ui64 pageNo = ref.FirstPageNo + i;

            if (missingIndex < missing.size() &&
                missing[missingIndex].FirstPageNo + missingOffset == pageNo)
            {
                pages.push_back(std::move(devicePages[deviceIndex++]));
                ++missingOffset;
                skipEmptyMissing();
                continue;
            }

            if (journalIndex == journalPages.size()) {
                return MakeError(
                    E_INVALID_STATE,
                    TStringBuilder()
                        << "page " << pageNo
                        << " is missing"
                           " in both the journal and the device responses");
            }

            pages.push_back(std::move(journalPages[journalIndex++]));
        }
    }

    if (missingIndex != missing.size() || journalIndex != journalPages.size()) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "the journal returned " << journalPages.size()
                << " pages and " << missingPageCount
                << " missing pages for a request of " << pageCount << " pages");
    }

    return pages;
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

    TFuture<TResultOrError<TReadPagesResult>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) override;

    TFuture<NProto::TError> WriteLogRecord(
        TVector<TPageRange> ranges,
        ui64 lsn,
        ui64 prevLsn) override;

    TFuture<TResultOrError<TJournalTail>> ReadJournalTail(
        ui64 afterLsn,
        ui32 maxRecordCount) override;

    TFuture<NProto::TError> AdvanceWatermark(ui64 lsnLowWatermark) override;

    TFuture<NProto::TError> WritePages(TVector<TPageRange> ranges) override;

private:
    NProto::TError CheckRestored() const;

    // Runs |func| as a request that Stop waits for.
    template <typename F>
    std::invoke_result_t<F> RunRequest(F func);

    // Runs |func| in the executor.
    template <typename F>
    TFuture<std::invoke_result_t<F, TJournalledDeviceV2&>> Execute(F func);

    NProto::TError DoStart();

    TFuture<TResultOrError<TReadPagesResult>> DoReadPages(
        TVector<TPageRangeRef> rangeRefs);

    static TResultOrError<TReadPagesResult> OnReadPages(
        TVector<TPageRangeRef> rangeRefs,
        TVector<TPageRangeRef> missing,
        TFuture<TResultOrError<TReadPagesResult>> journalFuture,
        TFuture<TResultOrError<TVector<TBuffer>>> deviceFuture);

    TFuture<NProto::TError> DoWriteLogRecord(
        TVector<TPageRange> ranges,
        ui64 lsn,
        ui64 prevLsn);

    NProto::TError DoWritePages(TVector<TPageRange> ranges);

    void ScheduleFlushCycle();

    void RunFlushCycle();

    void CleanupFlushedRecords(ui64 lastFlushedLsn);
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

TFuture<TResultOrError<TReadPagesResult>> TJournalledDeviceV2::ReadPages(
    TVector<TPageRangeRef> rangeRefs)
{
    return RunRequest([&]() { return DoReadPages(std::move(rangeRefs)); });
}

TFuture<TResultOrError<TReadPagesResult>> TJournalledDeviceV2::DoReadPages(
    TVector<TPageRangeRef> rangeRefs)
{
    using TResult = TResultOrError<TReadPagesResult>;

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

    auto journalResult = Journal->Read(rangeRefs);
    if (HasError(journalResult)) {
        return MakeFuture<TResult>(journalResult.GetError());
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
             rangeRefs = std::move(rangeRefs),
             missing = std::move(missing),
             journalFuture = std::move(journalFuture),
             deviceFuture = std::move(deviceFuture)](const auto&) mutable
            {
                // Release the indexed lsn when the read is done and not when
                // the callback object is destroyed, which can be much later
                auto guard = std::move(*lsnBarrierGuard);

                return OnReadPages(
                    std::move(rangeRefs),
                    std::move(missing),
                    std::move(journalFuture),
                    std::move(deviceFuture));
            });
}

TResultOrError<TReadPagesResult> TJournalledDeviceV2::OnReadPages(
    TVector<TPageRangeRef> rangeRefs,
    TVector<TPageRangeRef> missing,
    TFuture<TResultOrError<TReadPagesResult>> journalFuture,
    TFuture<TResultOrError<TVector<TBuffer>>> deviceFuture)
{
    auto journalResult = journalFuture.ExtractValue();
    if (HasError(journalResult)) {
        return journalResult.GetError();
    }

    auto [lastAckedLsn, journalPages] = journalResult.ExtractResult();

    TVector<TBuffer> devicePages;

    if (missing) {
        auto result = deviceFuture.ExtractValue();
        if (HasError(result)) {
            return result.GetError();
        }

        devicePages = result.ExtractResult();
    }

    auto pages = MergePages(
        rangeRefs,
        missing,
        std::move(journalPages),
        std::move(devicePages));
    if (HasError(pages)) {
        return pages.GetError();
    }

    return TReadPagesResult{
        .LastAckedLsn = lastAckedLsn,
        .Pages = pages.ExtractResult()};
}

TFuture<NProto::TError> TJournalledDeviceV2::WriteLogRecord(
    TVector<TPageRange> ranges,
    ui64 lsn,
    ui64 prevLsn)
{
    return RunRequest(
        [&]() { return DoWriteLogRecord(std::move(ranges), lsn, prevLsn); });
}

TFuture<NProto::TError> TJournalledDeviceV2::DoWriteLogRecord(
    TVector<TPageRange> ranges,
    ui64 lsn,
    ui64 prevLsn)
{
    auto future = Journal->Write(std::move(ranges), lsn, prevLsn);
    return future.Apply(
        [lsn, weakSelf = weak_from_this()](const auto& future)
        {
            auto self = weakSelf.lock();
            if (!self) {
                return MakeError(E_CANCELLED, "TJournalledDevice is destroyed");
            }

            const auto& error = future.GetValue();
            if (!HasError(error)) {
                self->IndexedLsnBarrier->Advance(lsn);
            }

            return error;
        });
}

TFuture<TResultOrError<TJournalTail>> TJournalledDeviceV2::ReadJournalTail(
    ui64 afterLsn,
    ui32 maxRecordCount)
{
    return RunRequest(
        [&]()
        {
            return Execute(
                [afterLsn, maxRecordCount](auto& self)
                { return self.Journal->ReadTail(afterLsn, maxRecordCount); });
        });
}

TFuture<NProto::TError> TJournalledDeviceV2::AdvanceWatermark(
    ui64 lsnLowWatermark)
{
    return RunRequest(
        [&]()
        {
            return Execute(
                [lsnLowWatermark](auto& self) {
                    return self.Journal->AdvanceLsnLowWatermark(
                        lsnLowWatermark);
                });
        });
}

TFuture<NProto::TError> TJournalledDeviceV2::WritePages(
    TVector<TPageRange> ranges)
{
    return RunRequest(
        [&]()
        {
            return Execute([ranges = std::move(ranges)](auto& self) mutable
                           { return self.DoWritePages(std::move(ranges)); });
        });
}

NProto::TError TJournalledDeviceV2::DoWritePages(TVector<TPageRange> ranges)
{
    auto acquired = PagesInFlight.TryAcquire(ranges);
    if (HasError(acquired)) {
        return acquired.GetError();
    }

    Y_DEFER
    {
        PagesInFlight.Release(acquired.GetResult());
    };

    auto future = DataStore->WritePages(std::move(ranges));
    return Executor->ExtractResponse(std::move(future));
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
    ui64 lastFlushedLsn = 0;

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

        const ui64 lsn = records.back().Lsn;
        const size_t recordCount = records.size();

        //
        // Write the pages of the whole batch in one request, the later
        // records over the earlier ones, and only then mark the batch as
        // flushed: a reader goes to the data store for the flushed
        // records, so every page of the batch must be there first
        //

        auto ranges = MergeRecordPages(std::move(records));
        if (!ranges.empty()) {
            auto acquired = PagesInFlight.TryAcquire(ranges);
            if (HasError(acquired)) {
                // WritePages holds the pages, the next cycle retries
                STORAGE_INFO(
                    "postpone the flush of "
                    << recordCount << " records up to lsn " << lsn << ": "
                    << FormatError(acquired.GetError()));
                break;
            }

            auto writeFuture = DataStore->WritePages(std::move(ranges));
            auto writeError = Executor->WaitFor(writeFuture);

            PagesInFlight.Release(acquired.GetResult());

            if (HasError(writeError)) {
                STORAGE_ERROR(
                    "unable to flush " << recordCount << " records up to lsn "
                                       << lsn << ": "
                                       << FormatError(writeError));
                break;
            }
        }

        Journal->MarkRecordAsFlushed(lsn);
        lastFlushedLsn = lsn;

        STORAGE_DEBUG(
            "flushed " << recordCount << " records up to lsn " << lsn);

        //
        // Free the journal pages of the batch right away and not when the
        // whole backlog is flushed: a journal that is catching up needs
        // the pages for the records coming in
        //

        CleanupFlushedRecords(lastFlushedLsn);
    }

    if (ShouldStop.load()) {
        FlushCycleStopped.SetValue();
        return;
    }

    CleanupFlushedRecords(lastFlushedLsn);

    RunningCont()->SleepT(IdleFlushDelay);

    ScheduleFlushCycle();
}

void TJournalledDeviceV2::CleanupFlushedRecords(ui64 lastFlushedLsn)
{
    auto response = Journal->CleanupFlushedRecords();
    if (HasError(response)) {
        STORAGE_ERROR(
            "unable to cleanup flushed records up to lsn "
            << lastFlushedLsn << ": " << FormatError(response));
    }
}

NProto::TError TJournalledDeviceV2::CheckRestored() const
{
    if (RestoreFailed.load()) {
        return MakeError(
            E_IO,
            TStringBuilder() << "the journal on " << DeviceUUID.Quote()
                             << " failed to restore");
    }

    return {};
}

template <typename F>
std::invoke_result_t<F> TJournalledDeviceV2::RunRequest(F func)
{
    using TResponse = typename std::invoke_result_t<F>::value_type;

    if (auto error = CheckRestored(); HasError(error)) {
        return MakeFuture<TResponse>(std::move(error));
    }

    if (ShouldStop.load() || !RequestsInFlight->TryAcquire()) {
        return MakeFuture<TResponse>(MakeError(
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
        [weakSelf = weak_from_this(),
         func = std::move(func)]() mutable -> TResponse
        {
            auto self = weakSelf.lock();
            if (!self) {
                return MakeError(E_CANCELLED, "TJournalledDevice is destroyed");
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
