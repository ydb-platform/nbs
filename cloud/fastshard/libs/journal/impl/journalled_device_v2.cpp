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

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration IdleFlushDelay = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

NProto::TReadPagesRequest MakeMissingRequest(
    const NProto::TReadPagesRequest& originalRequest,
    const NProto::TReadPagesResponse& journalResponse)
{
    TMap<ui64, ui64> covered;
    for (const auto& group: journalResponse.GetPageGroups()) {
        if (!group.ContentSize()) {
            continue;
        }

        const ui64 firstPageNo = group.GetFirstPageNo();
        covered.emplace(firstPageNo, firstPageNo + group.ContentSize());
    }

    NProto::TReadPagesRequest missing;
    missing.MutableHeaders()->CopyFrom(originalRequest.GetHeaders());
    missing.SetDeviceUUID(originalRequest.GetDeviceUUID());

    for (const auto& ref: originalRequest.GetPageGroupRefs()) {
        const ui64 endPageNo = ref.GetFirstPageNo() + ref.GetPageCount();
        ui64 pageNo = ref.GetFirstPageNo();

        auto addRef = [&](ui64 begin, ui64 end)
        {
            if (begin >= end) {
                return;
            }

            auto& missingRef = *missing.AddPageGroupRefs();
            missingRef.SetFirstPageNo(begin);
            missingRef.SetPageCount(end - begin);
            missingRef.SetPageSize(ref.GetPageSize());
        };

        auto it = covered.upper_bound(pageNo);
        if (it != covered.begin() && std::prev(it)->second > pageNo) {
            --it;
        }

        for (; it != covered.end() && it->first < endPageNo; ++it) {
            addRef(pageNo, it->first);
            pageNo = it->second;

            if (pageNo >= endPageNo) {
                break;
            }
        }

        addRef(pageNo, endPageNo);
    }

    return missing;
}

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

    TLsnBarrier IndexedLsnBarrier;

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

private:
    template <typename TRequest>
    NProto::TError ValidateRequest(const TRequest& request) const;

    template <typename T, typename F>
    TFuture<T> Execute(F func);

    NProto::TError DoStart();

    NProto::TReadPagesResponse DoReadPages(NProto::TReadPagesRequest request);

    NProto::TWriteLogRecordResponse DoWriteLogRecord(
        NProto::TWriteLogRecordRequest request);

    void ScheduleFlushCycle();

    void RunFlushCycle();
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
    return Execute<NProto::TError>([](auto& self) { return self.DoStart(); });
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

    IndexedLsnBarrier.Advance(response.GetResult());

    FlushCycleStopped = NewPromise<void>();
    ScheduleFlushCycle();
    return {};
}

TFuture<void> TJournalledDeviceV2::Stop() noexcept
{
    ShouldStop.store(true);

    if (!FlushCycleStopped.Initialized()) {
        return MakeFuture();
    }

    return FlushCycleStopped.GetFuture();
}

TFuture<NProto::TReadPagesResponse> TJournalledDeviceV2::ReadPages(
    NProto::TReadPagesRequest request)
{
    if (auto error = ValidateRequest(request); HasError(error)) {
        return MakeFuture<NProto::TReadPagesResponse>(
            TErrorResponse(std::move(error)));
    }

    return Execute<NProto::TReadPagesResponse>(
        [request = std::move(request)](auto& self) mutable
        { return self.DoReadPages(std::move(request)); });
}

NProto::TReadPagesResponse TJournalledDeviceV2::DoReadPages(
    NProto::TReadPagesRequest request)
{
    const auto lsnBarrierGuard = IndexedLsnBarrier.Acquire();

    auto journalFuture = Journal->Read(request);
    auto journalResp = Executor->ExtractResponse(std::move(journalFuture));
    if (HasError(journalResp)) {
        return journalResp;
    }

    ui64 lastAckedLsn = journalResp.GetLastAckedLogSequenceNumber();

    auto missing = MakeMissingRequest(request, journalResp);

    NProto::TReadPagesResponse deviceResp;

    if (missing.PageGroupRefsSize()) {
        const auto rangeRefs = MakePageRangeRefs(missing);

        auto deviceFuture = DataStore->ReadPages(rangeRefs);

        auto result = Executor->ExtractResponse(std::move(deviceFuture));
        if (HasError(result)) {
            return TErrorResponse(result.GetError());
        }

        deviceResp = MakeReadPagesResponse(rangeRefs, result.GetResult());
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
    if (auto error = ValidateRequest(request); HasError(error)) {
        return MakeFuture<NProto::TWriteLogRecordResponse>(
            TErrorResponse(std::move(error)));
    }

    return Execute<NProto::TWriteLogRecordResponse>(
        [request = std::move(request)](auto& self) mutable
        { return self.DoWriteLogRecord(std::move(request)); });
}

NProto::TWriteLogRecordResponse TJournalledDeviceV2::DoWriteLogRecord(
    NProto::TWriteLogRecordRequest request)
{
    ui64 lsn = request.GetLogSequenceNumber();
    auto future = Journal->Write(std::move(request));
    auto response = Executor->ExtractResponse(std::move(future));
    if (HasError(response)) {
        return response;
    }

    IndexedLsnBarrier.Advance(lsn);
    return response;
}

TFuture<NProto::TReadJournalTailResponse> TJournalledDeviceV2::ReadJournalTail(
    NProto::TReadJournalTailRequest request)
{
    if (auto error = ValidateRequest(request); HasError(error)) {
        return MakeFuture<NProto::TReadJournalTailResponse>(
            TErrorResponse(std::move(error)));
    }

    return Execute<NProto::TReadJournalTailResponse>(
        [request = std::move(request)](auto& self) mutable
        {
            return self.Executor->ExtractResponse(
                self.Journal->ReadTail(std::move(request)));
        });
}

auto TJournalledDeviceV2::AdvanceLsnLowWatermark(
    NProto::TAdvanceLsnLowWatermarkRequest request)
    -> TFuture<NProto::TAdvanceLsnLowWatermarkResponse>
{
    if (auto error = ValidateRequest(request); HasError(error)) {
        return MakeFuture<NProto::TAdvanceLsnLowWatermarkResponse>(
            TErrorResponse(std::move(error)));
    }

    return Execute<NProto::TAdvanceLsnLowWatermarkResponse>(
        [request = std::move(request)](auto& self) mutable
        {
            return self.Executor->ExtractResponse(
                self.Journal->AdvanceLsnLowWatermark(std::move(request)));
        });
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
    ui64 maxAllowedLsn = IndexedLsnBarrier.GetBarrierLsn();
    ui64 lastFlushedLsn = 0;

    while (!ShouldStop.load()) {
        auto future = Journal->GetRecordToFlush(maxAllowedLsn);
        auto response = Executor->ExtractResponse(future);
        if (HasError(response)) {
            STORAGE_ERROR(
                "unable to get the next record to flush: "
                << FormatError(response.GetError()));
            break;
        }

        auto record = response.ExtractResult();
        ui64 lsn = record.GetLogSequenceNumber();
        if (!lsn) {
            // no record to flush
            break;
        }

        if (record.PageGroupsSize()) {
            auto writeFuture = DataStore->WritePages(MakePageRanges(record));

            auto writeError = Executor->WaitFor(writeFuture);
            if (HasError(writeError)) {
                STORAGE_ERROR(
                    "unable to flush the record with lsn "
                    << lsn << ": " << FormatError(writeError));
                break;
            }
        }

        Journal->MarkRecordAsFlushed(lsn);
        lastFlushedLsn = lsn;
    }

    if (ShouldStop.load()) {
        FlushCycleStopped.SetValue();
        return;
    }

    auto future = Journal->CleanupFlushedRecords();
    const auto& response = Executor->WaitFor(future);
    if (HasError(response)) {
        STORAGE_ERROR(
            "unable to cleanup flushed records up to lsn "
            << lastFlushedLsn << ": " << FormatError(response));
    }

    RunningCont()->SleepT(IdleFlushDelay);

    ScheduleFlushCycle();
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

template <typename T, typename F>
TFuture<T> TJournalledDeviceV2::Execute(F func)
{
    return Executor->Execute(
        [weakSelf = weak_from_this(), func = std::move(func)]() mutable -> T
        {
            auto self = weakSelf.lock();
            if (!self) {
                return TErrorResponse(E_FAIL, "TJournalledDevice is destroyed");
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
