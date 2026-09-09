#include "journalled_device_v2.h"

#include "device.h"
#include "journal.h"
#include "journalled_device.h"
#include "lsn_barrier.h"

#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/coroutine/engine/impl.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/string/builder.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration IdleFlushDelay = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TReadPagesRequest MakeMissingRequest(
    const NCloud::NProto::TReadPagesRequest& originalRequest,
    const NCloud::NProto::TReadPagesResponse& journalResponse)
{
    TMap<ui64, ui64> covered;
    for (const auto& group: journalResponse.GetPageGroups()) {
        if (!group.ContentSize()) {
            continue;
        }

        const ui64 firstPageNo = group.GetFirstPageNo();
        covered.emplace(firstPageNo, firstPageNo + group.ContentSize());
    }

    NCloud::NProto::TReadPagesRequest missing;
    missing.MutableHeaders()->CopyFrom(originalRequest.GetHeaders());
    missing.SetDeviceUUID(originalRequest.GetDeviceUUID());

    for (const auto& ref: originalRequest.GetPageGroupRefs()) {
        const ui64 endPageNo = ref.GetFirstPageNo() + ref.GetPageCount();
        ui64 pageNo = ref.GetFirstPageNo();

        auto addRef = [&] (ui64 begin, ui64 end)
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

NCloud::NProto::TReadPagesResponse MergeResponses(
    const NCloud::NProto::TReadPagesRequest& request,
    NCloud::NProto::TReadPagesResponse* journalResp,
    NCloud::NProto::TReadPagesResponse* deviceResp)
{
    THashMap<ui64, TString*> pages;

    auto index = [&] (NCloud::NProto::TReadPagesResponse* source)
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

    NCloud::NProto::TReadPagesResponse response;

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
                return ErrorResponse<NCloud::NProto::TReadPagesResponse>(
                    E_INVALID_STATE,
                    TStringBuilder() << "page " << pageNo << " is missing"
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

    TLog Log;

    TLsnBarrier IndexedLsnBarrier;

    std::atomic_bool ShouldStop = false;

    TPromise<void> FlushCycleStopped;

public:
    TJournalledDeviceV2(
            ILoggingServicePtr logging,
            TExecutorPtr executor,
            IJournalPtr journal,
            IDevicePtr dataStore)
        : Logging(std::move(logging))
        , Executor(std::move(executor))
        , Journal(std::move(journal))
        , DataStore(std::move(dataStore))
        , Log(Logging->CreateLog("JOURNALLED_DEVICE"))
    {}

    void Start() override {
        auto future = Executor->Execute(
            [weakSelf = weak_from_this()] () {
                auto self = weakSelf.lock();
                if (!self) {
                    return MakeError(E_FAIL, "TJournalledDevice is destroyed");
                }

                return self->DoStart();
            });

        auto error = future.GetValueSync();
        Y_ENSURE(!HasError(error), FormatError(error));
    }

    void Stop() override {
        ShouldStop.store(true);

        if (FlushCycleStopped.Initialized()) {
            FlushCycleStopped.GetFuture().Wait();
        }
    }

    TFuture<NCloud::NProto::TReadPagesResponse> ReadPages(
        NCloud::NProto::TReadPagesRequest request) override
    {
        return Execute<NCloud::NProto::TReadPagesResponse>(
            [request = std::move(request)] (auto& self) mutable
            {
                return self.DoReadPages(std::move(request));
            });
    }

    TFuture<NCloud::NProto::TWriteLogRecordResponse> WriteLogRecord(
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        return Execute<NCloud::NProto::TWriteLogRecordResponse>(
            [request = std::move(request)] (auto& self) mutable
            {
                return self.DoWriteLogRecord(std::move(request));
            });
    }

    TFuture<NCloud::NProto::TReadJournalTailResponse> ReadJournalTail(
        NCloud::NProto::TReadJournalTailRequest request) override
    {
        return Execute<NCloud::NProto::TReadJournalTailResponse>(
            [request = std::move(request)] (auto& self) mutable
            {
                return self.Executor->ExtractResponse(
                    self.Journal->ReadTail(std::move(request)));
            });
    }

    auto AdvanceLsnLowWatermark(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> override
    {
        return Execute<NCloud::NProto::TAdvanceLsnLowWatermarkResponse>(
            [request = std::move(request)] (auto& self) mutable
            {
                return self.Executor->ExtractResponse(
                    self.Journal->AdvanceLastAckedLsn(std::move(request)));
            });
    }

private:
    template <typename T, typename F>
    TFuture<T> Execute(F func)
    {
        return Executor->Execute(
            [weakSelf = weak_from_this(), func = std::move(func)] () mutable -> T
            {
                auto self = weakSelf.lock();
                if (!self) {
                    return ErrorResponse<T>(
                        E_FAIL,
                        "TJournalledDevice is destroyed");
                }

                return func(*self);
            });
    }

    NCloud::NProto::TError DoStart()
    {
        auto response = Executor->ExtractResponse(Journal->Restore());
        if (HasError(response)) {
            return response.GetError();
        }

        IndexedLsnBarrier.Advance(response.GetResult());

        FlushCycleStopped = NewPromise<void>();
        ScheduleFlushCycle();

        return {};
    }

    NCloud::NProto::TReadPagesResponse DoReadPages(
        NCloud::NProto::TReadPagesRequest request)
    {
        const auto lsnBarrierGuard = IndexedLsnBarrier.Acquire();

        auto journalFuture = Journal->Read(request);
        auto journalResp = Executor->ExtractResponse(std::move(journalFuture));
        if (HasError(journalResp)) {
            return journalResp;
        }

        ui64 lastAckedLsn = journalResp.GetLastAckedLogSequenceNumber();

        auto missing = MakeMissingRequest(request, journalResp);

        NCloud::NProto::TReadPagesResponse deviceResp;

        if (missing.PageGroupRefsSize()) {
            auto deviceFuture = DataStore->ReadPages(std::move(missing));
            deviceResp = Executor->ExtractResponse(std::move(deviceFuture));
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

    NCloud::NProto::TWriteLogRecordResponse DoWriteLogRecord(
        NCloud::NProto::TWriteLogRecordRequest request)
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

    void ScheduleFlushCycle()
    {
        Executor->Execute([weakSelf = weak_from_this()] () {
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }

            self->RunFlushCycle();
        });
    }

    void RunFlushCycle()
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

            NCloud::NProto::TWriteLogRecordRequest request;
            request.MutablePageGroups()->Swap(record.MutablePageGroups());

            auto writeFuture = DataStore->WritePages(std::move(request));
            auto writeResponse = Executor->WaitFor(writeFuture);
            if (HasError(writeResponse)) {
                STORAGE_ERROR(
                    "unable to flush the record with lsn " << lsn << ": "
                    << FormatError(writeResponse.GetError()));
                break;
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
                "unable to cleanup flushed records up to lsn " << lastFlushedLsn
                << ": " << FormatError(response));
        }

        RunningCont()->SleepT(IdleFlushDelay);

        ScheduleFlushCycle();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceV2(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IJournalPtr journal,
    IDevicePtr dataStore)
{
    return std::make_shared<TJournalledDeviceV2>(
        std::move(logging),
        std::move(executor),
        std::move(journal),
        std::move(dataStore));
}

}   // namespace NCloud::NJournalled
