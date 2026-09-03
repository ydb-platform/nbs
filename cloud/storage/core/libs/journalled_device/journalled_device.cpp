#include "journalled_device.h"

#include "journal.h"
#include "lsn_barriers.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/scope.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

THashSet<ui64> CollectPageNumbers(
    const NCloud::NProto::TReadPagesResponse& response)
{
    THashSet<ui64> pageNumbers;

    for (const auto& group: response.GetPageGroups()) {
        ui64 pageNo = group.GetFirstPageNo();
        for ([[maybe_unused]] const auto& content: group.GetContent()) {
            pageNumbers.insert(pageNo++);
        }
    }

    return pageNumbers;
}

NCloud::NProto::TReadPagesRequest MakeMissingRequest(
    const NCloud::NProto::TReadPagesRequest& request,
    const THashSet<ui64>& covered)
{
    NCloud::NProto::TReadPagesRequest missing;
    missing.MutableHeaders()->CopyFrom(request.GetHeaders());
    missing.SetDeviceUUID(request.GetDeviceUUID());

    for (const auto& ref: request.GetPageGroupRefs()) {
        ui64 runFirstPageNo = 0;
        ui64 runPageCount = 0;

        auto flush = [&]
        {
            if (!runPageCount) {
                return;
            }

            auto& missingRef = *missing.AddPageGroupRefs();
            missingRef.SetFirstPageNo(runFirstPageNo);
            missingRef.SetPageCount(runPageCount);
            missingRef.SetPageSize(ref.GetPageSize());
            runPageCount = 0;
        };

        for (ui64 i = 0; i < ref.GetPageCount(); ++i) {
            const ui64 pageNo = ref.GetFirstPageNo() + i;

            if (covered.contains(pageNo)) {
                flush();
                continue;
            }

            if (!runPageCount) {
                runFirstPageNo = pageNo;
            }
            ++runPageCount;
        }

        flush();
    }

    return missing;
}

// Both answers are ours and are dropped afterwards, so their page contents are
// moved into the response rather than copied. The journal is indexed first, so
// its version of a page wins over the device's.
void FillResponse(
    const NCloud::NProto::TReadPagesRequest& request,
    NCloud::NProto::TReadPagesResponse* journalPages,
    NCloud::NProto::TReadPagesResponse* devicePages,
    NCloud::NProto::TReadPagesResponse* response)
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

    index(journalPages);
    index(devicePages);

    for (const auto& ref: request.GetPageGroupRefs()) {
        NCloud::NProto::TDevicePageGroup* group = nullptr;

        for (ui64 i = 0; i < ref.GetPageCount(); ++i) {
            const ui64 pageNo = ref.GetFirstPageNo() + i;

            auto it = pages.find(pageNo);
            if (it == pages.end()) {
                group = nullptr;
                continue;
            }

            if (!group) {
                group = response->AddPageGroups();
                group->SetFirstPageNo(pageNo);
            }

            *group->AddContent() = std::move(*it->second);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceV2 final: public IJournalledDevice
{
private:
    TJournal Journal;
    const IDevicePtr Device;

    TLsnBarrier LsnBarrier;

public:
    TJournalledDeviceV2(
            IKeyBufferStorePtr logMetaStore,
            IPageStorePtr logDataStore,
            IDevicePtr device)
        : Journal(std::move(logMetaStore), std::move(logDataStore))
        , Device(std::move(device))
    {}

    TFuture<NCloud::NProto::TError> Restore()
    {
        return Journal.Restore();
    }

    auto ReadPages(TInstant now, NCloud::NProto::TReadPagesRequest request)
        -> TFuture<NCloud::NProto::TReadPagesResponse> override
    {
        auto future = Journal.Read(request);
        future.Wait();  //
        auto journalResponse = future.GetValue();
        if (HasError(journalResponse)) {
            return MakeFuture(journalResponse);
        }

        auto missing = MakeMissingRequest(
            request,
            CollectPageNumbers(journalResponse));

        auto lastLsn = journalResponse.GetLastLogSequenceNumber();

        if (missing.PageGroupRefsSize() == 0) {
            NCloud::NProto::TReadPagesResponse response;
            response.SetLastLogSequenceNumber(lastLsn);
            FillResponse(request, &journalResponse, nullptr, &response);
            return MakeFuture(std::move(response));
        }

        LsnBarrier.Acquire(lastLsn);

        return Device->ReadPages(now, std::move(missing)).Apply(
            [this,
             req = std::move(request),
             journalPages = std::move(journalResponse),
             lastLsn]
            (const auto& future) mutable
            {
                Y_DEFER {
                    LsnBarrier.Release(lastLsn);
                };

                auto deviceResponse = future.GetValue();
                if (HasError(deviceResponse)) {
                    return deviceResponse;
                }

                NCloud::NProto::TReadPagesResponse response;
                response.SetLastLogSequenceNumber(lastLsn);
                FillResponse(req, &journalPages, &deviceResponse, &response);
                return response;
            });
    }

    auto WriteLogRecord(
        TInstant now,
        NCloud::NProto::TWriteLogRecordRequest request)
        -> TFuture<NCloud::NProto::TWriteLogRecordResponse> override
    {
        Y_UNUSED(now);

        return Journal.Write(request).Apply(
            [] (const auto& future)
            {
                NCloud::NProto::TWriteLogRecordResponse response;
                *response.MutableError() = future.GetValue();
                return response;
            });
    }

    auto ReadJournalTail(
        TInstant now,
        NCloud::NProto::TReadJournalTailRequest request)
        -> TFuture<NCloud::NProto::TReadJournalTailResponse> override
    {
        Y_UNUSED(now);

        return Journal.ReadTail(
            request.GetAfterLogSequenceNumber(),
            request.GetMaxRecordCount());
    }

    auto AdvanceLsnLowWatermark(
        TInstant now,
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> override
    {
        Y_UNUSED(now);

        return Journal.AdvanceLastAckedLsn(request.GetLsnLowWatermark()).Apply(
            [] (const auto& future)
            {
                NCloud::NProto::TAdvanceLsnLowWatermarkResponse response;
                *response.MutableError() = future.GetValue();
                return response;
            });
    }

    void Start() override
    {
        // TODO: drive Process() from here
    }

    void Stop() override
    {
        // TODO: stop the background flush
    }

    // run in background
    void Process()
    {
        auto now = TInstant::Now();

        while (true) {
            auto future = Journal.GetFirstRecordToFlush();
            future.Wait();  // TODO:
            auto response = future.ExtractValue();
            if (HasError(response)) {
                // TODO: log error
                break;
            }

            auto record = response.ExtractResult();

            const ui64 lsn = record.GetLogSequenceNumber();
            if (!lsn) {
                // no record to flush
                break;
            }

            auto minAcquiredLsn = LsnBarrier.GetMinAcquired();
            if (minAcquiredLsn > 0 && minAcquiredLsn < lsn) {
                break;
            }

            NCloud::NProto::TWriteLogRecordRequest writeRequest;
            writeRequest.MutablePageGroups()->Swap(record.MutablePageGroups());

            auto writeFuture = Device->WritePages(now, std::move(writeRequest));
            writeFuture.Wait();  // TODO:
            auto writeResponse = writeFuture.GetValue();
            if (HasError(writeResponse)) {
                // TODO: log error
                break;
            }

            Journal.MarkRecordAsFlushed(lsn);
        }

        auto future = Journal.CleanupFlushedRecords();
        future.Wait();  // TODO:
        auto response = future.ExtractValue();
        if (HasError(response)) {
            // log error
            return;
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceV2(
    IKeyBufferStorePtr logMetaStore,
    IPageStorePtr logDataStore,
    IDevicePtr device)
{
    auto journalledDevice = std::make_shared<TJournalledDeviceV2>(
        std::move(logMetaStore),
        std::move(logDataStore),
        std::move(device));

    // TODO:
    auto future = journalledDevice->Restore();
    future.Wait();
    auto error = future.GetValue();
    STORAGE_VERIFY(!HasError(error), "JournalledDevice", error);

    return journalledDevice;
}

}   // namespace NCloud::NJournalled
