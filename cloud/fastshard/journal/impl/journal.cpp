#include "journal.h"

#include "device_page_store.h"
#include "key_buffer_store.h"
#include "log_chain.h"
#include "log_index.h"
#include "lsn_barrier.h"

#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

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

public:
    TJournal(
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        IKeyBufferStorePtr metaStore,
        IDevicePageStorePtr dataStore,
        ui64 devicePageCount);

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
    NCloud::NProto::TError ValidateWriteRequest(
        const NCloud::NProto::TWriteLogRecordRequest& request) const;
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

    return MakeFuture<TResponse>(TErrorResponse(E_NOT_IMPLEMENTED, "Write"));
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

TFuture<NCloud::NProto::TReadPagesResponse> TJournal::Read(
    NCloud::NProto::TReadPagesRequest request) const
{
    using TResponse = NCloud::NProto::TReadPagesResponse;

    auto err = ValidatePageRanges(request.GetPageGroupRefs(), DevicePageCount);
    if (HasError(err)) {
        return MakeFuture<TResponse>(TErrorResponse(std::move(err)));
    }

    return MakeFuture<TResponse>(TErrorResponse(E_NOT_IMPLEMENTED, "Read"));
}

TFuture<NCloud::NProto::TReadJournalTailResponse> TJournal::ReadTail(
    NCloud::NProto::TReadJournalTailRequest request) const
{
    Y_UNUSED(request);

    return MakeFuture<NCloud::NProto::TReadJournalTailResponse>(
        TErrorResponse(E_NOT_IMPLEMENTED, "ReadTail"));
}

TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse>
TJournal::AdvanceLastAckedLsn(
    NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
{
    Y_UNUSED(request);

    return MakeFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse>(
        TErrorResponse(E_NOT_IMPLEMENTED, "AdvanceLastAckedLsn"));
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
