#pragma once

#include "public.h"

#include <cloud/fastshard/libs/journal/iface/device.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

using TJournalRecords = TVector<NProto::TJournalRecord>;

struct TJournalReadResult
{
    TVector<TPageRangeRef> MissingRanges;
    NThreading::TFuture<NProto::TReadPagesResponse> Future;
};

////////////////////////////////////////////////////////////////////////////////

struct IJournal
{
    virtual ~IJournal() = default;

    //
    // Restoring
    //

    // Restores the journal state and returns lsn of the last indexed record.
    // Must be called on the executor thread.
    [[nodiscard]] virtual TResultOrError<ui64> Restore() = 0;

    //
    // Device API
    //

    [[nodiscard]] virtual auto Write(NProto::TWriteLogRecordRequest request)
        -> NThreading::TFuture<NProto::TWriteLogRecordResponse> = 0;

    [[nodiscard]] virtual TResultOrError<TJournalReadResult> Read(
        NProto::TReadPagesRequest request) const = 0;

    [[nodiscard]] virtual NProto::TReadJournalTailResponse ReadTail(
        NProto::TReadJournalTailRequest request) const = 0;

    [[nodiscard]] virtual auto AdvanceLsnLowWatermark(
        NProto::TAdvanceLsnLowWatermarkRequest request)
        -> NProto::TAdvanceLsnLowWatermarkResponse = 0;

    //
    // Background cleanup
    //

    [[nodiscard]] virtual TResultOrError<TJournalRecords> GetRecordsToFlush(
        ui64 maxAllowedLsn,
        ui64 maxPageCount) const = 0;

    virtual void MarkRecordsAsFlushed(
        ui64 lastFlushedLsn,
        const TVector<TPageRangeRef>& ranges) = 0;

    [[nodiscard]] virtual NProto::TError CleanupFlushedRecords() = 0;

    // Fails the writers of the records that can no longer join the chain and
    // frees their pages. It walks all records that are not chained yet, so
    // call it rarely.
    virtual void CleanupStrandedRecords() = 0;
};

////////////////////////////////////////////////////////////////////////////////

IJournalPtr CreateJournal(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IKeyBufferStorePtr metaStore,
    IDevicePageStorePtr dataStore,
    TString deviceUUID,
    ui64 devicePageCount);

}   // namespace NCloud::NJournalled
