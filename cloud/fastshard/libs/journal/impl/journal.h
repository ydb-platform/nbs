#pragma once

#include "public.h"

#include <cloud/fastshard/libs/journal/iface/types.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

using TJournalRecords = TVector<TJournalRecord>;

struct TJournalReadResult
{
    // The parts of the requested ranges that the journal does not hold, in
    // the order of the request.
    TVector<TPageRangeRef> MissingRanges;

    // The pages that the journal holds: the requested pages without the
    // missing ones, in the order of the request.
    NThreading::TFuture<TResultOrError<TReadPagesResult>> Future;
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

    // The caller must keep the pages of the record alive until the future is
    // set.
    [[nodiscard]] virtual NThreading::TFuture<NProto::TError> Write(
        TVector<TPageRangeView> ranges,
        ui64 lsn,
        ui64 prevLsn) = 0;

    [[nodiscard]] virtual TResultOrError<TJournalReadResult> Read(
        TVector<TPageRangeRef> rangeRefs) const = 0;

    [[nodiscard]] virtual TResultOrError<TJournalTail> ReadTail(
        ui64 afterLsn,
        ui32 maxRecordCount) const = 0;

    [[nodiscard]] virtual NProto::TError AdvanceLsnLowWatermark(
        ui64 lsnLowWatermark) = 0;

    //
    // Background cleanup
    //

    [[nodiscard]] virtual TResultOrError<TJournalRecords> GetRecordsToFlush(
        ui64 maxAllowedLsn,
        ui64 maxPageCount) const = 0;

    virtual void MarkRecordAsFlushed(
        ui64 lsn,
        const TVector<TPageRangeRef>& ranges) = 0;

    [[nodiscard]] virtual NProto::TError CleanupFlushedRecords() = 0;
};

////////////////////////////////////////////////////////////////////////////////

// The journal keeps the pages of the records in memory until the records are
// cleaned up, up to |maxCachedPageBytes| in total. The flush and the tail
// read take such pages from memory. The pages of the records that do not fit
// are read back from the data store.
IJournalPtr CreateJournal(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IKeyBufferStorePtr metaStore,
    IDevicePageStorePtr dataStore,
    TString deviceUUID,
    ui64 devicePageCount,
    ui64 maxCachedPageBytes);

}   // namespace NCloud::NJournalled
