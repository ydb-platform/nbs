#pragma once

#include "public.h"

#include "types.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IJournalledDevice
{
    virtual ~IJournalledDevice() = default;

    [[nodiscard]] virtual auto Start() noexcept
        -> NThreading::TFuture<NProto::TError> = 0;

    [[nodiscard]] virtual NThreading::TFuture<void> Stop() noexcept = 0;

    [[nodiscard]] virtual auto ReadPages(TVector<TPageRangeRef> rangeRefs)
        -> NThreading::TFuture<TResultOrError<TReadPagesResult>> = 0;

    // The caller must keep the pages of the record alive until the future is
    // set.
    [[nodiscard]] virtual NThreading::TFuture<NProto::TError> WriteLogRecord(
        TVector<TPageRangeView> ranges,
        ui64 lsn,
        ui64 prevLsn) = 0;

    [[nodiscard]] virtual auto ReadJournalTail(
        ui64 afterLsn,
        ui32 maxRecordCount)
        -> NThreading::TFuture<TResultOrError<TJournalTail>> = 0;

    [[nodiscard]] virtual NThreading::TFuture<NProto::TError> AdvanceWatermark(
        ui64 lsnLowWatermark) = 0;

    // Writes the pages right to the data area, past the journal. The caller
    // must not read or write the same pages at the same time, and must keep
    // the pages alive until the future is set.
    [[nodiscard]] virtual NThreading::TFuture<NProto::TError> WritePages(
        TVector<TPageRangeView> ranges) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceStub();

}   // namespace NCloud::NJournalled
