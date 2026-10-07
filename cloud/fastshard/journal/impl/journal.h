#pragma once

#include "public.h"

#include <cloud/fastshard/journal/iface/public.h>
#include <cloud/fastshard/protos/device.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IJournal
{
    virtual ~IJournal() = default;

    //
    // Restoring
    //

    // Restores the journal state and returns lsn of the last indexed record
    [[nodiscard]] virtual auto Restore()
        -> NThreading::TFuture<TResultOrError<ui64>> = 0;

    //
    // Device API
    //

    [[nodiscard]] virtual auto Write(
        NCloud::NProto::TWriteLogRecordRequest request)
        -> NThreading::TFuture<NCloud::NProto::TWriteLogRecordResponse> = 0;

    [[nodiscard]] virtual auto Read(
        NCloud::NProto::TReadPagesRequest request) const
        -> NThreading::TFuture<NCloud::NProto::TReadPagesResponse> = 0;

    [[nodiscard]] virtual auto ReadTail(
        NCloud::NProto::TReadJournalTailRequest request) const
        -> NThreading::TFuture<NCloud::NProto::TReadJournalTailResponse> = 0;

    [[nodiscard]] virtual auto AdvanceLsnLowWatermark(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> NThreading::TFuture<
            NCloud::NProto::TAdvanceLsnLowWatermarkResponse> = 0;

    //
    // Background cleanup
    //

    [[nodiscard]] virtual auto GetRecordToFlush(ui64 maxAllowedLsn) const
        -> NThreading::TFuture<
            TResultOrError<NCloud::NProto::TJournalRecord>> = 0;

    virtual void MarkRecordAsFlushed(ui64 lsn) = 0;

    [[nodiscard]] virtual auto CleanupFlushedRecords()
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IJournalFactory
{
    virtual ~IJournalFactory() = default;

    [[nodiscard]] virtual IJournalPtr CreateJournal() = 0;
};

////////////////////////////////////////////////////////////////////////////////

IJournalPtr CreateJournal(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IKeyBufferStorePtr metaStore,
    IDevicePageStorePtr dataStore,
    ui64 devicePageCount);

TResultOrError<IJournalFactoryPtr> CreateJournalFactory(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IDevicePtr logMetaDevice,
    IDevicePtr logDataDevice,
    ui64 logMetaPageCount,
    ui64 logDataPageCount,
    ui64 dataPageCount,
    ui32 pageSize);

}   // namespace NCloud::NJournalled
