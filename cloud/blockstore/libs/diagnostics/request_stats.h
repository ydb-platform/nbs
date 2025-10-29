#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/request_counters.h>

#include <util/datetime/base.h>

#include <span>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IRequestStats
{
    virtual ~IRequestStats() = default;

    virtual ui64 RequestStarted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 requestBytes) = 0;

    virtual TDuration RequestCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        TDuration postponedTime,
        ui64 requestBytes,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags,
        bool unaligned,
        ECalcMaxTime calcMaxTime,
        ui64 responseSent) = 0;

    virtual void AddIncompleteStats(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        TRequestTime requestTime,
        ECalcMaxTime calcMaxTime) = 0;

    virtual void AddRetryStats(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags) = 0;

    virtual void RequestPostponed(EBlockStoreRequest requestType) = 0;
    virtual void RequestPostponedServer(EBlockStoreRequest requestType) = 0;
    virtual void RequestAdvanced(EBlockStoreRequest requestType) = 0;
    virtual void RequestAdvancedServer(EBlockStoreRequest requestType) = 0;
    virtual void RequestFastPathHit(EBlockStoreRequest requestType) = 0;

    using TTimeBucket = std::pair<TDuration, ui64>;
    using TSizeBucket = std::pair<ui64, ui64>;

    virtual void BatchCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) = 0;

    virtual void UpdateStats(bool updateIntervalFinished) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRequestStatsPtr CreateClientRequestStats(
    NMonitoring::TDynamicCountersPtr counters,
    ITimerPtr timer,
    EHistogramCounterOptions histogramCounterOptions);
IRequestStatsPtr CreateServerRequestStats(
    NMonitoring::TDynamicCountersPtr counters,
    ITimerPtr timer,
    EHistogramCounterOptions histogramCounterOptions,
    TVector<std::pair<ui64, ui64>> executionTimeSizeClasses);
IRequestStatsPtr CreateRequestStatsStub();

}   // namespace NCloud::NBlockStore
