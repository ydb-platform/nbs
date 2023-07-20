#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/datetime/base.h>
#include <util/generic/flags.h>

#include <span>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

enum class ECalcMaxTime
{
    ENABLE,
    DISABLE
};

////////////////////////////////////////////////////////////////////////////////

class TRequestCounters
{
    struct TSpecialCounters;
    struct TStatCounters;

public:
    enum class EOption
    {
        OnlyReadWriteRequests       = (1 << 0),
        ReportDataPlaneHistogram    = (1 << 1),
        ReportControlPlaneHistogram = (1 << 2),
        AddSpecialCounters          = (1 << 3),
    };

    using TRequestType = TDiagnosticsRequestType;

    Y_DECLARE_FLAGS(EOptions, EOption);

private:
    const std::function<TString(TRequestType)> RequestType2Name;
    const std::function<bool(TRequestType)> IsReadWriteRequestType;
    const EOptions Options;

    THolder<TSpecialCounters> SpecialCounters;
    TVector<TStatCounters> CountersByRequest;
    TVector<TRequestCountersPtr> Subscribers;

public:
    TRequestCounters(
        ITimerPtr timer,
        ui32 requestCount,
        std::function<TString(TRequestType)> requestType2Name,
        std::function<bool(TRequestType)> isReadWriteRequestType,
        EOptions options = {});
    ~TRequestCounters();

    void Register(NMonitoring::TDynamicCounters& counters);

    void Subscribe(TRequestCountersPtr subscriber);

    ui64 RequestStarted(
        TRequestType requestType,
        ui32 requestBytes);

    //TODO: rollback commit after NBS-4239 is fixed
    TDuration RequestCompleted(
        TRequestType requestType,
        ui64 requestStarted,
        TDuration postponedTime,
        ui32 requestBytes,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags,
        bool unaligned,
        ECalcMaxTime calMaxTime = ECalcMaxTime::ENABLE,
        ui64 responseSent = 0);

    void AddRetryStats(
        TRequestType requestType,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags);

    void RequestPostponed(TRequestType requestType);
    void RequestPostponedServer(TRequestType requestType);
    void RequestAdvanced(TRequestType requestType);
    void RequestAdvancedServer(TRequestType requestType);
    void RequestFastPathHit(TRequestType requestType);

    //TODO: rollback commit after NBS-4239 is fixed
    void AddIncompleteStats(
        TRequestType requestType,
        TDuration executionTime,
        TDuration totalTime,
        ECalcMaxTime calcMaxTime = ECalcMaxTime::ENABLE);

    using TTimeBucket = std::pair<TDuration, ui64>;
    using TSizeBucket = std::pair<ui64, ui64>;

    void BatchCompleted(
        TRequestType requestType,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist
    );

    void UpdateStats(bool updatePercentiles = false);

private:
    void RequestStartedImpl(
        TRequestType requestType,
        ui32 requestBytes);

    void RequestCompletedImpl(
        TRequestType requestType,
        ui64 requestStarted,
        ui64 responseSent,
        ui64 requestCompleted,
        TDuration postponedTime,
        ui32 requestBytes,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags,
        bool unaligned,
        ECalcMaxTime calMaxTime = ECalcMaxTime::ENABLE);

    bool ShouldReport(TRequestType requestType) const;

    template<typename TMethod, typename... TArgs>
    void NotifySubscribers(TMethod&& m, TArgs&&... args);
};

Y_DECLARE_OPERATORS_FOR_FLAGS(TRequestCounters::EOptions);

}   // namespace NCloud
