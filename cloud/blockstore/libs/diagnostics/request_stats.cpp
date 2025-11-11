#include "request_stats.h"

#include "stats_helpers.h"

#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/common/disjoint_interval_map.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>
#include <cloud/storage/core/libs/diagnostics/request_counters.h>
#include <cloud/storage/core/libs/diagnostics/weighted_percentile.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/cputimer.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

class THdrRequestPercentiles
{
    using TDynamicCounterPtr = TDynamicCounters::TCounterPtr;

    struct TSizeClassCounters
    {
        // TLatencyHistogram is not movable, thats why we should wrap it in
        // unique_ptr.
        std::unique_ptr<TLatencyHistogram> ExecutionTimeHist;
        TVector<TDynamicCounterPtr> CountersExecutionTime;
    };

private:
    TVector<TDynamicCounterPtr> CountersExecutionTime;
    TVector<TDynamicCounterPtr> CountersTotal;
    TVector<TDynamicCounterPtr> CountersSize;

    TDisjointIntervalMap<ui64, TSizeClassCounters> ExecutionTimeSizeClasses;

    TLatencyHistogram ExecutionTimeHist;
    TLatencyHistogram TotalHist;
    TSizeHistogram SizeHist;

public:
    explicit THdrRequestPercentiles(
        const TVector<TSizeInterval>& executionTimeSizeClasses)
    {
        for (const auto& [start, end]: executionTimeSizeClasses) {
            ExecutionTimeSizeClasses.Add(
                start,
                end,
                {.ExecutionTimeHist = std::make_unique<TLatencyHistogram>()});
        }
    }

    void Register(TDynamicCounters& counters, const TString& request)
    {
        auto requestGroup = counters.GetSubgroup("request", request);

        auto executionTimeGroup = requestGroup->GetSubgroup("percentiles", "ExecutionTime");
        Register(*executionTimeGroup, CountersExecutionTime);

        auto totalTimeGroup = requestGroup->GetSubgroup("percentiles", "Time");
        Register(*totalTimeGroup, CountersTotal);

        auto sizeGroup = requestGroup->GetSubgroup("percentiles", "Size");
        Register(*sizeGroup, CountersSize);

        for (auto& [_, item]: ExecutionTimeSizeClasses) {
            const auto sizeClassName =
                ToString(TSizeInterval{item.Begin, item.End});

            auto sizeClassCounters =
                executionTimeGroup->GetSubgroup("sizeclass", sizeClassName);

            Register(*sizeClassCounters, item.Value.CountersExecutionTime);
        }
    }

    void UpdateStats()
    {
        Update(CountersTotal, TotalHist);
        Update(CountersSize, SizeHist);
        Update(CountersExecutionTime, ExecutionTimeHist);
        for (auto& [_, item]: ExecutionTimeSizeClasses) {
            Update(item.Value.CountersExecutionTime, *item.Value.ExecutionTimeHist);
        }
    }

    void AddStats(
        TDuration requestExecutionTime,
        TDuration requestTime,
        ui32 requestBytes)
    {
        ExecutionTimeHist.RecordValue(requestExecutionTime);
        TotalHist.RecordValue(requestTime);
        SizeHist.RecordValue(requestBytes);
        ExecutionTimeSizeClasses.VisitOverlapping(
            requestBytes,
            requestBytes + 1,
            [&](TDisjointIntervalMap<ui64, TSizeClassCounters>::TIterator it)
            { it->second.Value.ExecutionTimeHist->RecordValue(requestTime); });
    }

    void BatchCompleted(
        std::span<IRequestStats::TTimeBucket> timeHist,
        std::span<IRequestStats::TSizeBucket> sizeHist)
    {
        for (auto [dt, count]: timeHist) {
            ExecutionTimeHist.RecordValues(dt, count);
        }

        for (auto [dt, count]: timeHist) {
            TotalHist.RecordValues(dt, count);
        }

        for (auto [size, count]: sizeHist) {
            SizeHist.RecordValues(size, count);
        }
    }

private:
    void Register(
        TDynamicCounters& countersGroup,
        TVector<TDynamicCounterPtr>& counters)
    {
        const auto& percentiles = GetDefaultPercentiles();
        for (ui32 i = 0; i < percentiles.size(); ++i) {
            counters.emplace_back(countersGroup.GetCounter(percentiles[i].second));
        }
    }

    void Update(TVector<TDynamicCounterPtr>& counters, THistogramBase& histogram)
    {
        const auto& percentiles = GetDefaultPercentiles();
        for (ui32 i = 0; i < counters.size(); ++i) {
            *counters[i] = histogram.GetValueAtPercentile(percentiles[i].first * 100);
        }
        histogram.Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

class THdrPercentiles
{
private:
    THdrRequestPercentiles ReadBlocksPercentiles;
    THdrRequestPercentiles WriteBlocksPercentiles;
    THdrRequestPercentiles ZeroBlocksPercentiles;

public:
    explicit THdrPercentiles(
            const TVector<TSizeInterval>& executionTimeSizeClasses)
        : ReadBlocksPercentiles(executionTimeSizeClasses)
        , WriteBlocksPercentiles(executionTimeSizeClasses)
        , ZeroBlocksPercentiles(executionTimeSizeClasses)
    {}

    void Register(TDynamicCounters& counters)
    {
        ReadBlocksPercentiles.Register(
            counters,
            GetBlockStoreRequestName(EBlockStoreRequest::ReadBlocks));

        WriteBlocksPercentiles.Register(
            counters,
            GetBlockStoreRequestName(EBlockStoreRequest::WriteBlocks));

        ZeroBlocksPercentiles.Register(
            counters,
            GetBlockStoreRequestName(EBlockStoreRequest::ZeroBlocks));
    }

    void UpdateStats()
    {
        ReadBlocksPercentiles.UpdateStats();
        WriteBlocksPercentiles.UpdateStats();
        ZeroBlocksPercentiles.UpdateStats();
    }

    void AddStats(
        EBlockStoreRequest requestType,
        TDuration requestExecutionTime,
        TDuration requestTime,
        ui32 requestBytes)
    {
        GetPercentiles(requestType)
            .AddStats(requestExecutionTime, requestTime, requestBytes);
    }

    void BatchCompleted(
        EBlockStoreRequest requestType,
        std::span<IRequestStats::TTimeBucket> timeHist,
        std::span<IRequestStats::TSizeBucket> sizeHist)
    {
        GetPercentiles(requestType)
            .BatchCompleted(timeHist, sizeHist);
    }

private:
    THdrRequestPercentiles& GetPercentiles(EBlockStoreRequest requestType)
    {
        switch (requestType) {
            case EBlockStoreRequest::ReadBlocks:
                return ReadBlocksPercentiles;

            case EBlockStoreRequest::WriteBlocks:
                return WriteBlocksPercentiles;

            case EBlockStoreRequest::ZeroBlocks:
                return ZeroBlocksPercentiles;

            default:
                Y_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

constexpr TRequestCounters::EOptions DefaultOptions =
    TRequestCounters::EOption::ReportDataPlaneHistogram |
    TRequestCounters::EOption::AddSpecialCounters |
    TRequestCounters::EOption::OnlyReadWriteRequests;

constexpr TRequestCounters::EOptions GeneralOptions =
    TRequestCounters::EOption::ReportDataPlaneHistogram |
    TRequestCounters::EOption::AddSpecialCounters;

constexpr TRequestCounters::EOptions SSDOrHDDOptions =
    TRequestCounters::EOption::ReportDataPlaneHistogram |
    TRequestCounters::EOption::OnlyReadWriteRequests;

#define BLOCKSTORE_MEDIA_KIND(xxx, ...)                                        \
    xxx(SSD,               SSDOrHDDOptions,   "ssd",           __VA_ARGS__    )\
    xxx(HDD,               SSDOrHDDOptions,   "hdd",           __VA_ARGS__    )\
    xxx(SSDNonrepl,        DefaultOptions,    "ssd_nonrepl",   __VA_ARGS__    )\
    xxx(SSDMirror2,        DefaultOptions,    "ssd_mirror2",   __VA_ARGS__    )\
    xxx(SSDMirror3,        DefaultOptions,    "ssd_mirror3",   __VA_ARGS__    )\
    xxx(SSDLocal,          DefaultOptions,    "ssd_local",     __VA_ARGS__    )\
    xxx(HDDLocal,          DefaultOptions,    "hdd_local",     __VA_ARGS__    )\
    xxx(HDDNonrepl,        DefaultOptions,    "hdd_nonrepl",   __VA_ARGS__    )\
// BLOCKSTORE_MEDIA_KIND

class TRequestStats final
    : public IRequestStats
    , public std::enable_shared_from_this<TRequestStats>
{
private:
    const TDynamicCountersPtr Counters;
    const bool IsServerSide;

    TRequestCounters Total;
    THdrPercentiles HdrTotal;

#define DEFINE_COUNTERS(name, ...)                                              \
    TRequestCounters Total##name;                                               \
    THdrPercentiles HdrTotal##name;                                             \
// DEFINE_REQUEST_COUNTERS

    BLOCKSTORE_MEDIA_KIND(DEFINE_COUNTERS)

#undef DEFINE_COUNTERS

public:

#define INITIALIZE_COUNTERS(name, options, ...)                                \
    , Total##name(MakeRequestCounters(                                         \
          timer,                                                               \
          options,                                                             \
          histogramCounterOptions,                                             \
          executionTimeSizeClasses))                                           \
    , HdrTotal##name(                                                          \
          executionTimeSizeClasses)                                            \
// INITIALIZE_REQUEST_COUNTERS

    TRequestStats(
            TDynamicCountersPtr counters,
            bool isServerSide,
            ITimerPtr timer,
            EHistogramCounterOptions histogramCounterOptions,
            const TVector<TSizeInterval>& executionTimeSizeClasses)
        : Counters(std::move(counters))
        , IsServerSide(isServerSide)
        , Total(MakeRequestCounters(
              timer,
              GeneralOptions,
              histogramCounterOptions,
              executionTimeSizeClasses))
        , HdrTotal(executionTimeSizeClasses)
        BLOCKSTORE_MEDIA_KIND(INITIALIZE_COUNTERS)
    {
        Total.Register(*Counters);
        if (IsServerSide) {
            HdrTotal.Register(*Counters);
        }

#define REGISTER_COUNTERS(name, _, countersName, ...)                          \
    do {                                                                       \
        auto counters = Counters->GetSubgroup("type", #name);                  \
        Total##name.Register(*counters);                                       \
        if (IsServerSide) {                                                    \
            HdrTotal##name.Register(*counters);                                \
        }                                                                      \
    } while (false);                                                           \
// REGISTER_COUNTERS

        BLOCKSTORE_MEDIA_KIND(REGISTER_COUNTERS)
    }

#undef REGISTER_COUNTERS
#undef INITIALIZE_COUNTERS

    ui64 RequestStarted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 requestBytes) override
    {
        auto requestStarted = Total.RequestStarted(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            requestBytes);

        if (IsReadWriteRequest(requestType) &&
            mediaKind != NProto::STORAGE_MEDIA_DEFAULT)
        {
            GetRequestCounters(mediaKind).RequestStarted(
                static_cast<TRequestCounters::TRequestType>(
                    TranslateLocalRequestType(requestType)),
                requestBytes);
        }

        return requestStarted;
    }

    TDuration RequestCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        TDuration postponedTime,
        ui64 requestBytes,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags,
        bool unaligned,
        ECalcMaxTime calcMaxTime,
        ui64 responseSent) override
    {
        auto [execTime, requestTime] = Total.RequestCompleted(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            requestStarted,
            postponedTime,
            requestBytes,
            errorKind,
            errorFlags,
            unaligned,
            calcMaxTime,
            responseSent);

        if (IsReadWriteRequest(requestType) &&
            mediaKind != NProto::STORAGE_MEDIA_DEFAULT)
        {
            GetRequestCounters(mediaKind).RequestCompleted(
                static_cast<TRequestCounters::TRequestType>(
                    TranslateLocalRequestType(requestType)),
                requestStarted,
                postponedTime,
                requestBytes,
                errorKind,
                errorFlags,
                unaligned,
                calcMaxTime,
                responseSent);

            if (IsServerSide) {
                HdrTotal.AddStats(
                    TranslateLocalRequestType(requestType),
                    execTime,
                    requestTime,
                    requestBytes);

                GetHdrPercentiles(mediaKind).AddStats(
                    TranslateLocalRequestType(requestType),
                    execTime,
                    requestTime,
                    requestBytes);
            }
        }

        return requestTime;
    }

    void AddIncompleteStats(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        TRequestTime requestTime,
        ECalcMaxTime calcMaxTime) override
    {
        Total.AddIncompleteStats(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            requestTime.ExecutionTime,
            requestTime.TotalTime,
            calcMaxTime);

        if (IsReadWriteRequest(requestType) &&
            mediaKind != NProto::STORAGE_MEDIA_DEFAULT)
        {
            GetRequestCounters(mediaKind).AddIncompleteStats(
                static_cast<TRequestCounters::TRequestType>(
                    TranslateLocalRequestType(requestType)),
                requestTime.ExecutionTime,
                requestTime.TotalTime,
                calcMaxTime);
        }
    }

    void AddRetryStats(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags) override
    {
        Total.AddRetryStats(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            errorKind,
            errorFlags);

        if (IsReadWriteRequest(requestType) &&
            mediaKind != NProto::STORAGE_MEDIA_DEFAULT)
        {
            GetRequestCounters(mediaKind).AddRetryStats(
                static_cast<TRequestCounters::TRequestType>(
                    TranslateLocalRequestType(requestType)),
                errorKind,
                errorFlags);
        }
    }

    void RequestPostponed(EBlockStoreRequest requestType) override
    {
        Total.RequestPostponed(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestPostponedServer(EBlockStoreRequest requestType) override
    {
        Total.RequestPostponedServer(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestAdvanced(EBlockStoreRequest requestType) override
    {
        Total.RequestAdvanced(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestAdvancedServer(EBlockStoreRequest requestType) override
    {
        Total.RequestAdvancedServer(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestFastPathHit(EBlockStoreRequest requestType) override
    {
        Total.RequestFastPathHit(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void BatchCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) override
    {
        requestType = TranslateLocalRequestType(requestType);

        Total.BatchCompleted(
            static_cast<TRequestCounters::TRequestType>(requestType),
            count,
            bytes,
            errors,
            timeHist,
            sizeHist);

        if (IsReadWriteRequest(requestType) &&
            mediaKind != NProto::STORAGE_MEDIA_DEFAULT)
        {
            GetRequestCounters(mediaKind).BatchCompleted(
                static_cast<TRequestCounters::TRequestType>(requestType),
                count,
                bytes,
                errors,
                timeHist,
                sizeHist);

            if (IsServerSide) {
                HdrTotal.BatchCompleted(
                    requestType,
                    timeHist,
                    sizeHist);

                GetHdrPercentiles(mediaKind).BatchCompleted(
                    requestType,
                    timeHist,
                    sizeHist);
            }
        }
    }

    void UpdateStats(bool updatePercentiles) override
    {
        Total.UpdateStats(updatePercentiles);
        if (updatePercentiles && IsServerSide) {
            HdrTotal.UpdateStats();
        }

#define UPDATE_STATS(name, ...)                 \
    Total##name.UpdateStats(updatePercentiles); \
    if (updatePercentiles && IsServerSide) {    \
        HdrTotal##name.UpdateStats();           \
    }                                           \
    // OO_UPDATE_STATS

        BLOCKSTORE_MEDIA_KIND(UPDATE_STATS)

#undef UPDATE_STATS
    }

private:
    TRequestCounters& GetRequestCounters(
        NCloud::NProto::EStorageMediaKind mediaKind)
    {
        switch (mediaKind) {
            case NCloud::NProto::STORAGE_MEDIA_SSD:
                return TotalSSD;
            case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED:
                return TotalSSDNonrepl;
            case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2:
                return TotalSSDMirror2;
            case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3:
                return TotalSSDMirror3;
            case NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL:
                return TotalSSDLocal;
            case NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL:
                return TotalHDDLocal;
            case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED:
                return TotalHDDNonrepl;
            default:
                return TotalHDD;
        }
    }

    THdrPercentiles& GetHdrPercentiles(
        NCloud::NProto::EStorageMediaKind mediaKind)
    {
        switch (mediaKind) {
            case NCloud::NProto::STORAGE_MEDIA_SSD:
                return HdrTotalSSD;
            case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED:
                return HdrTotalSSDNonrepl;
            case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2:
                return HdrTotalSSDMirror2;
            case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3:
                return HdrTotalSSDMirror3;
            case NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL:
                return HdrTotalSSDLocal;
            case NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL:
                return HdrTotalHDDLocal;
            case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED:
                return HdrTotalHDDNonrepl;
            default:
                return HdrTotalHDD;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestStatsStub final
    : public IRequestStats
{
    ui64 RequestStarted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 requestBytes) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(requestBytes);
        return GetCycleCount();
    }

    TDuration RequestCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        TDuration postponedTime,
        ui64 requestBytes,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags,
        bool unaligned,
        ECalcMaxTime calcMaxTime,
        ui64 responseSent) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(postponedTime);
        Y_UNUSED(requestBytes);
        Y_UNUSED(errorKind);
        Y_UNUSED(errorFlags);
        Y_UNUSED(unaligned);
        Y_UNUSED(calcMaxTime);
        Y_UNUSED(responseSent);
        return CyclesToDurationSafe(GetCycleCount() - requestStarted);
    }

    void AddIncompleteStats(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        TRequestTime requestTime,
        ECalcMaxTime calcMaxTime) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(requestTime);
        Y_UNUSED(calcMaxTime);
    }

    void AddRetryStats(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(errorKind);
        Y_UNUSED(errorFlags);
    }

    void RequestPostponed(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void RequestPostponedServer(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void RequestAdvanced(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void RequestAdvancedServer(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void RequestFastPathHit(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void BatchCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(count);
        Y_UNUSED(bytes);
        Y_UNUSED(errors);
        Y_UNUSED(timeHist);
        Y_UNUSED(sizeHist);
    }

    void UpdateStats(bool updatePercentiles) override
    {
        Y_UNUSED(updatePercentiles);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestStatsPtr CreateClientRequestStats(
    TDynamicCountersPtr counters,
    ITimerPtr timer,
    EHistogramCounterOptions histogramCounterOptions)
{
    return std::make_shared<TRequestStats>(
        std::move(counters),
        false,
        std::move(timer),
        histogramCounterOptions,
        TVector<TSizeInterval>{});
}

IRequestStatsPtr CreateServerRequestStats(
    TDynamicCountersPtr counters,
    ITimerPtr timer,
    EHistogramCounterOptions histogramCounterOptions,
    const TVector<TSizeInterval>& executionTimeSizeClasses)
{
    return std::make_shared<TRequestStats>(
        std::move(counters),
        true,
        std::move(timer),
        histogramCounterOptions,
        executionTimeSizeClasses);
}

IRequestStatsPtr CreateRequestStatsStub()
{
    return std::make_shared<TRequestStatsStub>();
}

}   // namespace NCloud::NBlockStore
