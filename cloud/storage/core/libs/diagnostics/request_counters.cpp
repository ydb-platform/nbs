#include "request_counters.h"

#include "counters_helper.h"
#include "histogram_types.h"
#include "max_calculator.h"
#include "weighted_percentile.h"

#include <cloud/storage/core/libs/common/disjoint_interval_map.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/verify.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/cputimer.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

#include <utility>
#include <algorithm>

namespace NCloud {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
struct THistBase
{
    const TString Name;
    const TString Units;
    const double PercentileMultiplier;
    const TBucketBounds HistBounds;
    const EHistogramCounterOptions CounterOptions;

    THistogramPtr Hist;
    std::array<TDynamicCounters::TCounterPtr, TDerived::BUCKETS_COUNT> Counters;

    THistBase(TString name, EHistogramCounterOptions counterOptions)
        : Name(std::move(name))
        , Units(TDerived::Units)
        , PercentileMultiplier(TDerived::PercentileMultiplier)
        , HistBounds(ConvertToHistBounds(TDerived::Buckets))
        , CounterOptions(counterOptions)
        , Hist(
            new THistogramCounter(NMonitoring::ExplicitHistogram(HistBounds)))
    {
        std::fill(Counters.begin(), Counters.end(), new TCounterForPtr(true));
    }

    void Register(
        TDynamicCounters& counters,
        TCountableBase::EVisibility vis = TCountableBase::EVisibility::Public)
    {
        auto subgroup = MakeVisibilitySubgroup(
            counters,
            "histogram",
            Name,
            vis);
        if (GetUnits()) {
            subgroup = subgroup->GetSubgroup("units", GetUnits());
        }
        if (CounterOptions & EHistogramCounterOption::ReportSingleCounter) {
            Hist = subgroup->GetHistogram(Name,
                NMonitoring::ExplicitHistogram(HistBounds),
                true,
                vis);
        }
        if (CounterOptions & EHistogramCounterOption::ReportMultipleCounters) {
            const auto names = TDerived::MakeNames();
            for (size_t i = 0; i < Counters.size(); ++i) {
                Counters[i] = subgroup->GetCounter(names[i], true, vis);
            }
        }
    }

    void Increment(double value, ui64 count)
    {
        Hist->Collect(value, count);

        auto it = LowerBound(
            TDerived::Buckets.begin(),
            TDerived::Buckets.end(),
            value);
        STORAGE_VERIFY(
            it != TDerived::Buckets.end(),
            "Bucket",
            value);
        size_t index = std::distance(TDerived::Buckets.begin(), it);
        Counters[index]->Add(count);
    }

    void IncrementBucket(size_t index, ui64 count)
    {
        if (!count) {
            return;
        }

        STORAGE_VERIFY(index < Counters.size(), "BucketIndex", index);
        Hist->Collect(TDerived::Buckets[index], count);
        Counters[index]->Add(count);
    }

    TVector<TBucketInfo> GetBuckets() const
    {
        const auto snapshot = Hist->Snapshot();

        TVector<TBucketInfo> result(snapshot->Count());
        for (size_t i = 0; i < snapshot->Count(); ++i) {
            result.emplace_back(snapshot->UpperBound(i), snapshot->Value(i));
        }
        return result;
    }

    const TString& GetUnits() const
    {
        return Units;
    }

    const TString& GetName() const
    {
        return Name;
    }

    double GetPercentileMultiplier() const
    {
        return PercentileMultiplier;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TBucket>
struct TTimeHist: public THistBase<TBucket>
{
    using THistBase<TBucket>::THistBase;

    void Increment(TDuration requestTime, ui64 count = 1)
    {
        THistBase<TBucket>::Increment(
            requestTime.MicroSeconds() /
                THistBase<TBucket>::GetPercentileMultiplier(),
            count);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TUsTimeHist: public TTimeHist<TRequestUsTimeBuckets>
{
    using TTimeHist<TRequestUsTimeBuckets>::TTimeHist;
};

////////////////////////////////////////////////////////////////////////////////

struct TMsTimeHist: public TTimeHist<TRequestMsTimeBuckets>
{
    using TTimeHist<TRequestMsTimeBuckets>::TTimeHist;
};

////////////////////////////////////////////////////////////////////////////////

// Allows to use either microseconds or milliseconds for time histograms.
class TAdaptiveTimeHist
{
private:
    using TTimeHistVariant = std::variant<TUsTimeHist, TMsTimeHist>;
    std::unique_ptr<TTimeHistVariant> TimeHistPtr;

public:
    TAdaptiveTimeHist(TString name, EHistogramCounterOptions counterOptions)
    {
        if (counterOptions &
            EHistogramCounterOption::UseMsUnitsForTimeHistogram)
        {
            TimeHistPtr = std::make_unique<TTimeHistVariant>(
                std::in_place_type<TMsTimeHist>,
                std::move(name),
                counterOptions);
        } else {
            TimeHistPtr = std::make_unique<TTimeHistVariant>(
                std::in_place_type<TUsTimeHist>,
                std::move(name),
                counterOptions);
        }
    }
    ~TAdaptiveTimeHist() = default;

    TAdaptiveTimeHist(const TAdaptiveTimeHist&) = delete;
    TAdaptiveTimeHist(TAdaptiveTimeHist&&) = default;
    TAdaptiveTimeHist& operator=(const TAdaptiveTimeHist&) = delete;
    TAdaptiveTimeHist& operator=(TAdaptiveTimeHist&&) = default;

    void Increment(TDuration requestTime, ui64 count = 1)
    {
        std::visit(
            [&](auto& timeHist) { timeHist.Increment(requestTime, count); },
            *TimeHistPtr);
    }

    void IncrementBucket(size_t index, ui64 count)
    {
        std::visit(
            [&](auto& timeHist) { timeHist.IncrementBucket(index, count); },
            *TimeHistPtr);
    }

    template <typename... Args>
    void Register(Args&&... args)
    {
        std::visit(
            [&](auto& timeHist)
            { timeHist.Register(std::forward<Args>(args)...); },
            *TimeHistPtr);
    }

    [[nodiscard]] TVector<TBucketInfo> GetBuckets() const
    {
        return std::visit(
            [&](auto& timeHist) -> TVector<TBucketInfo>
            { return timeHist.GetBuckets(); },
            *TimeHistPtr);
    }

    [[nodiscard]] const TString& GetUnits() const
    {
        return std::visit(
            [&](auto& timeHist) -> const TString&
            { return timeHist.GetUnits(); },
            *TimeHistPtr);
    }

    [[nodiscard]] const TString& GetName() const
    {
        return std::visit(
            [&](auto& timeHist) -> const TString&
            { return timeHist.GetName(); },
            *TimeHistPtr);
    }

    [[nodiscard]] double GetPercentileMultiplier() const
    {
        return std::visit(
            [&](auto& timeHist) -> double
            { return timeHist.GetPercentileMultiplier(); },
            *TimeHistPtr);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSizeHist
    : public THistBase<TKbSizeBuckets>
{
    using THistBase::THistBase;
    using THistBase::IncrementBucket;

    void Increment(double requestBytes, ui64 count = 1)
    {
        THistBase::Increment(requestBytes / 1024, count);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TSrcHistogram>
class TRequestPercentiles
{
    using TDynamicCounterPtr = TDynamicCounters::TCounterPtr;

private:
    TVector<TDynamicCounterPtr> Counters;

    TVector<ui64> Prev;

    const TSrcHistogram& SrcHistogram;

public:
    explicit TRequestPercentiles(const TSrcHistogram& srcHistogram)
        : SrcHistogram(srcHistogram)
    {}

    void Register(TDynamicCounters& counters)
    {
        auto subgroup =
            counters.GetSubgroup("percentiles", SrcHistogram.GetName());
        if (SrcHistogram.GetUnits()) {
            subgroup = subgroup->GetSubgroup("units", SrcHistogram.GetUnits());
        }
        const auto& percentiles = GetDefaultPercentiles();
        for (const auto& [value, name]: percentiles) {
            Counters.emplace_back(subgroup->GetCounter(name, false));
        }
    }

    void Update()
    {
        const auto update = SrcHistogram.GetBuckets();
        if (Prev.size() < update.size()) {
            Prev.resize(update.size());
        }

        TVector<TBucketInfo> delta(Reserve(update.size()));
        for (size_t i = 0; i < update.size(); ++i) {
            const auto& [bound, value] = update[i];
            delta.emplace_back(bound, value - Prev[i]);
            Prev[i] = value;
        }

        auto result = CalculateWeightedPercentiles(
            delta,
            GetDefaultPercentiles());

        for (ui32 i = 0; i < Min(Counters.size(), result.size()); ++i) {
            *Counters[i] =
                std::lround(result[i] * SrcHistogram.GetPercentileMultiplier());
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

namespace {

template <size_t N>
size_t GetBucketIndex(const std::array<double, N>& buckets, double value)
{
    const auto it = LowerBound(buckets.begin(), buckets.end(), value);
    STORAGE_VERIFY(it != buckets.end(), "BucketValue", value);
    return std::distance(buckets.begin(), it);
}

ui32 NormalizeStripeCount(ui32 stripeCount)
{
    if (stripeCount < 2) {
        return 2;
    }
    if (!IsPowerOf2(stripeCount)) {
        return FastClp2(stripeCount);
    }
    return stripeCount;
}

}   // namespace

struct TRequestCounters::TSpecialCounters
{
    TDynamicCounters::TCounterPtr HwProblems;

    TSpecialCounters() = default;

    TSpecialCounters(const TSpecialCounters&) = delete;
    TSpecialCounters(TSpecialCounters&&) = default;

    TSpecialCounters& operator = (const TSpecialCounters&) = delete;
    TSpecialCounters& operator = (TSpecialCounters&&) = default;

    void Init(TDynamicCounters& counters)
    {
        HwProblems = counters.GetCounter("HwProblems", true);
    }

    void AddStats(
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags)
    {
        if ((errorKind != EDiagnosticsErrorKind::Success)
            && HasProtoFlag(
                errorFlags,
                NCloud::NProto::EF_HW_PROBLEMS_DETECTED))
        {
            HwProblems->Inc();
        }
    }

    void AddRetryStats(
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags)
    {
        AddStats(errorKind, errorFlags);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestCounters::TStatCounters
{
    template<typename Type>
    struct TFailedAndSuccess
    {
        Type Success;
        Type Failed;
    };

    struct TSizeClassCounters
    {
        TAdaptiveTimeHist ExecutionTimeHist;
        TRequestPercentiles<TAdaptiveTimeHist> ExecutionTimePercentiles;

        explicit TSizeClassCounters(
                EHistogramCounterOptions histogramCounterOptions)
            : ExecutionTimeHist("ExecutionTime", histogramCounterOptions)
            , ExecutionTimePercentiles(ExecutionTimeHist)
        {}
    };

    bool IsReadWriteRequest = false;
    bool ReportDataPlaneHistogram = false;
    bool ReportControlPlaneHistogram = false;

    TIntrusivePtr<TDynamicCounters> CountersGroup;

    TDynamicCounters::TCounterPtr Count;
    TDynamicCounters::TCounterPtr MaxCount;
    TDynamicCounters::TCounterPtr UnalignedCount;
    TDynamicCounters::TCounterPtr Time;
    TDynamicCounters::TCounterPtr MaxTime;
    TDynamicCounters::TCounterPtr MaxTotalTime;
    TDynamicCounters::TCounterPtr MaxSize;
    TDynamicCounters::TCounterPtr RequestBytes;
    TDynamicCounters::TCounterPtr MaxRequestBytes;
    TDynamicCounters::TCounterPtr InProgress;
    TDynamicCounters::TCounterPtr MaxInProgress;
    TDynamicCounters::TCounterPtr InProgressBytes;
    TDynamicCounters::TCounterPtr MaxInProgressBytes;
    TDynamicCounters::TCounterPtr PostponedQueueSize;
    TDynamicCounters::TCounterPtr MaxPostponedQueueSize;
    TDynamicCounters::TCounterPtr PostponedCount;
    TDynamicCounters::TCounterPtr PostponedQueueSizeGrpc;
    TDynamicCounters::TCounterPtr MaxPostponedQueueSizeGrpc;
    TDynamicCounters::TCounterPtr PostponedCountGrpc;
    TDynamicCounters::TCounterPtr FastPathHits;

    TDynamicCounters::TCounterPtr Errors;
    TDynamicCounters::TCounterPtr ErrorsAborted;
    TDynamicCounters::TCounterPtr ErrorsFatal;
    TDynamicCounters::TCounterPtr ErrorsRetriable;
    TDynamicCounters::TCounterPtr ErrorsThrottling;
    TDynamicCounters::TCounterPtr ErrorsWriteRejectdByCheckpoint;
    TDynamicCounters::TCounterPtr ErrorsSession;
    TDynamicCounters::TCounterPtr ErrorsSilent;

    TDynamicCounters::TCounterPtr Retries;

    TSizeHist SizeHist;
    TRequestPercentiles<TSizeHist> SizePercentiles;

    TAdaptiveTimeHist TimeHist;
    TAdaptiveTimeHist TimeHistUnaligned;
    TRequestPercentiles<TAdaptiveTimeHist> TimePercentiles;

    TAdaptiveTimeHist ExecutionTimeHist;
    TAdaptiveTimeHist ExecutionTimeHistUnaligned;
    TRequestPercentiles<TAdaptiveTimeHist> ExecutionTimePercentiles;

    TDisjointIntervalMap<ui64, std::unique_ptr<TSizeClassCounters>>
        ExecutionTimeSizeClasses;

    TAdaptiveTimeHist RequestCompletionTimeHist;
    TRequestPercentiles<TAdaptiveTimeHist> RequestCompletionTimePercentiles;

    TAdaptiveTimeHist PostponedTimeHist;
    TRequestPercentiles<TAdaptiveTimeHist> PostponedTimePercentiles;

    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxTimeCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxTotalTimeCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxSizeCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxInProgressCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxInProgressBytesCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxPostponedQueueSizeCalc;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxPostponedQueueSizeGrpcCalc;
    TMaxPerSecondCalculator<DEFAULT_BUCKET_COUNT> MaxCountCalc;
    TMaxPerSecondCalculator<DEFAULT_BUCKET_COUNT> MaxRequestBytesCalc;

    struct alignas(64) TStripe
    {
        TMutex Lock;

        i64 InProgressDelta = 0;
        i64 InProgressBytesDelta = 0;
        i64 PostponedQueueSizeDelta = 0;
        i64 PostponedQueueSizeGrpcDelta = 0;
        i64 InProgressPeak = 0;
        i64 InProgressBytesPeak = 0;
        i64 PostponedQueueSizePeak = 0;
        i64 PostponedQueueSizeGrpcPeak = 0;

        ui64 Count = 0;
        ui64 Errors = 0;
        ui64 ErrorsAborted = 0;
        ui64 ErrorsFatal = 0;
        ui64 ErrorsRetriable = 0;
        ui64 ErrorsThrottling = 0;
        ui64 ErrorsWriteRejectedByCheckpoint = 0;
        ui64 ErrorsSession = 0;
        ui64 ErrorsSilent = 0;
        ui64 Retries = 0;

        ui64 TimeMicros = 0;
        ui64 RequestBytes = 0;
        ui64 UnalignedCount = 0;
        ui64 PostponedCount = 0;
        ui64 PostponedCountGrpc = 0;
        ui64 FastPathHits = 0;

        ui64 MaxTimeMicros = 0;
        ui64 MaxTotalTimeMicros = 0;
        ui64 MaxSize = 0;
        ui64 MaxCount = 0;
        ui64 MaxRequestBytes = 0;

        static constexpr size_t TIME_BUCKETS_COUNT =
            TRequestUsTimeBuckets::BUCKETS_COUNT;
        static constexpr size_t SIZE_BUCKETS_COUNT = TKbSizeBuckets::BUCKETS_COUNT;

        std::array<ui64, TIME_BUCKETS_COUNT> TimeBuckets = {};
        std::array<ui64, TIME_BUCKETS_COUNT> TimeBucketsUnaligned = {};
        std::array<ui64, TIME_BUCKETS_COUNT> ExecutionTimeBuckets = {};
        std::array<ui64, TIME_BUCKETS_COUNT> ExecutionTimeBucketsUnaligned = {};
        std::array<ui64, TIME_BUCKETS_COUNT> RequestCompletionTimeBuckets = {};
        std::array<ui64, TIME_BUCKETS_COUNT> PostponedTimeBuckets = {};
        std::array<ui64, SIZE_BUCKETS_COUNT> SizeBuckets = {};
        TVector<std::array<ui64, TIME_BUCKETS_COUNT>> ExecutionTimeSizeClassBuckets;

        explicit TStripe(size_t sizeClassCount = 0)
            : ExecutionTimeSizeClassBuckets(sizeClassCount)
        {}
    };

    struct TStripeAggregate
    {
        i64 InProgressDelta = 0;
        i64 InProgressBytesDelta = 0;
        i64 PostponedQueueSizeDelta = 0;
        i64 PostponedQueueSizeGrpcDelta = 0;
        i64 InProgressPeak = 0;
        i64 InProgressBytesPeak = 0;
        i64 PostponedQueueSizePeak = 0;
        i64 PostponedQueueSizeGrpcPeak = 0;

        ui64 Count = 0;
        ui64 Errors = 0;
        ui64 ErrorsAborted = 0;
        ui64 ErrorsFatal = 0;
        ui64 ErrorsRetriable = 0;
        ui64 ErrorsThrottling = 0;
        ui64 ErrorsWriteRejectedByCheckpoint = 0;
        ui64 ErrorsSession = 0;
        ui64 ErrorsSilent = 0;
        ui64 Retries = 0;

        ui64 TimeMicros = 0;
        ui64 RequestBytes = 0;
        ui64 UnalignedCount = 0;
        ui64 PostponedCount = 0;
        ui64 PostponedCountGrpc = 0;
        ui64 FastPathHits = 0;

        ui64 MaxTimeMicros = 0;
        ui64 MaxTotalTimeMicros = 0;
        ui64 MaxSize = 0;
        ui64 MaxCount = 0;
        ui64 MaxRequestBytes = 0;

        std::array<ui64, TStripe::TIME_BUCKETS_COUNT> TimeBuckets = {};
        std::array<ui64, TStripe::TIME_BUCKETS_COUNT> TimeBucketsUnaligned = {};
        std::array<ui64, TStripe::TIME_BUCKETS_COUNT> ExecutionTimeBuckets = {};
        std::array<ui64, TStripe::TIME_BUCKETS_COUNT> ExecutionTimeBucketsUnaligned = {};
        std::array<ui64, TStripe::TIME_BUCKETS_COUNT> RequestCompletionTimeBuckets = {};
        std::array<ui64, TStripe::TIME_BUCKETS_COUNT> PostponedTimeBuckets = {};
        std::array<ui64, TStripe::SIZE_BUCKETS_COUNT> SizeBuckets = {};
        TVector<std::array<ui64, TStripe::TIME_BUCKETS_COUNT>> ExecutionTimeSizeClassBuckets;

        explicit TStripeAggregate(size_t sizeClassCount = 0)
            : ExecutionTimeSizeClassBuckets(sizeClassCount)
        {}
    };

    bool UseStripedAccumulators = false;
    bool UseMsUnitsForTimeHistograms = false;
    ui32 StripeMask = 0;
    TVector<std::unique_ptr<TStripe>> Stripes;
    TVector<TSizeInterval> SizeClassIntervals;

    i64 CurrentInProgress = 0;
    i64 CurrentInProgressBytes = 0;
    i64 CurrentPostponedQueueSize = 0;
    i64 CurrentPostponedQueueSizeGrpc = 0;

    TMutex FullInitLock;
    TAtomic FullyInitialized = false;

    explicit TStatCounters(
        ITimerPtr timer,
        EHistogramCounterOptions histogramCounterOptions,
        const TVector<TSizeInterval>& executionTimeSizeClasses,
        bool useStripedAccumulators,
        ui32 stripeCount)
        : SizeHist("Size", histogramCounterOptions)
        , SizePercentiles(SizeHist)
        , TimeHist("Time", histogramCounterOptions)
        , TimeHistUnaligned("Time", histogramCounterOptions)
        , TimePercentiles(TimeHist)
        , ExecutionTimeHist("ExecutionTime", histogramCounterOptions)
        , ExecutionTimeHistUnaligned("ExecutionTime", histogramCounterOptions)
        , ExecutionTimePercentiles(ExecutionTimeHist)
        , RequestCompletionTimeHist(
              "RequestCompletionTime",
              histogramCounterOptions)
        , RequestCompletionTimePercentiles(RequestCompletionTimeHist)
        , PostponedTimeHist("ThrottlerDelay", histogramCounterOptions)
        , PostponedTimePercentiles(PostponedTimeHist)
        , MaxTimeCalc(timer)
        , MaxTotalTimeCalc(timer)
        , MaxSizeCalc(timer)
        , MaxInProgressCalc(timer)
        , MaxInProgressBytesCalc(timer)
        , MaxPostponedQueueSizeCalc(timer)
        , MaxPostponedQueueSizeGrpcCalc(timer)
        , MaxCountCalc(timer)
        , MaxRequestBytesCalc(timer)
        , UseStripedAccumulators(useStripedAccumulators)
        , UseMsUnitsForTimeHistograms(
              histogramCounterOptions &
              EHistogramCounterOption::UseMsUnitsForTimeHistogram)
    {
        if (UseStripedAccumulators) {
            const auto normalizedStripeCount = NormalizeStripeCount(stripeCount);
            StripeMask = normalizedStripeCount - 1;
            Stripes.reserve(normalizedStripeCount);
            for (ui32 i = 0; i < normalizedStripeCount; ++i) {
                Stripes.emplace_back(
                    std::make_unique<TStripe>(executionTimeSizeClasses.size()));
            }
        }

        for (auto [start, end]: executionTimeSizeClasses) {
            SizeClassIntervals.emplace_back(start, end);
            ExecutionTimeSizeClasses.Add(
                start,
                end,
                std::make_unique<TSizeClassCounters>(histogramCounterOptions));
        }
    }

    TStatCounters(const TStatCounters&) = delete;
    TStatCounters(TStatCounters&&) = default;

    TStatCounters& operator = (const TStatCounters&) = delete;
    TStatCounters& operator = (TStatCounters&&) = default;

    void Init(
        TDynamicCountersPtr countersGroup,
        bool isReadWriteRequest,
        bool reportDataPlaneHistogram,
        bool reportControlPlaneHistogram)
    {
        CountersGroup = std::move(countersGroup);
        auto& counters = *CountersGroup;

        IsReadWriteRequest = isReadWriteRequest;
        ReportDataPlaneHistogram = reportDataPlaneHistogram;
        ReportControlPlaneHistogram = reportControlPlaneHistogram;

        // always reporting the most important counters even when lazy
        // initialization is enabled and the values are zeroes
        Count = counters.GetCounter("Count", true);
        ErrorsFatal = counters.GetCounter("Errors/Fatal", true);
        Time = counters.GetCounter("Time", true);
        if (ReportControlPlaneHistogram) {
            TimeHist.Register(counters);
        } else {
            TimePercentiles.Register(counters);
        }
    }

    void FullInitIfNeeded()
    {
        if (AtomicGet(FullyInitialized)) {
            return;
        }

        auto g = Guard(FullInitLock);
        if (AtomicGet(FullyInitialized)) {
            return;
        }

        auto& counters = *CountersGroup;

        MaxTime = counters.GetCounter("MaxTime");
        MaxTotalTime = counters.GetCounter("MaxTotalTime");

        InProgress = counters.GetCounter("InProgress");
        MaxInProgress = counters.GetCounter("MaxInProgress");

        Errors = counters.GetCounter("Errors", true);
        ErrorsAborted = counters.GetCounter("Errors/Aborted", true);
        ErrorsRetriable = counters.GetCounter("Errors/Retriable", true);
        ErrorsThrottling = counters.GetCounter("Errors/Throttling", true);
        ErrorsWriteRejectdByCheckpoint = counters.GetCounter("Errors/CheckpointReject", true);
        ErrorsSession = counters.GetCounter("Errors/Session", true);
        Retries = counters.GetCounter("Retries", true);

        if (IsReadWriteRequest) {
            ErrorsSilent = counters.GetCounter("Errors/Silent", true);

            MaxSize = counters.GetCounter("MaxSize");
            MaxCount = counters.GetCounter("MaxCount");

            RequestBytes = counters.GetCounter("RequestBytes", true);
            MaxRequestBytes = counters.GetCounter("MaxRequestBytes");

            InProgressBytes = counters.GetCounter("InProgressBytes");
            MaxInProgressBytes = counters.GetCounter("MaxInProgressBytes");

            UnalignedCount = counters.GetCounter("UnalignedCount", true);

            if (ReportDataPlaneHistogram) {
                auto unalignedClassGroup = counters.GetSubgroup("sizeclass", "Unaligned");

                SizeHist.Register(counters);
                TimeHistUnaligned.Register(*unalignedClassGroup);
                ExecutionTimeHist.Register(counters);
                ExecutionTimeHistUnaligned.Register(*unalignedClassGroup);
            } else {
                SizePercentiles.Register(counters);
                ExecutionTimePercentiles.Register(counters);
                PostponedTimePercentiles.Register(counters);
                TimePercentiles.Register(counters);
            }

            for (auto& [_, sizeClass]: ExecutionTimeSizeClasses) {
                auto sizeClassCounters = counters.GetSubgroup(
                    "sizeclass",
                    ToString(TSizeInterval{sizeClass.Begin, sizeClass.End}));
                if (ReportDataPlaneHistogram) {
                    sizeClass.Value->ExecutionTimeHist.Register(
                        *sizeClassCounters);
                } else {
                    sizeClass.Value->ExecutionTimePercentiles.Register(
                        *sizeClassCounters);
                }
            }

            const auto visibleHistogram = ReportDataPlaneHistogram
                ? TCountableBase::EVisibility::Public
                : TCountableBase::EVisibility::Private;

            PostponedTimeHist.Register(counters, visibleHistogram);
            TimeHist.Register(counters, visibleHistogram);

            // Always enough only percentiles.
            RequestCompletionTimeHist.Register(
                counters,
                TCountableBase::EVisibility::Private);
            RequestCompletionTimePercentiles.Register(counters);

            PostponedQueueSize = counters.GetCounter("PostponedQueueSize");
            MaxPostponedQueueSize = counters.GetCounter("MaxPostponedQueueSize");
            PostponedCount = counters.GetCounter("PostponedCount", true);

            PostponedQueueSizeGrpc =
                counters.GetCounter("PostponedQueueSizeGrpc");
            MaxPostponedQueueSizeGrpc =
                counters.GetCounter("MaxPostponedQueueSizeGrpc");
            PostponedCountGrpc =
                counters.GetCounter("PostponedCountGrpc", true);

            FastPathHits = counters.GetCounter("FastPathHits", true);
        }

        AtomicSet(FullyInitialized, true);
    }

    TStripe& AccessStripe()
    {
        const auto tid = static_cast<ui64>(TThread::CurrentThreadId());
        return *Stripes[THash<ui64>()(tid) & StripeMask];
    }

    size_t TimeBucketIndex(TDuration duration) const
    {
        const auto value = duration.MicroSeconds();
        if (UseMsUnitsForTimeHistograms) {
            return GetBucketIndex(
                TRequestMsTimeBuckets::Buckets,
                value / 1000.0);
        }
        return GetBucketIndex(TRequestUsTimeBuckets::Buckets, value);
    }

    size_t SizeBucketIndex(ui64 requestBytes) const
    {
        return GetBucketIndex(
            TKbSizeBuckets::Buckets,
            requestBytes / 1024.0);
    }

    size_t SizeClassIndex(ui64 requestBytes) const
    {
        for (size_t i = 0; i < SizeClassIntervals.size(); ++i) {
            const auto& [begin, end] = SizeClassIntervals[i];
            if (begin <= requestBytes && requestBytes < end) {
                return i;
            }
        }
        return SizeClassIntervals.size();
    }

    void PublishStripedStats()
    {
        TStripeAggregate aggregate(SizeClassIntervals.size());

        for (auto& stripePtr: Stripes) {
            auto& stripe = *stripePtr;
            auto g = Guard(stripe.Lock);
            aggregate.InProgressDelta += stripe.InProgressDelta;
            aggregate.InProgressBytesDelta += stripe.InProgressBytesDelta;
            aggregate.PostponedQueueSizeDelta += stripe.PostponedQueueSizeDelta;
            aggregate.PostponedQueueSizeGrpcDelta +=
                stripe.PostponedQueueSizeGrpcDelta;
            aggregate.InProgressPeak += stripe.InProgressPeak;
            aggregate.InProgressBytesPeak += stripe.InProgressBytesPeak;
            aggregate.PostponedQueueSizePeak += stripe.PostponedQueueSizePeak;
            aggregate.PostponedQueueSizeGrpcPeak +=
                stripe.PostponedQueueSizeGrpcPeak;

            aggregate.Count += stripe.Count;
            aggregate.Errors += stripe.Errors;
            aggregate.ErrorsAborted += stripe.ErrorsAborted;
            aggregate.ErrorsFatal += stripe.ErrorsFatal;
            aggregate.ErrorsRetriable += stripe.ErrorsRetriable;
            aggregate.ErrorsThrottling += stripe.ErrorsThrottling;
            aggregate.ErrorsWriteRejectedByCheckpoint +=
                stripe.ErrorsWriteRejectedByCheckpoint;
            aggregate.ErrorsSession += stripe.ErrorsSession;
            aggregate.ErrorsSilent += stripe.ErrorsSilent;
            aggregate.Retries += stripe.Retries;

            aggregate.TimeMicros += stripe.TimeMicros;
            aggregate.RequestBytes += stripe.RequestBytes;
            aggregate.UnalignedCount += stripe.UnalignedCount;
            aggregate.PostponedCount += stripe.PostponedCount;
            aggregate.PostponedCountGrpc += stripe.PostponedCountGrpc;
            aggregate.FastPathHits += stripe.FastPathHits;

            aggregate.MaxTimeMicros =
                Max(aggregate.MaxTimeMicros, stripe.MaxTimeMicros);
            aggregate.MaxTotalTimeMicros =
                Max(aggregate.MaxTotalTimeMicros, stripe.MaxTotalTimeMicros);
            aggregate.MaxSize = Max(aggregate.MaxSize, stripe.MaxSize);
            aggregate.MaxCount += stripe.MaxCount;
            aggregate.MaxRequestBytes += stripe.MaxRequestBytes;

            for (size_t i = 0; i < aggregate.TimeBuckets.size(); ++i) {
                aggregate.TimeBuckets[i] += stripe.TimeBuckets[i];
                aggregate.TimeBucketsUnaligned[i] += stripe.TimeBucketsUnaligned[i];
                aggregate.ExecutionTimeBuckets[i] += stripe.ExecutionTimeBuckets[i];
                aggregate.ExecutionTimeBucketsUnaligned[i] +=
                    stripe.ExecutionTimeBucketsUnaligned[i];
                aggregate.RequestCompletionTimeBuckets[i] +=
                    stripe.RequestCompletionTimeBuckets[i];
                aggregate.PostponedTimeBuckets[i] += stripe.PostponedTimeBuckets[i];
                stripe.TimeBuckets[i] = 0;
                stripe.TimeBucketsUnaligned[i] = 0;
                stripe.ExecutionTimeBuckets[i] = 0;
                stripe.ExecutionTimeBucketsUnaligned[i] = 0;
                stripe.RequestCompletionTimeBuckets[i] = 0;
                stripe.PostponedTimeBuckets[i] = 0;
            }

            for (size_t i = 0; i < aggregate.SizeBuckets.size(); ++i) {
                aggregate.SizeBuckets[i] += stripe.SizeBuckets[i];
                stripe.SizeBuckets[i] = 0;
            }

            for (size_t i = 0; i < aggregate.ExecutionTimeSizeClassBuckets.size(); ++i) {
                for (size_t j = 0;
                     j < aggregate.ExecutionTimeSizeClassBuckets[i].size();
                     ++j)
                {
                    aggregate.ExecutionTimeSizeClassBuckets[i][j] +=
                        stripe.ExecutionTimeSizeClassBuckets[i][j];
                    stripe.ExecutionTimeSizeClassBuckets[i][j] = 0;
                }
            }

            stripe.InProgressDelta = 0;
            stripe.InProgressBytesDelta = 0;
            stripe.PostponedQueueSizeDelta = 0;
            stripe.PostponedQueueSizeGrpcDelta = 0;
            stripe.InProgressPeak = 0;
            stripe.InProgressBytesPeak = 0;
            stripe.PostponedQueueSizePeak = 0;
            stripe.PostponedQueueSizeGrpcPeak = 0;
            stripe.Count = 0;
            stripe.Errors = 0;
            stripe.ErrorsAborted = 0;
            stripe.ErrorsFatal = 0;
            stripe.ErrorsRetriable = 0;
            stripe.ErrorsThrottling = 0;
            stripe.ErrorsWriteRejectedByCheckpoint = 0;
            stripe.ErrorsSession = 0;
            stripe.ErrorsSilent = 0;
            stripe.Retries = 0;
            stripe.TimeMicros = 0;
            stripe.RequestBytes = 0;
            stripe.UnalignedCount = 0;
            stripe.PostponedCount = 0;
            stripe.PostponedCountGrpc = 0;
            stripe.FastPathHits = 0;
            stripe.MaxTimeMicros = 0;
            stripe.MaxTotalTimeMicros = 0;
            stripe.MaxSize = 0;
            stripe.MaxCount = 0;
            stripe.MaxRequestBytes = 0;
        }

        const auto currentInProgressBefore = CurrentInProgress;
        const auto currentInProgressBytesBefore = CurrentInProgressBytes;
        const auto currentPostponedQueueSizeBefore = CurrentPostponedQueueSize;
        const auto currentPostponedQueueSizeGrpcBefore =
            CurrentPostponedQueueSizeGrpc;

        CurrentInProgress += aggregate.InProgressDelta;
        CurrentInProgressBytes += aggregate.InProgressBytesDelta;
        CurrentPostponedQueueSize += aggregate.PostponedQueueSizeDelta;
        CurrentPostponedQueueSizeGrpc += aggregate.PostponedQueueSizeGrpcDelta;

        InProgress->Set(std::max<i64>(CurrentInProgress, 0));
        MaxInProgressCalc.Add(std::max<i64>(
            currentInProgressBefore + aggregate.InProgressPeak,
            std::max<i64>(currentInProgressBefore, 0)));
        if (IsReadWriteRequest) {
            InProgressBytes->Set(std::max<i64>(CurrentInProgressBytes, 0));
            MaxInProgressBytesCalc.Add(std::max<i64>(
                currentInProgressBytesBefore + aggregate.InProgressBytesPeak,
                std::max<i64>(currentInProgressBytesBefore, 0)));
        }

        Count->Add(aggregate.Count);
        Errors->Add(aggregate.Errors);
        ErrorsAborted->Add(aggregate.ErrorsAborted);
        ErrorsFatal->Add(aggregate.ErrorsFatal);
        ErrorsRetriable->Add(aggregate.ErrorsRetriable);
        ErrorsThrottling->Add(aggregate.ErrorsThrottling);
        ErrorsWriteRejectdByCheckpoint->Add(
            aggregate.ErrorsWriteRejectedByCheckpoint);
        ErrorsSession->Add(aggregate.ErrorsSession);
        Retries->Add(aggregate.Retries);
        Time->Add(aggregate.TimeMicros);

        MaxTimeCalc.Add(aggregate.MaxTimeMicros);
        MaxTotalTimeCalc.Add(aggregate.MaxTotalTimeMicros);

        for (size_t i = 0; i < aggregate.TimeBuckets.size(); ++i) {
            TimeHist.IncrementBucket(i, aggregate.TimeBuckets[i]);
            RequestCompletionTimeHist.IncrementBucket(
                i,
                aggregate.RequestCompletionTimeBuckets[i]);
        }

        if (IsReadWriteRequest) {
            MaxCountCalc.Add(aggregate.MaxCount);
            RequestBytes->Add(aggregate.RequestBytes);
            MaxRequestBytesCalc.Add(aggregate.MaxRequestBytes);
            MaxSizeCalc.Add(aggregate.MaxSize);

            UnalignedCount->Add(aggregate.UnalignedCount);
            ErrorsSilent->Add(aggregate.ErrorsSilent);
            PostponedCount->Add(aggregate.PostponedCount);
            PostponedCountGrpc->Add(aggregate.PostponedCountGrpc);
            FastPathHits->Add(aggregate.FastPathHits);
            PostponedQueueSize->Set(std::max<i64>(CurrentPostponedQueueSize, 0));
            PostponedQueueSizeGrpc->Set(
                std::max<i64>(CurrentPostponedQueueSizeGrpc, 0));
            MaxPostponedQueueSizeCalc.Add(
                std::max<i64>(
                    currentPostponedQueueSizeBefore
                        + aggregate.PostponedQueueSizePeak,
                    std::max<i64>(currentPostponedQueueSizeBefore, 0)));
            MaxPostponedQueueSizeGrpcCalc.Add(
                std::max<i64>(
                    currentPostponedQueueSizeGrpcBefore
                        + aggregate.PostponedQueueSizeGrpcPeak,
                    std::max<i64>(currentPostponedQueueSizeGrpcBefore, 0)));

            for (size_t i = 0; i < aggregate.SizeBuckets.size(); ++i) {
                SizeHist.IncrementBucket(i, aggregate.SizeBuckets[i]);
            }
            for (size_t i = 0; i < aggregate.ExecutionTimeBuckets.size(); ++i) {
                ExecutionTimeHist.IncrementBucket(i, aggregate.ExecutionTimeBuckets[i]);
                ExecutionTimeHistUnaligned.IncrementBucket(
                    i,
                    aggregate.ExecutionTimeBucketsUnaligned[i]);
                TimeHistUnaligned.IncrementBucket(i, aggregate.TimeBucketsUnaligned[i]);
                PostponedTimeHist.IncrementBucket(i, aggregate.PostponedTimeBuckets[i]);
            }

            size_t index = 0;
            for (auto& [_, sizeClass]: ExecutionTimeSizeClasses) {
                for (size_t i = 0; i < aggregate.ExecutionTimeSizeClassBuckets[index].size(); ++i) {
                    sizeClass.Value->ExecutionTimeHist.IncrementBucket(
                        i,
                        aggregate.ExecutionTimeSizeClassBuckets[index][i]);
                }
                ++index;
            }
        }
    }

    void Started(ui64 requestBytes)
    {
        if (UseStripedAccumulators) {
            auto& stripe = AccessStripe();
            auto g = Guard(stripe.Lock);
            ++stripe.InProgressDelta;
            stripe.InProgressPeak = Max(
                stripe.InProgressPeak,
                stripe.InProgressDelta);
            if (IsReadWriteRequest) {
                stripe.InProgressBytesDelta += requestBytes;
                stripe.InProgressBytesPeak = Max(
                    stripe.InProgressBytesPeak,
                    stripe.InProgressBytesDelta);
            }
            return;
        }

        MaxInProgressCalc.Add(InProgress->Inc());

        if (IsReadWriteRequest) {
            MaxInProgressBytesCalc.Add(InProgressBytes->Add(requestBytes));
        }
    }

    void Completed(ui64 requestBytes)
    {
        if (UseStripedAccumulators) {
            auto& stripe = AccessStripe();
            auto g = Guard(stripe.Lock);
            --stripe.InProgressDelta;
            if (IsReadWriteRequest) {
                stripe.InProgressBytesDelta -= requestBytes;
            }
            return;
        }

        InProgress->Dec();

        if (IsReadWriteRequest) {
            InProgressBytes->Sub(requestBytes);
        }
    }

    void AddStats(
        TDuration requestTime,
        TDuration requestCompletionTime,
        TDuration postponedTime,
        ui64 requestBytes,
        EDiagnosticsErrorKind errorKind,
        bool unaligned,
        ECalcMaxTime calcMaxTime)
    {
        const bool failed = errorKind != EDiagnosticsErrorKind::Success
            && (errorKind != EDiagnosticsErrorKind::ErrorSilent
                || !IsReadWriteRequest);

        const auto time = requestTime - requestCompletionTime;
        const auto execTime = time - postponedTime;

        if (UseStripedAccumulators) {
            auto& stripe = AccessStripe();
            auto g = Guard(stripe.Lock);

            if (failed) {
                ++stripe.Errors;
            } else {
                ++stripe.Count;
            }

            switch (errorKind) {
                case EDiagnosticsErrorKind::Success:
                    break;
                case EDiagnosticsErrorKind::ErrorAborted:
                    ++stripe.ErrorsAborted;
                    break;
                case EDiagnosticsErrorKind::ErrorFatal:
                    ++stripe.ErrorsFatal;
                    break;
                case EDiagnosticsErrorKind::ErrorRetriable:
                    ++stripe.ErrorsRetriable;
                    break;
                case EDiagnosticsErrorKind::ErrorThrottling:
                    ++stripe.ErrorsThrottling;
                    break;
                case EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint:
                    ++stripe.ErrorsWriteRejectedByCheckpoint;
                    break;
                case EDiagnosticsErrorKind::ErrorSession:
                    ++stripe.ErrorsSession;
                    break;
                case EDiagnosticsErrorKind::ErrorSilent:
                    if (IsReadWriteRequest) {
                        ++stripe.ErrorsSilent;
                    }
                    break;
                case EDiagnosticsErrorKind::Max:
                    Y_DEBUG_ABORT_UNLESS(false);
                    return;
            }

            const ui64 timeMicros = time.MicroSeconds();
            stripe.TimeMicros += timeMicros;
            stripe.TimeBuckets[TimeBucketIndex(time)]++;

            if (requestCompletionTime != TDuration::Zero()) {
                stripe.RequestCompletionTimeBuckets[
                    TimeBucketIndex(requestCompletionTime)]++;
            }

            if (calcMaxTime == ECalcMaxTime::ENABLE) {
                stripe.MaxTimeMicros = Max(stripe.MaxTimeMicros, execTime.MicroSeconds());
            }
            stripe.MaxTotalTimeMicros = Max(
                stripe.MaxTotalTimeMicros,
                requestTime.MicroSeconds());

            if (IsReadWriteRequest) {
                ++stripe.MaxCount;
                stripe.RequestBytes += requestBytes;
                stripe.MaxRequestBytes += requestBytes;

                stripe.SizeBuckets[SizeBucketIndex(requestBytes)]++;
                stripe.MaxSize = Max(stripe.MaxSize, requestBytes);

                const auto execBucketIndex = TimeBucketIndex(execTime);
                stripe.ExecutionTimeBuckets[execBucketIndex]++;
                stripe.PostponedTimeBuckets[TimeBucketIndex(postponedTime)]++;

                if (unaligned) {
                    ++stripe.UnalignedCount;
                    stripe.TimeBucketsUnaligned[TimeBucketIndex(time)]++;
                    stripe.ExecutionTimeBucketsUnaligned[execBucketIndex]++;
                }

                if (const auto idx = SizeClassIndex(requestBytes); idx < stripe.ExecutionTimeSizeClassBuckets.size()) {
                    stripe.ExecutionTimeSizeClassBuckets[idx][execBucketIndex]++;
                }
            }

            return;
        }

        if (failed) {
            Errors->Inc();
        } else {
            Count->Inc();
        }

        switch (errorKind) {
            case EDiagnosticsErrorKind::Success:
                break;
            case EDiagnosticsErrorKind::ErrorAborted:
                ErrorsAborted->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorFatal:
                ErrorsFatal->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorRetriable:
                ErrorsRetriable->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorThrottling:
                ErrorsThrottling->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint:
                ErrorsWriteRejectdByCheckpoint->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorSession:
                ErrorsSession->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorSilent:
                if (IsReadWriteRequest) {
                    ErrorsSilent->Inc();
                }
                break;
            case EDiagnosticsErrorKind::Max:
                Y_DEBUG_ABORT_UNLESS(false);
                return;
        }

        if (calcMaxTime == ECalcMaxTime::ENABLE) {
            MaxTimeCalc.Add(execTime.MicroSeconds());
        }
        MaxTotalTimeCalc.Add(requestTime.MicroSeconds());

        Time->Add(time.MicroSeconds());
        TimeHist.Increment(time);

        if (requestCompletionTime != TDuration::Zero()) {
            RequestCompletionTimeHist.Increment(requestCompletionTime);
        }

        if (IsReadWriteRequest) {
            MaxCountCalc.Add(1);
            RequestBytes->Add(requestBytes);
            MaxRequestBytesCalc.Add(requestBytes);

            SizeHist.Increment(requestBytes);
            MaxSizeCalc.Add(requestBytes);

            if (unaligned) {
                UnalignedCount->Inc();
                TimeHistUnaligned.Increment(time);
                ExecutionTimeHistUnaligned.Increment(execTime);
            }

            ExecutionTimeHist.Increment(execTime);

            ExecutionTimeSizeClasses.VisitOverlapping(
                requestBytes,
                requestBytes + 1,
                [&](TDisjointIntervalMap<
                    ui64,
                    std::unique_ptr<TSizeClassCounters>>::TIterator it)
                { it->second.Value->ExecutionTimeHist.Increment(execTime); });

            PostponedTimeHist.Increment(postponedTime);
        }
    }

    void BatchCompleted(
        ui64 requestCount,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist,
        bool unaligned)
    {
        if (UseStripedAccumulators) {
            auto& stripe = AccessStripe();
            auto g = Guard(stripe.Lock);

            stripe.Count += requestCount;
            stripe.Errors += errors;

            for (auto [dt, count]: timeHist) {
                stripe.TimeMicros += dt.MicroSeconds() * count;
                stripe.TimeBuckets[TimeBucketIndex(dt)] += count;
                stripe.MaxTimeMicros = Max(stripe.MaxTimeMicros, dt.MicroSeconds());
            }

            if (IsReadWriteRequest) {
                stripe.MaxCount += requestCount;
                stripe.RequestBytes += bytes;
                stripe.MaxRequestBytes += bytes;

                for (auto [size, count]: sizeHist) {
                    stripe.SizeBuckets[SizeBucketIndex(size)] += count;
                    stripe.MaxSize = Max(stripe.MaxSize, size);
                }

                if (unaligned) {
                    stripe.UnalignedCount += requestCount + errors;
                }

                for (auto [dt, count]: timeHist) {
                    const auto timeBucket = TimeBucketIndex(dt);
                    stripe.ExecutionTimeBuckets[timeBucket] += count;
                    if (unaligned) {
                        stripe.TimeBucketsUnaligned[timeBucket] += count;
                        stripe.ExecutionTimeBucketsUnaligned[timeBucket] += count;
                    }
                }
            }
            return;
        }

        Count->Add(requestCount);
        Errors->Add(errors);

        for (auto [dt, count]: timeHist) {
            Time->Add(dt.MicroSeconds());
            TimeHist.Increment(dt, count);
            MaxTimeCalc.Add(dt.MicroSeconds());
        }

        if (IsReadWriteRequest) {
            MaxCountCalc.Add(requestCount);
            RequestBytes->Add(bytes);
            MaxRequestBytesCalc.Add(bytes);

            for (auto [size, count]: sizeHist) {
                SizeHist.Increment(size, count);
                MaxSizeCalc.Add(size);
            }

            if (unaligned) {
                UnalignedCount->Add(requestCount);
                UnalignedCount->Add(errors);
            }

            for (auto [dt, count]: timeHist) {
                ExecutionTimeHist.Increment(dt, count);

                if (unaligned) {
                    TimeHistUnaligned.Increment(dt, count);
                    ExecutionTimeHistUnaligned.Increment(dt, count);
                }
            }
        }
    }

    void AddRetryStats(EDiagnosticsErrorKind errorKind)
    {
        if (UseStripedAccumulators) {
            auto& stripe = AccessStripe();
            auto g = Guard(stripe.Lock);

            switch (errorKind) {
                case EDiagnosticsErrorKind::ErrorRetriable:
                    ++stripe.ErrorsRetriable;
                    break;
                case EDiagnosticsErrorKind::ErrorThrottling:
                    ++stripe.ErrorsThrottling;
                    break;
                case EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint:
                    ++stripe.ErrorsWriteRejectedByCheckpoint;
                    break;
                case EDiagnosticsErrorKind::ErrorSession:
                    ++stripe.ErrorsSession;
                    break;
                case EDiagnosticsErrorKind::Success:
                case EDiagnosticsErrorKind::ErrorAborted:
                case EDiagnosticsErrorKind::ErrorFatal:
                case EDiagnosticsErrorKind::ErrorSilent:
                case EDiagnosticsErrorKind::Max:
                    Y_DEBUG_ABORT_UNLESS(false);
                    return;
            }

            ++stripe.Errors;
            ++stripe.Retries;
            return;
        }

        switch (errorKind) {
            case EDiagnosticsErrorKind::ErrorRetriable:
                ErrorsRetriable->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorThrottling:
                ErrorsThrottling->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint:
                ErrorsWriteRejectdByCheckpoint->Inc();
                break;
            case EDiagnosticsErrorKind::ErrorSession:
                ErrorsSession->Inc();
                break;
            case EDiagnosticsErrorKind::Success:
            case EDiagnosticsErrorKind::ErrorAborted:
            case EDiagnosticsErrorKind::ErrorFatal:
            case EDiagnosticsErrorKind::ErrorSilent:
                Y_DEBUG_ABORT_UNLESS(false);
                return;
            case EDiagnosticsErrorKind::Max:
                Y_DEBUG_ABORT_UNLESS(false);
                return;
        }

        Errors->Inc();
        Retries->Inc();
    }

    void RequestPostponed()
    {
        if (UseStripedAccumulators && IsReadWriteRequest) {
            auto& stripe = AccessStripe();
            auto g = Guard(stripe.Lock);
            ++stripe.PostponedCount;
            ++stripe.PostponedQueueSizeDelta;
            stripe.PostponedQueueSizePeak = Max(
                stripe.PostponedQueueSizePeak,
                stripe.PostponedQueueSizeDelta);
            return;
        }

        if (IsReadWriteRequest) {
            PostponedCount->Inc();
            MaxPostponedQueueSizeCalc.Add(PostponedQueueSize->Inc());
        }
    }

    void RequestPostponedServer()
    {
        if (UseStripedAccumulators && IsReadWriteRequest) {
            auto& stripe = AccessStripe();
            auto g = Guard(stripe.Lock);
            ++stripe.PostponedCountGrpc;
            ++stripe.PostponedQueueSizeGrpcDelta;
            stripe.PostponedQueueSizeGrpcPeak = Max(
                stripe.PostponedQueueSizeGrpcPeak,
                stripe.PostponedQueueSizeGrpcDelta);
            return;
        }

        if (IsReadWriteRequest) {
            PostponedCountGrpc->Inc();
            MaxPostponedQueueSizeGrpcCalc.Add(PostponedQueueSizeGrpc->Inc());
        }
    }

    void RequestFastPathHit()
    {
        if (UseStripedAccumulators && IsReadWriteRequest) {
            auto& stripe = AccessStripe();
            auto g = Guard(stripe.Lock);
            ++stripe.FastPathHits;
            return;
        }

        if (IsReadWriteRequest) {
            FastPathHits->Inc();
        }
    }

    void RequestAdvanced()
    {
        if (UseStripedAccumulators && IsReadWriteRequest) {
            auto& stripe = AccessStripe();
            auto g = Guard(stripe.Lock);
            --stripe.PostponedQueueSizeDelta;
            return;
        }

        if (IsReadWriteRequest) {
            PostponedQueueSize->Dec();
        }
    }

    void RequestAdvancedServer()
    {
        if (UseStripedAccumulators && IsReadWriteRequest) {
            auto& stripe = AccessStripe();
            auto g = Guard(stripe.Lock);
            --stripe.PostponedQueueSizeGrpcDelta;
            return;
        }

        if (IsReadWriteRequest) {
            PostponedQueueSizeGrpc->Dec();
        }
    }

    void AddIncompleteStats(
        TDuration executionTime,
        TDuration totalTime,
        ECalcMaxTime calcMaxTime)
    {
        if (UseStripedAccumulators) {
            auto& stripe = AccessStripe();
            auto g = Guard(stripe.Lock);
            if (calcMaxTime == ECalcMaxTime::ENABLE) {
                stripe.MaxTimeMicros = Max(
                    stripe.MaxTimeMicros,
                    executionTime.MicroSeconds());
            }
            stripe.MaxTotalTimeMicros = Max(
                stripe.MaxTotalTimeMicros,
                totalTime.MicroSeconds());
            return;
        }

        if (calcMaxTime == ECalcMaxTime::ENABLE) {
            MaxTimeCalc.Add(executionTime.MicroSeconds());
        }
        MaxTotalTimeCalc.Add(totalTime.MicroSeconds());
    }

    void UpdateStats(bool updatePercentiles)
    {
        if (UseStripedAccumulators) {
            PublishStripedStats();
        }

        *MaxInProgress = MaxInProgressCalc.NextValue();
        *MaxTime = MaxTimeCalc.NextValue();
        *MaxTotalTime = MaxTotalTimeCalc.NextValue();

        if (IsReadWriteRequest) {
            *MaxCount = MaxCountCalc.NextValue();
            *MaxSize = MaxSizeCalc.NextValue();
            *MaxRequestBytes = MaxRequestBytesCalc.NextValue();
            *MaxInProgressBytes = MaxInProgressBytesCalc.NextValue();
            *MaxPostponedQueueSize = MaxPostponedQueueSizeCalc.NextValue();
            *MaxPostponedQueueSizeGrpc =
                MaxPostponedQueueSizeGrpcCalc.NextValue();
            if (updatePercentiles && !ReportDataPlaneHistogram) {
                SizePercentiles.Update();
                TimePercentiles.Update();
                ExecutionTimePercentiles.Update();
                RequestCompletionTimePercentiles.Update();
                PostponedTimePercentiles.Update();

                for (auto& [_, sizeClass]: ExecutionTimeSizeClasses) {
                    sizeClass.Value->ExecutionTimePercentiles.Update();
                }
            }
        } else if (updatePercentiles && !ReportControlPlaneHistogram) {
            TimePercentiles.Update();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TRequestCounters::TRequestCounters(
        ITimerPtr timer,
        ui32 requestCount,
        std::function<TString(TRequestType)> requestType2Name,
        std::function<bool(TRequestType)> isReadWriteRequestType,
        std::function<bool(TRequestType)> isStartEndpointRequestType,
        EOptions options,
        EHistogramCounterOptions histogramCounterOptions,
        const TVector<TSizeInterval>& executionTimeSizeClasses,
        ui32 stripeCount)
    : RequestType2Name(std::move(requestType2Name))
    , IsReadWriteRequestType(std::move(isReadWriteRequestType))
    , IsStartEndpointRequestType(std::move(isStartEndpointRequestType))
    , Options(options)
{
    if (Options & EOption::AddSpecialCounters) {
        SpecialCounters = MakeHolder<TSpecialCounters>();
    }

    CountersByRequest.reserve(requestCount);
    for (ui32 i = 0; i < requestCount; ++i) {
        CountersByRequest.emplace_back(
            timer,
            histogramCounterOptions,
            executionTimeSizeClasses,
            Options & EOption::UseStripedAccumulators,
            stripeCount);
    }
}

TRequestCounters::~TRequestCounters()
{}

TRequestCounters::TStatCounters& TRequestCounters::AccessRequestStats(
    TRequestType t)
{
    auto& statCounters = CountersByRequest[t];
    if (statCounters.CountersGroup) {
        statCounters.FullInitIfNeeded();
    }
    return statCounters;
}

void TRequestCounters::Register(TDynamicCounters& counters)
{
    if (SpecialCounters) {
        SpecialCounters->Init(counters);
    }

    for (TRequestType t = 0; t < CountersByRequest.size(); ++t) {
        if (ShouldReport(t)) {
            auto requestGroup = counters.GetSubgroup(
                "request",
                RequestType2Name(t));

            CountersByRequest[t].Init(
                std::move(requestGroup),
                IsReadWriteRequestType(t),
                Options & EOption::ReportDataPlaneHistogram,
                Options & EOption::ReportControlPlaneHistogram);

            // ReadWrite counters are usually the most important ones so let's
            // report zeroes for them instead of not reporting anything at all
            const bool lazyInit = Options & EOption::LazyRequestInitialization
                && !IsReadWriteRequestType(t);

            if (!lazyInit) {
                CountersByRequest[t].FullInitIfNeeded();
            }
        }
    }
}

void TRequestCounters::Subscribe(TRequestCountersPtr subscriber)
{
    Subscribers.push_back(std::move(subscriber));
}

ui64 TRequestCounters::RequestStarted(
    TRequestType requestType,
    ui64 requestBytes)
{
    RequestStartedImpl(requestType, requestBytes);
    return GetCycleCount();
}

TRequestCounters::TRequestTime TRequestCounters::RequestCompleted(
    TRequestType requestType,
    ui64 requestStarted,
    TDuration postponedTime,
    ui64 requestBytes,
    EDiagnosticsErrorKind errorKind,
    ui32 errorFlags,
    bool unaligned,
    ECalcMaxTime calcMaxTime,
    ui64 responseSent)
{
    auto requestCompleted = GetCycleCount();
    auto requestTime = CyclesToDurationSafe(requestCompleted - requestStarted);
    auto requestCompletionTime =
        responseSent ? CyclesToDurationSafe(requestCompleted - responseSent)
                     : TDuration::Zero();

    const auto time = requestTime - requestCompletionTime;
    const auto execTime = time - postponedTime;

    RequestCompletedImpl(
        requestType,
        requestTime,
        requestCompletionTime,
        postponedTime,
        requestBytes,
        errorKind,
        errorFlags,
        unaligned,
        calcMaxTime);

    return {.ExecutionTime = execTime, .Time = requestTime};
}

void TRequestCounters::AddRetryStats(
    TRequestType requestType,
    EDiagnosticsErrorKind errorKind,
    ui32 errorFlags)
{
    if (SpecialCounters) {
        SpecialCounters->AddRetryStats(errorKind, errorFlags);
    }

    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).AddRetryStats(errorKind);
    }
    NotifySubscribers(
        &TRequestCounters::AddRetryStats,
        requestType,
        errorKind,
        errorFlags);
}

void TRequestCounters::RequestPostponed(TRequestType requestType)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).RequestPostponed();
    }
    NotifySubscribers(
        &TRequestCounters::RequestPostponed,
        requestType);
}

void TRequestCounters::RequestPostponedServer(TRequestType requestType)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).RequestPostponedServer();
    }
    NotifySubscribers(
        &TRequestCounters::RequestPostponedServer,
        requestType);
}

void TRequestCounters::RequestFastPathHit(TRequestType requestType)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).RequestFastPathHit();
    }
    NotifySubscribers(
        &TRequestCounters::RequestFastPathHit,
        requestType);
}

void TRequestCounters::RequestAdvanced(TRequestType requestType)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).RequestAdvanced();
    }
    NotifySubscribers(
        &TRequestCounters::RequestAdvanced,
        requestType);
}

void TRequestCounters::RequestAdvancedServer(TRequestType requestType)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).RequestAdvancedServer();
    }
    NotifySubscribers(
        &TRequestCounters::RequestAdvancedServer,
        requestType);
}

void TRequestCounters::AddIncompleteStats(
    TRequestType requestType,
    TDuration executionTime,
    TDuration totalTime,
    ECalcMaxTime calcMaxTime)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).AddIncompleteStats(
            executionTime,
            totalTime,
            calcMaxTime);
    }
    NotifySubscribers(
        &TRequestCounters::AddIncompleteStats,
        requestType,
        executionTime,
        totalTime,
        calcMaxTime);
}

void TRequestCounters::BatchCompleted(
    TRequestType requestType,
    ui64 count,
    ui64 bytes,
    ui64 errors,
    std::span<TTimeBucket> timeHist,
    std::span<TSizeBucket> sizeHist)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).BatchCompleted(
            count,
            bytes,
            errors,
            timeHist,
            sizeHist,
            false); // unaligned
    }
    NotifySubscribers(
        &TRequestCounters::BatchCompleted,
        requestType,
        count,
        bytes,
        errors,
        timeHist,
        sizeHist);
}

void TRequestCounters::UpdateStats(bool updatePercentiles)
{
    for (auto& statCounters: CountersByRequest) {
        if (AtomicGet(statCounters.FullyInitialized)) {
            statCounters.UpdateStats(updatePercentiles);
        }
    }
    // NOTE subscribers are updated by their owners
}

void TRequestCounters::RequestStartedImpl(
    TRequestType requestType,
    ui64 requestBytes)
{
    if (ShouldReport(requestType)) {
        AccessRequestStats(requestType).Started(requestBytes);
    }
    NotifySubscribers(
        &TRequestCounters::RequestStartedImpl,
        requestType,
        requestBytes);
}

void TRequestCounters::RequestCompletedImpl(
    TRequestType requestType,
    TDuration requestTime,
    TDuration requestCompletionTime,
    TDuration postponedTime,
    ui64 requestBytes,
    EDiagnosticsErrorKind errorKind,
    ui32 errorFlags,
    bool unaligned,
    ECalcMaxTime calcMaxTime)
{
    if (SpecialCounters) {
        SpecialCounters->AddStats(errorKind, errorFlags);
    }

    if (ShouldReport(requestType)) {
        auto& statCounters = AccessRequestStats(requestType);
        statCounters.Completed(requestBytes);
        statCounters.AddStats(
            requestTime,
            requestCompletionTime,
            postponedTime,
            requestBytes,
            errorKind,
            unaligned,
            calcMaxTime);
    }
    NotifySubscribers(
        &TRequestCounters::RequestCompletedImpl,
        requestType,
        requestTime,
        requestCompletionTime,
        postponedTime,
        requestBytes,
        errorKind,
        errorFlags,
        unaligned,
        calcMaxTime);
}

bool TRequestCounters::ShouldReport(TRequestType requestType) const
{
    if (requestType >= CountersByRequest.size()) {
        return false;
    }

    if (Options & EOption::OnlyReadWriteRequests) {
        return IsReadWriteRequestType(requestType);
    }

    if (Options & EOption::OnlyStartEndpointRequests) {
        return IsStartEndpointRequestType(requestType);
    }

    return true;
}

template<typename TMethod, typename... TArgs>
void TRequestCounters::NotifySubscribers(TMethod&& m, TArgs&&... args)
{
    for (auto& s: Subscribers) {
        (s.get()->*m)(std::forward<TArgs>(args)...);
    }
}

}   // namespace NCloud
