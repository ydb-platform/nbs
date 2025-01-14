#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/storage/core/libs/diagnostics/counters_helper.h>
#include <cloud/storage/core/libs/diagnostics/histogram_counter_options.h>
#include <cloud/storage/core/libs/diagnostics/histogram_types.h>
#include <cloud/storage/core/libs/diagnostics/weighted_percentile.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename TBuckets>
struct THistogram
{
    using TSolomonCounters =
        TVector<NMonitoring::TDynamicCounters::TCounterPtr>;

    std::array<ui64, TBuckets::BUCKETS_COUNT> Buckets;

    EHistogramCounterOptions CounterOptions;
    std::optional<NMonitoring::THistogramPtr> Hist;
    std::optional<TSolomonCounters> HistSolomonCounters;
    std::optional<TSolomonCounters> PercSolomonCounters;

    TString GroupName;

    explicit THistogram(EHistogramCounterOptions counterOptions)
        : CounterOptions(counterOptions)
    {
        Fill(Buckets.begin(), Buckets.end(), 0);
    }

    void Increment(ui64 value)
    {
        Increment(value, 1);
    }

    void Increment(ui64 value, ui64 count)
    {
        auto it = LowerBound(
            TBuckets::Buckets.begin(),
            TBuckets::Buckets.end(),
            value);
        Y_ABORT_UNLESS(it != TBuckets::Buckets.end());

        auto idx = std::distance(TBuckets::Buckets.begin(), it);
        Buckets[idx] += count;
        if (Hist) {
            Hist.value()->Collect(static_cast<i64>(value), count);
        }
        if (HistSolomonCounters) {
            *HistSolomonCounters.value()[idx] += count;
        }
        if (PercSolomonCounters) {
            *PercSolomonCounters.value()[idx] += count;
        }
    }

    TVector<TBucketInfo> GetBuckets() const
    {
        TVector<TBucketInfo> result(Reserve(Buckets.size()));
        for (size_t i = 0; i < Buckets.size(); ++i) {
            result.emplace_back(
                TBuckets::Buckets[i],
                Buckets[i]);
        }

        return result;
    }

    TVector<TString> GetBucketNames(bool histogram) const
    {
        if (histogram) {
            return TBuckets::MakeNames();
        }
        return GetDefaultPercentileNames();
    }

    TVector<double> CalculatePercentiles() const
    {
        return CalculatePercentiles(GetDefaultPercentiles());
    }

    TVector<double> CalculatePercentiles(
        const TVector<TPercentileDesc>& percentiles) const
    {
        auto buckets = GetBuckets();

        auto result = CalculateWeightedPercentiles(
            buckets,
            percentiles);

        return result;
    }

    void Reset()
    {
        Buckets.fill(0);
    }

    void Add(const THistogram& source)
    {
        for (ui32 i = 0; i <  Buckets.size(); ++i) {
            Buckets[i] += source.Buckets[i];
        }
    }

    void AggregateWith(const THistogram& source)
    {
        Add(source);
    }

    TVector<TBucketInfo> GetPercentileBuckets() const
    {
        TVector<TBucketInfo> buckets(Reserve(Buckets.size()));
        for (ui32 idxRange = 0; idxRange < Buckets.size(); ++idxRange) {
            auto value = Buckets[idxRange];
            buckets.emplace_back(
                TBuckets::Buckets[idxRange],
                value);
        }
        return buckets;
    }

    TVector<ui64> GetSolomonHistogram() const
    {
        TVector<ui64> r(Reserve(Buckets.size()));
        for (ui32 i = 0; i < Buckets.size(); ++i) {
            r.push_back(Buckets[i]);
        }
        return r;
    }

    void Register(
        NMonitoring::TDynamicCountersPtr counters,
        bool reportHistogram = false)
    {
        Register(std::move(counters), {}, reportHistogram);
    }

    void ForceRegister(
        NMonitoring::TDynamicCountersPtr counters,
        const TString& groupName,
        bool reportHistogram = false)
    {
        Register(counters, groupName, reportHistogram);

        if (reportHistogram) {
            return;
        }

        const auto visibleHistogram =
            NMonitoring::TCountableBase::EVisibility::Private;

        auto& histGroup = *MakeVisibilitySubgroup(
            *counters,
            "histogram",
            GroupName,
            visibleHistogram);

        const auto bounds = ConvertToHistBounds(TBuckets::Buckets);
        Hist = histGroup.GetHistogram(
            GroupName,
            NMonitoring::ExplicitHistogram(bounds),
            true,
            visibleHistogram);

        const auto histBuckets = GetBucketNames(true);
        HistSolomonCounters = std::make_optional<TSolomonCounters>();
        for (size_t i = 0; i < histBuckets.size(); ++i) {
            HistSolomonCounters->push_back(
                histGroup.GetCounter(histBuckets[i], true, visibleHistogram));
        }
    }

    void Register(
        NMonitoring::TDynamicCountersPtr counters,
        const TString& groupName,
        bool reportHistogram = false)
    {
        GroupName = groupName;
        if (!GroupName) {
            GroupName = "Time";
        }

        const auto& group = counters->GetSubgroup(
            reportHistogram ? "histogram" : "percentiles",
            GroupName);

        const auto buckets = GetBucketNames(reportHistogram);
        if (reportHistogram) {
            if (CounterOptions & EHistogramCounterOption::ReportSingleCounter) {
                const auto bounds = ConvertToHistBounds(TBuckets::Buckets);
                Hist = group->GetHistogram(
                    GroupName,
                    NMonitoring::ExplicitHistogram(bounds),
                    true);
            }
            if (CounterOptions &
                EHistogramCounterOption::ReportMultipleCounters)
            {
                HistSolomonCounters = std::make_optional<TSolomonCounters>();
                for (size_t i = 0; i < buckets.size(); ++i) {
                    HistSolomonCounters->push_back(
                        group->GetCounter(buckets[i], true));
                }
            }
        } else {
            PercSolomonCounters = std::make_optional<TSolomonCounters>();
            for (ui32 i = 0; i < buckets.size(); ++i) {
                PercSolomonCounters->push_back(group->GetCounter(buckets[i]));
            }
        }
    }

    void Publish()
    {
        if (!Hist && !HistSolomonCounters && !PercSolomonCounters) {
            Y_DEBUG_ABORT_UNLESS(0);
            return;
        }

        if (Hist) {
            const auto hist = GetSolomonHistogram();
            for (size_t i = 0; i < hist.size(); ++i) {
                Hist.value()->Collect(static_cast<i64>(Buckets[i]), hist[i]);
            }
        }

        if (HistSolomonCounters) {
            const auto hist = GetSolomonHistogram();

            Y_ABORT_UNLESS(HistSolomonCounters->size() == hist.size());

            for (size_t i = 0; i < hist.size(); ++i) {
                *HistSolomonCounters.value()[i] += hist[i];
            }
        }

        if (PercSolomonCounters) {
            const auto percentiles = CalculatePercentiles();
            Y_ABORT_UNLESS(PercSolomonCounters->size() == percentiles.size());

            for (ui32 i = 0; i < percentiles.size(); ++i) {
                *PercSolomonCounters.value()[i] = std::lround(percentiles[i]);
            }
        }
    }
};

}   // namespace NCloud::NBlockStore::NStorage
