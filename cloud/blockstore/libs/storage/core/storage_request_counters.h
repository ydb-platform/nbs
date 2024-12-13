#pragma once

#include "public.h"

#include "histogram.h"

#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>

#include <cloud/storage/core/libs/diagnostics/histogram_types.h>

#include <util/generic/flags.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class ERequestCounterOption
{
    ReportHistogram = (1 << 0),
    OnlySimple      = (1 << 1),
    HasKind         = (1 << 2),
    HasVoidBytes    = (1 << 3),
};

Y_DECLARE_FLAGS(ERequestCounterOptions, ERequestCounterOption);

Y_DECLARE_OPERATORS_FOR_FLAGS(ERequestCounterOptions);

////////////////////////////////////////////////////////////////////////////////

template <typename THistogram>
struct TRequestCounters
{
    ui64 Count = 0;
    ui64 RequestBytes = 0;
    ui64 RequestVoidBytes = 0;
    ui64 RequestNonVoidBytes = 0;

    ui64 FreshCount = 0;
    ui64 FreshRequestBytes = 0;
    ui64 MixedCount = 0;
    ui64 MixedRequestBytes = 0;
    ui64 MergedCount = 0;
    ui64 MergedRequestBytes = 0;
    ui64 ExternalCount = 0;
    ui64 ExternalRequestBytes = 0;

    THistogram Total;

    THistogram Fresh;
    THistogram Mixed;
    THistogram Merged;
    THistogram External;

    NMonitoring::TDynamicCounters::TCounterPtr SolomonCount;
    NMonitoring::TDynamicCounters::TCounterPtr SolomonRequestBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SolomonRequestVoidBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SolomonRequestNonVoidBytes;

    NMonitoring::TDynamicCounters::TCounterPtr SolomonFreshCount;
    NMonitoring::TDynamicCounters::TCounterPtr SolomonFreshRequestBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SolomonMixedCount;
    NMonitoring::TDynamicCounters::TCounterPtr SolomonMixedRequestBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SolomonMergedCount;
    NMonitoring::TDynamicCounters::TCounterPtr SolomonMergedRequestBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SolomonExternalCount;
    NMonitoring::TDynamicCounters::TCounterPtr SolomonExternalRequestBytes;

    ERequestCounterOptions Options;

    explicit TRequestCounters(EHistogramCounterOptions histCounterOptions)
        : Total(histCounterOptions)
        , Fresh(histCounterOptions)
        , Mixed(histCounterOptions)
        , Merged(histCounterOptions)
        , External(histCounterOptions)
    {}

    void AddRequest(
        ui64 time,
        ui64 size = 0,
        ui32 requestCount = 1,
        TMaybe<EChannelDataKind> kind = {})
    {
        if (kind) {
            switch (*kind) {
                case EChannelDataKind::Fresh:
                    Fresh.Increment(time, requestCount);
                    FreshCount += requestCount;
                    FreshRequestBytes += size;
                    break;
                case EChannelDataKind::Mixed:
                    Mixed.Increment(time, requestCount);
                    MixedCount += requestCount;
                    MixedRequestBytes += size;
                    break;
                case EChannelDataKind::Merged:
                    Merged.Increment(time, requestCount);
                    MergedCount += requestCount;
                    MergedRequestBytes += size;
                    break;
                case EChannelDataKind::External:
                    External.Increment(time, requestCount);
                    ExternalCount += requestCount;
                    ExternalRequestBytes += size;
                    break;
                default:
                    Y_DEBUG_ABORT_UNLESS(0, "unsupported kind");
            }
        }
        Total.Increment(time, requestCount);

        RequestBytes += size;
        Count += requestCount;
    }

    const THistogram& GetTotal() const
    {
        return Total;
    }

    ui64 GetCount() const
    {
        return Count;
    }

    ui64 GetRequestBytes() const
    {
        return RequestBytes;
    }

    ui64 GetRequestVoidBytes() const
    {
        return RequestVoidBytes;
    }

    ui64 GetRequestNonVoidBytes() const
    {
        return RequestNonVoidBytes;
    }

    void Add(const TRequestCounters& source)
    {
        Count += source.Count;
        RequestBytes += source.RequestBytes;
        RequestVoidBytes += source.RequestVoidBytes;
        RequestNonVoidBytes += source.RequestNonVoidBytes;

        FreshCount += source.FreshCount;
        FreshRequestBytes += source.FreshRequestBytes;
        MixedCount += source.MixedCount;
        MixedRequestBytes += source.MixedRequestBytes;
        MergedCount += source.MergedCount;
        MergedRequestBytes += source.MergedRequestBytes;
        ExternalCount += source.ExternalCount;
        ExternalRequestBytes += source.ExternalRequestBytes;

        Fresh.Add(source.Fresh);
        Mixed.Add(source.Mixed);
        Merged.Add(source.Merged);
        External.Add(source.External);
        Total.Add(source.Total);
    }

    void AggregateWith(const TRequestCounters& source)
    {
        Add(source);
    }

    void Reset()
    {
        Count = 0;
        RequestBytes = 0;
        RequestVoidBytes = 0;
        RequestNonVoidBytes = 0;

        FreshCount = 0;
        FreshRequestBytes = 0;
        MixedCount = 0;
        MixedRequestBytes = 0;
        MergedCount = 0;
        MergedRequestBytes = 0;
        ExternalCount = 0;
        ExternalRequestBytes = 0;

        Fresh.Reset();
        Mixed.Reset();
        Merged.Reset();
        External.Reset();
        Total.Reset();
    }

    void Register(
        NMonitoring::TDynamicCountersPtr counters,
        ERequestCounterOptions options)
    {
        Register(std::move(counters), {}, options);
    }

    void Register(
        NMonitoring::TDynamicCountersPtr counters,
        const TString& groupName,
        ERequestCounterOptions options)
    {
        Options = options;

        SolomonCount = counters->GetCounter("Count", true);
        SolomonRequestBytes = counters->GetCounter("RequestBytes", true);

        if (options & ERequestCounterOption::HasVoidBytes) {
            SolomonRequestVoidBytes = counters->GetCounter("RequestVoidBytes", true);
            SolomonRequestNonVoidBytes = counters->GetCounter("RequestNonVoidBytes", true);
        }

        if (options & ERequestCounterOption::HasKind) {
            auto freshCounters = counters->GetSubgroup("kind", "Fresh");
            auto mixedCounters = counters->GetSubgroup("kind", "Mixed");
            auto mergedCounters = counters->GetSubgroup("kind", "Merged");
            auto externalCounters = counters->GetSubgroup("kind", "External");

            SolomonFreshCount = freshCounters->GetCounter("Count", true);
            SolomonFreshRequestBytes =
                freshCounters->GetCounter("RequestBytes", true);

            SolomonMixedCount = mixedCounters->GetCounter("Count", true);
            SolomonMixedRequestBytes =
                mixedCounters->GetCounter("RequestBytes", true);

            SolomonMergedCount = mergedCounters->GetCounter("Count", true);
            SolomonMergedRequestBytes =
                mergedCounters->GetCounter("RequestBytes", true);

            SolomonExternalCount = externalCounters->GetCounter("Count", true);
            SolomonExternalRequestBytes =
                externalCounters->GetCounter("RequestBytes", true);
        }

        if (!(options & ERequestCounterOption::OnlySimple)) {
            Total.Register(counters, groupName, options & ERequestCounterOption::ReportHistogram);
            if ((options & ERequestCounterOption::HasKind) &&
                (options & ERequestCounterOption::ReportHistogram))
            {
                Fresh.Register(
                    counters->GetSubgroup("kind", "Fresh"),
                    groupName,
                    options & ERequestCounterOption::ReportHistogram);
                Mixed.Register(
                    counters->GetSubgroup("kind", "Mixed"),
                    groupName,
                    options & ERequestCounterOption::ReportHistogram);
                Merged.Register(
                    counters->GetSubgroup("kind", "Merged"),
                    groupName,
                    options & ERequestCounterOption::ReportHistogram);
                External.Register(
                    counters->GetSubgroup("kind", "External"),
                    groupName,
                    options & ERequestCounterOption::ReportHistogram);
            }
        }
    }

    void Publish()
    {
        *SolomonCount += Count;
        *SolomonRequestBytes += RequestBytes;

        if (Options & ERequestCounterOption::HasVoidBytes) {
            *SolomonRequestVoidBytes += RequestVoidBytes;
            *SolomonRequestNonVoidBytes += RequestNonVoidBytes;
        }

        if (!(Options & ERequestCounterOption::OnlySimple)) {
            Total.Publish();
            if ((Options & ERequestCounterOption::HasKind) &&
                (Options & ERequestCounterOption::ReportHistogram))
            {
                Fresh.Publish();
                Mixed.Publish();
                Merged.Publish();
                External.Publish();
            }
        }
        if (Options & ERequestCounterOption::HasKind) {
            *SolomonFreshCount += FreshCount;
            *SolomonMixedCount += MixedCount;
            *SolomonMergedCount += MergedCount;
            *SolomonExternalCount += ExternalCount;

            *SolomonFreshRequestBytes += FreshRequestBytes;
            *SolomonMixedRequestBytes += MixedRequestBytes;
            *SolomonMergedRequestBytes += MergedRequestBytes;
            *SolomonExternalRequestBytes += ExternalRequestBytes;
        }
    }
};

}   // namespace NCloud::NBlockStore::NStorage
