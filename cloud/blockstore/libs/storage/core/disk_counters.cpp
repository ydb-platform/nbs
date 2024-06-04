#include "disk_counters.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TPartitionDiskCounters::Add(const TPartitionDiskCounters& source)
{
    for (auto counterPtr: TSimpleDiskCounters::AllCounters) {
        auto& counter = Simple.*counterPtr;
        counter.Add(source.Simple.*counterPtr);
    }

    for (auto counterPtr: TCumulativeDiskCounters::AllCounters) {
        auto& counter = Cumulative.*counterPtr;
        counter.Add(source.Cumulative.*counterPtr);
    }

    for (auto counterPtr: THistogramRequestCounters::AllLowResCounters) {
        auto& counter = RequestCounters.*counterPtr;
        counter.Add(source.RequestCounters.*counterPtr);
    }

    for (auto counterPtr: THistogramRequestCounters::AllHighResCounters) {
        auto& counter = RequestCounters.*counterPtr;
        counter.Add(source.RequestCounters.*counterPtr);
    }

    for (auto counterPtr: THistogramCounters::AllCounters) {
        auto& counter = Histogram.*counterPtr;
        counter.Add(source.Histogram.*counterPtr);
    }
}

void TPartitionDiskCounters::AggregateWith(const TPartitionDiskCounters& source)
{
    for (auto counterPtr: TSimpleDiskCounters::AllCounters) {
        auto& counter = Simple.*counterPtr;
        counter.AggregateWith(source.Simple.*counterPtr);
    }

    for (auto counterPtr: TCumulativeDiskCounters::AllCounters) {
        auto& counter = Cumulative.*counterPtr;
        counter.AggregateWith(source.Cumulative.*counterPtr);
    }

    for (auto counterPtr: THistogramRequestCounters::AllLowResCounters) {
        auto& counter = RequestCounters.*counterPtr;
        counter.AggregateWith(source.RequestCounters.*counterPtr);
    }

    for (auto counterPtr: THistogramRequestCounters::AllHighResCounters) {
        auto& counter = RequestCounters.*counterPtr;
        counter.AggregateWith(source.RequestCounters.*counterPtr);
    }

    for (auto counterPtr: THistogramCounters::AllCounters) {
        auto& counter = Histogram.*counterPtr;
        counter.AggregateWith(source.Histogram.*counterPtr);
    }
}

void TPartitionDiskCounters::Publish(TInstant now)
{
    for (auto counterPtr: TSimpleDiskCounters::AllCounters) {
        auto& counter = (Simple.*counterPtr);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish(now);
        }
    }

    for (auto counterPtr: TCumulativeDiskCounters::AllCounters) {
        auto& counter = (Cumulative.*counterPtr);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish(now);
        }
    }

    for (auto counterPtr: THistogramRequestCounters::AllLowResCounters) {
        auto& counter = RequestCounters.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }

    for (auto counterPtr: THistogramRequestCounters::AllHighResCounters) {
        auto& counter = RequestCounters.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }

    for (auto counterPtr: THistogramCounters::AllCounters) {
        auto& counter = Histogram.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }

    Reset();
}

void TPartitionDiskCounters::Register(
    NMonitoring::TDynamicCountersPtr counters,
    bool aggregate)
{
    ERequestCounterOptions requestCounterOptions;
    if (aggregate) {
        requestCounterOptions =
            requestCounterOptions | ERequestCounterOption::ReportHistogram;
    }

    for (auto counterPtr: TSimpleDiskCounters::AllCounters) {
        auto& counter = (Simple.*counterPtr);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(counters, counter.Name);
        }
    }

    for (auto counterPtr: TCumulativeDiskCounters::AllCounters) {
        auto& counter = (Cumulative.*counterPtr);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(counters, counter.Name);
        }
    }

    for (auto counterPtr: THistogramRequestCounters::AllLowResCounters) {
        auto& counter = RequestCounters.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(
                counters->GetSubgroup("request", counter.Name),
                requestCounterOptions | counter.CounterOption);
        }
    }

    for (auto counterPtr: THistogramRequestCounters::AllHighResCounters) {
        auto& counter = RequestCounters.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(
                counters->GetSubgroup("request", counter.Name),
                requestCounterOptions | counter.CounterOption);
        }
    }

    for (auto counterPtr: THistogramCounters::AllCounters) {
        auto& counter = Histogram.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(
                counters->GetSubgroup("queue", counter.Name),
                aggregate);
        }
    }
}

void TPartitionDiskCounters::Reset()
{
    for (auto counterPtr: TSimpleDiskCounters::AllCounters) {
        auto& counter = (Simple.*counterPtr);
        counter.Reset();
    }

    for (auto counterPtr: TCumulativeDiskCounters::AllCounters) {
        auto& counter = (Cumulative.*counterPtr);
        counter.Reset();
    }

    for (auto counterPtr: THistogramRequestCounters::AllLowResCounters) {
        auto& counter = RequestCounters.*counterPtr;
        counter.Reset();
    }

    for (auto counterPtr: THistogramRequestCounters::AllHighResCounters) {
        auto& counter = RequestCounters.*counterPtr;
        counter.Reset();
    }

    for (auto counterPtr: THistogramCounters::AllCounters) {
        auto& counter = Histogram.*counterPtr;
        counter.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeSelfCounters::AggregateWith(const TVolumeSelfCounters& source)
{
    for (auto counterPtr: TVolumeSelfSimpleCounters::AllCounters) {
        auto& counter = Simple.*counterPtr;
        counter.AggregateWith(source.Simple.*counterPtr);
    }

    for (auto counterPtr: TVolumeSelfCumulativeCounters::AllCounters) {
        auto& counter = Cumulative.*counterPtr;
        counter.AggregateWith(source.Cumulative.*counterPtr);
    }

    for (auto counterPtr: TVolumeSelfRequestCounters::AllCounters) {
        auto& counter = RequestCounters.*counterPtr;
        counter.AggregateWith(source.RequestCounters.*counterPtr);
    }
}

void TVolumeSelfCounters::Add(const TVolumeSelfCounters& source)
{
    for (auto counterPtr: TVolumeSelfSimpleCounters::AllCounters) {
        auto& counter = Simple.*counterPtr;
        counter.Add(source.Simple.*counterPtr);
    }

    for (auto counterPtr: TVolumeSelfCumulativeCounters::AllCounters) {
        auto& counter = Cumulative.*counterPtr;
        counter.Add(source.Cumulative.*counterPtr);
    }

    for (auto counterPtr: TVolumeSelfRequestCounters::AllCounters) {
        auto& counter = RequestCounters.*counterPtr;
        counter.Add(source.RequestCounters.*counterPtr);
    }
}

void TVolumeSelfCounters::Reset()
{
    for (auto counterPtr: TVolumeSelfSimpleCounters::AllCounters) {
        auto& counter = Simple.*counterPtr;
        counter.Reset();
    }

    for (auto counterPtr: TVolumeSelfCumulativeCounters::AllCounters) {
        auto& counter = Cumulative.*counterPtr;
        counter.Reset();
    }

    for (auto counterPtr: TVolumeSelfRequestCounters::AllCounters) {
        auto& counter = RequestCounters.*counterPtr;
        counter.Reset();
    }
}

void TVolumeSelfCounters::Register(
    NMonitoring::TDynamicCountersPtr counters,
    bool aggregate)
{
    for (auto counterPtr: TVolumeSelfSimpleCounters::AllCounters) {
        auto& counter = Simple.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(counters, counter.Name);
        }
    }

    for (auto counterPtr: TVolumeSelfCumulativeCounters::AllCounters) {
        auto& counter = Cumulative.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(counters, counter.Name);
        }
    }

    for (auto counterPtr: TVolumeSelfRequestCounters::AllCounters) {
        auto& counter = RequestCounters.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.ForceRegister(
                counters->GetSubgroup("request", counter.Name),
                "ThrottlerDelay",
                aggregate);
        }
    }
}

void TVolumeSelfCounters::Publish(TInstant now)
{
    for (auto counterPtr: TVolumeSelfSimpleCounters::AllCounters) {
        auto& counter = Simple.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish(now);
        }
    }

    for (auto counterPtr: TVolumeSelfCumulativeCounters::AllCounters) {
        auto& counter = Cumulative.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish(now);
        }
    }

    for (auto counterPtr: TVolumeSelfRequestCounters::AllCounters) {
        auto& counter = RequestCounters.*counterPtr;
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TVolumeSelfCountersPtr CreateVolumeSelfCounters(EPublishingPolicy policy)
{
    return std::make_unique<TVolumeSelfCounters>(policy);
}

TPartitionDiskCountersPtr CreatePartitionDiskCounters(EPublishingPolicy policy)
{
    return std::make_unique<TPartitionDiskCounters>(policy);
}

}   // namespace NCloud::NBlockStore::NStorage
