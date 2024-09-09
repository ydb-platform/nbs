#include "disk_counters.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TPartitionDiskCounters::Add(const TPartitionDiskCounters& source)
{
    for (auto meta: TSimpleDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        counter.Add(meta.GetValue(source.Simple));
    }

    for (auto meta: TCumulativeDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        counter.Add(meta.GetValue(source.Cumulative));
    }

    for (auto meta: THistogramRequestCounters::AllLowResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        counter.Add(meta.GetValue(source.RequestCounters));
    }

    for (auto meta: THistogramRequestCounters::AllHighResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        counter.Add(meta.GetValue(source.RequestCounters));
    }

    for (auto meta: THistogramCounters::AllCounters) {
        auto& counter = meta.GetValue(Histogram);
        counter.Add(meta.GetValue(source.Histogram));
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        counter.Add(meta.GetValue(source.Rdma));
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        counter.Add(meta.GetValue(source.Interconnect));
    }
}

void TPartitionDiskCounters::AggregateWith(const TPartitionDiskCounters& source)
{
    for (auto meta: TSimpleDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        counter.AggregateWith(meta.GetValue(source.Simple));
    }

    for (auto meta: TCumulativeDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        counter.AggregateWith(meta.GetValue(source.Cumulative));
    }

    for (auto meta: THistogramRequestCounters::AllLowResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        counter.AggregateWith(meta.GetValue(source.RequestCounters));
    }

    for (auto meta: THistogramRequestCounters::AllHighResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        counter.AggregateWith(meta.GetValue(source.RequestCounters));
    }

    for (auto meta: THistogramCounters::AllCounters) {
        auto& counter = meta.GetValue(Histogram);
        counter.AggregateWith(meta.GetValue(source.Histogram));
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        counter.AggregateWith(meta.GetValue(source.Rdma));
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        counter.AggregateWith(meta.GetValue(source.Interconnect));
    }
}

void TPartitionDiskCounters::Publish(TInstant now)
{
    for (auto meta: TSimpleDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish(now);
        }
    }

    for (auto meta: TCumulativeDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish(now);
        }
    }

    for (auto meta: THistogramRequestCounters::AllLowResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }

    for (auto meta: THistogramRequestCounters::AllHighResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }

    for (auto meta: THistogramCounters::AllCounters) {
        auto& counter = meta.GetValue(Histogram);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
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

    for (auto meta: TSimpleDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(counters, TString(meta.Name));
        }
    }

    for (auto meta: TCumulativeDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(counters, TString(meta.Name));
        }
    }

    for (auto meta: THistogramRequestCounters::AllLowResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(
                counters->GetSubgroup("request", TString(meta.Name)),
                requestCounterOptions | counter.CounterOption);
        }
    }

    for (auto meta: THistogramRequestCounters::AllHighResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(
                counters->GetSubgroup("request", TString(meta.Name)),
                requestCounterOptions | counter.CounterOption);
        }
    }

    for (auto meta: THistogramCounters::AllCounters) {
        auto& counter = meta.GetValue(Histogram);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(
                counters->GetSubgroup("queue", TString(meta.Name)),
                aggregate);
        }
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(
                counters->GetSubgroup("request", TString(meta.Name)),
                requestCounterOptions | counter.CounterOption);
        }
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(
                counters->GetSubgroup("request", TString(meta.Name)),
                requestCounterOptions | counter.CounterOption);
        }
    }
}

void TPartitionDiskCounters::Reset()
{
    for (auto meta: TSimpleDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        counter.Reset();
    }

    for (auto meta: TCumulativeDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        counter.Reset();
    }

    for (auto meta: THistogramRequestCounters::AllLowResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        counter.Reset();
    }

    for (auto meta: THistogramRequestCounters::AllHighResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        counter.Reset();
    }

    for (auto meta: THistogramCounters::AllCounters) {
        auto& counter = meta.GetValue(Histogram);
        counter.Reset();
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        counter.Reset();
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        counter.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeSelfCounters::AggregateWith(const TVolumeSelfCounters& source)
{
    for (auto meta: TVolumeSelfSimpleCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        counter.AggregateWith(meta.GetValue(source.Simple));
    }

    for (auto meta: TVolumeSelfCumulativeCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        counter.AggregateWith(meta.GetValue(source.Cumulative));
    }

    for (auto meta: TVolumeSelfRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        counter.AggregateWith(meta.GetValue(source.RequestCounters));
    }
}

void TVolumeSelfCounters::Add(const TVolumeSelfCounters& source)
{
    for (auto meta: TVolumeSelfSimpleCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        counter.Add(meta.GetValue(source.Simple));
    }

    for (auto meta: TVolumeSelfCumulativeCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        counter.Add(meta.GetValue(source.Cumulative));
    }

    for (auto meta: TVolumeSelfRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        counter.Add(meta.GetValue(source.RequestCounters));
    }
}

void TVolumeSelfCounters::Reset()
{
    for (auto meta: TVolumeSelfSimpleCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        counter.Reset();
    }

    for (auto meta: TVolumeSelfCumulativeCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        counter.Reset();
    }

    for (auto meta: TVolumeSelfRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        counter.Reset();
    }
}

void TVolumeSelfCounters::Register(
    NMonitoring::TDynamicCountersPtr counters,
    bool aggregate)
{
    for (auto meta: TVolumeSelfSimpleCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(counters, TString(meta.Name));
        }
    }

    for (auto meta: TVolumeSelfCumulativeCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(counters, TString(meta.Name));
        }
    }

    for (auto meta: TVolumeSelfRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.ForceRegister(
                counters->GetSubgroup("request", TString(meta.Name)),
                "ThrottlerDelay",
                aggregate);
        }
    }
}

void TVolumeSelfCounters::Publish(TInstant now)
{
    for (auto meta: TVolumeSelfSimpleCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish(now);
        }
    }

    for (auto meta: TVolumeSelfCumulativeCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish(now);
        }
    }

    for (auto meta: TVolumeSelfRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }

    Reset();
}

////////////////////////////////////////////////////////////////////////////////

void TTransportDiskCounters::Add(const TTransportDiskCounters& source)
{
    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        counter.Add(meta.GetValue(source.Rdma));
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        counter.Add(meta.GetValue(source.Interconnect));
    }
}

void TTransportDiskCounters::AggregateWith(const TTransportDiskCounters& source)
{
    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        counter.AggregateWith(meta.GetValue(source.Rdma));
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        counter.AggregateWith(meta.GetValue(source.Interconnect));
    }
}

void TTransportDiskCounters::AggregateWith(const TPartitionDiskCounters& source)
{
    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        counter.AggregateWith(meta.GetValue(source.Rdma));
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        counter.AggregateWith(meta.GetValue(source.Interconnect));
    }
}

void TTransportDiskCounters::Publish()
{
    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }
    Reset();
}

void TTransportDiskCounters::Register(
    NMonitoring::TDynamicCountersPtr counters,
    bool aggregate)
{
    ERequestCounterOptions requestCounterOptions;
    if (aggregate) {
        requestCounterOptions =
            requestCounterOptions | ERequestCounterOption::ReportHistogram;
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(
                counters->GetSubgroup("request", TString(meta.Name)),
                requestCounterOptions | counter.CounterOption);
        }
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(
                counters->GetSubgroup("request", TString(meta.Name)),
                requestCounterOptions | counter.CounterOption);
        }
    }
}

void TTransportDiskCounters::Reset()
{
    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        counter.Reset();
    }

    for (auto meta: TTransportRequestCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        counter.Reset();
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
