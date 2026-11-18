#include "disk_counters.h"

#include <cloud/blockstore/libs/diagnostics/config.h>

#include <cloud/storage/core/libs/common/format.h>

#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <
    typename TCounters,
    typename TMetas = decltype(TCounters::AllCounters)>
void AddCounters(
    TCounters& dest,
    const TCounters& source,
    const TMetas& metas = TCounters::AllCounters)
{
    for (auto meta: metas) {
        auto& counter = meta.GetValue(dest);
        counter.Add(meta.GetValue(source));
    }
}

template <
    typename TCounters,
    typename TMetas = decltype(TCounters::AllCounters)>
void AggregateCounters(
    TCounters& dest,
    const TCounters& source,
    const TMetas& metas = TCounters::AllCounters)
{
    for (auto meta: metas) {
        auto& counter = meta.GetValue(dest);
        counter.AggregateWith(meta.GetValue(source));
    }
}

template <
    typename TCounters,
    typename TMetas = decltype(TCounters::AllCounters)>
void ResetCounters(
    TCounters& counters,
    const TMetas& metas = TCounters::AllCounters)
{
    for (auto meta: metas) {
        auto& counter = meta.GetValue(counters);
        counter.Reset();
    }
}

bool ShouldPublishOrRegisterCounter(
    EPublishingPolicy policy,
    EPublishingPolicy publishingPolicy,
    EPublishingPolicy neededPolicy = EPublishingPolicy::All)
{
    return policy == neededPolicy || publishingPolicy == neededPolicy ||
           policy == publishingPolicy;
}

template <
    typename TCounters,
    typename TMetas = decltype(TCounters::AllCounters)>
void PublishCounters(
    TInstant now,
    TCounters& counters,
    EPublishingPolicy policy,
    EPublishingPolicy neededPolicy = EPublishingPolicy::All,
    const TMetas& metas = TCounters::AllCounters)
{
    for (auto meta: metas) {
        auto& counter = meta.GetValue(counters);
        if (ShouldPublishOrRegisterCounter(
                policy,
                counter.PublishingPolicy,
                neededPolicy))
        {
            counter.Publish(now);
        }
    }
}

}   // namespace

TPartitionDiskCounters::TPartitionDiskCounters(
        EPublishingPolicy policy,
        const TDiagnosticsConfig& diagnosticsConfig)
    : RequestCounters(diagnosticsConfig.GetHistogramCounterOptions())
    , Histogram(diagnosticsConfig.GetHistogramCounterOptions())
    , Policy(policy)
{
    // We will add counters for all type of disks. they will be published and
    // reported only for YDB blobstorage based disks.
    for (const ui64 chunkSize: diagnosticsConfig.GetBlobCompressionChunkSizes())
    {
        BlobCompression.try_emplace(chunkSize);
    }
}

void TPartitionDiskCounters::Add(const TPartitionDiskCounters& source)
{
    for (const auto& [chunkSize, sourceCounters]: source.BlobCompression) {
        auto* destCounter = BlobCompression.FindPtr(chunkSize);
        Y_ABORT_UNLESS(destCounter);

        AddCounters(*destCounter, sourceCounters);
    }

    AddCounters(Simple, source.Simple);
    AddCounters(Cumulative, source.Cumulative);
    AddCounters(
        RequestCounters,
        source.RequestCounters,
        THistogramRequestCounters::AllLowResCounters);
    AddCounters(
        RequestCounters,
        source.RequestCounters,
        THistogramRequestCounters::AllHighResCounters);
    AddCounters(Histogram, source.Histogram);
    AddCounters(Rdma, source.Rdma);
    AddCounters(Interconnect, source.Interconnect);
}

void TPartitionDiskCounters::AggregateWith(const TPartitionDiskCounters& source)
{
    for (const auto& [chunkSize, sourceCounters]: source.BlobCompression) {
        auto* destCounter = BlobCompression.FindPtr(chunkSize);
        Y_ABORT_UNLESS(destCounter);

        AggregateCounters(*destCounter, sourceCounters);
    }

    AggregateCounters(Simple, source.Simple);
    AggregateCounters(Cumulative, source.Cumulative);
    AggregateCounters(
        RequestCounters,
        source.RequestCounters,
        THistogramRequestCounters::AllLowResCounters);
    AggregateCounters(
        RequestCounters,
        source.RequestCounters,
        THistogramRequestCounters::AllHighResCounters);
    AggregateCounters(Histogram, source.Histogram);
    AggregateCounters(Rdma, source.Rdma);
    AggregateCounters(Interconnect, source.Interconnect);
}

void TPartitionDiskCounters::Publish(TInstant now)
{
    for (auto& [chunkSize, counters]: BlobCompression) {
        PublishCounters(now, counters, Policy);
    }

    PublishCounters(now, Simple, Policy);
    PublishCounters(now, Cumulative, Policy);
    PublishCounters(
        now,
        RequestCounters,
        Policy,
        EPublishingPolicy::All,
        THistogramRequestCounters::AllLowResCounters);
    PublishCounters(
        now,
        RequestCounters,
        Policy,
        EPublishingPolicy::All,
        THistogramRequestCounters::AllHighResCounters);
    PublishCounters(now, Histogram, Policy);
    PublishCounters(now, Rdma, Policy);
    PublishCounters(now, Interconnect, Policy);

    Reset();
}

void TPartitionDiskCounters::Register(
    NMonitoring::TDynamicCountersPtr counters,
    bool aggregate)
{
    for (auto& [chunkSize, compression]: BlobCompression) {
        TIntrusivePtr<NMonitoring::TDynamicCounters> subgroup = nullptr;
        for (auto meta: TBlobCompressionCounters::AllCounters) {
            auto& counter = meta.GetValue(compression);
            if (ShouldPublishOrRegisterCounter(
                    Policy,
                    counter.PublishingPolicy))
            {
                if (!subgroup) {
                    subgroup = counters->GetSubgroup(
                        "compression_chunk_size",
                        FormatByteSize(chunkSize));
                }
                counter.Register(subgroup, TString(meta.Name));
            }
        }
    }

    ERequestCounterOptions requestCounterOptions;
    if (aggregate) {
        requestCounterOptions =
            requestCounterOptions | ERequestCounterOption::ReportHistogram;
    }

    for (auto meta: TSimpleDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        if (ShouldPublishOrRegisterCounter(Policy, counter.PublishingPolicy)) {
            counter.Register(counters, TString(meta.Name));
        }
    }

    for (auto meta: TCumulativeDiskCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        if (ShouldPublishOrRegisterCounter(Policy, counter.PublishingPolicy)) {
            counter.Register(counters, TString(meta.Name));
        }
    }

    for (auto meta: THistogramRequestCounters::AllLowResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        if (ShouldPublishOrRegisterCounter(Policy, counter.PublishingPolicy)) {
            counter.Register(
                counters->GetSubgroup("request", TString(meta.Name)),
                requestCounterOptions | counter.CounterOption);
        }
    }

    for (auto meta: THistogramRequestCounters::AllHighResCounters) {
        auto& counter = meta.GetValue(RequestCounters);
        if (ShouldPublishOrRegisterCounter(Policy, counter.PublishingPolicy)) {
            counter.Register(
                counters->GetSubgroup("request", TString(meta.Name)),
                requestCounterOptions | counter.CounterOption);
        }
    }

    for (auto meta: THistogramCounters::AllCounters) {
        auto& counter = meta.GetValue(Histogram);
        if (ShouldPublishOrRegisterCounter(Policy, counter.PublishingPolicy)) {
            counter.Register(
                counters->GetSubgroup("queue", TString(meta.Name)),
                aggregate);
        }
    }

    for (auto meta: TTransportCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        if (ShouldPublishOrRegisterCounter(
                Policy,
                counter.PublishingPolicy,
                EPublishingPolicy::DiskRegistryBased))
        {
            counter.Register(
                counters->GetSubgroup("request", TString(meta.Tag))
                    ->GetSubgroup("transport", "Interconnect"),
                TString(meta.Name));
        }
    }

    for (auto meta: TTransportCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        if (ShouldPublishOrRegisterCounter(
                Policy,
                counter.PublishingPolicy,
                EPublishingPolicy::DiskRegistryBased))
        {
            counter.Register(
                counters->GetSubgroup("request", TString(meta.Tag))
                    ->GetSubgroup("transport", "RDMA"),
                TString(meta.Name));
        }
    }
}

void TPartitionDiskCounters::Reset()
{
    for (auto& [chunkSize, counters]: BlobCompression) {
        ResetCounters(counters);
    }

    ResetCounters(Simple);
    ResetCounters(Cumulative);
    ResetCounters(
        RequestCounters,
        THistogramRequestCounters::AllLowResCounters);
    ResetCounters(
        RequestCounters,
        THistogramRequestCounters::AllHighResCounters);
    ResetCounters(Histogram);
    ResetCounters(Rdma);
    ResetCounters(Interconnect);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeSelfCounters::AggregateWith(const TVolumeSelfCounters& source)
{
    AggregateCounters(Simple, source.Simple);
    AggregateCounters(Cumulative, source.Cumulative);
    AggregateCounters(
        ThrottlerDelayRequestCounters,
        source.ThrottlerDelayRequestCounters);
    AggregateCounters(
        IngestTimeRequestCounters,
        source.IngestTimeRequestCounters);
}

void TVolumeSelfCounters::Add(const TVolumeSelfCounters& source)
{
    AddCounters(Simple, source.Simple);
    AddCounters(Cumulative, source.Cumulative);
    AddCounters(
        ThrottlerDelayRequestCounters,
        source.ThrottlerDelayRequestCounters);
    AddCounters(IngestTimeRequestCounters, source.IngestTimeRequestCounters);
}

void TVolumeSelfCounters::Reset()
{
    ResetCounters(Simple);
    ResetCounters(Cumulative);
    ResetCounters(ThrottlerDelayRequestCounters);
    ResetCounters(IngestTimeRequestCounters);
}

void TVolumeSelfCounters::Register(
    NMonitoring::TDynamicCountersPtr counters,
    bool aggregate)
{
    for (auto meta: TVolumeSelfSimpleCounters::AllCounters) {
        auto& counter = meta.GetValue(Simple);
        if (ShouldPublishOrRegisterCounter(Policy, counter.PublishingPolicy)) {
            counter.Register(counters, TString(meta.Name));
        }
    }

    for (auto meta: TVolumeSelfCumulativeCounters::AllCounters) {
        auto& counter = meta.GetValue(Cumulative);
        if (ShouldPublishOrRegisterCounter(Policy, counter.PublishingPolicy)) {
            counter.Register(counters, TString(meta.Name));
        }
    }

    for (auto meta: TVolumeThrottlerDelayCounters::AllCounters) {
        auto& counter = meta.GetValue(ThrottlerDelayRequestCounters);
        if (ShouldPublishOrRegisterCounter(Policy, counter.PublishingPolicy)) {
            counter.ForceRegister(
                counters->GetSubgroup("request", TString(meta.Name)),
                "ThrottlerDelay",
                aggregate);
        }
    }

    for (auto meta: TVolumeIngestTimeCounters::AllCounters) {
        auto& counter = meta.GetValue(IngestTimeRequestCounters);
        if (ShouldPublishOrRegisterCounter(Policy, counter.PublishingPolicy)) {
            counter.ForceRegister(
                counters->GetSubgroup("request", TString(meta.Name)),
                "IngestTime",
                aggregate);
        }
    }
}

void TVolumeSelfCounters::Publish(TInstant now)
{
    PublishCounters(now, Simple, Policy);
    PublishCounters(now, Cumulative, Policy);
    PublishCounters(now, ThrottlerDelayRequestCounters, Policy);
    PublishCounters(now, IngestTimeRequestCounters, Policy);

    Reset();
}

////////////////////////////////////////////////////////////////////////////////

TVolumeSelfCountersPtr CreateVolumeSelfCounters(
    EPublishingPolicy policy,
    EHistogramCounterOptions histCounterOptions)
{
    return std::make_unique<TVolumeSelfCounters>(policy, histCounterOptions);
}

TPartitionDiskCountersPtr CreatePartitionDiskCounters(
    EPublishingPolicy policy,
    const TDiagnosticsConfig& diagnosticsConfig)
{
    return std::make_unique<TPartitionDiskCounters>(
        policy,
        diagnosticsConfig);
}

}   // namespace NCloud::NBlockStore::NStorage
