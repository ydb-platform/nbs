#include "disk_counters.h"

#include <cloud/blockstore/libs/diagnostics/config.h>

#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TPartitionDiskCounters::TPartitionDiskCounters(
        EPublishingPolicy policy,
        const TDiagnosticsConfig& diagnosticsConfig)
    : RequestCounters(diagnosticsConfig.GetHistogramCounterOptions())
    , Histogram(diagnosticsConfig.GetHistogramCounterOptions())
    , Policy(policy)
{
    if (Policy != EPublishingPolicy::DiskRegistryBased) {
        for (const ui32 chunkSize:
             diagnosticsConfig.GetBlobCompressionChunkSizes())
        {
            BlobCompression.try_emplace(chunkSize);
        }
    }
}

void TPartitionDiskCounters::Add(const TPartitionDiskCounters& source)
{
    for (const auto& [chunkSize, sourceCounters]: source.BlobCompression) {
        if (auto it = BlobCompression.find(chunkSize);
            it != BlobCompression.end())
        {
            it->second.UncompressedBytesWritten.Add(
                sourceCounters.UncompressedBytesWritten);
            it->second.CompressedBytesWritten.Add(
                sourceCounters.CompressedBytesWritten);
        }
    }

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

    for (auto meta: TTransportCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        counter.Add(meta.GetValue(source.Rdma));
    }

    for (auto meta: TTransportCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        counter.Add(meta.GetValue(source.Interconnect));
    }
}

void TPartitionDiskCounters::AggregateWith(const TPartitionDiskCounters& source)
{
    for (const auto& [chunkSize, sourceCounters]: source.BlobCompression) {
        if (auto it = BlobCompression.find(chunkSize);
            it != BlobCompression.end())
        {
            it->second.UncompressedBytesWritten.AggregateWith(
                sourceCounters.UncompressedBytesWritten);
            it->second.CompressedBytesWritten.AggregateWith(
                sourceCounters.CompressedBytesWritten);
        }
    }

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

    for (auto meta: TTransportCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        counter.AggregateWith(meta.GetValue(source.Rdma));
    }

    for (auto meta: TTransportCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        counter.AggregateWith(meta.GetValue(source.Interconnect));
    }
}

void TPartitionDiskCounters::Publish(TInstant now)
{
    for (auto& [chunkSize, counters]: BlobCompression) {
        counters.UncompressedBytesWritten.Publish(now);
        counters.CompressedBytesWritten.Publish(now);
    }

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

    for (auto meta: TTransportCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish(now);
        }
    }

    for (auto meta: TTransportCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish(now);
        }
    }

    Reset();
}

void TPartitionDiskCounters::Register(
    NMonitoring::TDynamicCountersPtr counters,
    bool aggregate)
{
    for (auto& [chunkSize, compression]: BlobCompression) {
        auto subgroup = counters->GetSubgroup(
            "compression_chunk_size",
            ToString(chunkSize));
        compression.UncompressedBytesWritten.Register(
            subgroup,
            "UncompressedBytesWritten");
        compression.CompressedBytesWritten.Register(
            subgroup,
            "CompressedBytesWritten");
    }

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

    for (auto meta: TTransportCounters::AllCounters) {
        auto& counter = meta.GetValue(Interconnect);
        if (Policy == EPublishingPolicy::DiskRegistryBased ||
            counter.PublishingPolicy == EPublishingPolicy::DiskRegistryBased ||
            Policy == counter.PublishingPolicy)
        {
            counter.Register(
                counters->GetSubgroup("request", TString(meta.Tag))
                    ->GetSubgroup("transport", "Interconnect"),
                TString(meta.Name));
        }
    }

    for (auto meta: TTransportCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        if (Policy == EPublishingPolicy::DiskRegistryBased ||
            counter.PublishingPolicy == EPublishingPolicy::DiskRegistryBased ||
            Policy == counter.PublishingPolicy)
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
        counters.UncompressedBytesWritten.Reset();
        counters.CompressedBytesWritten.Reset();
    }

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

    for (auto meta: TTransportCounters::AllCounters) {
        auto& counter = meta.GetValue(Rdma);
        counter.Reset();
    }

    for (auto meta: TTransportCounters::AllCounters) {
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

    for (auto meta: TVolumeThrottlerDelayCounters::AllCounters) {
        auto& counter = meta.GetValue(ThrottlerDelayRequestCounters);
        counter.AggregateWith(
            meta.GetValue(source.ThrottlerDelayRequestCounters));
    }

    for (auto meta: TVolumeIngestTimeCounters::AllCounters) {
        auto& counter = meta.GetValue(IngestTimeRequestCounters);
        counter.AggregateWith(meta.GetValue(source.IngestTimeRequestCounters));
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

    for (auto meta: TVolumeThrottlerDelayCounters::AllCounters) {
        auto& counter = meta.GetValue(ThrottlerDelayRequestCounters);
        counter.Add(meta.GetValue(source.ThrottlerDelayRequestCounters));
    }

    for (auto meta: TVolumeIngestTimeCounters::AllCounters) {
        auto& counter = meta.GetValue(IngestTimeRequestCounters);
        counter.Add(meta.GetValue(source.IngestTimeRequestCounters));
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

    for (auto meta: TVolumeThrottlerDelayCounters::AllCounters) {
        auto& counter = meta.GetValue(ThrottlerDelayRequestCounters);
        counter.Reset();
    }

    for (auto meta: TVolumeIngestTimeCounters::AllCounters) {
        auto& counter = meta.GetValue(IngestTimeRequestCounters);
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

    for (auto meta: TVolumeThrottlerDelayCounters::AllCounters) {
        auto& counter = meta.GetValue(ThrottlerDelayRequestCounters);
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

    for (auto meta: TVolumeIngestTimeCounters::AllCounters) {
        auto& counter = meta.GetValue(IngestTimeRequestCounters);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.ForceRegister(
                counters->GetSubgroup("request", TString(meta.Name)),
                "IngestTime",
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

    for (auto meta: TVolumeThrottlerDelayCounters::AllCounters) {
        auto& counter = meta.GetValue(ThrottlerDelayRequestCounters);
        if (Policy == EPublishingPolicy::All ||
            counter.PublishingPolicy == EPublishingPolicy::All ||
            Policy == counter.PublishingPolicy)
        {
            counter.Publish();
        }
    }

    for (auto meta: TVolumeIngestTimeCounters::AllCounters) {
        auto& counter = meta.GetValue(IngestTimeRequestCounters);
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
