#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/public.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TPersistentStorageMetrics
{
    struct TMetrics
    {
        NMetrics::IMetricPtr RawCapacityByteCount;
        NMetrics::IMetricPtr RawUsedByteCount;
        NMetrics::IMetricPtr RawUsedByteMaxCount;
        NMetrics::IMetricPtr EntryCount;
        NMetrics::IMetricPtr EntryMaxCount;
        NMetrics::IMetricPtr Corrupted;
    };

    TMetrics Storage;

    void Register(
        NMetrics::IMetricsRegistry& localMetricsRegistry,
        NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const;
};

////////////////////////////////////////////////////////////////////////////////

struct IPersistentStorageStats
{
    virtual ~IPersistentStorageStats() = default;

    virtual void SetPersistentStorageCounters(
        ui64 rawCapacityBytesCount,
        ui64 rawUsedBytesCount,
        ui64 entryCount,
        bool isCorrupted) = 0;

    virtual TPersistentStorageMetrics CreateMetrics() const = 0;
    virtual void UpdateStats() = 0;
};

using IPersistentStorageStatsPtr = std::shared_ptr<IPersistentStorageStats>;

IPersistentStorageStatsPtr CreatePersistentStorageStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
