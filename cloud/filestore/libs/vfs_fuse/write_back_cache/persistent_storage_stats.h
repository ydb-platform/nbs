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
        NMetrics::IMetricPtr MaxObservedEntryByteCount;
        NMetrics::IMetricPtr Version;
        NMetrics::IMetricPtr Corrupted;
    };

    TMetrics Storage;

    void Register(
        NMetrics::IMetricsRegistry& localMetricsRegistry,
        NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TPersistentStorageCounters
{
    ui64 RawCapacityBytesCount = 0;
    ui64 RawUsedBytesCount = 0;
    ui64 EntryCount = 0;
    ui64 MaxObservedEntryByteCount = 0;
    ui32 Version = 0;
    bool IsCorrupted = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IPersistentStorageStats
{
    virtual ~IPersistentStorageStats() = default;

    virtual void SetPersistentStorageCounters(
        const TPersistentStorageCounters& counters) = 0;

    virtual TPersistentStorageMetrics CreateMetrics() const = 0;
    virtual void UpdateStats() = 0;
};

using IPersistentStorageStatsPtr = std::shared_ptr<IPersistentStorageStats>;

IPersistentStorageStatsPtr CreatePersistentStorageStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
