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
};

////////////////////////////////////////////////////////////////////////////////

struct IPersistentStorageStats
{
    virtual ~IPersistentStorageStats() = default;

    virtual void Set(
        ui64 rawCapacityBytesCount,
        ui64 rawUsedBytesCount,
        ui64 entryCount,
        bool isCorrupted) = 0;

    virtual TPersistentStorageMetrics
    CreatePersistentStorageMetrics() const = 0;

    virtual void UpdatePersistentStorageStats() = 0;
};

using IPersistentStorageStatsPtr = std::shared_ptr<IPersistentStorageStats>;

IPersistentStorageStatsPtr CreatePersistentStorageStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
