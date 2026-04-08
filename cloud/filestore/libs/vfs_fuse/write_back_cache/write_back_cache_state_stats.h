#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/public.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCacheStateMetrics
{
    struct TFlushMetrics
    {
        NMetrics::IMetricPtr InProgressCount;
        NMetrics::IMetricPtr InProgressMaxCount;
        NMetrics::IMetricPtr CompletedCount;
        NMetrics::IMetricPtr FailedCount;
    };

    TFlushMetrics Flush;
    NMetrics::IMetricPtr WriteDataRequestDroppedCount;
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStateStats
{
    virtual ~IWriteBackCacheStateStats() = default;

    virtual void FlushStarted() = 0;
    virtual void FlushCompleted() = 0;
    virtual void FlushFailed() = 0;
    virtual void WriteDataRequestDropped() = 0;

    virtual TWriteBackCacheStateMetrics CreateMetrics() const = 0;
    virtual void UpdateStats() = 0;
};

using IWriteBackCacheStateStatsPtr = std::shared_ptr<IWriteBackCacheStateStats>;

IWriteBackCacheStateStatsPtr CreateWriteBackCacheStateStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
