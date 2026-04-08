#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/public.h>

#include <util/datetime/base.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataRequestManagerMetrics
{
    struct TQueueMetrics
    {
        NMetrics::IMetricPtr Count;
        NMetrics::IMetricPtr MaxCount;
        NMetrics::IMetricPtr ProcessedCount;
    };

    struct TExtendedQueueMetrics
    {
        NMetrics::IMetricPtr Count;
        NMetrics::IMetricPtr MaxCount;
        NMetrics::IMetricPtr ProcessedCount;
        NMetrics::IMetricPtr ProcessedTime;
        NMetrics::IMetricPtr MaxTime;
    };

    TExtendedQueueMetrics PendingQueue;
    TExtendedQueueMetrics UnflushedQueue;
    TQueueMetrics FlushedQueue;
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteDataRequestManagerStats
{
    virtual ~IWriteDataRequestManagerStats() = default;

    virtual void AddedPendingRequest() = 0;
    virtual void RemovedPendingRequest(TDuration duration) = 0;

    virtual void AddedUnflushedRequest() = 0;
    virtual void RemovedUnflushedRequest(TDuration duration) = 0;

    virtual void AddedFlushedRequest() = 0;
    virtual void RemovedFlushedRequest() = 0;

    virtual TWriteDataRequestManagerMetrics CreateMetrics() const = 0;

    virtual void UpdateStats(
        TDuration maxPendingRequestDuration,
        TDuration maxUnflushedRequestDuration) = 0;
};

using IWriteDataRequestManagerStatsPtr =
    std::shared_ptr<IWriteDataRequestManagerStats>;

IWriteDataRequestManagerStatsPtr CreateWriteDataRequestManagerStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
