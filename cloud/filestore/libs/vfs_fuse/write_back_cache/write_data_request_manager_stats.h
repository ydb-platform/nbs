#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/public.h>

#include <util/datetime/base.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

/**
 * WriteData request life cycle:
 * [Pending] -> Unflushed -> Flushed
 *
 * For each NodeId it is guaranteed that there are no requests with out-of-order
 * statuses: if two requests A and B have the same NodeId, and the request A was
 * added to the queue later than B, then A.Status <= B.Status.
 */

enum class EWriteDataRequestStatus
{
    // Write request is waiting until there is enough space in the persistent
    // storage to store the request.
    Pending,

    // Write request has been stored in the persistent storage and is waiting
    // for flushing
    Unflushed,

    // Write request has been written to the session and can be removed from
    // the persistent storage
    Flushed
};

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

    virtual TWriteDataRequestManagerMetrics
    CreateWriteDataRequestManagerMetrics() const = 0;

    virtual void UpdateWriteDataRequestManagerStats(
        TDuration maxPendingRequestDuration,
        TDuration maxUnflushedRequestDuration) = 0;
};

using IWriteDataRequestManagerStatsPtr =
    std::shared_ptr<IWriteDataRequestManagerStats>;

IWriteDataRequestManagerStatsPtr CreateWriteDataRequestManagerStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
