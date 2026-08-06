#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/public.h>

#include <util/datetime/base.h>

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

    struct TBarrierMetrics
    {
        NMetrics::IMetricPtr ActiveCount;
        NMetrics::IMetricPtr ActiveMaxCount;
        NMetrics::IMetricPtr ReleasedCount;
        NMetrics::IMetricPtr ReleasedTime;
        NMetrics::IMetricPtr MaxTime;
    };

    struct TRequestMetrics
    {
        NMetrics::IMetricPtr InProgressCount;
        NMetrics::IMetricPtr InProgressMaxCount;
        NMetrics::IMetricPtr CompletedCount;
        NMetrics::IMetricPtr CompletedTime;
        NMetrics::IMetricPtr MaxTime;
        NMetrics::IMetricPtr CompletedImmediately;
        NMetrics::IMetricPtr FailedCount;
    };

    TFlushMetrics Flush;
    NMetrics::IMetricPtr WriteDataRequestDroppedCount;
    TBarrierMetrics Barriers;

    TRequestMetrics FlushRequests;
    TRequestMetrics FlushAllRequests;
    TRequestMetrics ReleaseHandleRequests;
    TRequestMetrics AcquireBarrierRequests;

    void Register(
        NMetrics::IMetricsRegistry& localMetricsRegistry,
        NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const;
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStateStats
{
    enum class ERequestType
    {
        Flush,
        FlushAll,
        ReleaseHandle,
        AcquireBarrier
    };

    struct TMaxInProgressDurations
    {
        const TDuration ActiveBarrier;
        const TDuration FlushRequest;
        const TDuration FlushAllRequest;
        const TDuration ReleaseHandleRequest;
        const TDuration AcquireBarrierRequest;
    };

    virtual ~IWriteBackCacheStateStats() = default;

    virtual void FlushStarted() = 0;
    virtual void FlushCompleted() = 0;
    virtual void FlushFailed() = 0;
    virtual void WriteDataRequestDropped() = 0;

    virtual void BarrierAcquired() = 0;
    virtual void BarrierReleased(TDuration duration) = 0;

    virtual void RequestAdded(ERequestType type) = 0;
    virtual void RequestCompleted(ERequestType type, TDuration duration) = 0;
    virtual void RequestCompletedImmediately(ERequestType type) = 0;
    virtual void RequestFailed(ERequestType type, TDuration duration) = 0;

    virtual TWriteBackCacheStateMetrics CreateMetrics() const = 0;
    virtual void UpdateStats(const TMaxInProgressDurations& values) = 0;
};

using IWriteBackCacheStateStatsPtr = std::shared_ptr<IWriteBackCacheStateStats>;

IWriteBackCacheStateStatsPtr CreateWriteBackCacheStateStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
