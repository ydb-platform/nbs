#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/public.h>

#include <util/datetime/base.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

enum class EOperationalState
{
    // WriteBackCache accepts new cached WriteData requests normally
    Active,

    // WriteBackCache is draining pending and unflushed requests.
    // New WriteData requests are not accepted during this state.
    Stopping,

    // WriteBackCache has stopped draining requests.
    // This state also covers the case when ServerWriteBackCacheEnabled = false,
    // but WriteBackCache was previously initialized.
    Inactive,

    // WriteBackCache encountered an unrecoverable error.
    // This corresponds to persistent storage corruption or an internal problem
    // in the logic.
    Failed,
};

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

    struct TOperationalStateMetrics
    {
        NMetrics::IMetricPtr Active;
        NMetrics::IMetricPtr Stopping;
        NMetrics::IMetricPtr Inactive;
        NMetrics::IMetricPtr Failed;
    };

    TFlushMetrics Flush;
    NMetrics::IMetricPtr WriteDataRequestDroppedCount;
    TBarrierMetrics Barriers;

    TRequestMetrics FlushRequests;
    TRequestMetrics FlushAllRequests;
    TRequestMetrics ReleaseHandleRequests;
    TRequestMetrics AcquireBarrierRequests;

    TOperationalStateMetrics OperationalState;

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

    virtual void UpdateStats(
        EOperationalState state,
        const TMaxInProgressDurations& values) = 0;
};

using IWriteBackCacheStateStatsPtr = std::shared_ptr<IWriteBackCacheStateStats>;

IWriteBackCacheStateStatsPtr CreateWriteBackCacheStateStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
