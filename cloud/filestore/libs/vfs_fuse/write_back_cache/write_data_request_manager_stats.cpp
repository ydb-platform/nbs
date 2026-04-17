#include "write_data_request_manager_stats.h"

#include "relaxed_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWriteDataRequestManagerStats
    : public std::enable_shared_from_this<TWriteDataRequestManagerStats>
    , public IWriteDataRequestManagerStats
{
private:
    TRelaxedEventCounterWithTimeStats<> PendingRequestCounter;
    TRelaxedEventCounterWithTimeStats<> UnflushedRequestCounter;
    TRelaxedEventCounter<> FlushedRequestCounter;

public:
    void AddedPendingRequest() override
    {
        PendingRequestCounter.Started();
    }

    void RemovedPendingRequest(TDuration duration) override
    {
        PendingRequestCounter.Completed(duration);
    }

    void AddedUnflushedRequest() override
    {
        UnflushedRequestCounter.Started();
    }

    void RemovedUnflushedRequest(TDuration duration) override
    {
        UnflushedRequestCounter.Completed(duration);
    }

    void AddedFlushedRequest() override
    {
        FlushedRequestCounter.Started();
    }

    void RemovedFlushedRequest() override
    {
        FlushedRequestCounter.Completed();
    }

    TWriteDataRequestManagerMetrics CreateMetrics() const override
    {
        auto self = shared_from_this();

        return {
            .PendingQueue =
                {.Count = CreateMetric(
                     [self]
                     { return self->PendingRequestCounter.GetActiveCount(); }),
                 .MaxCount = CreateMetric(
                     [self]
                     {
                         return self->PendingRequestCounter.GetActiveMaxCount();
                     }),
                 .ProcessedCount = CreateMetric(
                     [self]
                     {
                         return self->PendingRequestCounter.GetCompletedCount();
                     }),
                 .ProcessedTime = CreateMetric(
                     [self]
                     {
                         return self->PendingRequestCounter.GetCompletedTime();
                     }),
                 .MaxTime = CreateMetric(
                     [self]
                     { return self->PendingRequestCounter.GetMaxTime(); })},
            .UnflushedQueue =
                {.Count = CreateMetric(
                     [self]
                     {
                         return self->UnflushedRequestCounter.GetActiveCount();
                     }),
                 .MaxCount = CreateMetric(
                     [self]
                     {
                         return self->UnflushedRequestCounter
                             .GetActiveMaxCount();
                     }),
                 .ProcessedCount = CreateMetric(
                     [self]
                     {
                         return self->UnflushedRequestCounter
                             .GetCompletedCount();
                     }),
                 .ProcessedTime = CreateMetric(
                     [self]
                     {
                         return self->UnflushedRequestCounter
                             .GetCompletedTime();
                     }),
                 .MaxTime = CreateMetric(
                     [self]
                     { return self->UnflushedRequestCounter.GetMaxTime(); })},
            .FlushedQueue =
                {.Count = CreateMetric(
                     [self]
                     { return self->FlushedRequestCounter.GetActiveCount(); }),
                 .MaxCount = CreateMetric(
                     [self]
                     {
                         return self->FlushedRequestCounter.GetActiveMaxCount();
                     }),
                 .ProcessedCount = CreateMetric(
                     [self]
                     {
                         return self->FlushedRequestCounter.GetCompletedCount();
                     })},
        };
    }

    void UpdateStats(
        TDuration maxPendingRequestDuration,
        TDuration maxUnflushedRequestDuration) override
    {
        PendingRequestCounter.Update(maxPendingRequestDuration);
        UnflushedRequestCounter.Update(maxUnflushedRequestDuration);
        FlushedRequestCounter.Update();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TWriteDataRequestManagerMetrics::Register(
    NMetrics::IMetricsRegistry& localMetricsRegistry,
    NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const
{
    localMetricsRegistry.Register(
        {CreateSensor("PendingQueue_Count")},
        PendingQueue.Count,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("PendingQueue_MaxCount")},
        PendingQueue.MaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("PendingQueue_ProcessedCount")},
        PendingQueue.ProcessedCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    localMetricsRegistry.Register(
        {CreateSensor("PendingQueue_ProcessedTime")},
        PendingQueue.ProcessedTime,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    localMetricsRegistry.Register(
        {CreateSensor("PendingQueue_MaxTime")},
        PendingQueue.MaxTime,
        EAggregationType::AT_MAX,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("UnflushedQueue_Count")},
        UnflushedQueue.Count,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("UnflushedQueue_MaxCount")},
        UnflushedQueue.MaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("UnflushedQueue_ProcessedCount")},
        UnflushedQueue.ProcessedCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    localMetricsRegistry.Register(
        {CreateSensor("UnflushedQueue_ProcessedTime")},
        UnflushedQueue.ProcessedTime,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    aggregatableMetricsRegistry.Register(
        {CreateSensor("UnflushedQueue_MaxTime")},
        UnflushedQueue.MaxTime,
        EAggregationType::AT_MAX,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("FlushedQueue_Count")},
        FlushedQueue.Count,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("FlushedQueue_MaxCount")},
        FlushedQueue.MaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("FlushedQueue_ProcessedCount")},
        FlushedQueue.ProcessedCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);
}

////////////////////////////////////////////////////////////////////////////////

IWriteDataRequestManagerStatsPtr CreateWriteDataRequestManagerStats()
{
    return std::make_shared<TWriteDataRequestManagerStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
