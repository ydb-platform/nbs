#include "write_back_cache_state_stats.h"

#include "relaxed_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheStateStats
    : public std::enable_shared_from_this<TWriteBackCacheStateStats>
    , public IWriteBackCacheStateStats
{
private:
    TRelaxedEventCounter<> FlushEventCounter;
    TRelaxedCounter FailedFlushCounter;
    TRelaxedCounter WriteDataRequestDroppedCounter;

public:
    void FlushStarted() override
    {
        FlushEventCounter.Started();
    }

    void FlushCompleted() override
    {
        FlushEventCounter.Completed();
    }

    void FlushFailed() override
    {
        FailedFlushCounter.Inc();
    }

    void WriteDataRequestDropped() override
    {
        WriteDataRequestDroppedCounter.Inc();
    }

    TWriteBackCacheStateMetrics CreateMetrics() const override
    {
        auto self = shared_from_this();

        return {
            .Flush =
                {.InProgressCount = NMetrics::CreateMetric(
                     [self]
                     { return self->FlushEventCounter.GetActiveCount(); }),
                 .InProgressMaxCount = NMetrics::CreateMetric(
                     [self]
                     { return self->FlushEventCounter.GetActiveMaxCount(); }),
                 .CompletedCount = NMetrics::CreateMetric(
                     [self]
                     { return self->FlushEventCounter.GetCompletedCount(); }),
                 .FailedCount = NMetrics::CreateMetric(
                     [self] { return self->FailedFlushCounter.Get(); })},
            .WriteDataRequestDroppedCount = NMetrics::CreateMetric(
                [self] { return self->WriteDataRequestDroppedCounter.Get(); })};
    }

    void UpdateStats() override
    {
        FlushEventCounter.Update();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TWriteBackCacheStateMetrics::Register(
    NMetrics::IMetricsRegistry& localMetricsRegistry,
    NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const
{
    localMetricsRegistry.Register(
        {CreateSensor("Flush_InProgressCount")},
        Flush.InProgressCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Flush_InProgressMaxCount")},
        Flush.InProgressMaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Flush_CompletedCount")},
        Flush.CompletedCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    aggregatableMetricsRegistry.Register(
        {CreateSensor("Flush_FailedCount")},
        Flush.FailedCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    aggregatableMetricsRegistry.Register(
        {CreateSensor("WriteDataRequest_DroppedCount")},
        WriteDataRequestDroppedCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);
}

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStateStatsPtr CreateWriteBackCacheStateStats()
{
    return std::make_shared<TWriteBackCacheStateStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
