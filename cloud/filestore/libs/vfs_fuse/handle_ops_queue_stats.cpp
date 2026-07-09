#include "handle_ops_queue_stats.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

namespace NCloud::NFileStore::NFuse {

using namespace NMetrics;

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueueStats::THandleOpsQueueStats(ui64 capacityBytes)
    : CapacityBytes(capacityBytes)
{}

TStringBuf THandleOpsQueueStats::GetName() const
{
    return "HandleOpsQueue";
}

void THandleOpsQueueStats::RegisterCounters(
    const IMetricsRegistryPtr& localMetricsRegistry,
    const IMetricsRegistryPtr& aggregatableMetricsRegistry)
{
    Y_UNUSED(aggregatableMetricsRegistry);

    auto self = shared_from_this();

    localMetricsRegistry->Register(
        {CreateSensor("EntryCount")},
        CreateMetric([self] { return self->EntryCount.Get(); }));
    localMetricsRegistry->Register(
        {CreateSensor("CapacityBytes")},
        CreateMetric([self] { return static_cast<i64>(self->CapacityBytes); }),
        EAggregationType::AT_MAX);
    localMetricsRegistry->Register(
        {CreateSensor("OverflowErrorCount")},
        CreateMetric([self] { return self->OverflowErrorCount.Get(); }),
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);
    localMetricsRegistry->Register(
        {CreateSensor("SerializationErrorCount")},
        CreateMetric([self] { return self->SerializationErrorCount.Get(); }),
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);
    localMetricsRegistry->Register(
        {CreateSensor("ParseErrorCount")},
        CreateMetric([self] { return self->ParseErrorCount.Get(); }),
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);
}

void THandleOpsQueueStats::UpdateStats(TInstant now)
{
    Y_UNUSED(now);
}

void THandleOpsQueueStats::SetEntryCount(ui64 entryCount)
{
    EntryCount.Set(static_cast<i64>(entryCount));
}

void THandleOpsQueueStats::IncrementOverflowErrorCount()
{
    OverflowErrorCount.Inc();
}

void THandleOpsQueueStats::IncrementSerializationErrorCount()
{
    SerializationErrorCount.Inc();
}

void THandleOpsQueueStats::IncrementParseErrorCount()
{
    ParseErrorCount.Inc();
}

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueueStatsPtr CreateHandleOpsQueueStats(ui64 capacityBytes)
{
    return std::make_shared<THandleOpsQueueStats>(capacityBytes);
}

}   // namespace NCloud::NFileStore::NFuse
