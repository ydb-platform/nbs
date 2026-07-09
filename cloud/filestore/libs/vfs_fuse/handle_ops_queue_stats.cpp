#include "handle_ops_queue_stats.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

namespace NCloud::NFileStore::NFuse {

using namespace NMetrics;

////////////////////////////////////////////////////////////////////////////////

THandleOpsQueueStats::THandleOpsQueueStats(ui64 maxSize)
    : MaxSize(maxSize)
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
        {CreateSensor("Size")},
        CreateMetric([self] { return self->Size.Get(); }));
    localMetricsRegistry->Register(
        {CreateSensor("MaxSize")},
        CreateMetric([self] { return static_cast<i64>(self->MaxSize); }),
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

void THandleOpsQueueStats::SetSize(ui64 size)
{
    Size.Set(static_cast<i64>(size));
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

THandleOpsQueueStatsPtr CreateHandleOpsQueueStats(ui64 maxSize)
{
    return std::make_shared<THandleOpsQueueStats>(maxSize);
}

}   // namespace NCloud::NFileStore::NFuse
