#include "node_state_holder_stats.h"

#include "relaxed_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNodeStateHolderStats
    : public std::enable_shared_from_this<TNodeStateHolderStats>
    , public INodeStateHolderStats
{
private:
    TRelaxedCombinedMaxCounter<> NodeCounter;
    TRelaxedCombinedMaxCounter<> DeletedNodeCounter;
    TRelaxedEventCounterWithTimeStats<> PinCounter;

public:
    void IncrementNodeCount() override
    {
        NodeCounter.Inc();
    }

    void DecrementNodeCount() override
    {
        NodeCounter.Dec();
    }

    void IncrementDeletedNodeCount() override
    {
        DeletedNodeCounter.Inc();
    }

    void DecrementDeletedNodeCount() override
    {
        DeletedNodeCounter.Dec();
    }

    void PinCreated() override
    {
        PinCounter.Started();
    }

    void PinReleased(TDuration holdDuration) override
    {
        PinCounter.Completed(holdDuration);
    }

    TNodeStateHolderMetrics CreateMetrics() const override
    {
        auto self = shared_from_this();

        return {
            .Nodes =
                {.Count = CreateMetric(
                     [self] { return self->NodeCounter.GetCurrent(); }),
                 .MaxCount = CreateMetric(
                     [self] { return self->NodeCounter.GetMax(); })},
            .DeletedNodes =
                {.Count = CreateMetric(
                     [self] { return self->DeletedNodeCounter.GetCurrent(); }),
                 .MaxCount = CreateMetric(
                     [self] { return self->DeletedNodeCounter.GetMax(); })},
            .Pins =
                {.ActiveCount = NMetrics::CreateMetric(
                     [self] { return self->PinCounter.GetActiveCount(); }),
                 .ActiveMaxCount = NMetrics::CreateMetric(
                     [self] { return self->PinCounter.GetActiveMaxCount(); }),
                 .CompletedCount = NMetrics::CreateMetric(
                     [self] { return self->PinCounter.GetCompletedCount(); }),
                 .CompletedTime = NMetrics::CreateMetric(
                     [self] { return self->PinCounter.GetCompletedTime(); }),
                 .MaxTime = NMetrics::CreateMetric(
                     [self] { return self->PinCounter.GetMaxTime(); })},
        };
    }

    void UpdateStats(TDuration maxActivePinDuration) override
    {
        NodeCounter.Update();
        DeletedNodeCounter.Update();
        PinCounter.Update(maxActivePinDuration);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNodeStateHolderMetrics::Register(
    NMetrics::IMetricsRegistry& localMetricsRegistry,
    NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const
{
    localMetricsRegistry.Register(
        {CreateSensor("Nodes_Count")},
        Nodes.Count,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Nodes_MaxCount")},
        Nodes.MaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("DeletedNodes_Count")},
        DeletedNodes.Count,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("DeletedNodes_MaxCount")},
        DeletedNodes.MaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    // Pins

    localMetricsRegistry.Register(
        {CreateSensor("Pins_ActiveCount")},
        Pins.ActiveCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Pins_ActiveMaxCount")},
        Pins.ActiveMaxCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_ABSOLUTE);

    localMetricsRegistry.Register(
        {CreateSensor("Pins_CompletedCount")},
        Pins.CompletedCount,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    localMetricsRegistry.Register(
        {CreateSensor("Pins_CompletedTime")},
        Pins.CompletedTime,
        EAggregationType::AT_SUM,
        EMetricType::MT_DERIVATIVE);

    aggregatableMetricsRegistry.Register(
        {CreateSensor("Pins_MaxTime")},
        Pins.MaxTime,
        EAggregationType::AT_MAX,
        EMetricType::MT_ABSOLUTE);
}

////////////////////////////////////////////////////////////////////////////////

INodeStateHolderStatsPtr CreateNodeStateHolderStats()
{
    return std::make_shared<TNodeStateHolderStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
