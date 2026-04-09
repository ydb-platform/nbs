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

public:
    void IncrementNodeCount() override
    {
        NodeCounter.Inc();
    }

    void DecrementNodeCount() override
    {
        NodeCounter.Dec();
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
        };
    }

    void UpdateStats() override
    {
        NodeCounter.Update();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNodeStateHolderMetrics::Register(
    NMetrics::IMetricsRegistry& localMetricsRegistry,
    NMetrics::IMetricsRegistry& aggregatableMetricsRegistry) const
{
    Y_UNUSED(aggregatableMetricsRegistry);

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
}

////////////////////////////////////////////////////////////////////////////////

INodeStateHolderStatsPtr CreateNodeStateHolderStats()
{
    return std::make_shared<TNodeStateHolderStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
