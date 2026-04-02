#include "node_state_holder_stats.h"

#include "relaxed_counters.h"

#include <cloud/filestore/libs/diagnostics/metrics/metric.h>

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

    TNodeStateHolderMetrics CreateNodeStateHolderMetrics() const override
    {
        auto self = shared_from_this();

        return {
            .Nodes =
                {.Count = NMetrics::CreateMetric(
                     [self] { return self->NodeCounter.GetCurrent(); }),
                 .MaxCount = NMetrics::CreateMetric(
                     [self] { return self->NodeCounter.GetMax(); })},
        };
    }

    void UpdateNodeStateHolderStats() override
    {
        NodeCounter.Update();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

INodeStateHolderStatsPtr CreateNodeStateHolderStats()
{
    return std::make_shared<TNodeStateHolderStats>();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
