#include "node_state_holder.h"

#include "write_back_cache_stats.h"

#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    std::shared_ptr<TTestTimer> Timer;
    INodeStateHolderStatsPtr Stats;
    TNodeStateHolder NodeStateHolder;
    TNodeStateHolderMetrics Metrics;

    TBootstrap()
        : Timer(std::make_shared<TTestTimer>())
        , Stats(CreateNodeStateHolderStats())
        , NodeStateHolder(Stats)
        , Metrics(Stats->CreateNodeStateHolderMetrics())
    {}
};

}   // namespace

Y_UNIT_TEST_SUITE(TNodeStateHolderTest)
{
    Y_UNIT_TEST(ReportStats)
    {
        TBootstrap b;

        b.NodeStateHolder.GetOrCreateNodeState(1);
        b.NodeStateHolder.GetOrCreateNodeState(2);
        b.NodeStateHolder.GetOrCreateNodeState(3);
        b.NodeStateHolder.DeleteNodeState(2);
        auto pin = b.NodeStateHolder.Pin();
        b.NodeStateHolder.DeleteNodeState(1);

        b.Timer->AdvanceTime(TDuration::MilliSeconds(10));
        b.NodeStateHolder.UpdateStats();

        UNIT_ASSERT_VALUES_EQUAL(2, b.Metrics.Nodes.Count->Get());
        UNIT_ASSERT_VALUES_EQUAL(3, b.Metrics.Nodes.MaxCount->Get());

        b.Timer->AdvanceTime(TDuration::MilliSeconds(5));
        b.NodeStateHolder.Unpin(pin);
        b.NodeStateHolder.UpdateStats();

        UNIT_ASSERT_VALUES_EQUAL(1, b.Metrics.Nodes.Count->Get());
        UNIT_ASSERT_VALUES_EQUAL(3, b.Metrics.Nodes.MaxCount->Get());

        // Max value is calculated over a sliding window with 15 buckets
        for (int i = 0; i <= 15; i++) {
            b.NodeStateHolder.UpdateStats();
        }

        UNIT_ASSERT_VALUES_EQUAL(1, b.Metrics.Nodes.MaxCount->Get());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
