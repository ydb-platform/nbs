#include "node_state_holder.h"

#include "node_state_holder_stats.h"

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
        , NodeStateHolder(Timer, Stats)
        , Metrics(Stats->CreateMetrics())
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
        UNIT_ASSERT_VALUES_EQUAL(1, b.Metrics.DeletedNodes.Count->Get());
        UNIT_ASSERT_VALUES_EQUAL(1, b.Metrics.DeletedNodes.MaxCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(1, b.Metrics.Pins.ActiveCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(1, b.Metrics.Pins.ActiveMaxCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(0, b.Metrics.Pins.CompletedCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(0, b.Metrics.Pins.CompletedTime->Get());
        UNIT_ASSERT_VALUES_EQUAL(10'000, b.Metrics.Pins.MaxTime->Get());

        b.Timer->AdvanceTime(TDuration::MilliSeconds(5));
        b.NodeStateHolder.Unpin(pin);
        b.NodeStateHolder.UpdateStats();

        UNIT_ASSERT_VALUES_EQUAL(1, b.Metrics.Nodes.Count->Get());
        UNIT_ASSERT_VALUES_EQUAL(3, b.Metrics.Nodes.MaxCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(0, b.Metrics.DeletedNodes.Count->Get());
        UNIT_ASSERT_VALUES_EQUAL(1, b.Metrics.DeletedNodes.MaxCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(0, b.Metrics.Pins.ActiveCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(1, b.Metrics.Pins.ActiveMaxCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(1, b.Metrics.Pins.CompletedCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(15'000, b.Metrics.Pins.CompletedTime->Get());
        UNIT_ASSERT_VALUES_EQUAL(15'000, b.Metrics.Pins.MaxTime->Get());

        // Max value is calculated over a sliding window with 15 buckets
        for (int i = 0; i <= 15; i++) {
            b.NodeStateHolder.UpdateStats();
        }

        UNIT_ASSERT_VALUES_EQUAL(1, b.Metrics.Nodes.MaxCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(0, b.Metrics.DeletedNodes.MaxCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(0, b.Metrics.Pins.ActiveMaxCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(0, b.Metrics.Pins.MaxTime->Get());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
