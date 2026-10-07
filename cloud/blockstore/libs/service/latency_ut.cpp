#include "latency.h"

#include <library/cpp/testing/unittest/registar.h>
#include <thread>

namespace NCloud::NBlockStore {
namespace {
struct TClock
{
    const ui64 Start = GetCycleCount();
    const ui64 Tick = DurationToCyclesSafe(TDuration::MilliSeconds(1));
    ui64 At(ui64 ms) const { return Start + Tick * ms; }
    TDuration Total(ui64 ms) const { return CyclesToDurationSafe(At(ms) - Start); }
    NProto::TLatencyDiagnostics Leaf(ui64 from, ui64 to, ui64 quotaEnd = 0) const
    {
        TLatencyOperation op(false, At(from));
        if (quotaEnd) op.AddQuota(At(from), At(quotaEnd), NProto::TLatencyDiagnostics::PROFILE_LIMIT);
        return op.FinishLeaf(At(to));
    }
};
void CheckLatency(const NProto::TLatencyDiagnostics& graph, TDuration total, ui64 expectedUs)
{
    const auto actual = ReplayLatencyGraph(graph, total);
    UNIT_ASSERT(actual);
    const auto value = actual->MicroSeconds();
    UNIT_ASSERT_C(value + 4 >= expectedUs && value <= expectedUs + 4, value);
}
}
Y_UNIT_TEST_SUITE(TLatencyOperationTest)
{
    Y_UNIT_TEST(ShouldCompactQuotaFreeParallelWork)
    {
        TClock c;
        TLatencyOperation op(true, c.Start);
        op.AddChild(c.At(0), c.At(100), c.Leaf(0, 100));
        op.AddChild(c.At(0), c.At(70), c.Leaf(0, 70));
        const auto graph = op.Finish(c.At(100));
        UNIT_ASSERT_VALUES_EQUAL(graph.NodesSize(), 1);
        CheckLatency(graph, c.Total(100), c.Total(100).MicroSeconds());
    }
    Y_UNIT_TEST(ShouldSwitchCriticalPathAfterRemovingQuota)
    {
        TClock c;
        for (bool reversed: {false, true}) {
            TLatencyOperation op(true, c.Start);
            auto a = [&] { op.AddChild(c.At(0), c.At(100), c.Leaf(0, 100, 80)); };
            auto b = [&] { op.AddChild(c.At(0), c.At(70), c.Leaf(0, 70)); };
            if (reversed) { b(); a(); } else { a(); b(); }
            CheckLatency(op.Finish(c.At(100)), c.Total(100), c.Total(70).MicroSeconds());
        }
    }
    Y_UNIT_TEST(ShouldPreserveSequentialGapsAndTail)
    {
        TClock c;
        TLatencyOperation op(false, c.Start);
        // Collect in completion order different from launch order.
        op.AddChild(c.At(60), c.At(80), c.Leaf(60, 80));
        op.AddChild(c.At(10), c.At(50), c.Leaf(10, 50, 40));
        CheckLatency(op.Finish(c.At(100)), c.Total(100), c.Total(70).MicroSeconds());
    }
    Y_UNIT_TEST(ShouldPreserveParallelLaunchGapAndTail)
    {
        TClock c;
        TLatencyOperation op(true, c.Start);
        op.AddChild(c.At(10), c.At(50), c.Leaf(10, 50, 40));
        op.AddChild(c.At(0), c.At(45), c.Leaf(0, 45));
        CheckLatency(op.Finish(c.At(100)), c.Total(100), c.Total(95).MicroSeconds());
    }
    Y_UNIT_TEST(ShouldPreserveNestedQuotaGraphsAndRepeatedFinish)
    {
        TClock c;
        TLatencyOperation inner(true, c.Start);
        inner.AddChild(c.At(0), c.At(100), c.Leaf(0, 100, 80));
        inner.AddChild(c.At(0), c.At(70), c.Leaf(0, 70));
        TLatencyOperation outer(false, c.Start);
        outer.AddChild(c.At(0), c.At(100), inner.Finish(c.At(100)));
        const auto graph = outer.Finish(c.At(100));
        CheckLatency(graph, c.Total(100), c.Total(70).MicroSeconds());
        UNIT_ASSERT_VALUES_EQUAL(graph.SerializeAsString(), outer.Finish(c.At(100)).SerializeAsString());
    }
    Y_UNIT_TEST(ShouldRetainQuotaRecordsIndependentOfResponseLifetime)
    {
        TClock c;
        TLatencyOperation op(false, c.Start);
        auto graph = c.Leaf(0, 100, 80);
        op.AddChild(c.At(0), c.At(100), graph);
        graph.Clear();
        CheckLatency(op.Finish(c.At(100)), c.Total(100), c.Total(20).MicroSeconds());
    }
    Y_UNIT_TEST(ShouldMergeOverlappingQuotaIntervals)
    {
        TClock c;
        TLatencyOperation op(false, c.Start);
        op.AddQuota(c.At(20), c.At(50), NProto::TLatencyDiagnostics::PROFILE_LIMIT);
        op.AddQuota(c.At(40), c.At(80), NProto::TLatencyDiagnostics::PROFILE_LIMIT);
        CheckLatency(op.FinishLeaf(c.At(100)), c.Total(100), c.Total(40).MicroSeconds());
    }
    Y_UNIT_TEST(ShouldRejectMalformedQuotaFreeGraphBeforeCompacting)
    {
        TClock c;
        auto graph = c.Leaf(0, 100);
        graph.MutableNodes(0)->AddDependencies(0);
        TLatencyOperation op(true, c.Start);
        op.AddChild(c.At(0), c.At(100), graph);
        UNIT_ASSERT(!ReplayLatencyGraph(op.Finish(c.At(100)), c.Total(100)));
        graph.MutableNodes(0)->ClearDependencies();
        graph.MutableNodes(0)->SetDurationUs(graph.GetTotalUs() + 1);
        UNIT_ASSERT(!ReplayLatencyGraph(graph, c.Total(100)));
    }
    Y_UNIT_TEST(ShouldRejectUnconfirmedQuotaAndOverlappingSequentialChildren)
    {
        TClock c;
        auto graph = c.Leaf(0, 100, 80);
        graph.MutableNodes(1)->SetQuotaReason(NProto::TLatencyDiagnostics::UNCONFIRMED);
        UNIT_ASSERT(!ReplayLatencyGraph(graph, c.Total(100)));
        TLatencyOperation op(false, c.Start);
        op.AddChild(c.At(0), c.At(100), c.Leaf(0, 100));
        op.AddChild(c.At(50), c.At(70), c.Leaf(50, 70));
        UNIT_ASSERT(!ReplayLatencyGraph(op.Finish(c.At(100)), c.Total(100)));
    }
    Y_UNIT_TEST(ShouldEnforceAggregateNodeAndEdgeLimits)
    {
        TClock c;
        auto graph = c.Leaf(0, 0);
        for (size_t i = 1; i < MaxLatencyNodes; ++i) *graph.AddNodes() = graph.GetNodes(0);
        TLatencyOperation op(true, c.Start);
        op.AddChild(c.At(0), c.At(0), graph);
        UNIT_ASSERT(!ReplayLatencyGraph(op.Finish(c.At(0)), TDuration::Zero()));
        graph = c.Leaf(0, 0);
        *graph.AddNodes() = graph.GetNodes(0);
        for (size_t i = 0; i <= MaxLatencyEdges; ++i) graph.MutableNodes(1)->AddDependencies(0);
        UNIT_ASSERT(!ReplayLatencyGraph(graph, TDuration::Zero()));
    }
    Y_UNIT_TEST(ShouldCollectConcurrentCallbacks)
    {
        TClock c;
        TLatencyOperation op(true, c.Start);
        TVector<std::thread> threads;
        for (ui32 i = 0; i < 16; ++i) {
            threads.emplace_back([&, i] { op.AddChild(c.At(i), c.At(i + 1), c.Leaf(i, i + 1)); });
        }
        for (auto& t: threads) t.join();
        CheckLatency(op.Finish(c.At(16)), c.Total(16), c.Total(16).MicroSeconds());
    }
}
} // namespace NCloud::NBlockStore
