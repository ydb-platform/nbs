#include "latency.h"

#include <cloud/blockstore/libs/diagnostics/latency_sli.h>
#include <algorithm>

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
    const auto actual = ReadLatencySummary(graph, total);
    UNIT_ASSERT(actual);
    const auto value = actual->MicroSeconds();
    UNIT_ASSERT_C(value + 4 >= expectedUs && value <= expectedUs + 4, value);
}
}
namespace {
// Independent test oracle: retain and replay the old observed dependency DAG.
// Production code has neither these nodes nor a replay pass.
struct TReferenceNode
{
    ui64 Start;
    ui64 Duration;
    bool Quota;
    TVector<ui32> Dependencies;
};
struct TReferenceGraph
{
    ui64 Total = 0;
    TVector<TReferenceNode> Nodes;
};
struct TScenario
{
    ui64 Started;
    ui64 Finished;
    NProto::TLatencyDiagnostics Summary;
    TReferenceGraph Graph;
};
ui64 Us(ui64 a, ui64 b)
{
    return CyclesToDurationSafe(b - a).MicroSeconds();
}
ui64 ReplayReference(const TReferenceGraph& graph)
{
    TVector<ui64> observed, adjusted;
    ui64 observedFinish = 0, adjustedFinish = 0;
    for (const auto& node: graph.Nodes) {
        ui64 observedReady = 0, adjustedReady = 0;
        for (ui32 d: node.Dependencies) {
            observedReady = std::max(observedReady, observed.at(d));
            adjustedReady = std::max(adjustedReady, adjusted.at(d));
        }
        UNIT_ASSERT(node.Start >= observedReady);
        observed.push_back(node.Start + node.Duration);
        adjusted.push_back(adjustedReady + node.Start - observedReady +
                           (node.Quota ? 0 : node.Duration));
        observedFinish = std::max(observedFinish, observed.back());
        adjustedFinish = std::max(adjustedFinish, adjusted.back());
    }
    UNIT_ASSERT(observedFinish <= graph.Total);
    return graph.Total - observedFinish + adjustedFinish;
}
TReferenceGraph JoinReference(
    ui64 started, ui64 finished, bool parallel, TVector<TScenario> children)
{
    std::sort(children.begin(), children.end(), [](const auto& a, const auto& b) {
        return a.Started < b.Started;
    });
    TReferenceGraph graph{Us(started, finished), {{0, 0, false, {}}}};
    ui32 previous = 0;
    TVector<ui32> sinks;
    ui64 last = 0;
    for (const auto& child: children) {
        const ui64 start = Us(started, child.Started);
        const ui64 end = Us(started, child.Finished);
        const ui32 offset = graph.Nodes.size();
        TVector<bool> terminal(child.Graph.Nodes.size(), true);
        ui64 childEnd = 0;
        for (const auto& node: child.Graph.Nodes) {
            TReferenceNode dst{start + node.Start, node.Duration, node.Quota, {}};
            childEnd = std::max(childEnd, node.Start + node.Duration);
            if (node.Dependencies.empty()) dst.Dependencies.push_back(parallel ? 0 : previous);
            for (ui32 d: node.Dependencies) {
                dst.Dependencies.push_back(offset + d);
                terminal[d] = false;
            }
            graph.Nodes.push_back(std::move(dst));
        }
        TReferenceNode tail{start + childEnd, end - start - childEnd, false, {}};
        for (ui32 i = 0; i < terminal.size(); ++i) {
            if (terminal[i]) tail.Dependencies.push_back(offset + i);
        }
        previous = graph.Nodes.size();
        graph.Nodes.push_back(std::move(tail));
        sinks.push_back(previous);
        last = std::max(last, end);
    }
    graph.Nodes.push_back({last, graph.Total - last, false,
                          parallel ? sinks : TVector<ui32>{previous}});
    return graph;
}
TScenario GenerateScenario(ui32& seed, ui64 started, ui64 tick, ui32 depth)
{
    auto random = [&] { seed = seed * 1664525u + 1013904223u; return seed; };
    if (!depth) {
        const ui64 finished = started + (10 + random() % 40) * tick;
        const ui64 quotaStart = started + (random() % 5) * tick;
        const ui64 quotaEnd = quotaStart + (random() % 5) * tick;
        TLatencyOperation op(false, started);
        op.AddQuota(quotaStart, quotaEnd, NProto::TLatencyDiagnostics::PROFILE_LIMIT);
        TReferenceGraph graph{Us(started, finished), {
            {0, Us(started, quotaStart), false, {}},
            {Us(started, quotaStart), Us(started, quotaEnd) - Us(started, quotaStart), true, {0}},
            {Us(started, quotaEnd), Us(started, finished) - Us(started, quotaEnd), false, {1}}
        }};
        return {started, finished, op.FinishLeaf(finished), std::move(graph)};
    }
    const bool parallel = random() & 0x100;
    const ui32 count = 2 + random() % 3;
    TLatencyOperation op(parallel, started);
    TVector<TScenario> children;
    ui64 last = started;
    for (ui32 i = 0; i < count; ++i) {
        const ui64 childStart = (parallel ? started : last) + (1 + random() % 5) * tick / 3;
        auto child = GenerateScenario(seed, childStart, tick, depth - 1);
        // Observer time outside the child's producer boundary stays service time.
        child.Finished += (random() % 3) * tick / 3;
        last = std::max(last, child.Finished);
        children.push_back(std::move(child));
    }
    for (auto it = children.rbegin(); it != children.rend(); ++it) {
        op.AddChild(it->Started, it->Finished, it->Summary);
    }
    const ui64 finished = last + (random() % 5) * tick / 3;
    auto graph = JoinReference(started, finished, parallel, children);
    auto summary = op.Finish(finished);
    const auto actual = ReadLatencySummary(summary, TDuration::MicroSeconds(graph.Total));
    UNIT_ASSERT(actual);
    UNIT_ASSERT_VALUES_EQUAL(actual->MicroSeconds(), ReplayReference(graph));
    return {started, finished, std::move(summary), std::move(graph)};
}
} // namespace
Y_UNIT_TEST_SUITE(TLatencyOperationTest)
{
    Y_UNIT_TEST(ShouldSummarizeQuotaFreeParallelWork)
    {
        TClock c;
        TLatencyOperation op(true, c.Start);
        op.AddChild(c.At(0), c.At(100), c.Leaf(0, 100));
        op.AddChild(c.At(0), c.At(70), c.Leaf(0, 70));
        const auto graph = op.Finish(c.At(100));
        UNIT_ASSERT_VALUES_EQUAL(graph.GetAdjustedUs(), graph.GetTotalUs());
        UNIT_ASSERT(graph.ByteSizeLong() < 32);
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
    Y_UNIT_TEST(ShouldPreserveNestedQuotaScopesAndRepeatedFinish)
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
    Y_UNIT_TEST(ShouldRejectMissingIncompatibleAndInconsistentSummaries)
    {
        TClock c;
        const auto valid = c.Leaf(0, 100, 80);
        for (ui32 error = 0; error != 10; ++error) {
            auto summary = valid;
            switch (error) {
                case 0: summary.ClearVersion(); break;
                case 1: summary.SetVersion(1); break;
                case 2: summary.SetVersion(LatencyVersion + 1); break;
                case 3: summary.ClearComplete(); break;
                case 4: summary.SetComplete(false); break;
                case 5: summary.ClearTotalUs(); break;
                case 6: summary.ClearAdjustedUs(); break;
                case 7: summary.ClearExclusion(); break;
                case 8: summary.SetAdjustedUs(summary.GetTotalUs() + 1); break;
                case 9: summary.SetTotalUs(c.Total(101).MicroSeconds()); break;
            }
            UNIT_ASSERT(!ReadLatencySummary(summary, c.Total(100)));
            TLatencyOperation parent(true, c.Start);
            parent.AddChild(c.At(0), c.At(100), summary);
            UNIT_ASSERT(!ReadLatencySummary(parent.Finish(c.At(100)), c.Total(100)));
        }
        // An actual version-1 protobuf with a SERVICE node in reserved field 4.
        NProto::TLatencyDiagnostics legacy;
        UNIT_ASSERT(legacy.ParseFromString(TString(
            "\x08\x01\x10\x01\x18\x64\x22\x06\x08\x00\x10\x64\x18\x00\x28\x00", 16)));
        UNIT_ASSERT(!ReadLatencySummary(legacy, TDuration::MicroSeconds(100)));
        NProto::TLatencyDiagnostics roundTrip;
        UNIT_ASSERT(roundTrip.ParseFromString(valid.SerializeAsString()));
        UNIT_ASSERT_VALUES_EQUAL(roundTrip.GetAdjustedUs(), valid.GetAdjustedUs());
        CheckLatency(roundTrip, c.Total(100), c.Total(20).MicroSeconds());
    }
    Y_UNIT_TEST(ShouldRejectUnconfirmedQuotaAndOverlappingSequentialChildren)
    {
        TClock c;
        TLatencyOperation leaf(false, c.Start);
        leaf.AddQuota(c.At(0), c.At(80), NProto::TLatencyDiagnostics::UNCONFIRMED);
        UNIT_ASSERT(!ReadLatencySummary(leaf.FinishLeaf(c.At(100)), c.Total(100)));
        // An overlapping confirmed wait must not mask an unconfirmed cause.
        leaf.AddQuota(c.At(0), c.At(90), NProto::TLatencyDiagnostics::PROFILE_LIMIT);
        UNIT_ASSERT(!ReadLatencySummary(leaf.FinishLeaf(c.At(100)), c.Total(100)));
        TLatencyOperation op(false, c.Start);
        op.AddChild(c.At(0), c.At(100), c.Leaf(0, 100));
        op.AddChild(c.At(50), c.At(70), c.Leaf(50, 70));
        UNIT_ASSERT(!ReadLatencySummary(op.Finish(c.At(100)), c.Total(100)));
    }
    Y_UNIT_TEST(ShouldBoundRetainedIntervalsWithoutLimitingParallelFanout)
    {
        TClock c;
        const auto zero = c.Leaf(0, 0);
        TLatencyOperation parallel(true, c.Start);
        TLatencyOperation sequential(false, c.Start);
        for (size_t i = 0; i <= MaxLatencyChildren; ++i) {
            parallel.AddChild(c.Start, c.Start, zero);
            sequential.AddChild(c.Start, c.Start, zero);
        }
        CheckLatency(parallel.Finish(c.Start), TDuration::Zero(), 0);
        UNIT_ASSERT(!ReadLatencySummary(sequential.Finish(c.Start), TDuration::Zero()));
        TLatencyOperation leaf(false, c.Start);
        for (size_t i = 0; i <= MaxLatencyQuotaIntervals; ++i) {
            leaf.AddQuota(c.At(0), c.At(1), NProto::TLatencyDiagnostics::PROFILE_LIMIT);
        }
        UNIT_ASSERT(!ReadLatencySummary(leaf.FinishLeaf(c.At(100)), c.Total(100)));
    }
    Y_UNIT_TEST(ShouldClipSortAndMergeQuotaWaitsAtCompletion)
    {
        TClock c;
        TLatencyOperation leaf(false, c.Start);
        leaf.AddQuota(c.At(40), c.At(110), NProto::TLatencyDiagnostics::PROFILE_BANDWIDTH);
        leaf.AddQuota(c.At(10), c.At(30), NProto::TLatencyDiagnostics::PROFILE_IOPS);
        leaf.AddQuota(c.At(20), c.At(50), NProto::TLatencyDiagnostics::PROFILE_BURST);
        leaf.EndQuota(c.At(80));
        CheckLatency(leaf.FinishLeaf(c.At(100)), c.Total(100), c.Total(30).MicroSeconds());
    }
    Y_UNIT_TEST(ShouldKeepObserverTimeAndValidateParentBounds)
    {
        TClock c;
        CheckLatency(c.Leaf(0, 100, 80), c.Total(130), c.Total(50).MicroSeconds());
        TLatencyOperation parent(true, c.Start);
        parent.AddChild(c.At(0), c.At(100), c.Leaf(0, 100));
        UNIT_ASSERT(!ReadLatencySummary(parent.Finish(c.At(99)), c.Total(100)));
        TLatencyOperation empty(true, c.Start);
        UNIT_ASSERT(!ReadLatencySummary(empty.Finish(c.At(100)), c.Total(100)));
        TLatencyOperation mixed(true, c.Start);
        mixed.AddChild(c.At(0), c.At(100), c.Leaf(0, 100));
        mixed.AddQuota(c.At(0), c.At(10), NProto::TLatencyDiagnostics::PROFILE_LIMIT);
        UNIT_ASSERT(!ReadLatencySummary(mixed.Finish(c.At(100)), c.Total(100)));
        TLatencyOperation invalid(false, c.Start);
        invalid.Invalidate();
        UNIT_ASSERT(!ReadLatencySummary(invalid.FinishLeaf(c.At(100)), c.Total(100)));
    }
    Y_UNIT_TEST(ShouldPreserveGoodBadUnknownAndExclusionClassification)
    {
        TClock c;
        NProto::TDiagnosticsConfig config;
        config.SetLatencyThresholdVersion(1);
        auto* row = config.AddLatencyThresholds();
        row->SetMediaKind(1);
        row->SetWrite(false);
        row->SetStartBytes(0);
        row->SetEndBytes(65536);
        row->SetThresholdUs(75000);
        const TLatencyThresholds thresholds(config);
        auto summary = c.Leaf(0, 100, 80);
        const auto evaluate = [&](bool success) {
            return EvaluateLatency(thresholds, 1, EBlockStoreRequest::ReadBlocks,
                4096, c.Total(100), &summary, success);
        };
        UNIT_ASSERT_VALUES_EQUAL(evaluate(true).Good, 1);
        UNIT_ASSERT_VALUES_EQUAL(evaluate(false).Bad, 1);
        summary = c.Leaf(0, 100);
        UNIT_ASSERT_VALUES_EQUAL(evaluate(true).Bad, 1);
        summary.ClearAdjustedUs();
        UNIT_ASSERT_VALUES_EQUAL(evaluate(true).Unknown, 1);
        summary = c.Leaf(0, 100);
        summary.SetExclusion(NProto::TLatencyDiagnostics::CLIENT_LIMIT);
        UNIT_ASSERT_VALUES_EQUAL(evaluate(false).ClientLimit, 1);
        UNIT_ASSERT_VALUES_EQUAL(evaluate(true).Unknown, 1);
        summary.SetExclusion(NProto::TLatencyDiagnostics::CLIENT_CANCELLATION);
        UNIT_ASSERT_VALUES_EQUAL(evaluate(false).ClientCancellation, 1);
        summary.SetExclusion(NProto::TLatencyDiagnostics::INVALID_CLIENT_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(evaluate(false).InvalidClientRequest, 1);
    }
    Y_UNIT_TEST(ShouldMatchGraphReplayForNestedRandomizedScopes)
    {
        TClock clock;
        ui32 seed = 7885;
        for (ui32 i = 0; i < 300; ++i) {
            GenerateScenario(seed, clock.Start, clock.Tick, 3);
        }
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
