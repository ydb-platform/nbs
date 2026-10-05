#include "latency.h"

#include <algorithm>

namespace NCloud::NBlockStore {

TMaybe<TDuration> ReplayLatencyGraph(
    const NProto::TLatencyDiagnostics& diagnostics, TDuration totalTime)
{
    if (!diagnostics.HasVersion() ||
        diagnostics.GetVersion() != LatencyVersion ||
        !diagnostics.HasComplete() || !diagnostics.GetComplete() ||
        !diagnostics.HasTotalUs() || !diagnostics.HasExclusion() ||
        diagnostics.NodesSize() == 0 ||
        diagnostics.NodesSize() > MaxLatencyNodes ||
        diagnostics.GetTotalUs() > totalTime.MicroSeconds())
    {
        return {};
    }

    TVector<ui64> observedEnds;
    TVector<ui64> adjustedEnds;
    observedEnds.reserve(diagnostics.NodesSize());
    adjustedEnds.reserve(diagnostics.NodesSize());
    ui64 observedFinish = 0;
    ui64 adjustedFinish = 0;
    size_t edges = 0;
    for (const auto& node: diagnostics.GetNodes()) {
        if (!node.HasStartUs() || !node.HasDurationUs() || !node.HasKind() ||
            (node.GetKind() != NProto::TLatencyDiagnostics::SERVICE &&
             node.GetKind() != NProto::TLatencyDiagnostics::QUOTA) ||
            (node.GetKind() == NProto::TLatencyDiagnostics::QUOTA &&
             (!node.HasQuotaReason() ||
              (node.GetQuotaReason() !=
                   NProto::TLatencyDiagnostics::PROFILE_IOPS &&
               node.GetQuotaReason() !=
                   NProto::TLatencyDiagnostics::PROFILE_BANDWIDTH &&
               node.GetQuotaReason() !=
                   NProto::TLatencyDiagnostics::PROFILE_BURST &&
               node.GetQuotaReason() !=
                   NProto::TLatencyDiagnostics::PROFILE_LIMIT))) ||
            node.GetStartUs() > diagnostics.GetTotalUs() ||
            node.GetDurationUs() > diagnostics.GetTotalUs() - node.GetStartUs())
        {
            return {};
        }
        ui64 observedReady = 0;
        ui64 adjustedReady = 0;
        edges += node.DependenciesSize();
        if (edges > MaxLatencyEdges) {
            return {};
        }
        for (const auto dependency: node.GetDependencies()) {
            if (dependency >= observedEnds.size()) {
                return {};   // cycle, forward edge or missing node
            }
            observedReady = std::max(observedReady, observedEnds[dependency]);
            adjustedReady = std::max(adjustedReady, adjustedEnds[dependency]);
        }
        if (observedReady > node.GetStartUs()) {
            return {};
        }
        const ui64 launchDelay = node.GetStartUs() - observedReady;
        const ui64 duration =
            node.GetKind() == NProto::TLatencyDiagnostics::QUOTA
                ? 0
                : node.GetDurationUs();
        // Every adjusted term is bounded by its observed counterpart.
        const ui64 adjustedEnd = adjustedReady + launchDelay + duration;
        const ui64 observedEnd = node.GetStartUs() + node.GetDurationUs();
        observedEnds.push_back(observedEnd);
        adjustedEnds.push_back(adjustedEnd);
        observedFinish = std::max(observedFinish, observedEnd);
        adjustedFinish = std::max(adjustedFinish, adjustedEnd);
    }
    // Keep the response tail and all time outside the producer's boundary.
    return TDuration::MicroSeconds(
        totalTime.MicroSeconds() - observedFinish + adjustedFinish);
}

namespace {
ui64 Micros(ui64 start, ui64 end)
{
    return end >= start ? CyclesToDurationSafe(end - start).MicroSeconds() : 0;
}

NProto::TLatencyDiagnostics NewGraph(ui64 started, ui64 finished, bool complete)
{
    NProto::TLatencyDiagnostics graph;
    graph.SetVersion(LatencyVersion);
    graph.SetComplete(complete && started && finished >= started);
    graph.SetTotalUs(Micros(started, finished));
    graph.SetExclusion(NProto::TLatencyDiagnostics::NONE);
    return graph;
}

ui32 AddNode(
    NProto::TLatencyDiagnostics& graph,
    ui64 start,
    ui64 duration,
    const TVector<ui32>& dependencies,
    NProto::TLatencyDiagnostics::EKind kind =
        NProto::TLatencyDiagnostics::SERVICE)
{
    const ui32 index = graph.NodesSize();
    auto* node = graph.AddNodes();
    node->SetStartUs(start);
    node->SetDurationUs(duration);
    node->SetKind(kind);
    for (ui32 dependency: dependencies) {
        node->AddDependencies(dependency);
    }
    return index;
}
}   // namespace

TLatencyOperation::TLatencyOperation(bool parallel, ui64 started)
    : Started(started)
    , Parallel(parallel)
{}

ui64 TLatencyOperation::GetStartedCycles() const
{
    return Started;
}

void TLatencyOperation::EndQuota(ui64 finished)
{
    std::lock_guard lock(Lock);
    for (auto& quota: Quota) {
        quota.Finished = std::min(quota.Finished, finished);
    }
}

void TLatencyOperation::Invalidate()
{
    std::lock_guard lock(Lock);
    Complete = false;
}

void TLatencyOperation::AddChild(ui64 started, ui64 finished,
                                 const NProto::TLatencyDiagnostics& graph)
{
    std::lock_guard lock(Lock);
    // Validate before copying untrusted wire data; total retained memory and
    // work are bounded across every child, not merely for each child alone.
    if (!Complete || started < Started || finished < started ||
        !ReplayLatencyGraph(graph,
                            TDuration::MicroSeconds(Micros(started, finished))))
    {
        Complete = false;
        return;
    }
    size_t edges = 0;
    for (const auto& node: graph.GetNodes()) {
        edges += node.DependenciesSize() + 1;
    }
    Nodes += graph.NodesSize() + 1;
    Edges += edges;
    if (Nodes + 2 > MaxLatencyNodes || Edges + Nodes > MaxLatencyEdges) {
        Complete = false;
        return;
    }
    Children.push_back({started, finished, graph});
}

void TLatencyOperation::AddQuota(
    ui64 started, ui64 finished,
    NProto::TLatencyDiagnostics::EQuotaReason reason)
{
    std::lock_guard lock(Lock);
    if (started < Started || finished < started ||
        Quota.size() >= MaxLatencyNodes / 2)
    {
        Complete = false;
        return;
    }
    if (finished > started) {
        Quota.push_back({started, finished, reason});
    }
}

NProto::TLatencyDiagnostics TLatencyOperation::FinishLeaf(
    ui64 finished, NProto::TLatencyDiagnostics::EExclusion exclusion) const
{
    std::lock_guard lock(Lock);
    auto graph = NewGraph(Started, finished, Complete && Children.empty());
    graph.SetExclusion(exclusion);
    ui64 cursor = 0;
    TVector<ui32> dependency;
    auto quotaIntervals = Quota;
    std::sort(
        quotaIntervals.begin(), quotaIntervals.end(),
        [](const auto& a, const auto& b) { return a.Started < b.Started; });
    TVector<TQuota> merged;
    for (auto quota: quotaIntervals) {
        quota.Finished = std::min(quota.Finished, finished);
        if (quota.Started >= quota.Finished) {
            continue;
        }
        if (!merged.empty() && quota.Started <= merged.back().Finished) {
            merged.back().Finished =
                std::max(merged.back().Finished, quota.Finished);
        } else {
            merged.push_back(quota);
        }
    }
    for (const auto& quota: merged) {
        const ui64 start = Micros(Started, quota.Started);
        const ui64 end = Micros(Started, quota.Finished);
        auto index = AddNode(graph, cursor, start - cursor, dependency);
        index = AddNode(graph, start, end - start, {index},
                        NProto::TLatencyDiagnostics::QUOTA);
        graph.MutableNodes(index)->SetQuotaReason(quota.Reason);
        dependency = {index};
        cursor = end;
    }
    AddNode(graph, cursor,
            graph.GetTotalUs() >= cursor ? graph.GetTotalUs() - cursor : 0,
            dependency);
    return graph;
}

NProto::TLatencyDiagnostics TLatencyOperation::Finish(ui64 finished) const
{
    std::lock_guard lock(Lock);
    auto graph = NewGraph(Started, finished,
                          Complete && !Children.empty() && Quota.empty());
    if (!graph.GetComplete()) {
        return graph;
    }
    auto children = Children;
    std::sort(
        children.begin(), children.end(),
        [](const auto& a, const auto& b) { return a.Started < b.Started; });
    ui32 previous = AddNode(graph, 0, 0, {});
    ui64 previousEnd = 0;
    TVector<ui32> sinks;
    for (const auto& child: children) {
        const ui64 start = Micros(Started, child.Started);
        const ui64 end = Micros(Started, child.Finished);
        if (child.Finished > finished || (!Parallel && start < previousEnd)) {
            graph.SetComplete(false);
            return graph;
        }
        const ui32 offset = graph.NodesSize();
        TVector<bool> terminal(child.Graph.NodesSize(), true);
        ui64 maxEnd = 0;
        for (const auto& node: child.Graph.GetNodes()) {
            auto* dst = graph.AddNodes();
            *dst = node;
            dst->SetStartUs(start + node.GetStartUs());
            dst->ClearDependencies();
            if (!node.DependenciesSize()) {
                dst->AddDependencies(Parallel ? 0 : previous);
            }
            for (auto dependency: node.GetDependencies()) {
                dst->AddDependencies(offset + dependency);
                terminal[dependency] = false;
            }
            maxEnd = std::max(maxEnd, node.GetStartUs() + node.GetDurationUs());
        }
        TVector<ui32> dependencies;
        for (ui32 i = 0; i < terminal.size(); ++i) {
            if (terminal[i]) {
                dependencies.push_back(offset + i);
            }
        }
        previous =
            AddNode(graph, start + maxEnd, end - start - maxEnd, dependencies);
        previousEnd = end;
        sinks.push_back(previous);
    }
    ui64 last = 0;
    for (ui32 sink: sinks) {
        const auto& node = graph.GetNodes(sink);
        last = std::max(last, node.GetStartUs() + node.GetDurationUs());
    }
    AddNode(graph, last, graph.GetTotalUs() - last,
            Parallel ? sinks : TVector<ui32>{previous});
    // Child exclusions do not establish the origin of the outer operation.
    // Only an explicit rejection at that operation's boundary can do that.
    return graph;
}

}   // namespace NCloud::NBlockStore
