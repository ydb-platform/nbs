#include "latency.h"

#include <algorithm>

namespace NCloud::NBlockStore {

namespace {
struct TGraphInfo
{
    bool HasQuota = false;
    size_t Edges = 0;
    ui64 MaxEnd = 0;
};

// Validate observed timestamps and topology without allocating replay buffers.
// A dependency always refers to an earlier, already validated node.
TMaybe<TGraphInfo> ValidateGraph(
    const NProto::TLatencyDiagnostics& graph, TDuration totalTime)
{
    if (!graph.HasVersion() || graph.GetVersion() != LatencyVersion ||
        !graph.HasComplete() || !graph.GetComplete() ||
        !graph.HasTotalUs() || !graph.HasExclusion() ||
        !graph.NodesSize() || graph.NodesSize() > MaxLatencyNodes ||
        graph.GetTotalUs() > totalTime.MicroSeconds())
    {
        return {};
    }
    TGraphInfo info;
    ui32 index = 0;
    for (const auto& node: graph.GetNodes()) {
        if (!node.HasStartUs() || !node.HasDurationUs() || !node.HasKind() ||
            (node.GetKind() != NProto::TLatencyDiagnostics::SERVICE &&
             node.GetKind() != NProto::TLatencyDiagnostics::QUOTA) ||
            (node.GetKind() == NProto::TLatencyDiagnostics::QUOTA &&
             (!node.HasQuotaReason() ||
              (node.GetQuotaReason() != NProto::TLatencyDiagnostics::PROFILE_IOPS &&
               node.GetQuotaReason() != NProto::TLatencyDiagnostics::PROFILE_BANDWIDTH &&
               node.GetQuotaReason() != NProto::TLatencyDiagnostics::PROFILE_BURST &&
               node.GetQuotaReason() != NProto::TLatencyDiagnostics::PROFILE_LIMIT))) ||
            node.GetStartUs() > graph.GetTotalUs() ||
            node.GetDurationUs() > graph.GetTotalUs() - node.GetStartUs())
        {
            return {};
        }
        info.Edges += node.DependenciesSize();
        if (info.Edges > MaxLatencyEdges) {
            return {};
        }
        for (const auto dependency: node.GetDependencies()) {
            if (dependency >= index) {
                return {};
            }
            const auto& preceding = graph.GetNodes(dependency);
            if (preceding.GetStartUs() + preceding.GetDurationUs() > node.GetStartUs()) {
                return {};
            }
        }
        info.HasQuota |= node.GetKind() == NProto::TLatencyDiagnostics::QUOTA;
        info.MaxEnd = std::max(info.MaxEnd, node.GetStartUs() + node.GetDurationUs());
        ++index;
    }
    return info;
}
}   // namespace

TMaybe<TDuration> ReplayLatencyGraph(
    const NProto::TLatencyDiagnostics& diagnostics, TDuration totalTime)
{
    const auto info = ValidateGraph(diagnostics, totalTime);
    if (!info) {
        return {};
    }
    // Every path consists entirely of SERVICE time. Removing quota changes
    // nothing, including launch gaps, parallel joins and the response tail.
    if (!info->HasQuota) {
        return totalTime;
    }
    TVector<ui64> adjustedEnds;
    adjustedEnds.reserve(diagnostics.NodesSize());
    ui64 adjustedFinish = 0;
    for (const auto& node: diagnostics.GetNodes()) {
        ui64 observedReady = 0;
        ui64 adjustedReady = 0;
        for (const auto dependency: node.GetDependencies()) {
            const auto& preceding = diagnostics.GetNodes(dependency);
            observedReady = std::max(observedReady,
                preceding.GetStartUs() + preceding.GetDurationUs());
            adjustedReady = std::max(adjustedReady, adjustedEnds[dependency]);
        }
        const ui64 duration = node.GetKind() == NProto::TLatencyDiagnostics::QUOTA
            ? 0 : node.GetDurationUs();
        const ui64 adjustedEnd = adjustedReady +
            (node.GetStartUs() - observedReady) + duration;
        adjustedEnds.push_back(adjustedEnd);
        adjustedFinish = std::max(adjustedFinish, adjustedEnd);
    }
    return TDuration::MicroSeconds(
        totalTime.MicroSeconds() - info->MaxEnd + adjustedFinish);
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
ui32 AddNode(
    NProto::TLatencyDiagnostics& graph, ui64 start, ui64 duration,
    ui32 dependency, NProto::TLatencyDiagnostics::EKind kind =
        NProto::TLatencyDiagnostics::SERVICE)
{
    const ui32 index = AddNode(graph, start, duration, TVector<ui32>{}, kind);
    graph.MutableNodes(index)->AddDependencies(dependency);
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
    if (!Complete || started < Started || finished < started) {
        Complete = false;
        return;
    }
    const auto info = ValidateGraph(
        graph, TDuration::MicroSeconds(Micros(started, finished)));
    if (!info) {
        Complete = false;
        return;
    }
    // Keep the existing aggregate limits, including root and tail edges,
    // before retaining any untrusted data or compacting quota-free children.
    Nodes += graph.NodesSize() + 1;
    Edges += info->Edges + graph.NodesSize();
    if (Nodes + 2 > MaxLatencyNodes || Edges + Nodes > MaxLatencyEdges) {
        Complete = false;
        return;
    }
    const ui32 first = ChildNodes.size();
    if (info->HasQuota) {
        for (const auto& node: graph.GetNodes()) {
            ChildNodes.push_back({node.GetStartUs(), node.GetDurationUs(),
                static_cast<ui32>(ChildDependencies.size()),
                static_cast<ui32>(node.DependenciesSize()), node.GetKind(),
                node.GetQuotaReason(), node.HasQuotaReason()});
            for (ui32 dependency: node.GetDependencies()) {
                ChildDependencies.push_back(dependency);
                ChildNodes[first + dependency].Terminal = false;
            }
        }
    }
    ChildrenOrdered &= Children.empty() || started >= Children.back().Started;
    Children.push_back({started, finished, info->MaxEnd, first,
        static_cast<ui32>(ChildNodes.size() - first)});
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
        index = AddNode(graph, start, end - start, index,
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
    // Sort only indices when callbacks arrived out of launch order. Never
    // copy a child graph, and avoid even this allocation for ordered children.
    TVector<ui32> order;
    if (!ChildrenOrdered && (!Parallel || !ChildNodes.empty())) {
        order.reserve(Children.size());
        for (ui32 i = 0; i < Children.size(); ++i) order.push_back(i);
        std::sort(order.begin(), order.end(), [&](ui32 a, ui32 b) {
            return Children[a].Started < Children[b].Started;
        });
    }
    ui64 previousEnd = 0;
    for (size_t i = 0; i < Children.size(); ++i) {
        const auto& child = Children[order.empty() ? i : order[i]];
        const ui64 start = Micros(Started, child.Started);
        const ui64 end = Micros(Started, child.Finished);
        if (child.Finished > finished || (!Parallel && start < previousEnd) ||
            child.MaxEnd > end - start)
        {
            graph.SetComplete(false);
            return graph;
        }
        previousEnd = end;
    }
    if (ChildNodes.empty()) {
        // A validated quota-free scope is equivalent to one SERVICE span at
        // every enclosing boundary. There is no quota to shift its critical path.
        AddNode(graph, 0, graph.GetTotalUs(), TVector<ui32>{});
        return graph;
    }
    graph.MutableNodes()->Reserve(ChildNodes.size() + Children.size() + 2);
    ui32 previous = AddNode(graph, 0, 0, TVector<ui32>{});
    TVector<ui32> sinks;
    sinks.reserve(Children.size());
    for (size_t i = 0; i < Children.size(); ++i) {
        const auto& child = Children[order.empty() ? i : order[i]];
        const ui64 start = Micros(Started, child.Started);
        const ui64 end = Micros(Started, child.Finished);
        if (!child.NodeCount) {
            previous = AddNode(graph, start, end - start, Parallel ? 0 : previous);
        } else {
            const ui32 offset = graph.NodesSize();
            TVector<ui32> dependencies;
            for (ui32 n = 0; n < child.NodeCount; ++n) {
                const auto& node = ChildNodes[child.FirstNode + n];
                auto* dst = graph.AddNodes();
                dst->SetStartUs(start + node.StartUs);
                dst->SetDurationUs(node.DurationUs);
                dst->SetKind(node.Kind);
                if (node.HasQuotaReason) dst->SetQuotaReason(node.QuotaReason);
                if (!node.DependencyCount) dst->AddDependencies(Parallel ? 0 : previous);
                for (ui32 d = 0; d < node.DependencyCount; ++d) {
                    dst->AddDependencies(offset + ChildDependencies[node.FirstDependency + d]);
                }
                if (node.Terminal) dependencies.push_back(offset + n);
            }
            previous = AddNode(graph, start + child.MaxEnd,
                               end - start - child.MaxEnd, dependencies);
        }
        sinks.push_back(previous);
    }
    ui64 last = 0;
    for (ui32 sink: sinks) {
        const auto& node = graph.GetNodes(sink);
        last = std::max(last, node.GetStartUs() + node.GetDurationUs());
    }
    if (Parallel) {
        AddNode(graph, last, graph.GetTotalUs() - last, sinks);
    } else {
        AddNode(graph, last, graph.GetTotalUs() - last, previous);
    }
    // Child exclusions do not establish the origin of the outer operation.
    return graph;
}

}   // namespace NCloud::NBlockStore
