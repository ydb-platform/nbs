#include "request_timing_graph_builder.h"

#include <cloud/storage/core/protos/request_timing.pb.h>

#include <algorithm>

namespace NCloud {

TRequestTimingGraphBuilder::TRequestTimingGraphBuilder(ui64 requestId)
    : RequestId(requestId)
{
    Spans.emplace_back();
}

ui32 TRequestTimingGraphBuilder::Seal(ui32 span, ui64 now, bool continueSpan)
{
    // Live callers hold Mutex and check Frozen; the one-time finalizer also
    // seals the root after Freeze, when live mutations are already disabled.
    if (span >= Spans.size() || Spans[span].Closed) {
        return InvalidId;
    }
    auto& s = Spans[span];
    if (s.Suspended || now < s.Current.Begin || ++Events > MaxEvents) {
        Failure = "missing_join_or_event_limit";
        return InvalidId;
    }
    s.Current.End = now;
    const ui32 id = Stages.size();
    for (auto& wait: s.Current.Waits) {
        if (wait.End == std::numeric_limits<ui64>::max()) {
            wait.End = now;
        }
    }
    Stages.push_back(std::move(s.Current));
    StageParts.push_back(span);
    DiagnosticOnly.push_back(false);
    s.Current = {};
    s.Current.Begin = now;
    // A closed or frozen part never resumes. Its completed stage already
    // owns every observation, so it needs no allocated continuation edge.
    if (continueSpan) {
        s.Current.Dependencies.push_back({id, 0});
    }
    s.Last = id;
    return id;
}

ui32 TRequestTimingGraphBuilder::Fork(ui32 span, ui64 now)
{
    std::lock_guard guard(Mutex);
    if (Frozen) {
        return InvalidId;
    }
    if (span < Spans.size()) {
        for (const auto& wait: Spans[span].Current.Waits) {
            if (wait.End == std::numeric_limits<ui64>::max()) {
                Failure = "wait_crosses_fork_without_dependency";
            }
        }
    }
    const auto node = Seal(span, now, true);
    if (node != InvalidId) {
        Spans[span].Suspended = true;
    }
    return node;
}

ui32 TRequestTimingGraphBuilder::Start(ui32 fork, ui64 now)
{
    std::lock_guard guard(Mutex);
    if (Frozen || fork >= Stages.size() || ++Events > MaxEvents) {
        if (!Frozen) {
            Failure = "missing_fork_or_event_limit";
        }
        return InvalidId;
    }
    if (now < Stages[fork].End) {
        Failure = "invalid_launch_time";
        return InvalidId;
    }
    const ui32 id = Spans.size();
    TSpan s;
    s.ParentFork = fork;
    s.ParentPart = StageParts[fork];
    s.Current.Begin = now;
    // The async dispatcher launches independently of child completion. Preserve
    // dispatch overhead relative to the fork, not to the root's wall clock.
    s.Current.Dependencies.push_back({fork, now - Stages[fork].End});
    Spans.push_back(std::move(s));
    return id;
}

void TRequestTimingGraphBuilder::Finish(ui32 span, ui64 now)
{
    std::lock_guard guard(Mutex);
    if (!Frozen && span < Spans.size() && !Spans[span].Closed) {
        Seal(span, now, false);
        Spans[span].Closed = true;
    }
}

void TRequestTimingGraphBuilder::Join(
    ui32 span, const TVector<ui32>& children, ui64 now)
{
    std::lock_guard guard(Mutex);
    if (Frozen || span >= Spans.size() || Spans[span].Closed) {
        return;
    }
    auto& s = Spans[span];
    if (!s.Suspended || children.empty()) {
        Failure = "missing_join_dependencies";
        return;
    }
    if (children.size() > MaxEvents - std::min(Events, MaxEvents)) {
        Failure = "dependency_limit";
        return;
    }
    auto uniqueChildren = children;
    std::sort(uniqueChildren.begin(), uniqueChildren.end());
    if (std::adjacent_find(uniqueChildren.begin(), uniqueChildren.end()) !=
        uniqueChildren.end())
    {
        Failure = "duplicate_join_child";
        return;
    }
    Events += children.size();
    const auto parentFork = s.Last;
    auto observations = std::move(s.Current);
    s.Current = {};
    s.Current.Waits = std::move(observations.Waits);
    s.Current.MissingCategories = observations.MissingCategories;
    s.Current.UnlocatedWaits = observations.UnlocatedWaits;
    s.Suspended = false;
    for (const auto child: children) {
        if (child >= Spans.size() || !Spans[child].Closed ||
            Spans[child].Last == InvalidId || Spans[child].Stopped ||
            Spans[child].ParentFork != parentFork)
        {
            Failure = "missing_child_completion";
            return;
        }
        const auto node = Spans[child].Last;
        s.Current.Begin = std::max(s.Current.Begin, Stages[node].End);
        s.Current.Dependencies.push_back({node, 0});
    }
    if (s.Current.Begin > now) {
        Failure = "invalid_join_time";
    }
    // [last required completion, now] belongs to the parent's post-join work.
}

void TRequestTimingGraphBuilder::Cancel(ui32 span, ui64 now)
{
    std::lock_guard guard(Mutex);
    if (Frozen || span >= Spans.size()) {
        return;
    }
    // Part IDs are assigned after their parent. Stop observing this subtree
    // without cancelling I/O or adding any dependency to the root response.
    for (ui32 i = span; i < Spans.size(); ++i) {
        auto& part = Spans[i];
        if (i != span && (part.ParentPart >= Spans.size() ||
                          !Spans[part.ParentPart].Stopped))
        {
            continue;
        }
        if (!part.Closed && !part.Suspended) {
            Seal(i, now, false);
        }
        part.Closed = true;
        part.Stopped = true;
    }
}

void TRequestTimingGraphBuilder::Wait(
    ui32 span, ui32 categories, ui64 begin, ui64 end)
{
    std::lock_guard guard(Mutex);
    // A stopped part can still block the dispatch thread before Execute
    // returns.
    if (Frozen || span >= Spans.size() || Spans[span].Closed) {
        return;
    }
    if (Spans[span].Suspended) {
        Failure = "wait_on_suspended_parent";
    }
    if (++Events > MaxEvents) {
        Failure = "event_limit";
        return;
    }
    auto& waits = Spans[span].Current.Waits;
    for (auto& wait: waits) {
        if (wait.Begin == begin && wait.Categories == categories &&
            wait.End == std::numeric_limits<ui64>::max())
        {
            wait.End = end;
            return;
        }
    }
    waits.push_back({begin, end, categories});
}

void TRequestTimingGraphBuilder::Missing(
    ui32 span, ui32 categories, ui64 duration)
{
    std::lock_guard guard(Mutex);
    if (Frozen || !duration || span >= Spans.size() || Spans[span].Closed) {
        return;
    }
    auto& current = Spans[span].Current;
    if (Spans[span].Suspended) {
        Failure = "unlocated_wait_on_suspended_parent";
    }
    current.MissingCategories |= categories;
    for (ui32 i = 0; i < 3; ++i) {
        if (categories & (1u << i)) {
            auto& sum = current.UnlocatedWaits[i];
            sum = duration > std::numeric_limits<ui64>::max() - sum
                      ? std::numeric_limits<ui64>::max()
                      : sum + duration;
        }
    }
}

void TRequestTimingGraphBuilder::Incomplete(TString reason)
{
    std::lock_guard guard(Mutex);
    if (!Frozen) {
        Failure = std::move(reason);
    }
}

void TRequestTimingGraphBuilder::Freeze(
    ui64 now, ui32 categories, ui32 errorCode)
{
    std::lock_guard guard(Mutex);
    if (!Frozen) {
        FrozenEnd = now;
        FrozenCategories = categories;
        FrozenError = errorCode;
        Frozen = true;
    }
}

TString TRequestTimingGraphBuilder::Complete(
    ui64 now, ui32 categories, ui32 errorCode)
{
    Freeze(now, categories, errorCode);
    std::call_once(
        SnapshotOnce,
        [this]
        {
            try {
                BuildSnapshot();
            } catch (...) {
                // Finalization can already have sealed stages when allocation
                // or serialization throws. Preserve that outcome instead of
                // retrying mutations on a partially finalized graph.
                SnapshotException = std::current_exception();
            }
        });
    if (SnapshotException) {
        std::rethrow_exception(SnapshotException);
    }
    return Snapshot;
}

void TRequestTimingGraphBuilder::Finalize()
{
    std::call_once(
        FinalizeOnce,
        [this]
        {
            try {
                // Frozen was published under Mutex before any finalizer enters.
                // Late mutators return before inspecting these containers.
                FrozenCompletion = Seal(0, FrozenEnd, false);
                for (ui32 i = 0; i < Spans.size(); ++i) {
                    const auto& current = Spans[i].Current;
                    if (!current.Waits.empty() || current.MissingCategories) {
                        auto pending = current;
                        pending.End = FrozenEnd;
                        Stages.push_back(std::move(pending));
                        StageParts.push_back(i);
                        DiagnosticOnly.push_back(true);
                    }
                }
            } catch (...) {
                // A failed seal can already have moved graph elements. Never
                // retry that mutation when another serializer observes this
                // snapshot.
                FinalizeException = std::current_exception();
            }
        });
    if (FinalizeException) {
        std::rethrow_exception(FinalizeException);
    }
}

void TRequestTimingGraphBuilder::FillTrace(NProto::TRequestTimingTrace& trace)
{
    // Snapshot callers have already frozen. Synchronize with that first freeze,
    // but do not hold the live recorder lock while walking frozen containers.
    {
        std::lock_guard guard(Mutex);
        Y_ABORT_UNLESS(Frozen);
    }
    Finalize();
    trace.Clear();
    trace.SetVersion(1);
    trace.SetRequestId(RequestId);
    trace.SetTotalMicros(FrozenEnd);
    trace.SetSelectedCategories(FrozenCategories);
    trace.SetErrorCode(FrozenError);
    trace.SetCompletionNode(FrozenCompletion);
    if (!Failure.empty()) {
        trace.SetFailure(Failure);
    }
    for (ui32 i = 0; i < Spans.size(); ++i) {
        auto* part = trace.AddParts();
        if (Spans[i].ParentPart != InvalidId) {
            part->SetParent(Spans[i].ParentPart);
        }
        part->SetClosed(Spans[i].Closed || i == 0);
        part->SetObservationStopped(Spans[i].Stopped);
    }
    for (ui32 i = 0; i < Stages.size(); ++i) {
        const auto& s = Stages[i];
        auto* stage = trace.AddStages();
        stage->SetBegin(s.Begin);
        stage->SetEnd(s.End);
        stage->SetPart(StageParts[i]);
        if (DiagnosticOnly[i]) {
            stage->SetDiagnosticOnly(true);
        }
        if (s.NotBefore) {
            stage->SetNotBefore(s.NotBefore);
        }
        if (s.MissingCategories) {
            stage->SetMissingCategories(s.MissingCategories);
        }
        if (!s.IncompleteReason.empty()) {
            stage->SetIncompleteReason(s.IncompleteReason);
        }
        for (auto duration: s.UnlocatedWaits) {
            stage->AddUnlocatedWaits(duration);
        }
        for (const auto& d: s.Dependencies) {
            auto* dep = stage->AddDependencies();
            dep->SetNode(d.Node);
            dep->SetLag(d.Lag);
        }
        for (const auto& w: s.Waits) {
            auto* wait = stage->AddWaits();
            wait->SetBegin(w.Begin);
            wait->SetEnd(w.End);
            wait->SetCategories(w.Categories);
        }
    }
}

void TRequestTimingGraphBuilder::BuildSnapshot()
{
    NProto::TRequestTimingTrace trace;
    FillTrace(trace);
    Snapshot = FormatRequestTimingTrace(trace);
}

}   // namespace NCloud
