#include "latency.h"

#include <algorithm>

namespace NCloud::NBlockStore {

namespace {

ui64 Micros(ui64 start, ui64 end)
{
    return end >= start ? CyclesToDurationSafe(end - start).MicroSeconds() : 0;
}

bool IsExclusionKnown(NProto::TLatencyDiagnostics::EExclusion exclusion)
{
    switch (exclusion) {
        case NProto::TLatencyDiagnostics::NONE:
        case NProto::TLatencyDiagnostics::INVALID_CLIENT_REQUEST:
        case NProto::TLatencyDiagnostics::CLIENT_CANCELLATION:
        case NProto::TLatencyDiagnostics::CLIENT_LIMIT:
            return true;
        default:
            return false;
    }
}

bool IsQuotaKnown(NProto::TLatencyDiagnostics::EQuotaReason reason)
{
    switch (reason) {
        case NProto::TLatencyDiagnostics::PROFILE_IOPS:
        case NProto::TLatencyDiagnostics::PROFILE_BANDWIDTH:
        case NProto::TLatencyDiagnostics::PROFILE_BURST:
        case NProto::TLatencyDiagnostics::PROFILE_LIMIT:
            return true;
        default:
            return false;
    }
}

NProto::TLatencyDiagnostics NewSummary(
    ui64 started, ui64 finished, bool complete)
{
    NProto::TLatencyDiagnostics summary;
    summary.SetVersion(LatencyVersion);
    summary.SetComplete(complete && started && finished >= started);
    summary.SetTotalUs(Micros(started, finished));
    summary.SetAdjustedUs(summary.GetTotalUs());
    summary.SetExclusion(NProto::TLatencyDiagnostics::NONE);
    return summary;
}

}   // namespace

TMaybe<TDuration> ReadLatencySummary(
    const NProto::TLatencyDiagnostics& summary, TDuration totalTime)
{
    if (!summary.HasVersion() || summary.GetVersion() != LatencyVersion ||
        !summary.HasComplete() || !summary.GetComplete() ||
        !summary.HasTotalUs() || !summary.HasAdjustedUs() ||
        !summary.HasExclusion() || !IsExclusionKnown(summary.GetExclusion()) ||
        summary.GetAdjustedUs() > summary.GetTotalUs() ||
        summary.GetTotalUs() > totalTime.MicroSeconds())
    {
        return {};
    }
    return TDuration::MicroSeconds(
        totalTime.MicroSeconds() -
        (summary.GetTotalUs() - summary.GetAdjustedUs()));
}

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
    if (finished < Started) {
        Complete = false;
        return;
    }
    for (auto& quota: Quota) {
        quota.Finished = std::min(quota.Finished, finished);
    }
}

void TLatencyOperation::Invalidate()
{
    std::lock_guard lock(Lock);
    Complete = false;
}

void TLatencyOperation::AddChild(
    ui64 started, ui64 finished, const NProto::TLatencyDiagnostics& summary)
{
    std::lock_guard lock(Lock);
    if (!Complete || started < Started || finished < started ||
        !ReadLatencySummary(summary, TDuration::MicroSeconds(Micros(started, finished))))
    {
        Complete = false;
        return;
    }
    const ui64 removed = summary.GetTotalUs() - summary.GetAdjustedUs();
    HasChildren = true;
    LastChildFinished = std::max(LastChildFinished, finished);
    if (Parallel) {
        // At this boundary the branch ends at observedEnd - removed. Taking
        // the maximum again allows the critical path to switch after quota.
        AdjustedChildFinishUs = std::max(
            AdjustedChildFinishUs, Micros(Started, finished) - removed);
    } else {
        if (Children.size() >= MaxLatencyChildren) {
            Complete = false;
            return;
        }
        ChildrenOrdered &= Children.empty() || started >= Children.back().Started;
        Children.push_back({started, finished, removed});
    }
}

void TLatencyOperation::AddQuota(
    ui64 started, ui64 finished,
    NProto::TLatencyDiagnostics::EQuotaReason reason)
{
    std::lock_guard lock(Lock);
    if (!Complete || started < Started || finished < started ||
        !IsQuotaKnown(reason) || Quota.size() >= MaxLatencyQuotaIntervals)
    {
        Complete = false;
        return;
    }
    if (finished > started) {
        QuotaOrdered &= Quota.empty() || started >= Quota.back().Started;
        Quota.push_back({started, finished});
    }
}

NProto::TLatencyDiagnostics TLatencyOperation::FinishLeaf(
    ui64 finished, NProto::TLatencyDiagnostics::EExclusion exclusion) const
{
    std::lock_guard lock(Lock);
    auto summary = NewSummary(
        Started, finished, Complete && !HasChildren && IsExclusionKnown(exclusion));
    summary.SetExclusion(exclusion);
    if (!summary.GetComplete()) {
        return summary;
    }
    TVector<ui32> order;
    if (!QuotaOrdered) {
        order.reserve(Quota.size());
        for (ui32 i = 0; i < Quota.size(); ++i) {
            order.push_back(i);
        }
        std::sort(order.begin(), order.end(), [&](ui32 a, ui32 b) {
            return Quota[a].Started < Quota[b].Started;
        });
    }
    // Subtract the union of confirmed waits, clipped at completion. Quantize
    // boundaries relative to Started, as in the former execution graph.
    ui64 removed = 0;
    ui64 mergedStart = 0;
    ui64 mergedEnd = 0;
    for (size_t i = 0; i < Quota.size(); ++i) {
        const auto& quota = Quota[order.empty() ? i : order[i]];
        const ui64 endCycles = std::min(quota.Finished, finished);
        if (quota.Started >= endCycles) {
            continue;
        }
        const ui64 start = Micros(Started, quota.Started);
        const ui64 end = Micros(Started, endCycles);
        if (start > mergedEnd) {
            removed += mergedEnd - mergedStart;
            mergedStart = start;
            mergedEnd = end;
        } else {
            mergedEnd = std::max(mergedEnd, end);
        }
    }
    removed += mergedEnd - mergedStart;
    summary.SetAdjustedUs(summary.GetTotalUs() - removed);
    return summary;
}

NProto::TLatencyDiagnostics TLatencyOperation::Finish(ui64 finished) const
{
    std::lock_guard lock(Lock);
    auto summary = NewSummary(
        Started, finished,
        Complete && HasChildren && Quota.empty() && LastChildFinished <= finished);
    if (!summary.GetComplete()) {
        return summary;
    }
    if (Parallel) {
        // Keep the parent's tail after the last observed child. Launch gaps
        // are already included in each child's absolute adjusted finish.
        summary.SetAdjustedUs(
            summary.GetTotalUs() - Micros(Started, LastChildFinished) +
            AdjustedChildFinishUs);
        return summary;
    }
    TVector<ui32> order;
    if (!ChildrenOrdered) {
        order.reserve(Children.size());
        for (ui32 i = 0; i < Children.size(); ++i) {
            order.push_back(i);
        }
        std::sort(order.begin(), order.end(), [&](ui32 a, ui32 b) {
            return Children[a].Started < Children[b].Started;
        });
    }
    ui64 previousEnd = 0;
    ui64 removed = 0;
    for (size_t i = 0; i < Children.size(); ++i) {
        const auto& child = Children[order.empty() ? i : order[i]];
        const ui64 start = Micros(Started, child.Started);
        const ui64 end = Micros(Started, child.Finished);
        if (start < previousEnd || child.RemovedUs > end - start ||
            child.RemovedUs > summary.GetTotalUs() - removed)
        {
            summary.SetComplete(false);
            return summary;
        }
        removed += child.RemovedUs;
        previousEnd = end;
    }
    // Sequential quota savings add; every launch/backoff gap and tail remains.
    summary.SetAdjustedUs(summary.GetTotalUs() - removed);
    // Child exclusions do not establish the origin of the outer operation.
    return summary;
}

}   // namespace NCloud::NBlockStore
