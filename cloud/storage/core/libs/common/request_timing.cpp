#include "request_timing.h"

#include <algorithm>
#include <limits>

namespace NCloud {

namespace {

bool TryAdd(ui64 lhs, ui64 rhs, ui64& result)
{
    if (rhs > std::numeric_limits<ui64>::max() - lhs) {
        return false;
    }
    result = lhs + rhs;
    return true;
}

struct TFrame
{
    ui32 Node;
    size_t NextDependency = 0;
};

}   // namespace

TRequestTimingResult TRequestTiming::Calculate(
    const TVector<TTimingStage>& stages,
    ui32 completionNode,
    ui32 selectedMask,
    ui64 totalMicros)
{
    TRequestTimingResult result;
    result.TotalTime = TDuration::MicroSeconds(totalMicros);
    const auto incomplete = [&result](const TString& reason) {
        result.IncompleteReason = reason;
        return result;
    };
    if (selectedMask & ~SupportedWaitCategories) {
        return incomplete("Unsupported selected wait categories");
    }
    if (completionNode >= stages.size()) {
        return incomplete("Missing completion node");
    }
    if (stages[completionNode].End != totalMicros) {
        return incomplete("Completion node does not match root response time");
    }

    TVector<ui8> state(stages.size(), 0);
    TVector<ui64> recalculatedEnds(stages.size(), 0);
    TVector<TFrame> stack;
    state[completionNode] = 1;
    stack.push_back({completionNode, 0});
    while (!stack.empty()) {
        auto& frame = stack.back();
        const ui32 node = frame.Node;
        const auto& stage = stages[node];
        if (frame.NextDependency < stage.Dependencies.size()) {
            const ui32 dependency =
                stage.Dependencies[frame.NextDependency++].Node;
            if (dependency >= stages.size()) {
                return incomplete("Missing dependency node");
            }
            if (state[dependency] == 1) {
                return incomplete("Cycle in timing dependencies");
            }
            if (state[dependency] == 0) {
                state[dependency] = 1;
                stack.push_back({dependency, 0});
            }
            continue;
        }
        if (!stage.IncompleteReason.empty()) {
            return incomplete(stage.IncompleteReason);
        }
        if (stage.MissingCategories & selectedMask) {
            return incomplete(
                "Missing interval positions for selected wait categories");
        }
        if (stage.End < stage.Begin) {
            return incomplete("Stage end precedes stage begin");
        }
        if (stage.End > totalMicros) {
            return incomplete("Stage exceeds root response boundary");
        }
        ui64 actualStart = stage.NotBefore;
        ui64 recalculatedStart = stage.NotBefore;
        for (const auto& dependency: stage.Dependencies) {
            ui64 dependencyStart = 0;
            if (!TryAdd(
                    stages[dependency.Node].End,
                    dependency.Lag,
                    dependencyStart))
            {
                return incomplete("Actual dependency timestamp overflow");
            }
            actualStart = std::max(actualStart, dependencyStart);
            if (!TryAdd(
                    recalculatedEnds[dependency.Node],
                    dependency.Lag,
                    dependencyStart))
            {
                return incomplete("Recalculated dependency timestamp overflow");
            }
            recalculatedStart =
                std::max(recalculatedStart, dependencyStart);
        }
        if (actualStart != stage.Begin) {
            return incomplete(
                "Stage begin does not match dependencies and not-before");
        }
        TVector<TTimingWait> selectedWaits;
        for (const auto& wait: stage.Waits) {
            if (wait.Begin < stage.Begin ||
                wait.End < wait.Begin ||
                wait.End > stage.End ||
                (wait.Categories & ~SupportedWaitCategories))
            {
                return incomplete("Invalid wait interval or categories");
            }
            if ((wait.Categories & selectedMask) && wait.Begin != wait.End) {
                selectedWaits.push_back(wait);
            }
        }
        std::sort(
            selectedWaits.begin(),
            selectedWaits.end(),
            [](const TTimingWait& lhs, const TTimingWait& rhs) {
                return lhs.Begin != rhs.Begin ? lhs.Begin < rhs.Begin
                                              : lhs.End < rhs.End;
            });
        ui64 removed = 0;
        ui64 coveredUntil = stage.Begin;
        for (const auto& wait: selectedWaits) {
            const ui64 uncoveredBegin = std::max(wait.Begin, coveredUntil);
            if (wait.End > uncoveredBegin) {
                ui64 newRemoved = 0;
                if (!TryAdd(removed, wait.End - uncoveredBegin, newRemoved)) {
                    return incomplete("Wait union duration overflow");
                }
                removed = newRemoved;
            }
            coveredUntil = std::max(coveredUntil, wait.End);
        }
        const ui64 actualDuration = stage.End - stage.Begin;
        if (removed > actualDuration) {
            return incomplete("Wait union exceeds stage duration");
        }
        ui64 recalculatedEnd = 0;
        if (!TryAdd(
                recalculatedStart,
                actualDuration - removed,
                recalculatedEnd))
        {
            return incomplete("Recalculated stage timestamp overflow");
        }
        if (recalculatedEnd > stage.End) {
            return incomplete("Recalculation increased stage completion time");
        }
        recalculatedEnds[node] = recalculatedEnd;
        state[node] = 2;
        stack.pop_back();
    }
    const ui64 withoutWaits = recalculatedEnds[completionNode];
    if (withoutWaits > totalMicros) {
        return incomplete("Recalculated duration exceeds total duration");
    }
    result.TimeWithoutWaits = TDuration::MicroSeconds(withoutWaits);
    result.WaitImpact = TDuration::MicroSeconds(totalMicros - withoutWaits);
    return result;
}

}   // namespace NCloud
