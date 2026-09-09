#pragma once

#include "stats.h"

#include <cloud/blockstore/libs/diagnostics/latency_thresholds.h>

#include <util/datetime/cputimer.h>

#include <utility>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

enum class ELatencyCompletion
{
    Success,
    Error,
    Skipped,
};

inline TCpuCycles SubtractLatencyWaitTime(
    TCpuCycles elapsedCycles,
    TDuration waitTime)
{
    const ui64 waitCycles = DurationToCyclesSafe(waitTime);
    return elapsedCycles > waitCycles ? elapsedCycles - waitCycles : 0;
}

// Classifies only final guest-visible read/write completions. The selected
// ladder is copied at endpoint startup and is never refreshed in the child.
// Enabled + an empty ladder is an explicit "media kind is unconfigured" mode:
// every operation is reported as skipped rather than making the payload
// disappear.
class TLatencyTracker
{
private:
    bool Enabled = false;
    TLatencyThresholdLadder Ladder;

public:
    TLatencyTracker() = default;

    TLatencyTracker(bool enabled, TLatencyThresholdLadder ladder)
        : Enabled(enabled)
        , Ladder(std::move(ladder))
    {}

    [[nodiscard]] bool IsEnabled() const
    {
        return Enabled;
    }

    template <typename T>
    void Record(
        TStats<T>& stats,
        int requestType,
        ui64 requestBytes,
        TCpuCycles elapsedCycles,
        ELatencyCompletion completion) const
    {
        if (!Enabled || requestType < 0 || requestType >= 2) {
            return;
        }

        auto& counters = stats.LatencyCounters[requestType];
        if (Ladder.empty() || completion == ELatencyCompletion::Skipped) {
            counters.Skipped += 1;
            return;
        }

        if (completion == ELatencyCompletion::Error) {
            counters.Bad += 1;
            return;
        }

        const auto& bucket = FindLatencyThresholdBucket(Ladder, requestBytes);
        const auto threshold = requestType == 1
            ? bucket.WriteThreshold
            : bucket.ReadThreshold;

        if (CyclesToDurationSafe(elapsedCycles) <= threshold) {
            counters.Good += 1;
        } else {
            counters.Bad += 1;
        }
    }

    template <typename T>
    void Record(
        TStats<T>& stats,
        int requestType,
        ui64 requestBytes,
        TCpuCycles elapsedCycles,
        const NProto::TError& error) const
    {
        if (!Enabled || requestType < 0 || requestType >= 2) {
            return;
        }

        const auto outcome = ClassifyLatencyOutcome(
            Ladder.empty() ? nullptr : &Ladder,
            error,
            requestType == 1,
            requestBytes,
            CyclesToDurationSafe(elapsedCycles));

        auto& counters = stats.LatencyCounters[requestType];
        if (outcome.CountSkipped) {
            counters.Skipped += 1;
        } else if (outcome.CountGood) {
            counters.Good += 1;
        } else if (outcome.CountTotal) {
            counters.Bad += 1;
        }
    }
};

}   // namespace NCloud::NBlockStore::NVHostServer
