#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/latency_sli.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NBlockStore {

class TLatencySliCounters
{
private:
    const TLatencySliConfig Config;
    const NMonitoring::TDynamicCountersPtr Group;
    struct TCounters
    {
        NMonitoring::TDynamicCounters::TCounterPtr Good;
        NMonitoring::TDynamicCounters::TCounterPtr Bad;
        NMonitoring::TDynamicCounters::TCounterPtr Total;
        NMonitoring::TDynamicCounters::TCounterPtr Unknown;
    };

    std::array<TCounters, 2> ByOperation;

public:
    TLatencySliCounters(TLatencySliConfig config,
                        NMonitoring::TDynamicCountersPtr group)
        : Config(std::move(config))
        , Group(std::move(group))
    {
        for (bool write: {false, true}) {
            const auto counters = Group->GetSubgroup(
                "request", write ? "WriteBlocks" : "ReadBlocks");
            ByOperation[write] = {
                counters->GetCounter("LatencyGoodOps", true),
                counters->GetCounter("LatencyBadOps", true),
                counters->GetCounter("LatencyTotalOps", true),
                counters->GetCounter("LatencyUnknownOps", true)};
        }
    }

    void Add(bool write, ui64 good, ui64 bad, ui64 unknown)
    {
        const auto& counters = ByOperation[write];
        // Add deltas to preserve existing values if the group is reused. Publish
        // Total first so the numerator cannot advance ahead of its denominator.
        // Dynamic counters do not provide an atomic multi-counter snapshot.
        if (good || bad) {
            counters.Total->Add(good + bad);
        }
        if (good) {
            counters.Good->Add(good);
        }
        if (bad) {
            counters.Bad->Add(bad);
        }
        if (unknown) {
            counters.Unknown->Add(unknown);
        }
    }

    void Complete(
        bool write,
        ui64 bytes,
        ui64 elapsedUs,
        bool failed)
    {
        const auto result = Config.Classify(
            write, bytes, elapsedUs, failed);
        Add(write, result == ELatencySliResult::Good,
            result == ELatencySliResult::Bad,
            result == ELatencySliResult::Unknown);
    }
};

}   // namespace NCloud::NBlockStore
