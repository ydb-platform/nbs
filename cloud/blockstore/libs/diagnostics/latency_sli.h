#pragma once

#include <cloud/blockstore/libs/common/latency_sli_fingerprint.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NBlockStore {

class TLatencySliCounters
{
private:
    const TLatencySliConfig Config;
    const TString ConfigFingerprint;
    struct TCounters
    {
        NMonitoring::TDynamicCounters::TCounterPtr Good;
        NMonitoring::TDynamicCounters::TCounterPtr Bad;
        NMonitoring::TDynamicCounters::TCounterPtr Total;
        NMonitoring::TDynamicCounters::TCounterPtr Unknown;
    };
    TCounters Counters;

public:
    TLatencySliCounters(
        TLatencySliConfig config, NMonitoring::TDynamicCounters& group)
        : Config(std::move(config))
        , ConfigFingerprint(GetLatencySliConfigFingerprint(Config))
    {
        Counters = {
            group.GetCounter("LatencyGoodOps", true),
            group.GetCounter("LatencyBadOps", true),
            group.GetCounter("LatencyTotalOps", true),
            group.GetCounter("LatencyUnknownOps", true)};
    }

    const TString& GetConfigFingerprint() const
    {
        return ConfigFingerprint;
    }

    void Add(ui64 good, ui64 bad, ui64 unknown)
    {
        // Add deltas to preserve existing values if the group is reused. Publish
        // Total first so the numerator cannot advance ahead of its denominator.
        // Dynamic counters do not provide an atomic multi-counter snapshot.
        if (good || bad) {
            Counters.Total->Add(good + bad);
        }
        if (good) {
            Counters.Good->Add(good);
        }
        if (bad) {
            Counters.Bad->Add(bad);
        }
        if (unknown) {
            Counters.Unknown->Add(unknown);
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
        Add(result == ELatencySliResult::Good,
            result == ELatencySliResult::Bad,
            result == ELatencySliResult::Unknown);
    }
};

}   // namespace NCloud::NBlockStore
