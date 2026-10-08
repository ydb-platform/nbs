#pragma once

#include <cloud/blockstore/libs/common/latency_sli.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NBlockStore {

class TLatencySliCounters
{
private:
    const TLatencySliConfig Config;
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
        TLatencySliConfig config,
        NMonitoring::TDynamicCounters& group)
        : Config(std::move(config))
    {
        Counters = {
            group.GetCounter("LatencyGoodOps", true),
            group.GetCounter("LatencyBadOps", true),
            group.GetCounter("LatencyTotalOps", true),
            group.GetCounter("LatencyUnknownOps", true)};
    }

    const TLatencySliConfig& GetConfig() const
    {
        return Config;
    }

    void Add(ui64 good, ui64 bad, ui64 unknown)
    {
        auto& c = Counters;
        if (good) {
            c.Good->Add(good);
        }
        if (bad) {
            c.Bad->Add(bad);
        }
        if (good || bad) {
            c.Total->Add(good + bad);
        }
        if (unknown) {
            c.Unknown->Add(unknown);
        }
    }

    void Complete(
        bool write,
        ui64 bytes,
        ui64 elapsedUs,
        ui64 quotaDelayUs,
        bool failed,
        bool validTiming)
    {
        const auto result = Config.Classify(
            write, bytes, elapsedUs, quotaDelayUs, failed, validTiming);
        Add(result == ELatencySliResult::Good,
            result == ELatencySliResult::Bad,
            result == ELatencySliResult::Unknown);
    }
};

}   // namespace NCloud::NBlockStore
