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
    std::array<TCounters, 2> Counters;

public:
    TLatencySliCounters(
        TLatencySliConfig config,
        NMonitoring::TDynamicCounters& group)
        : Config(std::move(config))
    {
        for (size_t write = 0; write < Counters.size(); ++write) {
            auto request = group.GetSubgroup(
                "request", write ? "WriteBlocks" : "ReadBlocks");
            Counters[write] = {
                request->GetCounter("LatencyGoodOps", true),
                request->GetCounter("LatencyBadOps", true),
                request->GetCounter("LatencyTotalOps", true),
                request->GetCounter("LatencyUnknownOps", true)};
        }
    }

    const TLatencySliConfig& GetConfig() const
    {
        return Config;
    }

    void Add(bool write, ui64 good, ui64 bad, ui64 unknown)
    {
        auto& c = Counters[write];
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
        ui64 postponedUs,
        bool failed,
        bool validTiming)
    {
        const auto result = Config.Classify(
            write, bytes, elapsedUs, postponedUs, failed, validTiming);
        Add(write, result == ELatencySliResult::Good,
            result == ELatencySliResult::Bad,
            result == ELatencySliResult::Unknown);
    }
};

}   // namespace NCloud::NBlockStore
