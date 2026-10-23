#pragma once

#include <cloud/blockstore/libs/common/latency_sli.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <atomic>
#include <mutex>

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

    // Only the outcome is updated on the request path. Exported counters are
    // advanced together by the periodic stats collector.
    std::atomic<ui64> PendingGood = 0;
    std::atomic<ui64> PendingBad = 0;
    std::atomic<ui64> PendingUnknown = 0;
    std::mutex PublishLock;

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

    ~TLatencySliCounters()
    {
        Publish();
    }

    const TLatencySliConfig& GetConfig() const
    {
        return Config;
    }

    void Add(ui64 good, ui64 bad, ui64 unknown)
    {
        if (good) {
            PendingGood.fetch_add(good, std::memory_order_relaxed);
        }
        if (bad) {
            PendingBad.fetch_add(bad, std::memory_order_relaxed);
        }
        if (unknown) {
            PendingUnknown.fetch_add(unknown, std::memory_order_relaxed);
        }
    }

    void Publish()
    {
        // Stats updates may overlap. Serialize publication, never completions.
        const std::lock_guard guard(PublishLock);
        const auto good = PendingGood.exchange(0, std::memory_order_relaxed);
        const auto bad = PendingBad.exchange(0, std::memory_order_relaxed);
        const auto unknown = PendingUnknown.exchange(0, std::memory_order_relaxed);

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
        bool failed,
        bool validTiming)
    {
        const auto result = Config.Classify(
            write, bytes, elapsedUs, failed, validTiming);
        Add(result == ELatencySliResult::Good,
            result == ELatencySliResult::Bad,
            result == ELatencySliResult::Unknown);
    }
};

}   // namespace NCloud::NBlockStore
