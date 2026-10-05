#pragma once
#include "stats.h"

#include <cloud/blockstore/libs/diagnostics/latency_config.h>
#include <cloud/blockstore/libs/diagnostics/latency_sli.h>
#include <cloud/blockstore/libs/service/latency.h>

namespace NCloud::NBlockStore::NVHostServer {
enum class ELatencyCompletion
{
    Success,
    Error
};

class TLatencyTracker
{
    bool Enabled = false;
    TLatencyConfig Config;
    TLatencyThresholds Thresholds;

public:
    TLatencyTracker() = default;

    TLatencyTracker(bool enabled, TLatencyConfig config)
        : Enabled(enabled)
        , Config(std::move(config))
        , Thresholds(Config.Config)
    {}

    bool IsEnabled() const
    {
        return Enabled;
    }

    template <typename T>
    void Record(TStats<T>& stats, int type, ui64 bytes, TCpuCycles elapsed,
                bool success, const NProto::TLatencyDiagnostics* graph) const
    {
        if (!Enabled || type < 0 || type >= 2) {
            return;
        }
        const auto result = EvaluateLatency(
            Thresholds,
            Config.MediaKind,
            type == 1 ? EBlockStoreRequest::WriteBlocks
                      : EBlockStoreRequest::ReadBlocks, bytes,
            CyclesToDurationSafe(elapsed), graph, success);
        auto& counts = stats.LatencyCounters[type];
        counts.Good += result.Good;
        counts.Bad += result.Bad;
        counts.Unknown += result.Unknown;
        counts.InvalidClientRequest += result.InvalidClientRequest;
        counts.ClientCancellation += result.ClientCancellation;
        counts.ClientLimit += result.ClientLimit;
    }

    template <typename T>
    void Record(TStats<T>& stats, int type, ui64 bytes, TCpuCycles elapsed,
                ELatencyCompletion completion) const
    {
        if (!Enabled || type < 0 || type >= 2) {
            return;
        }
        NProto::TLatencyDiagnostics graph;
        graph.SetVersion(LatencyVersion);
        graph.SetComplete(true);
        graph.SetTotalUs(CyclesToDurationSafe(elapsed).MicroSeconds());
        graph.SetExclusion(NProto::TLatencyDiagnostics::NONE);
        auto* node = graph.AddNodes();
        node->SetKind(NProto::TLatencyDiagnostics::SERVICE);
        node->SetStartUs(0);
        node->SetDurationUs(graph.GetTotalUs());
        Record(stats, type, bytes, elapsed,
               completion == ELatencyCompletion::Success, &graph);
    }
};
}   // namespace NCloud::NBlockStore::NVHostServer
