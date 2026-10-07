#pragma once
#include "stats.h"

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
    ui32 MediaKind = 0;
    TLatencyThresholds Thresholds;

public:
    TLatencyTracker() = default;

    TLatencyTracker(const NProto::TDiagnosticsConfig& config, ui32 mediaKind)
        : Enabled(config.GetEnableLatency())
        , MediaKind(mediaKind)
        , Thresholds(config)
    {}

    bool IsEnabled() const
    {
        return Enabled;
    }

    template <typename T>
    void Record(TStats<T>& stats, int type, ui64 bytes, TCpuCycles elapsed,
                bool success, const NProto::TLatencyDiagnostics* summary) const
    {
        if (!Enabled || type < 0 || type >= 2) {
            return;
        }
        const auto result = EvaluateLatency(
            Thresholds,
            MediaKind,
            type == 1 ? EBlockStoreRequest::WriteBlocks
                      : EBlockStoreRequest::ReadBlocks, bytes,
            CyclesToDurationSafe(elapsed), summary, success);
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
        NProto::TLatencyDiagnostics summary;
        summary.SetVersion(LatencyVersion);
        summary.SetComplete(true);
        summary.SetTotalUs(CyclesToDurationSafe(elapsed).MicroSeconds());
        summary.SetExclusion(NProto::TLatencyDiagnostics::NONE);
        summary.SetAdjustedUs(summary.GetTotalUs());
        Record(stats, type, bytes, elapsed,
               completion == ELatencyCompletion::Success, &summary);
    }
};
}   // namespace NCloud::NBlockStore::NVHostServer
