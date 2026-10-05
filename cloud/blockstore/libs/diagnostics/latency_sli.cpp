#include "latency_sli.h"

#include <cloud/blockstore/libs/service/request_helpers.h>

#include <util/generic/vector.h>

#include <algorithm>
#include <limits>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MaxLatencyNodes = 4096;
constexpr size_t MaxLatencyEdges = 16384;

TMaybe<TDuration> FindThreshold(const NProto::TDiagnosticsConfig& config,
                                ui32 mediaKind, EBlockStoreRequest requestType,
                                ui64 bytes)
{
    if (!ValidateLatencyThresholds(config)) {
        return {};
    }
    requestType = TranslateLocalRequestType(requestType);
    if (requestType != EBlockStoreRequest::ReadBlocks &&
        requestType != EBlockStoreRequest::WriteBlocks)
    {
        return {};
    }
    const bool write = requestType == EBlockStoreRequest::WriteBlocks;
    TMaybe<TDuration> result;
    for (const auto& row: config.GetLatencyThresholds()) {
        if (row.GetMediaKind() == mediaKind && row.GetWrite() == write &&
            row.GetStartBytes() <= bytes && bytes < row.GetEndBytes())
        {
            result = TDuration::MicroSeconds(row.GetThresholdUs());
        }
    }
    return result;
}

bool CountOperations(const TLatencyCounts& counts, ui64& total)
{
    total = 0;
    for (const auto value:
         {counts.Good, counts.Bad, counts.Unknown, counts.InvalidClientRequest,
          counts.ClientCancellation, counts.ClientLimit})
    {
        if (value > std::numeric_limits<ui64>::max() - total) {
            return false;
        }
        total += value;
    }
    return true;
}

bool SubtractCounts(const TLatencyCounts& current,
                    const TLatencyCounts& previous, TLatencyCounts& delta)
{
    if (current.Good < previous.Good || current.Bad < previous.Bad ||
        current.Unknown < previous.Unknown ||
        current.InvalidClientRequest < previous.InvalidClientRequest ||
        current.ClientCancellation < previous.ClientCancellation ||
        current.ClientLimit < previous.ClientLimit)
    {
        return false;
    }
    delta = {current.Good - previous.Good, current.Bad - previous.Bad,
             current.Unknown - previous.Unknown,
             current.InvalidClientRequest - previous.InvalidClientRequest,
             current.ClientCancellation - previous.ClientCancellation,
             current.ClientLimit - previous.ClientLimit};
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool ValidateLatencyThresholds(const NProto::TDiagnosticsConfig& config)
{
    if (!config.GetLatencyThresholdVersion() ||
        config.LatencyThresholdsSize() == 0 ||
        config.LatencyThresholdsSize() > 256)
    {
        return false;
    }
    for (const auto& row: config.GetLatencyThresholds()) {
        if (!row.HasMediaKind() || !row.HasWrite() || !row.HasStartBytes() ||
            !row.HasEndBytes() || !row.HasThresholdUs() ||
            row.GetStartBytes() >= row.GetEndBytes() || !row.GetThresholdUs() ||
            row.GetThresholdUs() == std::numeric_limits<ui64>::max())
        {
            return false;
        }
        for (const auto& other: config.GetLatencyThresholds()) {
            if (&row != &other && row.GetMediaKind() == other.GetMediaKind() &&
                row.GetWrite() == other.GetWrite() &&
                row.GetStartBytes() < other.GetEndBytes() &&
                other.GetStartBytes() < row.GetEndBytes())
            {
                return false;
            }
        }
    }
    return true;
}

TMaybe<TDuration> CalculateLatency(
    const NProto::TLatencyDiagnostics& diagnostics, TDuration totalTime)
{
    if (!diagnostics.HasVersion() ||
        diagnostics.GetVersion() != LatencyDiagnosticsVersion ||
        !diagnostics.HasComplete() || !diagnostics.GetComplete() ||
        !diagnostics.HasTotalUs() || !diagnostics.HasExclusion() ||
        diagnostics.NodesSize() == 0 ||
        diagnostics.NodesSize() > MaxLatencyNodes ||
        diagnostics.GetTotalUs() > totalTime.MicroSeconds())
    {
        return {};
    }

    TVector<ui64> observedEnds;
    TVector<ui64> adjustedEnds;
    observedEnds.reserve(diagnostics.NodesSize());
    adjustedEnds.reserve(diagnostics.NodesSize());
    ui64 observedFinish = 0;
    ui64 adjustedFinish = 0;
    size_t edges = 0;
    for (const auto& node: diagnostics.GetNodes()) {
        if (!node.HasStartUs() || !node.HasDurationUs() || !node.HasKind() ||
            (node.GetKind() != NProto::TLatencyDiagnostics::SERVICE &&
             node.GetKind() != NProto::TLatencyDiagnostics::QUOTA) ||
            (node.GetKind() == NProto::TLatencyDiagnostics::QUOTA &&
             (!node.HasQuotaReason() ||
              (node.GetQuotaReason() !=
                   NProto::TLatencyDiagnostics::PROFILE_IOPS &&
               node.GetQuotaReason() !=
                   NProto::TLatencyDiagnostics::PROFILE_BANDWIDTH &&
               node.GetQuotaReason() !=
                   NProto::TLatencyDiagnostics::PROFILE_BURST))) ||
            node.GetStartUs() > diagnostics.GetTotalUs() ||
            node.GetDurationUs() > diagnostics.GetTotalUs() - node.GetStartUs())
        {
            return {};
        }
        ui64 observedReady = 0;
        ui64 adjustedReady = 0;
        edges += node.DependenciesSize();
        if (edges > MaxLatencyEdges) {
            return {};
        }
        for (const auto dependency: node.GetDependencies()) {
            if (dependency >= observedEnds.size()) {
                return {};   // cycle, forward edge or missing node
            }
            observedReady = std::max(observedReady, observedEnds[dependency]);
            adjustedReady = std::max(adjustedReady, adjustedEnds[dependency]);
        }
        if (observedReady > node.GetStartUs()) {
            return {};
        }
        const ui64 launchDelay = node.GetStartUs() - observedReady;
        const ui64 duration =
            node.GetKind() == NProto::TLatencyDiagnostics::QUOTA
                ? 0
                : node.GetDurationUs();
        // Every adjusted term is bounded by its observed counterpart.
        const ui64 adjustedEnd = adjustedReady + launchDelay + duration;
        const ui64 observedEnd = node.GetStartUs() + node.GetDurationUs();
        observedEnds.push_back(observedEnd);
        adjustedEnds.push_back(adjustedEnd);
        observedFinish = std::max(observedFinish, observedEnd);
        adjustedFinish = std::max(adjustedFinish, adjustedEnd);
    }
    // Keep the response tail and all time outside the producer's boundary.
    return TDuration::MicroSeconds(
        totalTime.MicroSeconds() - observedFinish + adjustedFinish);
}

TLatencyCounts EvaluateLatency(
    const NProto::TDiagnosticsConfig& config, ui32 mediaKind,
    EBlockStoreRequest requestType, ui64 originalRequestBytes,
    TMaybe<TDuration> totalTime, const NProto::TLatencyDiagnostics* diagnostics,
    bool success)
{
    const auto threshold =
        FindThreshold(config, mediaKind, requestType, originalRequestBytes);
    const auto latency = diagnostics && totalTime
                             ? CalculateLatency(*diagnostics, *totalTime)
                             : TMaybe<TDuration>{};
    // The same completeness and threshold checks precede success, failure
    // and exclusion classification.
    if (!threshold || !latency) {
        return {.Unknown = 1};
    }
    // A successful operation that waited for its quota stays in the SLI.
    // An exclusion attached to success is inconsistent diagnostic evidence.
    if (success &&
        diagnostics->GetExclusion() != NProto::TLatencyDiagnostics::NONE)
    {
        return {.Unknown = 1};
    }
    switch (diagnostics->GetExclusion()) {
        case NProto::TLatencyDiagnostics::INVALID_CLIENT_REQUEST:
            return {.InvalidClientRequest = 1};
        case NProto::TLatencyDiagnostics::CLIENT_CANCELLATION:
            return {.ClientCancellation = 1};
        case NProto::TLatencyDiagnostics::CLIENT_LIMIT:
            return {.ClientLimit = 1};
        case NProto::TLatencyDiagnostics::NONE:
            break;
        default:
            return {.Unknown = 1};
    }
    return success && *latency <= *threshold ? TLatencyCounts{.Good = 1}
                                             : TLatencyCounts{.Bad = 1};
}

void TLatencyCounters::Register(NMonitoring::TDynamicCounters& counters)
{
    Good = counters.GetCounter("LatencyGoodOps", true);
    Total = counters.GetCounter("LatencyTotalOps", true);
    Unknown = counters.GetCounter("LatencyUnknownOps", true);
    InvalidClientRequest =
        counters.GetCounter("LatencyExcludedInvalidRequestOps", true);
    ClientCancellation =
        counters.GetCounter("LatencyExcludedClientCancellationOps", true);
    ClientLimit = counters.GetCounter("LatencyExcludedClientLimitOps", true);
}

void TLatencyCounters::Add(const TLatencyCounts& counts)
{
    if (!Good) {
        return;
    }
    *Good += counts.Good;
    *Total += counts.Good + counts.Bad;
    *Unknown += counts.Unknown;
    *InvalidClientRequest += counts.InvalidClientRequest;
    *ClientCancellation += counts.ClientCancellation;
    *ClientLimit += counts.ClientLimit;
}

TLatencyBatchResult TLatencyBatchTracker::Update(const TLatencyBatch& batch,
                                                 ui32 thresholdVersion,
                                                 TInstant now, TDuration maxAge)
{
    std::lock_guard lock(Lock);
    ui64 readCount = 0;
    ui64 writeCount = 0;
    if (!batch.Generation || !batch.Sequence || !batch.CapturedAt ||
        !CountOperations(batch.Read, readCount) ||
        !CountOperations(batch.Write, writeCount))
    {
        return {};
    }
    if (Last && (batch.Generation < Last->Generation ||
                 (batch.Generation == Last->Generation &&
                  batch.Sequence <= Last->Sequence)))
    {
        return {.Status = ELatencyBatchStatus::Duplicate};
    }

    const bool sameGeneration = Last && batch.Generation == Last->Generation;
    TLatencyBatchResult result;
    if (sameGeneration &&
        (batch.CapturedAt < Last->CapturedAt ||
         !SubtractCounts(batch.Read, Last->Read, result.Read) ||
         !SubtractCounts(batch.Write, Last->Write, result.Write)))
    {
        return {};
    }
    if (!sameGeneration) {
        result.Read = batch.Read;
        result.Write = batch.Write;
    }
    // Gaps, first contact/restart, incompatible versions
    // and stale snapshots must not manufacture Good. Account their deltas as
    // Unknown and consume their high-water mark so re-delivery is harmless.
    const bool contiguous =
        sameGeneration && batch.Sequence - Last->Sequence == 1;
    const bool fresh =
        batch.CapturedAt <= now && now - batch.CapturedAt <= maxAge;
    const bool compatible =
        thresholdVersion && batch.ThresholdVersion == thresholdVersion &&
        batch.Version == LatencyDiagnosticsVersion &&
        (!sameGeneration || (Last->Version == batch.Version &&
                             Last->ThresholdVersion == batch.ThresholdVersion));
    if (!contiguous || !fresh || !compatible) {
        CountOperations(result.Read, readCount);
        CountOperations(result.Write, writeCount);
        result.Read = {.Unknown = readCount};
        result.Write = {.Unknown = writeCount};
        result.Status = ELatencyBatchStatus::Unknown;
    } else {
        result.Status = ELatencyBatchStatus::Accepted;
    }
    Last = batch;
    return result;
}

}   // namespace NCloud::NBlockStore
