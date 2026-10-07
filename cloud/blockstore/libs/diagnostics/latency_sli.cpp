#include "latency_sli.h"

#include <cloud/blockstore/libs/service/latency.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <util/generic/vector.h>

#include <algorithm>
#include <limits>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool CountOperations(const TLatencyCounts& counts, ui64& total)
{
    total = 0;
    for (const auto value:
         {counts.Good, counts.Bad, counts.Unknown, counts.InvalidClientRequest,
          counts.ClientCancellation, counts.ClientLimit})
    {
        if (value > static_cast<ui64>(std::numeric_limits<i64>::max()) - total) {
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

TLatencyThresholds::TLatencyThresholds(
    const NProto::TDiagnosticsConfig& config)
    : Config(config)
    , Valid(ValidateLatencyThresholds(config))
{}

ui32 TLatencyThresholds::GetVersion() const
{
    return Valid ? Config.GetLatencyThresholdVersion() : 0;
}

TMaybe<TDuration> TLatencyThresholds::Find(
    ui32 mediaKind, EBlockStoreRequest requestType, ui64 bytes) const
{
    if (!Valid)
    {
        return {};
    }
    requestType = TranslateLocalRequestType(requestType);
    if (requestType != EBlockStoreRequest::ReadBlocks &&
        requestType != EBlockStoreRequest::WriteBlocks) {
            return {};
        }
        const bool write = requestType == EBlockStoreRequest::WriteBlocks;
    for (const auto& row: Config.GetLatencyThresholds()) {
        if (row.GetMediaKind() == mediaKind && row.GetWrite() == write &&
            row.GetStartBytes() <= bytes && bytes < row.GetEndBytes())
        {
            return TDuration::MicroSeconds(row.GetThresholdUs());
        }
    }
    return {};
}

TLatencyCounts EvaluateLatency(
    const NProto::TDiagnosticsConfig& config, ui32 mediaKind,
    EBlockStoreRequest requestType, ui64 originalRequestBytes,
    TMaybe<TDuration> totalTime, const NProto::TLatencyDiagnostics* diagnostics,
    bool success)
{
    return EvaluateLatency(TLatencyThresholds(config), mediaKind, requestType,
                           originalRequestBytes, totalTime, diagnostics,
                           success);
}

TMaybe<TDuration> CalculateLatency(
    const NProto::TLatencyDiagnostics& diagnostics, TDuration totalTime)
{
    return ReadLatencySummary(diagnostics, totalTime);
}

TLatencyCounts EvaluateLatency(
    const TLatencyThresholds& thresholds, ui32 mediaKind,
    EBlockStoreRequest requestType, ui64 originalRequestBytes,
    TMaybe<TDuration> totalTime, const NProto::TLatencyDiagnostics* diagnostics,
    bool success)
{
    const auto threshold =
        thresholds.Find(mediaKind, requestType, originalRequestBytes);
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
    if (!CheckpointHealthy) {
        return {};
    }
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
        const bool fresh =
            batch.CapturedAt <= now && now - batch.CapturedAt <= maxAge;
        return {.Status = fresh ? ELatencyBatchStatus::Duplicate
                            : ELatencyBatchStatus::Invalid};
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
        Established && sameGeneration && batch.Sequence - Last->Sequence == 1;
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
    if (!SaveCheckpoint(batch)) {
        CheckpointHealthy = false;
        return {};
    }
    Established = true;
    Last = batch;
    return result;
}

}   // namespace NCloud::NBlockStore
