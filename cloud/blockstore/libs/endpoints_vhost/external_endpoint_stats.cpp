#include "external_endpoint_stats.h"

#include <cloud/blockstore/libs/diagnostics/server_stats.h>

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <limits>
#include <type_traits>

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename F>
auto GetHist(const NJson::TJsonValue& value, F&& func)
{
    TVector<std::pair<std::invoke_result_t<F, ui64>, ui64>> hist;

    if (!value.IsArray()) {
        return hist;
    }

    const auto& array = value.GetArray();
    hist.reserve(array.size());

    for (const auto& v: array) {
        if (!v.IsArray()) {
            continue;
        }

        const auto& bucket = v.GetArray();

        hist.emplace_back(func(bucket[0].GetUInteger()),
                          bucket[1].GetUInteger());
    }

    return hist;
}

void BatchCompleted(IServerStats& serverStats, EBlockStoreRequest kind,
                    const NJson::TJsonValue& requestStats,
                    const TString& clientId, const TString& diskId)
{
    TMetricRequest request{kind};
    serverStats.PrepareMetricRequest(
        request,
        clientId,
        diskId,
        0,        // startIndex
        0,        // requestBytes
        false);   // unaligned

    auto times = GetHist(requestStats["times"],
                         [](ui64 us) { return TDuration::MicroSeconds(us); });

    auto sizes = GetHist(requestStats["sizes"], [](ui64 size) { return size; });

    serverStats.BatchCompleted(
        request,
        requestStats["count"].GetUInteger(),
        requestStats["bytes"].GetUInteger(),
        requestStats["errors"].GetUInteger() +
            requestStats["encryptor_errors"].GetUInteger(), times, sizes);
}

bool ReadUnsigned(const NJson::TJsonValue& value, ui64& number)
{
    // Accept only integral non-negative JSON values. Missing and malformed
    // fields must not become a zero-valued diagnostic sample.
    if (value.IsUInteger()) {
        number = value.GetUInteger();
        return true;
    }
    if (value.IsInteger() && value.GetInteger() >= 0) {
        number = value.GetInteger();
        return true;
    }
    return false;
}

bool ReadCounts(const NJson::TJsonValue& value, TLatencyCounts& counts)
{
    return ReadUnsigned(value["good"], counts.Good) &&
           ReadUnsigned(value["bad"], counts.Bad) &&
           ReadUnsigned(value["unknown"], counts.Unknown) &&
           ReadUnsigned(value["invalid_client_request"],
                        counts.InvalidClientRequest) &&
           ReadUnsigned(value["client_cancellation"],
                        counts.ClientCancellation) &&
           ReadUnsigned(value["client_limit"], counts.ClientLimit);
}

TMaybe<TLatencyBatch> ReadLatencyBatch(const NJson::TJsonValue& value)
{
    TLatencyBatch batch;
    ui64 version = 0;
    ui64 thresholdVersion = 0;
    ui64 capturedAt = 0;
    if (!ReadUnsigned(value["version"], version) ||
        !ReadUnsigned(value["threshold_version"], thresholdVersion) ||
        version > std::numeric_limits<ui32>::max() ||
        thresholdVersion > std::numeric_limits<ui32>::max() ||
        !ReadUnsigned(value["generation"], batch.Generation) ||
        !ReadUnsigned(value["sequence"], batch.Sequence) ||
        !ReadUnsigned(value["captured_at_us"], capturedAt) ||
        !ReadCounts(value["read"], batch.Read) ||
        !ReadCounts(value["write"], batch.Write))
    {
        return {};
    }
    batch.Version = version;
    batch.ThresholdVersion = thresholdVersion;
    batch.CapturedAt = TInstant::MicroSeconds(capturedAt);
    return batch;
}

void ReportLatency(TEndpointStats& endpoint, EBlockStoreRequest kind,
                   const TLatencyCounts& counts, ELatencyBatchStatus status)
{
    TMetricRequest request{kind};
    endpoint.ServerStats->PrepareMetricRequest(request, endpoint.ClientId,
                                               endpoint.DiskId, 0, 0, false);
    endpoint.ServerStats->LatencyBatchCompleted(request, counts, status);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TEndpointStats::Update(const NJson::TJsonValue& stats)
{
    BatchCompleted(*ServerStats, EBlockStoreRequest::ReadBlocks, stats["read"],
                   ClientId, DiskId);

    BatchCompleted(*ServerStats, EBlockStoreRequest::WriteBlocks,
                   stats["write"], ClientId, DiskId);

    // Supplement only: retain legacy batch delivery even if diagnostics are
    // missing, stale, incompatible or repeated.
    const auto batch = ReadLatencyBatch(stats["latency"]);
    auto result = ServerStats->UpdateLatencyBatch(*LatencyTracker,
                                                  batch ? &*batch : nullptr);
    if (stats.Has("latency") && !batch &&
        result.Status == ELatencyBatchStatus::Missing)
    {
        result.Status = ELatencyBatchStatus::Invalid;
    }
    if (result.Status != ELatencyBatchStatus::Disabled) {
        ReportLatency(*this, EBlockStoreRequest::ReadBlocks, result.Read,
                      result.Status);
        // Batch telemetry is per volume, so report its status only once.
        ReportLatency(*this, EBlockStoreRequest::WriteBlocks, result.Write,
                      ELatencyBatchStatus::Accepted);
    }

    // Report critical events
    if (stats.Has("crit_events")) {
        for (const auto& event: stats["crit_events"].GetArray()) {
            ReportCriticalEvent(
                GetCriticalEventFullName(event["name"].GetString()),
                event["message"].GetString(), false   // verifyDebug
            );
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
