#include "external_endpoint_stats.h"

#include <cloud/blockstore/libs/diagnostics/latency_sli.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

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
        if (!v.IsArray() || v.GetArray().size() != 2) {
            continue;
        }

        const auto& bucket = v.GetArray();

        hist.emplace_back(
            func(bucket[0].GetUInteger()),
            bucket[1].GetUInteger());
    }

    return hist;
}

void BatchCompleted(
    IServerStats& serverStats,
    EBlockStoreRequest kind,
    const NJson::TJsonValue& requestStats,
    const TString& clientId,
    const TString& diskId)
{
    TMetricRequest request {kind};
    serverStats.PrepareMetricRequest(
        request,
        clientId,
        diskId,
        0,      // startIndex
        0,      // requestBytes
        false); // unaligned

    auto times = GetHist(requestStats["times"], [] (ui64 us) {
        return TDuration::MicroSeconds(us);
    });

    auto sizes = GetHist(requestStats["sizes"], [] (ui64 size) {
        return size;
    });

    serverStats.BatchCompleted(
        request,
        requestStats["count"].GetUInteger(),
        requestStats["bytes"].GetUInteger(),
        requestStats["errors"].GetUInteger() +
            requestStats["encryptor_errors"].GetUInteger(),
        times,
        sizes);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TEndpointStats::RegisterExternalStats()
{
    TMetricRequest request{EBlockStoreRequest::ReadBlocks};
    ServerStats->PrepareMetricRequest(request, ClientId, DiskId, 0, 0, false);
    if (!request.VolumeInfo || !request.VolumeInfo->GetLatencySli()) {
        return;
    }
    // The child receives its threshold table when the endpoint starts. Keep
    // its decisions under that media type, including after a child restart.
    ExternalVolume = std::move(request.VolumeInfo);
    if (SocketPath) {
        auto group = ExternalVolume->GetLatencySli()->GetExternalEndpointGroup(
            SocketPath);
        LastFreshSnapshot =
            group->GetCounter("LatencyLastFreshSnapshotSeconds");
        IncompleteStreams = group->GetCounter("LatencyIncompleteStreams", true);
    }
}

void TEndpointStats::FinishExternalStats()
{
    if (IncompleteStreams && !CompleteFinalSnapshot) {
        ++*IncompleteStreams;
    }
}

void TEndpointStats::UpdateLatency(const NJson::TJsonValue& stats)
{
    if (!ExternalVolume) {
        RegisterExternalStats();
    }
    auto* counters = ExternalVolume ? ExternalVolume->GetLatencySli() : nullptr;
    const auto& meta = stats["latency_sli"];
    const auto version = meta["version"].GetUInteger();
    const bool matching =
        counters && version == 3 && meta["config_id"].IsString() &&
        meta["config_id"].GetString() == counters->GetConfigFingerprint();

    if (version == 3 &&
        (!meta["fresh"].IsUInteger() || meta["fresh"].GetUInteger() != 1))
    {
        // A backend timeout returns the previous snapshot. The next fresh
        // cumulative report will cover the missing interval.
        return;
    }
    bool complete = matching;

    // Old children count successful AIO reads before decryption, but add a
    // timing sample only after decryption succeeds. Recover that split when
    // the legacy histogram is available; new children report final outcomes.
    const ui64 encryptorErrors = stats["encryptor_errors"].GetUInteger();
    ui64 readEncryptorErrors = 0;
    const auto& times = stats["read"]["times"];
    if (version != 2 && version != 3 && encryptorErrors && times.IsArray()) {
        ui64 successfulReads = 0;
        bool valid = true;
        const ui64 reads = stats["read"]["count"].GetUInteger();
        for (const auto& bucket: times.GetArray()) {
            if (!bucket.IsArray() || bucket.GetArray().size() != 2 ||
                !bucket[1].IsUInteger() ||
                bucket[1].GetUInteger() > reads - successfulReads)
            {
                valid = false;
                break;
            }
            successfulReads += bucket[1].GetUInteger();
        }
        if (valid) {
            readEncryptorErrors = Min(encryptorErrors, reads - successfulReads);
        }
    }

    for (size_t write = 0; write < 2; ++write) {
        const auto& value = stats[write ? "write" : "read"];
        std::array<ui64, 3> current = {
            value["latency_good"].GetUInteger(),
            value["latency_bad"].GetUInteger(),
            value["latency_unknown"].GetUInteger()};
        auto& previous = LatencyPrevious[write];
        const bool present = value["latency_good"].IsUInteger() &&
            value["latency_bad"].IsUInteger() && value["latency_unknown"].IsUInteger();
        // Versions 2 and 3 both publish cumulative counts. Only v3 uses
        // the throttle-excluding model and the same eligibility for failures.
        const bool cumulative = version == 2 || version == 3;
        if (cumulative && !present) {
            // A later cumulative snapshot can recover this direction.
            complete = false;
            continue;
        }

        if (!cumulative || LatencyNeedsBaseline[write]) {
            // Old children report per-interval counts. Once consumed, do
            // not replay the same operations when cumulative data resumes.
            if (counters) {
                ui64 unknown = 0;
                if (value["final_count"].IsUInteger() &&
                    value["final_errors"].IsUInteger())
                {
                    unknown = value["final_count"].GetUInteger() +
                              value["final_errors"].GetUInteger();
                } else {
                    // Legacy reads include failed decryptions in "count";
                    // writes that failed encryption never reached the device.
                    unknown = value["count"].GetUInteger() +
                              value["errors"].GetUInteger();
                    if (write) {
                        unknown += encryptorErrors - readEncryptorErrors;
                    }
                }
                counters->Add(write, 0, 0, unknown);
            }
            LatencyNeedsBaseline[write] = !cumulative;
            if (cumulative) {
                previous = current;
            }
            complete = false;
            continue;
        }
        if (current[0] < previous[0] || current[1] < previous[1] ||
            current[2] < previous[2])
        {
            // A reader belongs to one child: counters cannot reset. Ignore a
            // regressing snapshot without moving the baseline backwards.
            complete = false;
            continue;
        }
        const ui64 good = current[0] - previous[0];
        const ui64 bad = current[1] - previous[1];
        const ui64 unknown = current[2] - previous[2];
        previous = current;
        if (counters) {
            if (matching) {
                counters->Add(write, good, bad, unknown);
            } else {
                counters->Add(write, 0, 0, good + bad + unknown);
            }
        }
    }

    if (complete && LastFreshSnapshot) {
        *LastFreshSnapshot = TInstant::Now().Seconds();
    }
    CompleteFinalSnapshot = complete && stats["final"].GetUInteger() == 1;
}

void TEndpointStats::Update(const NJson::TJsonValue& stats)
{
    UpdateLatency(stats);

    BatchCompleted(
        *ServerStats,
        EBlockStoreRequest::ReadBlocks,
        stats["read"],
        ClientId,
        DiskId);

    BatchCompleted(
        *ServerStats,
        EBlockStoreRequest::WriteBlocks,
        stats["write"],
        ClientId,
        DiskId);

    // Report critical events
    if (stats.Has("crit_events")) {
        for (const auto& event: stats["crit_events"].GetArray()) {
            ReportCriticalEvent(
                GetCriticalEventFullName(event["name"].GetString()),
                event["message"].GetString(),
                false   // verifyDebug
            );
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
