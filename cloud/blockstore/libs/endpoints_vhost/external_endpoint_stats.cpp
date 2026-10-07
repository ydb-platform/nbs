#include "external_endpoint_stats.h"

#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/latency_sli.h>
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
        if (!v.IsArray()) {
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

void TEndpointStats::UpdateLatency(const NJson::TJsonValue& stats)
{
    const auto& meta = stats["latency_sli"];
    const auto isCount = [](const NJson::TJsonValue& value) {
        return value.IsUInteger() ||
            (value.IsInteger() && value.GetInteger() >= 0);
    };
    const ui64 sequence = meta["sequence"].GetUInteger();
    const auto epoch = meta["epoch"].GetString();
    const bool versioned = meta["version"].GetUInteger() == 1 &&
        isCount(meta["sequence"]) && sequence && epoch &&
        meta["fresh"].IsBoolean();
    if (versioned) {
        if ((LatencyEpoch && LatencyEpoch != epoch) ||
            sequence <= LatencySequence)
        {
            return;
        }
        if (!meta["fresh"].GetBoolean()) {
            return;
        }
        LatencyEpoch = epoch;
    }

    for (size_t write = 0; write < 2; ++write) {
        TMetricRequest request{write ? EBlockStoreRequest::WriteBlocks
                                    : EBlockStoreRequest::ReadBlocks};
        ServerStats->PrepareMetricRequest(
            request, ClientId, DiskId, 0, 0, false);
        auto* counters = request.VolumeInfo
            ? request.VolumeInfo->GetLatencySli() : nullptr;
        const auto& value = stats[write ? "write" : "read"];
        if (!versioned) {
            LatencyNeedsBaseline[write] = true;
            if (counters) {
                // Old producers have separate size/time histograms, which
                // cannot establish a per-request latency SLI decision.
                counters->Add(write, 0, 0,
                    value["count"].GetUInteger() + value["errors"].GetUInteger());
            }
            continue;
        }

        std::array<ui64, 3> current = {
            value["latency_good"].GetUInteger(),
            value["latency_bad"].GetUInteger(),
            value["latency_unknown"].GetUInteger()};
        auto& previous = LatencyPrevious[write];
        const bool present = isCount(value["latency_good"]) &&
            isCount(value["latency_bad"]) && isCount(value["latency_unknown"]);
        const bool valid = present && current[0] >= previous[0] &&
            current[1] >= previous[1] && current[2] >= previous[2];
        if (!valid || LatencyNeedsBaseline[write]) {
            // Do not turn an unsigned reset/underflow into a huge delta.
            // Re-establish a baseline; this batch cannot be classified.
            if (counters) {
                counters->Add(write, 0, 0,
                    value["count"].GetUInteger() + value["errors"].GetUInteger());
            }
            LatencyNeedsBaseline[write] = !present;
            if (present) {
                previous = current;
            }
            continue;
        }
        const ui64 good = current[0] - previous[0];
        const ui64 bad = current[1] - previous[1];
        const ui64 unknown = current[2] - previous[2];
        previous = current;
        if (counters) {
            const bool matching = meta["config"].GetString() ==
                counters->GetConfig().Serialize();
            // After a gap, preserve coverage without attributing old data to
            // the current observation window. No history is retained.
            if (matching && sequence == LatencySequence + 1) {
                counters->Add(write, good, bad, unknown);
            } else {
                counters->Add(write, 0, 0, good + bad + unknown);
            }
        }
    }
    if (versioned) {
        LatencySequence = sequence;
    }
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
