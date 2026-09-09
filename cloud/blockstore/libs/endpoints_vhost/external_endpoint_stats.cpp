#include "external_endpoint_stats.h"

#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/deprecated/atomic/atomic.h>

#include <limits>
#include <optional>
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

struct TLatencyCounters
{
    ui64 Good = 0;
    ui64 Bad = 0;
    ui64 Skipped = 0;
};

struct TLatencyCountersBatch
{
    TLatencyCounters Read;
    TLatencyCounters Write;
};

bool TryReadLatencyCounters(
    const NJson::TJsonValue& value,
    TLatencyCounters& counters)
{
    auto read = [&] (TStringBuf key, ui64& result) {
        unsigned long long parsed = 0;
        if (!value[key].GetUInteger(&parsed)) {
            return false;
        }
        result = parsed;
        return true;
    };

    if (!value.IsMap() || !read("good", counters.Good) ||
        !read("bad", counters.Bad) || !read("skipped", counters.Skipped))
    {
        return false;
    }

    return true;
}

bool TryAddCounterValue(ui64 value, ui64 limit, ui64& sum)
{
    if (value > limit - sum) {
        return false;
    }
    sum += value;
    return true;
}

bool FitsDynamicCounters(const TLatencyCountersBatch& batch)
{
    // Dynamic counters use signed TAtomicBase. Validate the complete batch,
    // including Read+Write aggregation into the same exported series, before
    // performing any update. This also guarantees good+bad cannot overflow
    // the ui64 intermediate in RecordLatencyBatch.
    constexpr ui64 limit =
        static_cast<ui64>(std::numeric_limits<TAtomicBase>::max());

    ui64 good = 0;
    ui64 total = 0;
    ui64 skipped = 0;
    return TryAddCounterValue(batch.Read.Good, limit, good) &&
        TryAddCounterValue(batch.Write.Good, limit, good) &&
        TryAddCounterValue(batch.Read.Good, limit, total) &&
        TryAddCounterValue(batch.Read.Bad, limit, total) &&
        TryAddCounterValue(batch.Write.Good, limit, total) &&
        TryAddCounterValue(batch.Write.Bad, limit, total) &&
        TryAddCounterValue(batch.Read.Skipped, limit, skipped) &&
        TryAddCounterValue(batch.Write.Skipped, limit, skipped);
}

std::optional<TLatencyCountersBatch> TryReadLatencyCountersBatch(
    const NJson::TJsonValue& stats)
{
    if (!stats.Has("latency_counters")) {
        return std::nullopt;
    }

    const auto& latencyCounters = stats["latency_counters"];
    unsigned long long version = 0;
    TLatencyCountersBatch batch;
    if (!latencyCounters.IsMap() ||
        !latencyCounters["version"].GetUInteger(&version) || version != 1 ||
        !TryReadLatencyCounters(latencyCounters["read"], batch.Read) ||
        !TryReadLatencyCounters(latencyCounters["write"], batch.Write) ||
        !FitsDynamicCounters(batch))
    {
        return std::nullopt;
    }

    return batch;
}

void RecordLatencyBatch(
    IServerStats& serverStats,
    EBlockStoreRequest kind,
    const TLatencyCounters& counters,
    const TString& clientId,
    const TString& diskId)
{
    TMetricRequest request{kind};
    serverStats.PrepareMetricRequest(
        request,
        clientId,
        diskId,
        0,      // startIndex
        0,      // requestBytes
        false); // unaligned

    serverStats.RecordLatencyBatch(
        request,
        counters.Good,
        counters.Bad,
        counters.Skipped);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TEndpointStats::Update(const NJson::TJsonValue& stats)
{
    // Parse the whole versioned payload before recording either direction:
    // malformed/unknown versions are ignored atomically and are never
    // reconstructed from the legacy count/error/time histograms below.
    const auto latencyCounters = TryReadLatencyCountersBatch(stats);

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

    if (latencyCounters) {
        RecordLatencyBatch(
            *ServerStats,
            EBlockStoreRequest::ReadBlocks,
            latencyCounters->Read,
            ClientId,
            DiskId);
        RecordLatencyBatch(
            *ServerStats,
            EBlockStoreRequest::WriteBlocks,
            latencyCounters->Write,
            ClientId,
            DiskId);
    }

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
