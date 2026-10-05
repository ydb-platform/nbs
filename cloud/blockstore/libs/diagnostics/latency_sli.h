#pragma once

#include <cloud/blockstore/config/diagnostics.pb.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/public/api/protos/latency.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

#include <atomic>
#include <mutex>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

inline constexpr ui32 LatencyDiagnosticsVersion = 1;

struct TLatencyCounts
{
    ui64 Good = 0;
    ui64 Bad = 0;
    ui64 Unknown = 0;
    ui64 InvalidClientRequest = 0;
    ui64 ClientCancellation = 0;
    ui64 ClientLimit = 0;
};

// Replays the observed DAG with QUOTA durations removed. Work, dependency
// edges, launch gaps and the tail to the final response remain unchanged.
TMaybe<TDuration> CalculateLatency(
    const NProto::TLatencyDiagnostics& diagnostics, TDuration totalTime);

bool ValidateLatencyThresholds(const NProto::TDiagnosticsConfig& config);

TLatencyCounts EvaluateLatency(
    const NProto::TDiagnosticsConfig& config, ui32 mediaKind,
    EBlockStoreRequest requestType, ui64 originalRequestBytes,
    TMaybe<TDuration> totalTime, const NProto::TLatencyDiagnostics* diagnostics,
    bool success);

struct TLatencyRequestState
{
    ui64 StartedCycles = 0;
    ui64 OriginalRequestBytes = 0;
    std::atomic<bool> Completed = false;
};

struct TLatencyCounters
{
    NMonitoring::TDynamicCounters::TCounterPtr Good;
    NMonitoring::TDynamicCounters::TCounterPtr Total;
    NMonitoring::TDynamicCounters::TCounterPtr Unknown;
    NMonitoring::TDynamicCounters::TCounterPtr InvalidClientRequest;
    NMonitoring::TDynamicCounters::TCounterPtr ClientCancellation;
    NMonitoring::TDynamicCounters::TCounterPtr ClientLimit;

    void Register(NMonitoring::TDynamicCounters& counters);
    void Add(const TLatencyCounts& counts);
};

// Optional external-vhost diagnostics. The old count/bytes/errors/histogram
// batch remains untouched. These counters are cumulative since generation
// start; generation must increase durably on each producer restart.
struct TLatencyBatch
{
    ui32 Version = 0;
    ui32 ThresholdVersion = 0;
    ui64 Generation = 0;
    ui64 Sequence = 0;
    TInstant CapturedAt;
    TLatencyCounts Read;
    TLatencyCounts Write;
};

enum class ELatencyBatchStatus
{
    Accepted,
    Unknown,
    Duplicate,
    Invalid,
    Missing,
    Disabled
};

struct TLatencyBatchResult
{
    ELatencyBatchStatus Status = ELatencyBatchStatus::Invalid;
    TLatencyCounts Read;
    TLatencyCounts Write;
};

class TLatencyBatchTracker
{
private:
    std::mutex Lock;
    TMaybe<TLatencyBatch> Last;

public:
    TLatencyBatchResult Update(const TLatencyBatch& batch,
                               ui32 thresholdVersion, TInstant now,
                               TDuration maxAge);
};

}   // namespace NCloud::NBlockStore
