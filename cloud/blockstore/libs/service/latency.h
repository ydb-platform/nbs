#pragma once

#include "context.h"

#include <cloud/blockstore/public/api/protos/latency.pb.h>

#include <util/datetime/cputimer.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <memory>
#include <mutex>

namespace NCloud::NBlockStore {

inline constexpr ui32 LatencyVersion = 1;
inline constexpr size_t MaxLatencyNodes = 4096;
inline constexpr size_t MaxLatencyEdges = 16384;

TMaybe<TDuration> ReplayLatencyGraph(const NProto::TLatencyDiagnostics& graph,
                                     TDuration total);

// One observed execution scope. Parallel children fork from the scope entry;
// sequential children depend on the preceding child's completion. All local
// gaps and transport time remain SERVICE work. No legacy clocks are changed.
class TLatencyOperation
{
    struct TChild
    {
        ui64 Started;
        ui64 Finished;
        NProto::TLatencyDiagnostics Graph;
    };

    struct TQuota
    {
        ui64 Started;
        ui64 Finished;
        NProto::TLatencyDiagnostics::EQuotaReason Reason;
    };

    const ui64 Started;
    const bool Parallel;
    mutable std::mutex Lock;
    TVector<TChild> Children;
    TVector<TQuota> Quota;
    size_t Nodes = 0;
    size_t Edges = 0;
    bool Complete = true;

public:
    explicit TLatencyOperation(bool parallel = false,
                               ui64 started = GetCycleCount());
    ui64 GetStartedCycles() const;
    void AddChild(ui64 started, ui64 finished,
                  const NProto::TLatencyDiagnostics& graph);
    void AddQuota(ui64 started, ui64 finished,
                  NProto::TLatencyDiagnostics::EQuotaReason reason);
    void EndQuota(ui64 finished);
    void Invalidate();
    NProto::TLatencyDiagnostics Finish(ui64 finished = GetCycleCount()) const;
    NProto::TLatencyDiagnostics FinishLeaf(
        ui64 finished = GetCycleCount(),
        NProto::TLatencyDiagnostics::EExclusion exclusion =
            NProto::TLatencyDiagnostics::NONE) const;
};

using TLatencyOperationPtr = std::shared_ptr<TLatencyOperation>;

struct TLatencyVolumeRequest
{
    TLatencyOperation Operation;
    bool Waiting = false;
    ui64 WaitStarted = 0;
};

inline TLatencyOperationPtr StartLatency(const TCallContextPtr& context,
                                         bool parallel = false)
{
    return context && context->IsLatencyEnabled()
               ? std::make_shared<TLatencyOperation>(parallel)
               : nullptr;
}

template <typename TResponse>
void CollectLatency(const TLatencyOperationPtr& operation, ui64 started,
                    const TResponse& response)
{
    if (operation) {
        operation->AddChild(started, GetCycleCount(),
                            response.GetHeaders().GetLatency());
    }
}

template <typename TResponse>
void FinishLatency(const TLatencyOperationPtr& operation, TResponse& response)
{
    if (operation) {
        *response.MutableHeaders()->MutableLatency() = operation->Finish();
    }
}

template <typename TResponse>
TResponse WithLatencyLeaf(const TCallContextPtr& context, TResponse response)
{
    if (auto operation = StartLatency(context)) {
        *response.MutableHeaders()->MutableLatency() = operation->FinishLeaf();
    }
    return response;
}

// Only use at a known leaf with no purchased-profile limiter below it.
// Missing telemetry from a remote peer must go through CollectLatency instead.
template <typename TResponse>
void FinishLatencyLeaf(const TLatencyOperationPtr& operation,
                       TResponse& response)
{
    if (operation) {
        *response.MutableHeaders()->MutableLatency() = operation->FinishLeaf();
    }
}

}   // namespace NCloud::NBlockStore
