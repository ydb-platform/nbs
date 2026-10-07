#pragma once

#include "context.h"

#include <cloud/blockstore/public/api/protos/latency.pb.h>

#include <util/datetime/cputimer.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <memory>
#include <mutex>

namespace NCloud::NBlockStore {

inline constexpr ui32 LatencyVersion = 2;
inline constexpr size_t MaxLatencyChildren = 4096;
inline constexpr size_t MaxLatencyQuotaIntervals = 2048;

// Include the observer's transport/queue/tail time outside the producer scope.
// Missing, incompatible or inconsistent summaries have no usable latency.
TMaybe<TDuration> ReadLatencySummary(
    const NProto::TLatencyDiagnostics& summary, TDuration total);

// Children are either independent parallel branches or sequential attempts.
// A child exports observed and adjusted duration; its internal dependency
// structure has already been resolved at its own boundary.
class TLatencyOperation
{
    struct TChild
    {
        ui64 Started;
        ui64 Finished;
        ui64 RemovedUs;
    };

    struct TQuota
    {
        ui64 Started;
        ui64 Finished;
    };

    const ui64 Started;
    const bool Parallel;
    mutable std::mutex Lock;
    // Parallel callbacks reduce directly into these scalars without allocation.
    bool HasChildren = false;
    ui64 LastChildFinished = 0;
    ui64 AdjustedChildFinishUs = 0;
    // Sequential attempts retain only bounds, to validate out-of-order callbacks.
    TVector<TChild> Children;
    bool ChildrenOrdered = true;
    TVector<TQuota> Quota;
    bool QuotaOrdered = true;
    bool Complete = true;

public:
    explicit TLatencyOperation(bool parallel = false,
                               ui64 started = GetCycleCount());
    ui64 GetStartedCycles() const;
    void AddChild(ui64 started, ui64 finished,
                  const NProto::TLatencyDiagnostics& summary);
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
    if (context && context->IsLatencyEnabled()) {
        // This leaf finishes synchronously; no shared lifetime is needed.
        TLatencyOperation operation;
        *response.MutableHeaders()->MutableLatency() = operation.FinishLeaf();
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
