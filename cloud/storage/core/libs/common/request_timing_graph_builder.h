#pragma once

#include "request_timing.h"

#include <exception>
#include <memory>
#include <mutex>

namespace NCloud {

// The original graph construction semantics, shared by offline replay and
// lossless journal overflow. Times are already rounded monotonic microseconds
// relative to the request start; no clock conversion happens in this builder.
class TRequestTimingGraphBuilder
{
public:
    static constexpr ui32 InvalidId = std::numeric_limits<ui32>::max();
    static constexpr size_t MaxEvents = 4096;

private:
    struct TSpan
    {
        TTimingStage Current;
        ui32 Last = InvalidId;
        ui32 ParentFork = InvalidId;
        ui32 ParentPart = InvalidId;
        bool Suspended = false;
        bool Closed = false;
        bool Stopped = false;
    };

    std::mutex Mutex;
    TVector<TTimingStage> Stages;
    TVector<ui32> StageParts;
    TVector<bool> DiagnosticOnly;
    TVector<TSpan> Spans;
    size_t Events = 0;
    TString Failure;
    bool Frozen = false;
    TString Snapshot;
    std::once_flag SnapshotOnce;
    std::exception_ptr SnapshotException;
    std::once_flag FinalizeOnce;
    std::exception_ptr FinalizeException;
    const ui64 RequestId;
    ui64 FrozenEnd = 0;
    ui32 FrozenCategories = 0;
    ui32 FrozenCompletion = InvalidId;
    ui32 FrozenError = 0;

    ui32 Seal(ui32 span, ui64 now, bool continueSpan);
    void BuildSnapshot();
    void Finalize();

public:
    explicit TRequestTimingGraphBuilder(ui64 requestId = 0);

    ui32 Fork(ui32 span, ui64 now);
    ui32 Start(ui32 fork, ui64 now);
    void Finish(ui32 span, ui64 now);
    void Join(ui32 span, const TVector<ui32>& children, ui64 now);
    void Cancel(ui32 span, ui64 now);
    void Wait(ui32 span, ui32 categories, ui64 begin, ui64 end);
    void Missing(ui32 span, ui32 categories, ui64 duration);
    void Incomplete(TString reason);
    void Freeze(ui64 now, ui32 categories, ui32 errorCode = 0);
    TString Complete(ui64 now, ui32 categories, ui32 errorCode = 0);
    // Requires Freeze; records observations without evaluating or rendering
    // JSON.
    void FillTrace(NProto::TRequestTimingTrace& trace);
};

}   // namespace NCloud
