#pragma once

#include "request_timing.h"
#include "request_timing_journal.h"

#include <exception>
#include <initializer_list>
#include <memory>
#include <mutex>

namespace NCloud {

class TRequestTimingGraphBuilder;

// Record compact observations on the request path. Graph construction belongs
// to the offline reader, or to a lossless fallback for unusually long journals.
class TRequestTimingCollector
{
public:
    static constexpr ui32 InvalidId = std::numeric_limits<ui32>::max();
    static constexpr size_t MaxEvents = 4096;

private:
    struct TNodeMeta
    {
        ui64 End = 0;
        ui32 Part = InvalidId;
    };

    struct TSpanMeta
    {
        ui64 Begin = 0;
        ui32 Last = InvalidId;
        ui32 ParentFork = InvalidId;
        ui32 ParentPart = InvalidId;
        bool Suspended = false;
        bool Closed = false;
        bool Stopped = false;
    };

    std::mutex Mutex;
    TVector<ui64> Journal;
    TVector<TNodeMeta> Nodes;
    TVector<TSpanMeta> Spans;
    std::unique_ptr<TRequestTimingGraphBuilder> Builder;
    size_t Events = 0;
    bool Frozen = false;
    const ui64 RequestId;
    ui64 FrozenEnd = 0;
    ui32 FrozenCategories = 0;
    ui32 FrozenError = 0;
    TString Snapshot;
    std::once_flag SnapshotOnce;
    std::exception_ptr SnapshotException;

    size_t RemainingEvents() const;
    bool BeginRecord(size_t wordCount, size_t extraNodes, size_t extraSpans);
    void Append(ERequestTimingOp op, std::initializer_list<ui64> args);
    ui32 Seal(ui32 span, ui64 now);
    void Materialize();
    void BuildSnapshot();

public:
    explicit TRequestTimingCollector(ui64 requestId = 0);
    ~TRequestTimingCollector();

    static ui64 GetThreadEventSequence();
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
    // Requires Freeze; no replay, graph construction or JSON on the usual path.
    void FillTrace(NProto::TRequestTimingTrace& trace);
};

}   // namespace NCloud
