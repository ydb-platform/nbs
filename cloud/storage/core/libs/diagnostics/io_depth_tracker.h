#pragma once

#include <util/generic/guid.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <functional>
#include <memory>

namespace NCloud {

// Monotonic nanoseconds. Injected clocks must be safe for concurrent callers.
using TIoDepthClock = std::function<ui64()>;

struct TIoDepthLaneSnapshot
{
    ui64 Current = 0;
    // Cumulative request-microseconds within this source generation.
    ui64 IntegralUs = 0;
};

struct TIoDepthSnapshot
{
    TGUID Generation;
    // Source-local monotonic time; not comparable across generations/processes.
    ui64 TimestampNs = 0;
    // False after lost events, unbalanced completion, clock rollback or
    // overflow.
    bool Continuous = true;
    TVector<TIoDepthLaneSnapshot> Lanes;
};

class TIoDepthTracker
{
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    explicit TIoDepthTracker(ui32 laneCount, TIoDepthClock clock = {});

    ~TIoDepthTracker();

    TIoDepthTracker(const TIoDepthTracker&) = delete;
    TIoDepthTracker& operator=(const TIoDepthTracker&) = delete;

    // More than ui32::max active requests invalidates this generation.
    void Started(ui32 lane);
    // Returns false on underflow and permanently invalidates this generation.
    bool Completed(ui32 lane);

    // Credits pending requests to the observation time without resetting
    // totals.
    TIoDepthSnapshot Snapshot();
};

}   // namespace NCloud
