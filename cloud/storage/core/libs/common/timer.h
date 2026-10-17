#pragma once

#include "public.h"

#include <util/datetime/base.h>

#include <atomic>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct ITimer
{
    static constexpr TDuration SleepSlice = TDuration::MilliSeconds(100);

    virtual ~ITimer() = default;

    virtual TInstant Now() = 0;

    virtual void Sleep(TDuration duration) = 0;

    // Returns early once @p cancelled is set; by default in slices of
    // SleepSlice over Sleep(duration).
    virtual void Sleep(TDuration duration, const std::atomic<bool>& cancelled);
};

////////////////////////////////////////////////////////////////////////////////

ITimerPtr CreateWallClockTimer();

ITimerPtr CreateCpuCycleTimer();

}   // namespace NCloud
