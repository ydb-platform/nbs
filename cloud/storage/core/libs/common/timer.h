#pragma once

#include "public.h"

#include <util/datetime/base.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct ITimer
{
    virtual ~ITimer() = default;

    virtual TInstant Now() = 0;

    virtual void Sleep(TDuration duration) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ITimerPtr CreateWallClockTimer();

ITimerPtr CreateCpuCycleTimer();

}   // namespace NCloud
