#include "timer.h"

#include <util/datetime/cputimer.h>
#include <util/generic/utility.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWallClockTimer final
    : public ITimer
{
public:
    TInstant Now() override
    {
        return TInstant::Now();
    }

    void Sleep(TDuration duration) override
    {
        ::Sleep(duration);
    }
};

////////////////////////////////////////////////////////////////////////////////

TInstant InitTime = TInstant::Now();
ui64 InitCycleCount = GetCycleCount();

class TCpuCycleTimer final
    : public ITimer
{
    TInstant Now() override
    {
        return InitTime + CyclesToDurationSafe(GetCycleCount() - InitCycleCount);
    }

    void Sleep(TDuration duration) override
    {
        ::Sleep(duration);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void ITimer::Sleep(TDuration duration, const std::atomic<bool>& cancelled)
{
    while (duration && !cancelled.load(std::memory_order_acquire)) {
        const TDuration slice = Min(duration, SleepSlice);
        Sleep(slice);
        duration -= slice;
    }
}

ITimerPtr CreateWallClockTimer()
{
    return std::make_shared<TWallClockTimer>();
}

ITimerPtr CreateCpuCycleTimer()
{
    return std::make_shared<TCpuCycleTimer>();
}

}   // namespace NCloud
