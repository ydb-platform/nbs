#pragma once

#include <util/datetime/cputimer.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

class TAdaptiveWait
{
private:
    TDuration SleepTime;
    TDuration WaitBeforeSleep;
    ui64 ResetCycles;

public:
    TAdaptiveWait() noexcept
        : TAdaptiveWait(
            TDuration::MicroSeconds(100),
            TDuration::Seconds(5))
    {
    }

    TAdaptiveWait(
            TDuration sleepTime,
            TDuration waitBeforeSleep) noexcept
        : SleepTime(sleepTime)
        , WaitBeforeSleep(waitBeforeSleep)
    {
        Reset();
    }

    void Sleep() noexcept
    {
        auto now = GetCycleCount();
        auto usecSinceReset =
            CyclesToDurationSafe(now - ResetCycles).MicroSeconds();
        if (usecSinceReset >= WaitBeforeSleep.MicroSeconds()) {
            ::Sleep(SleepTime);
        }
    }

    void Reset() noexcept
    {
        ResetCycles = GetCycleCount();
    }
};

}   // namespace NCloud::NBlockStore::NRdma
