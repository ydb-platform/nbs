#pragma once

#include <util/datetime/cputimer.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

class TAdaptiveWait
{
private:
    TDuration SleepDuration;
    TDuration SleepDelay;
    ui64 ResetCycles;

public:
    TAdaptiveWait(TDuration sleepDuration, TDuration sleepDelay) noexcept
        : SleepDuration(sleepDuration)
        , SleepDelay(sleepDelay)
    {
        Reset();
    }

    void Sleep() noexcept
    {
        auto now = GetCycleCount();
        auto usecSinceReset =
            CyclesToDurationSafe(now - ResetCycles).MicroSeconds();
        if (usecSinceReset >= SleepDelay.MicroSeconds()) {
            ::Sleep(SleepDuration);
        }
    }

    void Reset() noexcept
    {
        ResetCycles = GetCycleCount();
    }
};

}   // namespace NCloud::NBlockStore::NRdma
