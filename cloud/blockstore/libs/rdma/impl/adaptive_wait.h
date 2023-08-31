#pragma once

#include <util/datetime/cputimer.h>

namespace NCloud::NBlockStore::NRdma {

class TAdaptiveWait {
private:
    ui32 SleepTimeUsec;
    ui32 WaitBeforeSleepUsec;
    ui64 ResetCycles;

public:
    TAdaptiveWait(
        ui32 sleepTimeUsec,
        ui32 waitBeforeSleepUsec) noexcept:
        SleepTimeUsec(sleepTimeUsec),
        WaitBeforeSleepUsec(waitBeforeSleepUsec)
    {
        Reset();
    }

    void Sleep() noexcept
    {
        auto now = GetCycleCount();
        auto usecSinceReset =
            CyclesToDurationSafe(now - ResetCycles).MicroSeconds();
        if (usecSinceReset >= WaitBeforeSleepUsec) {
            usleep(SleepTimeUsec);
        }
    }

    void Reset() noexcept
    {
        ResetCycles = GetCycleCount();
    }
};

}   // namespace NCloud::NBlockStore::NRdma
