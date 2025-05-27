#include "time_control_test.h"

namespace NCloud {

void TTestTimeControl::Sleep(TDuration duration)
{
    SleepDurations.push_back(duration);
    Timer.AdvanceTime(duration);
}

TInstant TTestTimeControl::Now()
{
    return Timer.Now();
}

}   // namespace NCloud
