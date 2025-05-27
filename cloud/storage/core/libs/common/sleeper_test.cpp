#include "sleeper_test.h"

namespace NCloud {

void TTestSleeper::Sleep(TDuration duration)
{
    SleepDurations.push_back(duration);
    Timer->AdvanceTime(duration);
}

}   // namespace NCloud
