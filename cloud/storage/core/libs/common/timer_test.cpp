#include "timer_test.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TInstant TTestTimer::Now()
{
    return TInstant::MilliSeconds(AtomicGet(Timestamp));
}

void TTestTimer::AdvanceTime(TDuration delay)
{
    AtomicAdd(Timestamp, delay.MilliSeconds());
}

}   // namespace NCloud
