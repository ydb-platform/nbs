#include "background_ops_throttling.h"

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

TDuration CalculateBackgroundOpThrottleDelay(
    TDuration execTimeForLastSecond,
    TDuration maxExecTimePerSecond,
    TDuration minDelay,
    TDuration maxDelay)
{
    if (!maxDelay) {
        return {};
    }

    auto delay = minDelay;
    if (maxExecTimePerSecond) {
        const auto throttlingFactor =
            static_cast<double>(execTimeForLastSecond.GetValue()) /
            maxExecTimePerSecond.GetValue();
        const auto throttleDelay =
            TDuration::Seconds(1) * throttlingFactor - execTimeForLastSecond;

        delay = Max(delay, throttleDelay);
    }

    return Min(delay, maxDelay);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
