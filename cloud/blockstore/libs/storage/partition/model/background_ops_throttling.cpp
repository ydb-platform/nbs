#include "background_ops_throttling.h"

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

TDuration CalculateBackgroundOpThrottleDelay(
    TDuration lastOperationExecTime,
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
            static_cast<double>(lastOperationExecTime.GetValue()) /
            maxExecTimePerSecond.GetValue();
        const auto throttleDelay =
            TDuration::Seconds(1) * throttlingFactor - lastOperationExecTime;

        delay = Max(delay, throttleDelay);
    }

    return Min(delay, maxDelay);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
