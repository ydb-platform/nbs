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
        const auto permittedExecutionPart =
            static_cast<double>(maxExecTimePerSecond.GetValue()) /
            TDuration::Seconds(1).GetValue();
        const auto permittedExecutionAndDelayInterval =
            lastOperationExecTime / permittedExecutionPart;
        const auto throttleDelay =
            permittedExecutionAndDelayInterval - lastOperationExecTime;

        delay = Max(delay, throttleDelay);
    }

    return Min(delay, maxDelay);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
