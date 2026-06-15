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
    Y_DEBUG_ABORT_UNLESS(minDelay <= maxDelay);

    TDuration delay = TDuration::Zero();
    if (maxExecTimePerSecond) {
        const auto permittedExecutionPart =
            static_cast<double>(maxExecTimePerSecond.GetValue()) /
            TDuration::Seconds(1).GetValue();
        const auto permittedExecutionAndDelayInterval =
            lastOperationExecTime / permittedExecutionPart;
        delay = permittedExecutionAndDelayInterval - lastOperationExecTime;
    }

    return std::clamp(delay, minDelay, maxDelay);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
