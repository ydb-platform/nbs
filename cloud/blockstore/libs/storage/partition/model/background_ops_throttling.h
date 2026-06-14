#pragma once

#include "public.h"

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

// Background ops (compaction, cleanup) are throttled by limiting how much
// tablet-thread time they are allowed to consume per second
// (maxExecTimePerSecond). The delay is derived from execution time of the
// previous run (lastOperationExecTime).
//
// The logic is as follows: maxExecTimePerSecond determines the share of time
// the tablet is allowed to spend on the operation. When throttling, we
// calculate a delay so that the share of operation execution time in the
// (operation + delay) interval is maxExecTimePerSecond / 1s.
//
// For example, assume that maxExecTimePerSecond is 50ms. Then the tablet is
// allowed to spend 5% of time on the operation. If lastOperationExecTime is
// 25ms, the next operation is postponed by 475ms, because 25ms is 5% of 500ms.
//
// The delay is clamped by minDelay and maxDelay.
TDuration CalculateBackgroundOpThrottleDelay(
    TDuration lastOperationExecTime,
    TDuration maxExecTimePerSecond,
    TDuration minDelay,
    TDuration maxDelay);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
