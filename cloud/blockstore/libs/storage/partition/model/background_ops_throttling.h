#pragma once

#include "public.h"

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

// Background ops (compaction, cleanup) are throttled by limiting how much
// tablet-thread time they are allowed to consume per second
// (maxExecTimePerSecond). The delay is derived from execution time of the
// previous run (execTimeForLastSecond).
//
// For example, assume that maxExecTimePerSecond is 50ms. If the operation spent
// 25ms in the tablet thread, the next operation is postponed by
// 475ms (500ms − 25ms). If it spent 100ms, the next one is postponed by
// 1.9s (2s − 100ms), and so on.
//
// The delay is clamped by minDelay and maxDelay.
TDuration CalculateBackgroundOpThrottleDelay(
    TDuration execTimeForLastSecond,
    TDuration maxExecTimePerSecond,
    TDuration minDelay,
    TDuration maxDelay);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
