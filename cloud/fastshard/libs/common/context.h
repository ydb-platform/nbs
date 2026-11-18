#pragma once

#include <silk/fibers/event.h>

#include <util/system/types.h>

#include <atomic>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct TFastShardContext
{
    const ui64 RequestId;
    std::atomic<ui64> ExecCycles = 0;
    std::atomic<ui64> WaitCycles = 0;
    std::atomic<ui64> BackoffCycles = 0;
    silk::FiberEvent Cancel;

    TFastShardContext(ui64 requestId)
        : RequestId(requestId)
    {}
};

}   // namespace NCloud::NFastShard
