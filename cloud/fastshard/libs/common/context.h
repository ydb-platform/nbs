#pragma once

#ifdef FASTSHARD_IPC_STUB

namespace NCloud::NFastShard {

struct TFastShardContext
{};

}   // namespace NCloud::NFastShard

#else

#include <silk/fibers/event.h>   // Y_IGNORE

#include <util/system/types.h>

#include <atomic>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct TFastShardContext
{
    const ui64 RequestId;
    // TODO(#6957): execution cycles
    std::atomic<ui64> BackoffCycles = 0;
    silk::FiberEvent Cancel;

    TFastShardContext(ui64 requestId)
        : RequestId(requestId)
    {}
};

}   // namespace NCloud::NFastShard

#endif
