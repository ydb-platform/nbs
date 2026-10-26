#pragma once

#if !defined(FASTSHARD_INCLUDE_SILK_CONTEXT)
#error "you should never include context_i.h directly"
#endif

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
