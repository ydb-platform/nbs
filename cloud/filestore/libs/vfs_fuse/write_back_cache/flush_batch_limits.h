#pragma once

#include <util/system/types.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TFlushBatchLimits
{
    // The maximum size of a single consolidated WriteData request
    // Zero value = the limit is not enforced
    ui32 MaxWriteRequestSize = 0;

    // The maximum number of consolidated WriteData requests
    ui32 MaxWriteRequestsCount = 0;

    // The maximum total size of all consolidated WriteData requests
    ui32 MaxSumWriteRequestsSize = 0;

    // Threshold for enabling WriteBackCache backpressure for a node.
    // The value is compared with the number of flush batches needed to drain
    // unflushed WriteData requests.
    ui32 MaxQueuedFlushBatchesPerNode = 0;
};

} // namespace NCloud::NFileStore::NFuse::NWriteBackCache
