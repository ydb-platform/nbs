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
    //
    // The value is compared against a cheap heuristic estimate of how many
    // flush batches may be needed to drain the node's unflushed WriteData
    // queue. This is intentionally not an exact simulation of
    // TWriteDataRequestBuilder and must not be treated as a strict upper bound.
    //
    // The estimate is based on aggregate cache statistics and may be
    // inaccurate. This is acceptable because the threshold is used only as an
    // admission-control signal to prevent pathological queue growth.
    ui32 MaxQueuedFlushBatchesPerNode = 0;
};

} // namespace NCloud::NFileStore::NFuse::NWriteBackCache
