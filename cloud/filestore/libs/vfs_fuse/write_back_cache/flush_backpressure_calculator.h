#pragma once

#include "flush_batch_limits.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TFlushBackpressureCalculator
{
private:
    const TFlushBatchLimits Limits;

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
    const ui32 FlushBatchCountBackpressureThreshold;

public:
    TFlushBackpressureCalculator();

    TFlushBackpressureCalculator(
        const TFlushBatchLimits& limits,
        ui32 flushBatchCountBackpressureThreshold);

    bool GetBackpressureStatus(
        size_t unflushedWriteDataRequestCount,
        size_t cachedDataContiguousIntervalCount,
        ui64 cachedDataByteCount) const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
