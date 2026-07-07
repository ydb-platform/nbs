#pragma once

#include "flush_batch_limits.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TFlushBackpressureCalculator
{
private:
    const TFlushBatchLimits Limits;

public:
    explicit TFlushBackpressureCalculator(const TFlushBatchLimits& limits);

    /**
     * Estimates whether backpressure should be applied for a node.
     * Under backpressure condition, new WriteData requests should wait in the
     * pending queue and cannot be cached.
     *
     * @param unflushedWriteDataRequestCount The number of WriteData requests
     *   that have not been flushed yet.
     * @param cachedDataContiguousIntervalCount The number of contiguous cached
     *   data intervals for the node.
     * @param cachedDataByteCount The total size of cached data for the node.
     * @return True when the estimated number of queued flush batches exceeds
     *   the configured per-node limit.
     */
    bool GetBackpressureStatus(
        size_t unflushedWriteDataRequestCount,
        size_t cachedDataContiguousIntervalCount,
        ui64 cachedDataByteCount) const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
