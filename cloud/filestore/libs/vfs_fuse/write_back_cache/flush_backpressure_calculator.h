#pragma once

#include "flush_batch_limits.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TFlushBackpressureCalculator
{
private:
    const TFlushBatchLimits Limits;

public:
    TFlushBackpressureCalculator();

    explicit TFlushBackpressureCalculator(const TFlushBatchLimits& limits);

    bool GetBackpressureStatus(
        size_t unflushedWriteDataRequestCount,
        size_t cachedDataContiguousIntervalCount,
        ui64 cachedDataByteCount) const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
