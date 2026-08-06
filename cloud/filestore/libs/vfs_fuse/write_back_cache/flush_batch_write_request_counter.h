#pragma once

#include "flush_batch_limits.h"

#include <cloud/storage/core/libs/common/disjoint_interval_map.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

// Counts the amount of WriteData requests needed to flush data in a single
// batch according to limits defined by TFlushBatchLimits
class TFlushBatchWriteRequestCounter
{
private:
    // Key = interval begin
    // Value = amount of write requests needed to flush the interval
    // Intervals cannot overlap or touch
    TDisjointIntervalMap<ui64, ui64> SeparatedIntervalsMap;
    ui64 WriteRequestCount = 0;
    ui64 SumWriteRequestsSize = 0;

public:
    // Add cached WriteData request affecting [begin, end) region to flush batch
    void AddRequestInterval(
        const TFlushBatchLimits& flushBatchLimits,
        ui64 begin,
        ui64 end);

    void Reset();

    bool IsEmpty() const
    {
        return SeparatedIntervalsMap.empty();
    }

    ui64 GetWriteRequestCount() const
    {
        return WriteRequestCount;
    }

    // Returns the total size of WriteData requests in bytes
    ui64 GetSumWriteRequestsSize() const
    {
        return SumWriteRequestsSize;
    }
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
