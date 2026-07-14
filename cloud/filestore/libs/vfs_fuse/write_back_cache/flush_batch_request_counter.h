#pragma once

#include "flush_batch_limits.h"

#include <cloud/storage/core/libs/common/disjoint_interval_map.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TFlushBatchRequestCounter
{
private:
    TDisjointIntervalMap<ui64, ui64> SeparatedIntervalsMap;
    ui64 WriteRequestCount = 0;
    ui64 SumWriteRequestsSize = 0;

public:
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

    ui64 GetSumWriteRequestsSize() const
    {
        return SumWriteRequestsSize;
    }
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
