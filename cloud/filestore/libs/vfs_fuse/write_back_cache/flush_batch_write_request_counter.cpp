#include "flush_batch_write_request_counter.h"

#include <util/generic/ylimits.h>
#include <util/system/yassert.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

void TFlushBatchWriteRequestCounter::AddRequestInterval(
    const TFlushBatchLimits& flushBatchLimits,
    ui64 begin,
    ui64 end)
{
    Y_ABORT_UNLESS(begin < end);

    // Remove all overlapping and touching intervals
    SeparatedIntervalsMap.VisitOverlapping(
        begin > 0 ? begin - 1 : 0,
        end < Max<ui64>() ? end + 1 : Max<ui64>(),
        [this, &begin, &end](auto it)
        {
            const TDisjointIntervalMap<ui64, ui64>::TItem& e = it->second;
            WriteRequestCount -= e.Value;
            SumWriteRequestsSize -= e.End - e.Begin;
            begin = Min(begin, e.Begin);
            end = Max(end, e.End);
            SeparatedIntervalsMap.Remove(it);
        });

    const ui64 count =
        flushBatchLimits.MaxWriteRequestSize > 0
            ? ((end - begin - 1) / flushBatchLimits.MaxWriteRequestSize) + 1
            : 1;

    SeparatedIntervalsMap.Add(begin, end, count);
    WriteRequestCount += count;
    SumWriteRequestsSize += end - begin;
}

void TFlushBatchWriteRequestCounter::Reset()
{
    SeparatedIntervalsMap = {};
    WriteRequestCount = 0;
    SumWriteRequestsSize = 0;
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
