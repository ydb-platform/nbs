#include "flush_backpressure_calculator.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

TFlushBackpressureCalculator::TFlushBackpressureCalculator()
    : Limits({})
    , FlushBatchCountBackpressureThreshold(0)
{}

TFlushBackpressureCalculator::TFlushBackpressureCalculator(
    const TFlushBatchLimits& limits,
    ui32 flushBatchCountBackpressureThreshold)
    : Limits(limits)
    , FlushBatchCountBackpressureThreshold(flushBatchCountBackpressureThreshold)
{}

bool TFlushBackpressureCalculator::GetBackpressureStatus(
    size_t unflushedWriteDataRequestCount,
    size_t cachedDataContiguousIntervalCount,
    ui64 cachedDataByteCount) const
{
    if (FlushBatchCountBackpressureThreshold == 0) {
        return false;
    }

    if (unflushedWriteDataRequestCount <= FlushBatchCountBackpressureThreshold)
    {
        // Each flush batch consists of at least one WriteData request
        // The number of flush batches cannot exceed the number of unflushed
        // requests
        return false;
    }

    // Accurate calculation of the number of flush batches is computationally
    // expensive and unnecessary - a rough heuristic estimate of the order of
    // the number of flush batches is sufficient.

    // Note that cachedDataByteCount and cachedDataContiguousIntervalCount are
    // calculated over both unflushed and flushed WriteData requests. It means
    // that backpressure condition will be lifted after requests are evicted.
    // This should not be problem because pins that prevent requests from
    // eviction are short-lived.

    if (cachedDataByteCount == 0 || cachedDataContiguousIntervalCount == 0) {
        return false;
    }

    if (Limits.MaxSumWriteRequestsSize != 0 &&
        (cachedDataByteCount + Limits.MaxSumWriteRequestsSize - 1) /
                Limits.MaxSumWriteRequestsSize >
            FlushBatchCountBackpressureThreshold)
    {
        return true;
    }

    if (Limits.MaxWriteRequestsCount == 0) {
        return false;
    }

    // Assume that the intervals are distributed uniformly
    ui64 avgByteCountPerInterval =
        (cachedDataByteCount + cachedDataContiguousIntervalCount - 1) /
        cachedDataContiguousIntervalCount;

    // Average number of WriteData requests needed to flush each interval
    ui64 avgRequestCountPerInterval =
        Limits.MaxWriteRequestSize != 0
            ? (avgByteCountPerInterval + Limits.MaxWriteRequestSize - 1) /
                  Limits.MaxWriteRequestSize
            : 1;

    ui64 totalRequestCount =
        avgRequestCountPerInterval * cachedDataContiguousIntervalCount;

    ui64 flushBatchCount =
        (totalRequestCount + Limits.MaxWriteRequestsCount - 1) /
        Limits.MaxWriteRequestsCount;

    return flushBatchCount > FlushBatchCountBackpressureThreshold;
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
