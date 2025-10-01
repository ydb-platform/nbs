#include "write_back_cache_stats_processor.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TStatsProcessor::TStatsProcessor(
        IWriteBackCacheStatsPtr stats)
    : Stats(std::move(stats))
{}

void TWriteBackCache::TStatsProcessor::FlushStarted(
    ui64 writeDataRequestCount,
    TInstant startTime)
{}

void TWriteBackCache::TStatsProcessor::FlushCompleted(
    ui64 writeDataRequestCount,
    TInstant startTime)
{}

void TWriteBackCache::TStatsProcessor::FlushFailed()
{}





void TWriteBackCacheStatsProcessor::FlushScheduled(TInstant startTime)
{
    if (Stats) {
        FlushStartTimeSet.insert(startTime);
        Stats->SetExecutingFlushCount(FlushStartTimeSet.size());
        Stats->SetEarliestExecutingFlushTime(*FlushStartTimeSet.begin());
    }
}

void TWriteBackCacheStatsProcessor::FlushCompleted(TInstant startTime)
{
    if (Stats) {
        auto it = FlushStartTimeSet.find(startTime);
        Y_DEBUG_ABORT_UNLESS(it != FlushStartTimeSet.end());
        FlushStartTimeSet.erase(it);
        if (FlushStartTimeSet.empty()) {
            Stats->SetEarliestExecutingFlushTime(TInstant::Zero());
        } else {
            Stats->SetEarliestExecutingFlushTime(*FlushStartTimeSet.begin());
        }

        Stats->SetExecutingFlushCount(FlushStartTimeSet.size());
        Stats->IncrementCompletedFlushCount();
    }
}

void TWriteBackCacheStatsProcessor::FlushFailed()
{
    if (Stats) {
        Stats->IncrementFailedFlushCount();
    }
}

void TWriteBackCacheStatsProcessor::UpdatePersistentQueueStats(
    const TFileRingBuffer& buffer)
{
    if (Stats) {
        Stats->SetPersistentQueueStats(
            {.RawCapacity = buffer.GetRawCapacity(),
             .RawUsedBytesCount = buffer.GetRawUsedBytesCount(),
             .MaxAllocationSize = buffer.GetMaxAllocationBytesCount(),
             .IsCorrupted = buffer.IsCorrupted()});
    }
}

void TWriteBackCacheStatsProcessor::UpdateNodeCount(ui64 value)
{
    if (Stats) {
        Stats->SetNodeCount(value);
    }
}

void TWriteBackCacheStatsProcessor::UpdateCachedRequestCount(ui64 value)
{
    if (Stats) {
        Stats->SetCachedWriteRequestCount(value);
    }
}

void TWriteBackCacheStatsProcessor::UpdatePendingRequestCount(ui64 value)
{
    if (Stats) {
        Stats->SetPendingWriteRequestCount(value);
    }
}

void TWriteBackCacheStatsProcessor::AddWriteDataRequestStats(
    TInstant pendingTime,
    TInstant cachedTime,
    TInstant startFlushTime,
    TInstant completeFlushTime) const
{
    Y_DEBUG_ABORT_UNLESS(
        pendingTime && cachedTime && startFlushTime && completeFlushTime);

    if (Stats) {
        Stats->AddWriteRequestStats(
            {.PendingDuration = cachedTime - pendingTime,
             .WaitingDuration = startFlushTime - cachedTime,
             .FlushDuration = completeFlushTime - startFlushTime});
    }
}

}   // namespace NCloud::NFileStore::NFuse
