#include "write_back_cache_stats_processor.h"

#include "write_back_cache_impl.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TStatsProcessor::TStatsProcessor(
        IWriteBackCacheStatsPtr stats)
    : Stats(std::move(stats))
{
    if (Stats) {
        Stats->SetExecutingFlushStats(0, 0, TInstant::Zero());
    }
}

void TWriteBackCache::TStatsProcessor::FlushStarted(
    ui64 writeDataRequestCount,
    TInstant startTime)
{
    FlushStartTimeSet.insert(startTime);
    FlushWriteDataRequestCount += writeDataRequestCount;

    if (Stats) {
        Stats->SetExecutingFlushStats(
            /* executingFlushCount = */ FlushStartTimeSet.size(),
            /* writeDataRequestCount = */ FlushWriteDataRequestCount,
            /* minTime = */ *FlushStartTimeSet.begin());
    }
}

void TWriteBackCache::TStatsProcessor::FlushCompleted(
    ui64 writeDataRequestCount,
    TInstant startTime)
{
    auto it = FlushStartTimeSet.find(startTime);
    Y_DEBUG_ABORT_UNLESS(it != FlushStartTimeSet.end());
    FlushStartTimeSet.erase(it);
    Y_DEBUG_ABORT_UNLESS(FlushWriteDataRequestCount >= writeDataRequestCount);
    FlushWriteDataRequestCount -= writeDataRequestCount;

    if (Stats) {
        auto minTime = FlushStartTimeSet.empty() ? TInstant::Zero()
                                                 : *FlushStartTimeSet.begin();
        Stats->SetExecutingFlushStats(
            /* executingFlushCount = */ FlushStartTimeSet.size(),
            /* writeDataRequestCount = */ FlushWriteDataRequestCount,
            /* minTime = */ minTime);
        Stats->IncrementCompletedFlushCount();
    }
}

void TWriteBackCache::TStatsProcessor::FlushFailed()
{
    if (Stats) {
        Stats->IncrementFailedFlushCount();
    }
}

void TWriteBackCache::TStatsProcessor::UpdateNodeCount(ui64 value)
{
    if (Stats) {
        Stats->SetNodeCount(value);
    }
}

void TWriteBackCache::TStatsProcessor::UpdatePendingQueueStats(
    const TDeque<std::unique_ptr<TWriteDataEntry>>& pendingEntries)
{
    if (Stats) {
        auto minTime = pendingEntries.empty()
                           ? TInstant::Zero()
                           : pendingEntries.front()->PendingTime;
        Stats->SetPendingWriteDataRequestStats(pendingEntries.size(), minTime);
    }
}

void TWriteBackCache::TStatsProcessor::UpdateCachedQueueStats(
    const TDeque<std::unique_ptr<TWriteDataEntry>>& cachedEntries)
{
    if (Stats) {
        auto minTime = cachedEntries.empty()
                           ? TInstant::Zero()
                           : cachedEntries.front()->PendingTime;
        Stats->SetCachedWriteDataRequestStats(cachedEntries.size(), minTime);
    }
}

void TWriteBackCache::TStatsProcessor::AddWriteDataRequestPendingDuration(
    TDuration pendingDuration)
{
    if (Stats) {
        Stats->AddWriteDataRequestPendingDuration(pendingDuration);
    }
}

void TWriteBackCache::TStatsProcessor::AddWriteDataRequestWaitingDuration(
    TDuration waitingDuration)
{
    if (Stats) {
        Stats->AddWriteDataRequestWaitingDuration(waitingDuration);
    }
}

void TWriteBackCache::TStatsProcessor::AddWriteDataRequestFlushDuration(
    TDuration flushDuration)
{
    if (Stats) {
        Stats->AddWriteDataRequestFlushDuration(flushDuration);
    }
}

void TWriteBackCache::TStatsProcessor::UpdatePersistentQueueStats(
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

}   // namespace NCloud::NFileStore::NFuse
