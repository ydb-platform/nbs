#include "write_back_cache_stats_processor.h"

#include "write_back_cache_impl.h"

namespace NCloud::NFileStore::NFuse {

using EWriteDataStats = IWriteBackCacheStats::EWriteDataRequestStats;

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TStatsProcessor::TStatsProcessor(
        IWriteBackCacheStatsPtr stats)
    : Stats(std::move(stats))
{
    if (Stats) {
        Stats->Reset();
    }
}

void TWriteBackCache::TStatsProcessor::FlushStarted(
    TInstant startTime)
{
    FlushStartTimeSet.insert(startTime);

    if (Stats) {
        Stats->IncrementInProgressFlushCount();
        Stats->SetWriteDataRequestMinInstant(
            EWriteDataStats::Flush,
            *FlushStartTimeSet.begin());
    }
}

void TWriteBackCache::TStatsProcessor::FlushCompleted(
    TInstant startTime)
{
    auto it = FlushStartTimeSet.find(startTime);
    Y_DEBUG_ABORT_UNLESS(it != FlushStartTimeSet.end());
    FlushStartTimeSet.erase(it);

    if (Stats) {
        auto minTime = FlushStartTimeSet.empty() ? TInstant::Zero()
                                                 : *FlushStartTimeSet.begin();
        Stats->DecrementInProgressFlushCount();
        Stats->IncrementCompletedFlushCount();
        Stats->SetWriteDataRequestMinInstant(EWriteDataStats::Flush, minTime);
    }
}

void TWriteBackCache::TStatsProcessor::FlushFailed()
{
    if (Stats) {
        Stats->IncrementFailedFlushCount();
    }
}

void TWriteBackCache::TStatsProcessor::IncrementNodeCount()
{
    if (Stats) {
        Stats->IncrementNodeCount();
    }
}

void TWriteBackCache::TStatsProcessor::DecrementNodeCount()
{
    if (Stats) {
        Stats->DecrementNodeCount();
    }
}


void TWriteBackCache::TStatsProcessor::UpdatePendingQueueStats(
    const TDeque<std::unique_ptr<TWriteDataEntry>>& pendingEntries)
{
    if (Stats) {
        auto minTime = pendingEntries.empty()
                           ? TInstant::Zero()
                           : pendingEntries.front()->PendingTime;
        Stats->SetWriteDataRequestMinInstant(EWriteDataStats::Pending, minTime);
    }
}

void TWriteBackCache::TStatsProcessor::UpdateCachedQueueStats(
    const TDeque<std::unique_ptr<TWriteDataEntry>>& cachedEntries)
{
    if (Stats) {
        auto minTime = cachedEntries.empty()
                           ? TInstant::Zero()
                           : cachedEntries.front()->PendingTime;
        Stats->SetWriteDataRequestMinInstant(EWriteDataStats::Cached, minTime);
    }
}

void TWriteBackCache::TStatsProcessor::WriteDataRequestEnteredState(
    IWriteBackCacheStats::EWriteDataRequestStats state)
{
    if (Stats) {
        Stats->IncrementWriteDataRequestCount(state);
    }
}

void TWriteBackCache::TStatsProcessor::WriteDataRequestExitedState(
    IWriteBackCacheStats::EWriteDataRequestStats state,
    TDuration duration)
{
    if (Stats) {
        Stats->DecrementWriteDataRequestCountAndAddStats(state, duration);
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
