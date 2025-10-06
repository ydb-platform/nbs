#include "write_back_cache_stats_processor.h"

#include "write_back_cache_impl.h"

namespace NCloud::NFileStore::NFuse {

using EWriteDataRequestState = IWriteBackCacheStats::EWriteDataRequestState;

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
            EWriteDataRequestState::Flush,
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
        Stats->DecrementInProgressFlushCount();
        Stats->IncrementCompletedFlushCount();
        Stats->SetWriteDataRequestMinInstant(
            EWriteDataRequestState::Flush,
            FlushStartTimeSet.empty() ? TInstant::Zero()
                                      : *FlushStartTimeSet.begin());
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
        Stats->SetWriteDataRequestMinInstant(
            EWriteDataRequestState::Pending,
            pendingEntries.empty() ? TInstant::Zero()
                                   : pendingEntries.front()->PendingTime);
    }
}

void TWriteBackCache::TStatsProcessor::UpdateCachedQueueStats(
    const TDeque<std::unique_ptr<TWriteDataEntry>>& cachedEntries)
{
    if (Stats) {
        Stats->SetWriteDataRequestMinInstant(
            EWriteDataRequestState::Cached,
            cachedEntries.empty() ? TInstant::Zero()
                                  : cachedEntries.front()->PendingTime);
    }
}

void TWriteBackCache::TStatsProcessor::WriteDataRequestEnteredState(
    IWriteBackCacheStats::EWriteDataRequestState state)
{
    if (Stats) {
        Stats->IncrementInProgressWriteDataRequestCount(state);
    }
}

void TWriteBackCache::TStatsProcessor::WriteDataRequestExitedState(
    IWriteBackCacheStats::EWriteDataRequestState state,
    TDuration duration)
{
    if (Stats) {
        Stats->DecrementInProgressWriteDataRequestCount(state);
        Stats->AddWriteDataRequestStats(state, duration);
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
