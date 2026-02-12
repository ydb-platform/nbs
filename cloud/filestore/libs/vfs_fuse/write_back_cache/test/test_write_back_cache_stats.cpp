#include "test_write_back_cache_stats.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

void TTestWriteDataRequestStats::ResetNonDerivativeCounters()
{
    InProgressCount = 0;
    MinTime = TInstant::Zero();
}

////////////////////////////////////////////////////////////////////////////////

void TTestWriteBackCacheStats::ResetNonDerivativeCounters()
{
    InProgressFlushCount = 0;
    NodeCount = 0;

    PendingStats.ResetNonDerivativeCounters();
    CachedStats.ResetNonDerivativeCounters();
    FlushingStats.ResetNonDerivativeCounters();
    FlushedStats.ResetNonDerivativeCounters();
}

void TTestWriteBackCacheStats::FlushStarted()
{
    InProgressFlushCount++;
}

void TTestWriteBackCacheStats::FlushCompleted()
{
    InProgressFlushCount--;
    CompletedFlushCount++;
}

void TTestWriteBackCacheStats::FlushFailed()
{
    FailedFlushCount++;
}

void TTestWriteBackCacheStats::IncrementNodeCount()
{
    NodeCount++;
}

void TTestWriteBackCacheStats::DecrementNodeCount()
{
    NodeCount--;
}

TTestWriteDataRequestStats& TTestWriteBackCacheStats::GetWriteStats(
    EWriteDataRequestStatus status)
{
    switch (status) {
        case EWriteDataRequestStatus::Pending:
            return PendingStats;
        case EWriteDataRequestStatus::Cached:
            return CachedStats;
        case EWriteDataRequestStatus::Flushing:
            return FlushingStats;
        case EWriteDataRequestStatus::Flushed:
            return FlushedStats;
        default:
            Y_ABORT("Unknown EWriteDataRequestStatus value");
    }
}

void TTestWriteBackCacheStats::WriteDataRequestEnteredStatus(
    EWriteDataRequestStatus status)
{
    auto& stats = GetWriteStats(status);
    stats.InProgressCount++;
}

void TTestWriteBackCacheStats::WriteDataRequestExitedStatus(
    EWriteDataRequestStatus status,
    TDuration duration)
{
    auto& stats = GetWriteStats(status);
    stats.Count++;
    stats.InProgressCount--;
    if (stats.Data.size() < MaxItems) {
        stats.Data.push_back(duration);
    }
}

void TTestWriteBackCacheStats::WriteDataRequestUpdateMinTime(
    EWriteDataRequestStatus status,
    TInstant minTime)
{
    auto& stats = GetWriteStats(status);
    stats.MinTime = minTime;
}

void TTestWriteBackCacheStats::AddReadDataStats(
    EReadDataRequestCacheStatus status,
    TDuration pendingDuration)
{
    if (ReadStats.Data.size() < MaxItems) {
        ReadStats.Data.push_back(pendingDuration);
    }
    switch (status) {
        case EReadDataRequestCacheStatus::Miss:
            ReadStats.CacheMissCount++;
            break;
        case EReadDataRequestCacheStatus::PartialHit:
            ReadStats.CachePartialHitCount++;
            break;
        case EReadDataRequestCacheStatus::FullHit:
            ReadStats.CacheFullHitCount++;
            break;
        default:
            Y_ABORT("Unknown EReadDataRequestCacheState value");
    }
}

void TTestWriteBackCacheStats::UpdatePersistentStorageStats(
    const TPersistentStorageStats& stats)
{
    StorageStats = stats;
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
