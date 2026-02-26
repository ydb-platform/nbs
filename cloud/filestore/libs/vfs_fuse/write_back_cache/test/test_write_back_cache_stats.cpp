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
    auto guard = Guard(Lock);

    InProgressFlushCount = 0;
    NodeCount = 0;

    PendingStats.ResetNonDerivativeCounters();
    UnflushedStats.ResetNonDerivativeCounters();
    FlushedStats.ResetNonDerivativeCounters();
}

void TTestWriteBackCacheStats::FlushStarted()
{
    auto guard = Guard(Lock);

    InProgressFlushCount++;
}

void TTestWriteBackCacheStats::FlushCompleted()
{
    auto guard = Guard(Lock);


    InProgressFlushCount--;
    CompletedFlushCount++;
}

void TTestWriteBackCacheStats::FlushFailed()
{
    auto guard = Guard(Lock);

    FailedFlushCount++;
}

void TTestWriteBackCacheStats::IncrementNodeCount()
{
    auto guard = Guard(Lock);

    NodeCount++;
}

void TTestWriteBackCacheStats::DecrementNodeCount()
{
    auto guard = Guard(Lock);

    NodeCount--;
}

TTestWriteDataRequestStats& TTestWriteBackCacheStats::GetWriteStats(
    EWriteDataRequestStatus status)
{
    switch (status) {
        case EWriteDataRequestStatus::Pending:
            return PendingStats;
        case EWriteDataRequestStatus::Unflushed:
            return UnflushedStats;
        case EWriteDataRequestStatus::Flushed:
            return FlushedStats;
        default:
            Y_ABORT("Unknown EWriteDataRequestStatus value");
    }
}

void TTestWriteBackCacheStats::WriteDataRequestEnteredStatus(
    EWriteDataRequestStatus status)
{
    auto guard = Guard(Lock);

    auto& stats = GetWriteStats(status);
    stats.InProgressCount++;
}

void TTestWriteBackCacheStats::WriteDataRequestExitedStatus(
    EWriteDataRequestStatus status,
    TDuration duration)
{
    auto guard = Guard(Lock);

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
    auto guard = Guard(Lock);

    auto& stats = GetWriteStats(status);
    stats.MinTime = minTime;
}

void TTestWriteBackCacheStats::AddReadDataStats(
    EReadDataRequestCacheStatus status)
{
    auto guard = Guard(Lock);

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
