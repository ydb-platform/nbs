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

void TTestWriteBackCacheStats::WriteDataRequestDropped()
{
    auto guard = Guard(Lock);

    WriteDataRequestDroppedCount++;
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
    }
}

void TTestWriteBackCacheStats::AddedPendingRequest()
{
    auto guard = Guard(Lock);

    PendingStats.InProgressCount++;
}

void TTestWriteBackCacheStats::RemovedPendingRequest(TDuration duration)
{
    auto guard = Guard(Lock);

    PendingStats.Count++;
    PendingStats.InProgressCount--;
    if (PendingStats.Data.size() < MaxItems) {
        PendingStats.Data.push_back(duration);
    }
}

void TTestWriteBackCacheStats::AddedUnflushedRequest()
{
    auto guard = Guard(Lock);

    UnflushedStats.InProgressCount++;
}

void TTestWriteBackCacheStats::RemovedUnflushedRequest(TDuration duration)
{
    auto guard = Guard(Lock);

    UnflushedStats.Count++;
    UnflushedStats.InProgressCount--;
    if (UnflushedStats.Data.size() < MaxItems) {
        UnflushedStats.Data.push_back(duration);
    }
}

void TTestWriteBackCacheStats::AddedFlushedRequest()
{
    auto guard = Guard(Lock);

    FlushedStats.InProgressCount++;
}

void TTestWriteBackCacheStats::RemovedFlushedRequest()
{
    auto guard = Guard(Lock);

    FlushedStats.Count++;
    FlushedStats.InProgressCount--;
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
    }
}

void TTestWriteBackCacheStats::UpdatePersistentStorageStats(
    const TPersistentStorageStats& stats)
{
    StorageStats = stats;
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
