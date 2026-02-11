#pragma once

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_back_cache_stats.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TTestWriteDataRequestStats
{
    ui64 InProgressCount = 0;
    TInstant MinTime = TInstant::Zero();
    TVector<TDuration> Data;
    ui64 Count = 0;

    void ResetNonDerivativeCounters();
};

struct TTestReadDataRequestStats
{
    TVector<TDuration> Data;
    ui64 CacheMissCount = 0;
    ui64 CachePartialHitCount = 0;
    ui64 CacheFullHitCount = 0;
};

struct TTestWriteBackCacheStats: public IWriteBackCacheStats
{
    ui64 InProgressFlushCount = 0;
    ui64 CompletedFlushCount = 0;
    ui64 FailedFlushCount = 0;

    ui64 NodeCount = 0;

    TTestWriteDataRequestStats PendingStats;
    TTestWriteDataRequestStats UnflushedStats;
    TTestWriteDataRequestStats FlushedStats;

    TTestReadDataRequestStats ReadStats;

    TPersistentStorageStats StorageStats;

    // Do not store more than the specified amount of elements in the following
    // vectors in order to prevent OOM for large tests
    ui64 MaxItems = 1000000;

    void ResetNonDerivativeCounters() override;

    void FlushStarted() override;

    void FlushCompleted() override;

    void FlushFailed() override;

    void IncrementNodeCount() override;

    void DecrementNodeCount() override;

    TTestWriteDataRequestStats& GetWriteStats(EWriteDataRequestStatus status);

    void WriteDataRequestEnteredStatus(EWriteDataRequestStatus status) override;

    void WriteDataRequestExitedStatus(
        EWriteDataRequestStatus status,
        TDuration duration) override;

    void WriteDataRequestUpdateMinTime(
        EWriteDataRequestStatus status,
        TInstant minTime) override;

    void AddReadDataStats(EReadDataRequestCacheStatus status) override;

    void UpdatePersistentStorageStats(
        const TPersistentStorageStats& stats) override;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
