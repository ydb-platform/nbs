#pragma once

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_back_cache_stats.h>

#include <util/generic/vector.h>
#include <util/system/spinlock.h>

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

struct TPersistentStorageStats
{
    ui64 RawCapacityByteCount = 0;
    ui64 RawUsedByteCount = 0;
    ui64 EntryCount = 0;
    bool IsCorrupted = false;
};

struct TTestWriteBackCacheStats
    : public std::enable_shared_from_this<TTestWriteBackCacheStats>
    , public IWriteBackCacheStats
    , public IWriteBackCacheInternalStats
    , public IWriteBackCacheStateStats
    , public INodeStateHolderStats
    , public IWriteDataRequestManagerStats
    , public IPersistentStorageStats
{
    TAdaptiveLock Lock;

    ui64 InProgressFlushCount = 0;
    ui64 CompletedFlushCount = 0;
    ui64 FailedFlushCount = 0;

    ui64 NodeCount = 0;

    ui64 WriteDataRequestDroppedCount = 0;

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

    void WriteDataRequestDropped() override;

    TTestWriteDataRequestStats& GetWriteStats(EWriteDataRequestStatus status);

    void AddedPendingRequest() override;
    void RemovedPendingRequest(TDuration duration) override;
    void AddedUnflushedRequest() override;
    void RemovedUnflushedRequest(TDuration duration) override;
    void AddedFlushedRequest() override;
    void RemovedFlushedRequest() override;

    void AddReadDataStats(EReadDataRequestCacheStatus status) override;

    void Set(
        ui64 rawCapacityBytesCount,
        ui64 rawUsedBytesCount,
        ui64 entryCount,
        bool isCorrupted) override;

    IWriteBackCacheInternalStatsPtr GetWriteBackCacheInternalStats() override
    {
        return shared_from_this();
    }

    IWriteBackCacheStateStatsPtr GetWriteBackCacheStateStats() override
    {
        return shared_from_this();
    }

    TWriteBackCacheStateMetrics
    CreateWriteBackCacheStateMetrics() const override
    {
        return {};
    }

    void UpdateWriteBackCacheStateStats() override
    {}

    INodeStateHolderStatsPtr GetNodeStateHolderStats() override
    {
        return shared_from_this();
    }

    TNodeStateHolderMetrics CreateNodeStateHolderMetrics() const override
    {
        return {};
    }

    void UpdateNodeStateHolderStats() override
    {}

    IWriteDataRequestManagerStatsPtr GetWriteDataRequestManagerStats() override
    {
        return shared_from_this();
    }

    TWriteDataRequestManagerMetrics
    CreateWriteDataRequestManagerMetrics() const override
    {
        return {};
    }

    virtual void UpdateWriteDataRequestManagerStats(
        TDuration maxPendingRequestDuration,
        TDuration maxUnflushedRequestDuration) override
    {
        Y_UNUSED(maxPendingRequestDuration);
        Y_UNUSED(maxUnflushedRequestDuration);
    }

    IPersistentStorageStatsPtr GetPersistentStorageStats() override
    {
        return shared_from_this();
    }

    TPersistentStorageMetrics CreatePersistentStorageMetrics() const override
    {
        return {};
    }

    void UpdatePersistentStorageStats() override
    {}
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
