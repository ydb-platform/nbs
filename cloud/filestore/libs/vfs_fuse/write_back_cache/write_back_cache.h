#pragma once

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/vfs_fuse/public.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/threading/future/future.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStats;
using IWriteBackCacheStatsPtr =
    std::shared_ptr<IWriteBackCacheStats>;

class TWriteBackCache final
{
private:
    class TImpl;

    std::shared_ptr<TImpl> Impl;

public:
    TWriteBackCache();

    TWriteBackCache(
        IFileStorePtr session,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        IWriteBackCacheStatsPtr stats,
        const TString& filePath,
        ui32 capacityBytes,
        TDuration automaticFlushPeriod,
        TDuration flushRetryPeriod,
        ui32 maxWriteRequestSize,
        ui32 maxWriteRequestsCount,
        ui32 maxSumWriteRequestsSize);

    ~TWriteBackCache();

    explicit operator bool() const
    {
        return !!Impl;
    }

    NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    NThreading::TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request);

    NThreading::TFuture<void> FlushNodeData(ui64 nodeId);

    NThreading::TFuture<void> FlushAllData();

    struct TPersistentQueueStats;
    struct TWriteDataStats;

private:
    // Only for testing purposes
    friend struct TCalculateDataPartsToReadTestBootstrap;

    enum class EWriteDataEntryStatus;
    class TWriteDataEntry;
    struct TWriteDataEntryPart;
    struct TNodeState;
    struct TFlushState;
    class TUtil;
    struct TPendingOperations;
    class TContiguousWriteDataEntryPartsReader;
    class TWriteDataEntryIntervalMap;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TPersistentQueueStats
{
    ui64 RawCapacity = 0;
    ui64 RawUsedBytesCount = 0;
    ui64 MaxAllocationSize = 0;
    bool IsCorrupted = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TWriteDataStats
{
    // Time spent in the pending state
    TDuration PendingDuration;

    // Time spent in the cache before the request is started to flush
    TDuration WaitingDuration;

    // Time spent in the cache while the request was being flushed
    TDuration FlushDuration;
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStats
{
    virtual ~IWriteBackCacheStats() = default;

    virtual void IncrementCompletedFlushCount() = 0;
    virtual void IncrementFailedFlushCount() = 0;
    virtual void SetExecutingFlushCount(ui64 value) = 0;
    virtual void SetEarliestFlushTime(TInstant time) = 0;

    virtual void SetNodeCount(ui64 value) = 0;
    virtual void SetCachedWriteRequestCount(ui64 value) = 0;
    virtual void SetPendingWriteRequestCount(ui64 value) = 0;

    virtual void SetPersistentQueueStats(
        const TWriteBackCache::TPersistentQueueStats& stats) = 0;

    virtual void AddWriteRequestStats(
        const TWriteBackCache::TWriteDataStats& stats) = 0;
};

}   // namespace NCloud::NFileStore::NFuse
