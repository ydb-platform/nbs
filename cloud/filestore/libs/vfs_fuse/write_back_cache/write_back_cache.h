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
    friend struct TWriteBackCacheExposePrivateIdentifiers;

    enum class EWriteDataEntryStatus;
    class TWriteDataEntry;
    struct TWriteDataEntryPart;
    struct TNodeState;
    struct TFlushState;
    class TUtil;
    struct TPendingOperations;
    class TContiguousWriteDataEntryPartsReader;
    class TWriteDataEntryIntervalMap;
    class TStatsProcessor;
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

struct IWriteBackCacheStats
{
    // Note: there is no one-by-one correspondence to EWriteDataEntryStatus
    enum class EWriteDataRequestState
    {
        // A request has been accepted but not yet stored in the cache
        Pending,

        // A request is stored in the cache and is waiting for a flush
        Wait,

        // A request is stored in the cached and is being flushed
        Flush,

        // A request has been flushed but still remains in the cache
        Evict,

        // Compound state: [Waiting + Flushing + Evicting]
        Cached
    };

    enum class EReadDataRequestCacheState
    {
        // A request wasn't served from the cache
        Miss,

        // A request was partially served from the cache
        PartialHit,

        // A request was fully served from the cache
        FullHit
    };

    virtual ~IWriteBackCacheStats() = default;

    // Reset all non-derivative counters to zero
    virtual void Reset() = 0;

    virtual void IncrementInProgressFlushCount() = 0;
    virtual void DecrementInProgressFlushCount() = 0;
    virtual void IncrementCompletedFlushCount() = 0;
    virtual void IncrementFailedFlushCount() = 0;

    virtual void IncrementNodeCount() = 0;
    virtual void DecrementNodeCount() = 0;

    // Track the minimal time over all requests of state |state|
    virtual void SetWriteDataRequestMinInstant(
        EWriteDataRequestState state,
        TInstant value) = 0;

    virtual void IncrementInProgressWriteDataRequestCount(
        EWriteDataRequestState state) = 0;

    virtual void DecrementInProgressWriteDataRequestCount(
        EWriteDataRequestState state) = 0;

    virtual void AddWriteDataRequestStats(
        EWriteDataRequestState state,
        TDuration duration) = 0;

    virtual void AddReadDataStats(
        EReadDataRequestCacheState state,
        TDuration waitDuration) = 0;

    virtual void SetPersistentQueueStats(
        const TWriteBackCache::TPersistentQueueStats& stats) = 0;
};

}   // namespace NCloud::NFileStore::NFuse
