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

    bool IsEmpty() const;

    enum class EWriteDataRequestStatus;
    struct TPersistentQueueStats;

private:
    // Only for testing purposes
    friend struct TCalculateDataPartsToReadTestBootstrap;

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

/**
 * WriteData request life cycle:
 * Initial -> Pending -> Cached -> (FlushRequested) -> Flushing -> Flushed
 *
 * Status FlushRequested may be skipped — Flush takes as much WriteData requests
 * as possible from |TWriteBackCache::TNodeState::CachedEntries|.
 *
 * For each NodeId it is guaranteed that there are no requests with out-of-order
 * statuses: if two requests A and B have the same NodeId, and the request A was
 * added to the queue later than B, then A.Status <= B.Status.
 */

enum class TWriteBackCache::EWriteDataRequestStatus
{
    // The object has just been created and does not hold a request.
    Initial,

    // Restoration from the persisent buffer was failed.
    // The request will not be processed further.
    Corrupted,

    // Write request is waiting for the conditions:
    // - enough space in the persistent buffer to store the request;
    // - no overlapping read requests in progress.
    Pending,

    // Write request has been stored in the persistent buffer
    // The caller code observes the request as completed
    Cached,

    // Flush has been requested for the write request
    FlushRequested,

    // Write request is being flushed
    Flushing,

    // Write request has been written to the session and can be removed from
    // the persistent buffer
    Flushed
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TPersistentQueueStats
{
    ui64 RawCapacity = 0;
    ui64 RawUsedBytesCount = 0;
    ui64 MaxAllocationBytesCount = 0;
    bool IsCorrupted = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStats
{
    enum class EReadDataRequestCacheStatus
    {
        // A request wasn't served from the cache
        Miss,

        // A request was partially served from the cache
        PartialHit,

        // A request was fully served from the cache
        FullHit
    };

    virtual ~IWriteBackCacheStats() = default;

    virtual void ResetNonDerivativeCounters() = 0;

    virtual void FlushStarted() = 0;
    virtual void FlushCompleted() = 0;
    virtual void FlushFailed() = 0;

    virtual void IncrementNodeCount() = 0;
    virtual void DecrementNodeCount() = 0;

    virtual void WriteDataRequestEnteredStatus(
        TWriteBackCache::EWriteDataRequestStatus status) = 0;

    virtual void WriteDataRequestExitedStatus(
        TWriteBackCache::EWriteDataRequestStatus status,
        TDuration duration) = 0;

    virtual void WriteDataRequestUpdateMinTime(
        TWriteBackCache::EWriteDataRequestStatus status,
        TInstant minTime) = 0;

    virtual void AddReadDataStats(
        IWriteBackCacheStats::EReadDataRequestCacheStatus status,
        TDuration pendingDuration) = 0;

    virtual void UpdatePersistentQueueStats(
        const TWriteBackCache::TPersistentQueueStats& stats) = 0;
};

IWriteBackCacheStatsPtr CreateDummyWriteBackCacheStats();

}   // namespace NCloud::NFileStore::NFuse
