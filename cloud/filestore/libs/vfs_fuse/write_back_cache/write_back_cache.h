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
    enum class EWriteDataRequestStats
    {
        Pending,
        Cached,
        Waiting,
        Flush
    };

    virtual ~IWriteBackCacheStats() = default;

    virtual void Reset() = 0;

    virtual void IncrementInProgressFlushCount() = 0;
    virtual void DecrementInProgressFlushCount() = 0;
    virtual void IncrementCompletedFlushCount() = 0;
    virtual void IncrementFailedFlushCount() = 0;

    virtual void IncrementNodeCount() = 0;
    virtual void DecrementNodeCount() = 0;

    virtual void SetWriteDataRequestMinInstant(
        EWriteDataRequestStats stats,
        TInstant value) = 0;

    virtual void IncrementWriteDataRequestCount(
        EWriteDataRequestStats stats) = 0;

    virtual void DecrementWriteDataRequestCountAndAddStats(
        EWriteDataRequestStats stats,
        TDuration duration) = 0;

    virtual void SetPersistentQueueStats(
        const TWriteBackCache::TPersistentQueueStats& stats) = 0;
};

}   // namespace NCloud::NFileStore::NFuse
