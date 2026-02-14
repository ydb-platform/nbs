#pragma once

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/vfs_fuse/public.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/future/future.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

namespace NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStats;
using IWriteBackCacheStatsPtr =
    std::shared_ptr<IWriteBackCacheStats>;

}   // namespace NWriteBackCache

////////////////////////////////////////////////////////////////////////////////

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
        NWriteBackCache::IWriteBackCacheStatsPtr stats,
        TLog log,
        const TString& fileSystemId,
        const TString& clientId,
        const TString& filePath,
        ui64 capacityBytes,
        TDuration automaticFlushPeriod,
        TDuration flushRetryPeriod,
        ui32 maxWriteRequestSize,
        ui32 maxWriteRequestsCount,
        ui32 maxSumWriteRequestsSize,
        bool zeroCopyWriteEnabled);

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

    // Keep information about MinNodeSize for flushed nodes
    ui64 AcquireNodeStateRef();
    void ReleaseNodeStateRef(ui64 refId);

    ui64 GetCachedNodeSize(ui64 nodeId) const;
    void SetCachedNodeSize(ui64 nodeId, ui64 size);

    struct TPersistentQueueStats;

private:
    // Only for testing purposes
    friend struct TTestUtilBootstrap;

    struct TWriteDataEntryPart;
    struct TNodeState;
    struct TFlushState;
    class TUtil;
    struct TQueuedOperations;
    class TContiguousWriteDataEntryPartsReader;
    class TWriteDataEntryIntervalMap;
};

}   // namespace NCloud::NFileStore::NFuse
