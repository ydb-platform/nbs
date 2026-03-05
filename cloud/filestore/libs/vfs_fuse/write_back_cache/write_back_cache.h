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

struct TWriteBackCacheArgs
{
    IFileStorePtr Session;
    ISchedulerPtr Scheduler;
    ITimerPtr Timer;
    NWriteBackCache::IWriteBackCacheStatsPtr Stats;
    TLog Log;
    TString FileSystemId;
    TString ClientId;

    // Path to the file used for persistent storage
    TString FilePath;

    // Maximum size of persistent storage
    ui64 CapacityBytes = 0;

    // WriteBackCache calls FlushAllData with the specified period
    TDuration AutomaticFlushPeriod = TDuration::Zero();

    // Retry period for failed WriteData operations in flush
    TDuration FlushRetryPeriod = TDuration::Zero();

    // Maximum size (in bytes) of a consolidated WriteData request for flush.
    // If this limit is exceeded, WriteData requests will be split into multiple
    // requests, each not exceeding this limit.
    ui32 FlushMaxWriteRequestSize = 0;

    // Maximum number of consolidated WriteData requests to be executed in
    // parallel in a flush batch. If this limit is exceeded, flush will execute
    // a sequence of batches, each not exceeding this limit.
    // Note: the limit is applied per node.
    ui32 FlushMaxWriteRequestsCount = 0;

    // Maximum total size (in bytes) of all WriteData requests in a flush batch.
    // If this limit is exceeded, flush will execute a sequence of batches, each
    // not exceeding this limit.
    // Note: the limit is applied per node.
    ui32 FlushMaxSumWriteRequestsSize = 0;

    // If the flag is enabled, WriteBackCache will generate WriteData requests
    // with iovecs.
    bool ZeroCopyWriteEnabled = false;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache final
{
private:
    class TImpl;

    std::shared_ptr<TImpl> Impl;

public:
    TWriteBackCache();
    ~TWriteBackCache();

    explicit TWriteBackCache(TWriteBackCacheArgs args);

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
};

}   // namespace NCloud::NFileStore::NFuse
