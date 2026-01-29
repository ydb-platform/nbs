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
    TString FilePath;
    ui64 CapacityBytes = 0;
    TDuration AutomaticFlushPeriod = TDuration::Zero();
    TDuration FlushRetryPeriod = TDuration::Zero();
    ui32 FlushMaxWriteRequestSize = 0;
    ui32 FlushMaxWriteRequestsCount = 0;
    ui32 FlushMaxSumWriteRequestsSize = 0;
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

    /* The method returns a future that is fulfilled when all WriteData requests
     * associated with the node and started before the call are flushed - this
     * also affects pending requests.
     *
     * If an error occurs while executing WriteData requests at Flush,
     * propagates the error to the FlushNodeData result. If multiple WriteData
     * requests fails, returns an error from any of them.
     *
     * Unflushed requests will remain in the cache and will be retried later.
     * If multiple errors occur, returns any of them.
     */
    NThreading::TFuture<NCloud::NProto::TError> FlushNodeData(ui64 nodeId);

    /* The method returns a future that is fulfilled when all WriteData requests
     * started before the call are flushed - this also affects pending requests.
     *
     * If an error occurs while flushing WriteData requests, returns the error
     * (the same as for FlushNodeData)
     */
    NThreading::TFuture<NCloud::NProto::TError> FlushAllData();

    /* Ensures that the handle is safe to be destroyed.
     *
     * The method returns a future that is fulfilled when there are no unflushed
     * or pending WriteData requests associated with the handle. Unlike
     * FlushNodeData, it doesn't fail if flush fails and waits instead.
     *
     * If all handles used by WriteBackCache for the node are requested for the
     * release and flush fails:
     * - unflushed WriteData requests are dropped;
     * - pending WriteData requests are failed;
     * - executing ReleaseHandle requests return an error.
     *
     * Note: the method doesn't call Session::DestroyHandle
     */
    NThreading::TFuture<NProto::TError> ReleaseHandle(
        ui64 nodeId,
        ui64 handle);

    bool IsEmpty() const;

    // Keep information about MinNodeSize for flushed nodes
    ui64 AcquireNodeStateRef();
    void ReleaseNodeStateRef(ui64 refId);

    ui64 GetCachedNodeSize(ui64 nodeId) const;
    void SetCachedNodeSize(ui64 nodeId, ui64 size);
};

}   // namespace NCloud::NFileStore::NFuse
