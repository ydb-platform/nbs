#pragma once

#include <cloud/filestore/libs/diagnostics/public.h>
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

// Only transition Normal -> Draining -> Drained is possible
enum class EWriteBackCacheMode
{
    // WriteBackCache is used normally
    // New cached WriteData requests are accepted
    Normal,

    // WriteBackCache was requested to drain and it contains unflushed data
    // New cached WriteData requests are not accepted and will return E_REJECTED
    // Clients should continue working with WriteBackCache but use
    // WriteDataDirect instead of WriteData
    Draining,

    // WriteBackCache was requested to drain and all the data is flushed
    // New cached WriteData requests are not accepted and will return E_REJECTED
    // Since WriteBackCache contains no data, clients may safely stop using it
    Drained
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

    /* Returns current mode of WriteBackCache with weak memory ordering.
     * The common EWriteBackCacheMode::Normal and EWriteBackCacheMode::Drained
     * cases are returned without mutex acquisition and are suitable for hot
     * paths. In order to ensure that WriteBackCache has been drained,
     * IsDrained() should be used, as drained mode detection may require
     * additional synchronization.
     */
    EWriteBackCacheMode GetMode() const;

    /* Puts WriteBackCache into draining mode - it prevents new WriteData
     * requests from being added to the cache and triggers flush
     * (WriteDataDirect calls will still be allowed).
     *
     * WriteBackCache will remain in EWriteBackCacheMode::Draining mode until
     * all pending and unflushed are flushed - then it will become
     * EWriteBackCacheMode::Drained.
     *
     * The returned future is completed successfully when WriteBackCache become
     * drained. An error is returned if flush is failed.
     *
     * The call has no effect if WriteBackCache is already drained and will
     * return a completed future immediately.
     *
     * Note: the call is not reversible (WriteBackCache cannot be returned to
     * EWriteBackCacheMode::Normal mode without restart).
     */
    NThreading::TFuture<NCloud::NProto::TError> Drain();

    // A reliable way to check that WriteBackCache has been drained
    bool IsDrained() const;

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

    /* Read directly from the underlying storage.
     * All prior cached WriteData requests are flushed and evicted.
     * No new WriteData requests will be flushed until the direct read is
     * completed.
     */
    NThreading::TFuture<NProto::TReadDataResponse> ReadDataDirect(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    /* Write directly to the underlying storage.
     * All prior cached WriteData requests are flushed and evicted.
     * No new WriteData requests will be flushed until the direct write is
     * completed.
     */
    NThreading::TFuture<NProto::TWriteDataResponse> WriteDataDirect(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request);

    // Execute SetNodeAttr with taking awareness of changing node size
    NThreading::TFuture<NProto::TSetNodeAttrResponse> SetNodeAttr(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TSetNodeAttrRequest> request);

    // Keep information about MinNodeSize for flushed nodes
    ui64 AcquireNodeStateRef();
    void ReleaseNodeStateRef(ui64 refId);

    // Used to adjust node size according to cached data
    ui64 GetMaxWrittenOffset(ui64 nodeId) const;

    IModuleStatsPtr CreateModuleStats() const;
};

}   // namespace NCloud::NFileStore::NFuse
