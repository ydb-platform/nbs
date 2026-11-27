#pragma once

#include <cloud/filestore/config/vfs.pb.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/vfs_fuse/public.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/size_literals.h>

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

    // The target size of persistent storage
    ui64 StateFileSize = 0;

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

    // This threshold limits WriteBackCache queue growth to avoid excessive
    // latency for operations that must wait for cached writes to be flushed,
    // such as SetNodeAttr, ReleaseHandle, data requests with O_DIRECT flag.
    // The value is compared with the number of flush batches needed to drain
    // unflushed WriteData requests.
    ui32 MaxQueuedFlushBatchesPerNode = 0;

    // Lock acquisition method for the shared cache state
    NProto::EWriteBackCacheLockMethod LockMethod = NProto::WBC_LOCK_ADAPTIVE;

    // If the flag is enabled, WriteBackCache will generate WriteData requests
    // with iovecs.
    bool ZeroCopyWriteEnabled = false;

    // Allows flushing WriteData requests to the underlying storage in parallel.
    // This significantly improves flushing performance but an external reader
    // may observe the effects of newer writes before older ones.
    // With parallel writes off, only sequential writes will be optimized.
    bool FlushWritesInParallelEnabled = false;

    // Negotiated FUSE max_write (in bytes). The cache must be able to store
    // any WriteData request up to this size; if the state file is too small,
    // the cache is drained on init: saved data is flushed and new requests
    // bypass the cache.
    ui64 MaxGuestWriteRequestSize = 1_MB;
};

////////////////////////////////////////////////////////////////////////////////

struct IFSyncQueueRegistration
{
    virtual ~IFSyncQueueRegistration() = default;
    virtual void Enqueue() = 0;
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

    /* Puts WriteBackCache into draining mode - it prevents new WriteData
     * requests from being added to the cache and triggers flush
     * (direct writes will still be allowed).
     *
     * The cache finishes draining after it flushes all pending and
     * unflushed writes.
     *
     * Does not drop cached data on flush errors except the scenario described
     * in ReleaseHandle().
     *
     * Use IsDrained() to check completion. Storage corruption or an internal
     * cache failure suspends draining.
     */
    void Drain();

    /* Irreversibly stops accepting cached writes and starts asynchronous
     * flushing similar to Drain(). Direct writes remain allowed.
     *
     * Unlike Drain(), cached data will be dropped on flush errors,
     * therefore completion does not guarantee persistence. Dropped requests
     * are reported as critical events.
     *
     * The future completes once the cache is healthy and no pending or
     * unflushed writes remain, otherwise it will hang indefinitely and wait
     * for manual resolution. Once the cache drains, a later eviction failure
     * leaves it drained and Shutdown() returns a ready future.
     */
    [[nodiscard]] NThreading::TFuture<void> Shutdown();

    // A reliable way to check that WriteBackCache has been drained
    bool IsDrained() const;

    /* After drain, reads go directly to the session. Before drain completes,
     * a direct read runs under a barrier. A buffered read uses cached writes
     * and reads from storage if the cache does not cover the full range.
     * It pins the cached data it needs until the read ends.
     * A cache failure before drain completes can leave the read pending.
     */
    NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request,
        bool isDirect);

    /* Buffered writes complete once the data is stored in the cache.
     * If drain starts before the cache accepts a buffered write, it can
     * return E_REJECTED.
     *
     * Before drain completes, direct writes and writes during drain enter the
     * fsync queue and run under a barrier. The barrier waits for earlier
     * cached writes and blocks flushes until the direct write ends.
     * After drain, writes enter the fsync queue and go straight to the session.
     */
    NThreading::TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request,
        IFSyncQueueRegistration& fsyncQueueRegistration,
        bool isDirect);

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
     * or pending WriteData requests associated with the handle.
     *
     * If an error occurs during flushing WriteData requests, returns the error
     * (the same as for FlushNodeData) and tags unflushed WriteData requests
     * associated with the handle - they will be attempted to be flushed again
     * later via another known live handle. If there are no remaining live
     * handles, all unflushed requests are dropped.
     *
     * Note: the method doesn't call Session::DestroyHandle
     */
    NThreading::TFuture<NProto::TError> ReleaseHandle(
        ui64 nodeId,
        ui64 handle);

    /* Execute SetNodeAttr with taking awareness of changing node size under a
     * barrier.
     */
    NThreading::TFuture<NProto::TSetNodeAttrResponse> SetNodeAttr(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TSetNodeAttrRequest> request);

    /* Execute CreateHandle with taking awareness of changing node size under a
     * barrier if E_TRUNCATE flag is set or bypass to the session otherwise
     */
    NThreading::TFuture<NProto::TCreateHandleResponse> CreateHandle(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TCreateHandleRequest> request);

    // Keep information about MinNodeSize for flushed nodes
    ui64 AcquireNodeStateRef();
    void ReleaseNodeStateRef(ui64 refId);

    // Used to adjust node size according to cached data
    ui64 GetMaxWrittenOffset(ui64 nodeId) const;

    IModuleStatsPtr CreateModuleStats() const;
};

}   // namespace NCloud::NFileStore::NFuse
