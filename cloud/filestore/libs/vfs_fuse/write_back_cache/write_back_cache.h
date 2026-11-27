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

// Only transition Normal -> Draining -> Drained is possible
enum class EWriteBackCacheMode
{
    // WriteBackCache is used normally
    // New cached WriteData requests are accepted
    Normal,

    // WriteBackCache was requested to drain and it contains unflushed data
    // New cached WriteData requests are not accepted and will return E_REJECTED
    // Clients should continue working with WriteBackCache but use
    // WriteDataDirect instead of WriteDataBuffered
    Draining,

    // WriteBackCache was requested to drain and no pending or unflushed writes
    // remain
    // New cached WriteData requests are not accepted and will return E_REJECTED
    // New operations may bypass the cache
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

    // Returns current mode of WriteBackCache with weak memory ordering.
    EWriteBackCacheMode GetMode() const;

    /* Puts WriteBackCache into draining mode - it prevents new WriteData
     * requests from being added to the cache and triggers flush
     * (WriteDataDirect calls will still be allowed).
     *
     * WriteBackCache will remain in EWriteBackCacheMode::Draining mode until
     * all pending and unflushed are flushed - then it will become
     * EWriteBackCacheMode::Drained.
     *
     * Does not drop cached data on flush errors except the scenario described
     * in ReleaseHandle().
     *
     * Use IsDrained() to check completion. Storage corruption or an internal
     * cache failure suspends draining.
     */
    void Drain();

    /* Irreversibly stops accepting cached writes and starts asynchronous
     * flushing similar to Drain(). WriteDataDirect remains allowed.
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

    /* Reads from storage and applies cached writes to the result. If cached
     * writes cover the full range, it reads only from the cache.
     *
     * Pins the data it needs until the read ends, even if a flush completes.
     *
     * Reads can run during or after drain while the cache is healthy.
     * If the cache fails, the read can stay pending.
     */
    NThreading::TFuture<NProto::TReadDataResponse> ReadDataBuffered(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    /* The future succeeds once the write is stored in the cache and can be
     * read by ReadDataBuffered().
     *
     * Use FlushNodeData() or FlushAllData() to wait for writes to reach the
     * underlying storage.
     *
     * After Drain() or Shutdown(), the cache rejects new buffered writes with
     * E_REJECTED. While draining, use WriteDataDirect(). It waits for earlier
     * cached writes under a barrier before sending the write to storage.
     */
    NThreading::TFuture<NProto::TWriteDataResponse> WriteDataBuffered(
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

    /* Read directly from the underlying storage under a barrier.
     * An acquired barrier ensures that operation will not interfere with cache:
     * - All cached WriteData requests prior to barrier acquisition are flushed
     *   and evicted.
     * - No flush may take place until the barrier is released.
     * Note: the sequence point is barrier acquisition, not the request itself.
     * It means that newer WriteData requests may be reordered and flushed while
     * the barrier is in pending state.
     */
    NThreading::TFuture<NProto::TReadDataResponse> ReadDataDirect(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    /* Write directly to the underlying storage under a barrier.
     * An acquired barrier ensures that operation will not interfere with cache:
     * - All cached WriteData requests prior to barrier acquisition are flushed
     *   and evicted.
     * - No flush may take place until the barrier is released.
     * Note: the sequence point is barrier acquisition, not the request itself.
     * It means that newer WriteData requests may be reordered and flushed while
     * the barrier is in pending state.
     */
    NThreading::TFuture<NProto::TWriteDataResponse> WriteDataDirect(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request);

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
