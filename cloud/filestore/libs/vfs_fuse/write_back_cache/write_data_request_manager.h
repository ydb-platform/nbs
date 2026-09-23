#pragma once

#include "persistent_storage.h"
#include "sequence_id_generator.h"
#include "write_data_request.h"
#include "write_data_request_manager_stats.h"

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/function_ref.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

// The class is not thread-safe
class TWriteDataRequestManager
{
private:
    ISequenceIdGeneratorPtr SequenceIdGenerator;
    IPersistentStoragePtr PersistentStorage;
    ITimerPtr Timer;
    IWriteDataRequestManagerStatsPtr Stats;

    TIntrusiveList<TPendingWriteDataRequest> UnallocatedPendingRequests;
    TIntrusiveList<TPendingWriteDataRequest> AllocatedPendingRequests;
    TIntrusiveList<TCachedWriteDataRequest> UnflushedRequests;
    TIntrusiveList<TCachedWriteDataRequest> FlushedRequests;

    THashSet<ui64> NodesWithBackpressure;

public:
    struct TAllocRequestResult
    {
        TPendingWriteDataRequest* Request = nullptr;
        bool StorageIsFull = false;
        bool Failed = false;
    };

    struct TGetNextReadyCachedRequestResult
    {
        std::unique_ptr<TCachedWriteDataRequest> Request = nullptr;
        bool Failed = false;
    };

    using TCachedRequestVisitor = TFunctionRef<void(
        std::unique_ptr<TCachedWriteDataRequest> request,
        bool handleReleased)>;

    TWriteDataRequestManager(TWriteDataRequestManager&&) = default;
    TWriteDataRequestManager& operator=(TWriteDataRequestManager&&) = default;

    TWriteDataRequestManager(
        ISequenceIdGeneratorPtr sequenceIdGenerator,
        IPersistentStoragePtr persistentStorage,
        ITimerPtr timer,
        IWriteDataRequestManagerStatsPtr stats);

    // Reads state from the persistent storage
    NProto::TError Init(const TCachedRequestVisitor& visitor);

    bool HasPendingRequests() const;
    bool HasPendingOrUnflushedRequests() const;

    // Returns Max<ui64>() when there are no pending and unflushed requests
    ui64 GetMinPendingOrUnflushedSequenceId() const;

    // Returns 0 when there are no pending and unflushed requests
    ui64 GetMaxPendingOrUnflushedSequenceId() const;

    // Returns 0 when there are no unflushed requests
    ui64 GetMaxUnflushedSequenceId() const;

    /**
     * Creates an unallocated pending request and registers it in the internal
     * queues.
     *
     * The caller must keep the returned request alive and drive request
     * processing through TryAllocPendingRequest and GetNextReadyCachedRequest.
     */
    [[nodiscard]] std::unique_ptr<TPendingWriteDataRequest> AddRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    /**
     * Attempts to allocate buffer in the persistent storage for the first
     * unallocated pending request.
     *
     * If persistent storage has enough space and the request's node is not
     * backpressured, it is assigned an allocation. The caller must then
     * serialize the returned request, mark it as serialized, and process ready
     * requests by repeatedly calling GetNextReadyCachedRequest.
     *
     * Returns an empty result if there are no unallocated requests or the first
     * request's node is backpressured. Otherwise:
     * - Request is set when allocation succeeds;
     * - StorageIsFull is set when there is not enough space;
     * - Failed is set when the storage operation fails.
     */
    [[nodiscard]] TAllocRequestResult TryAllocPendingRequest();

    /**
     * Examines the lowest-sequence allocated request awaiting commit.
     *
     * If serialization is complete, commits its allocation, removes the pending
     * request from its lifecycle queue, creates a cached request, registers it
     * in the unflushed queue, and returns it.
     *
     * Returns an empty result if there are no allocated requests or the first
     * request is not serialized yet.
     *
     * Sets Failed if the storage operation fails.
     */
    [[nodiscard]] TGetNextReadyCachedRequestResult GetNextReadyCachedRequest();

    /**
     * Returns the highest-sequence unallocated pending request, or nullptr if
     * there is none.
     */
    [[nodiscard]] const TPendingWriteDataRequest*
    GetBackUnallocatedPendingRequest() const;

    /**
     * Removes an unallocated pending request from the internal queue.
     * Passing an allocated request violates the method's precondition.
     */
    void RemoveUnallocated(std::unique_ptr<TPendingWriteDataRequest> request);

    /**
     * Marks the request as flushed. It remains in persistent storage until
     * Evict is called.
     *
     * Returns true on success and false on an invalid argument or the storage
     * operation fails.
     */
    [[nodiscard]] bool SetFlushed(TCachedWriteDataRequest* request);

    /**
     * Marks the request's handle as released in persistent storage. This allows
     * the request to be handled correctly after a restart.
     *
     * Returns true on success and false on an invalid argument or the storage
     * operation fails.
     */
    [[nodiscard]] bool SetHandleReleased(TCachedWriteDataRequest* request);

    /**
     * Removes a previously flushed request from persistent storage.
     *
     * After a successful eviction, the caller should resume pending-request
     * processing through TryAllocPendingRequest and GetNextReadyCachedRequest.
     *
     * Returns true on success and false if a storage operation fails.
     */
    [[nodiscard]] bool Evict(std::unique_ptr<TCachedWriteDataRequest> request);

    // Prevents requests for the node from being allocated.
    // Returns true if backpressure was newly set.
    bool SetBackpressureStatusForNode(ui64 nodeId);

    /**
     * Allows requests for the node to be allocated.
     *
     * This method only clears the backpressure marker. The caller should then
     * resume processing through TryAllocPendingRequest and
     * GetNextReadyCachedRequest.
     *
     * Returns true if backpressure was previously set, false otherwise.
     */
    bool ClearBackpressureStatusForNode(ui64 nodeId);

    void UpdateStats() const;

private:
    bool HasUnallocatedPendingRequests() const;
    bool HasAllocatedPendingRequests() const;
    bool HasUnflushedRequests() const;

    // Queue accessors that update statistics.
    void UnallocatedPendingRequestsPushBack(TPendingWriteDataRequest* request);
    void UnallocatedPendingRequestsRemove(TPendingWriteDataRequest* request);
    void AllocatedPendingRequestsRemove(TPendingWriteDataRequest* request);

    void UnflushedRequestsPushBack(TCachedWriteDataRequest* request);
    void UnflushedRequestsRemove(TCachedWriteDataRequest* request);

    void FlushedRequestsPushBack(TCachedWriteDataRequest* request);
    void FlushedRequestsRemove(TCachedWriteDataRequest* request);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
