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

    // Non-owning lifecycle queues; callers own the requests (TNodeCache in
    // production). Allocation is global FIFO, so every node's pending queue
    // consists of an allocated prefix followed by an unallocated suffix.
    // Failure paths may remove only the unallocated suffix. Allocated requests
    // remain alive for out-of-lock serialization and leave this queue only
    // through GetNextReadyCachedRequest().
    TIntrusiveList<TPendingWriteDataRequest> UnallocatedPendingRequests;
    TIntrusiveList<TPendingWriteDataRequest> AllocatedPendingRequests;
    TIntrusiveList<TCachedWriteDataRequest> UnflushedRequests;
    TIntrusiveList<TCachedWriteDataRequest> FlushedRequests;

    THashSet<ui64> NodesWithBackpressure;

public:
    struct TAllocPendingRequestResult
    {
        TPendingWriteDataRequest* Request = nullptr;
        bool Failed = false;
        bool StorageIsFull = false;
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
     * Creates a pending WriteData request and adds a non-owning pointer to the
     * unallocated queue; the returned object remains owned by the caller.
     *
     * After TryAllocPendingRequest() succeeds, the serializer may hold a raw
     * pointer while the cache-state lock is released. An allocated request
     * must remain alive until GetNextReadyCachedRequest() removes it from
     * AllocatedPendingRequests.
     */
    [[nodiscard]] std::unique_ptr<TPendingWriteDataRequest> AddRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    /**
     * Tries to allocate buffer in the persistent storage for the front
     * unallocated pending request.
     *
     * Returns an empty result if the queue is empty, the storage is full or
     * backpressure is in effect. Sets Failed on a storage error.
     */
    [[nodiscard]] TAllocPendingRequestResult TryAllocPendingRequest();

    /**
     * Examines the lowest-sequence allocated request awaiting commit.
     *
     * If serialization is complete, commits its allocation, removes the pending
     * request from its lifecycle queue, creates a cached request, registers it
     * in the unflushed queue, and returns it.
     *
     * Returns an empty result if there are no allocated requests or the first
     * request is not serialized yet. Sets Failed on a storage error.
     */
    [[nodiscard]] TGetNextReadyCachedRequestResult GetNextReadyCachedRequest();

    // Returns the highest-sequence unallocated pending request, or nullptr if
    // there are no unallocated pending requests.
    [[nodiscard]] const TPendingWriteDataRequest*
    GetBackUnallocatedPendingRequest() const;

    // Removes an unallocated request from the pending queue.
    void RemoveUnallocated(std::unique_ptr<TPendingWriteDataRequest> request);

    /**
     * Marks the request as flushed
     * It continues residing in the persistent storage until Evict is called
     *
     * Returns true on success
     * Returns false on invalid argument or corrupted state
     */
    [[nodiscard]] bool SetFlushed(TCachedWriteDataRequest* request);

    /**
     * Marks the request as related to a released handle and stores this in
     * the persistent storage.
     * This allows the request to be properly handled after restart.
     *
     * Returns true on success
     * Returns false on invalid argument or corrupted state
     */
    [[nodiscard]] bool SetHandleReleased(TCachedWriteDataRequest* request);

    /**
     * Removes previously flushed request from the persistent storage
     *
     * Returns true on success
     * Returns false on invalid argument or corrupted state
     */
    [[nodiscard]] bool Evict(std::unique_ptr<TCachedWriteDataRequest> request);

    // Prevent from adding new requests to the unflushed queue for the node
    // Returns true if backpressure was not previously set, false otherwise
    bool SetBackpressureStatusForNode(ui64 nodeId);

    // Allows adding new requests to the unflushed queue for the node
    // Returns true if backpressure was previously set, false otherwise
    bool ClearBackpressureStatusForNode(ui64 nodeId);

    void UpdateStats() const;

private:
    bool HasUnallocatedPendingRequests() const;
    bool HasAllocatedPendingRequests() const;

    // Access methods that trigger stats update
    void UnallocatedPendingRequestsPushBack(TPendingWriteDataRequest* request);
    void UnallocatedPendingRequestsRemove(TPendingWriteDataRequest* request);
    void AllocatedPendingRequestsRemove(TPendingWriteDataRequest* request);
    void UnflushedRequestsPushBack(TCachedWriteDataRequest* request);
    void UnflushedRequestsRemove(TCachedWriteDataRequest* request);
    void FlushedRequestsPushBack(TCachedWriteDataRequest* request);
    void FlushedRequestsRemove(TCachedWriteDataRequest* request);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
