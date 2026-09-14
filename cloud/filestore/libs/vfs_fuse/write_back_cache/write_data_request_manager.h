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

    TIntrusiveList<TPendingWriteDataRequest> PendingRequests;
    TIntrusiveList<TPendingWriteDataRequest> AllocatedRequests;
    TIntrusiveList<TPendingWriteDataRequest> SerializingRequests;

    TIntrusiveList<TCachedWriteDataRequest> UnflushedRequests;
    TIntrusiveList<TCachedWriteDataRequest> FlushedRequests;

    TDeque<TPendingWriteDataRequestPtr> CancelSerializingRequests;
    THashSet<ui64> NodesWithBackpressure;
    bool StorageIsFull = false;

public:
    struct TAllocRequestResult
    {
        bool Allocated = false;
        bool Failed = false;
    };

    struct TGetNextReadyCachedRequestResult
    {
        TCachedWriteDataRequestPtr Request = nullptr;
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

    // Returns whether the last allocation attempt failed because persistent
    // storage was full.
    bool GetStorageIsFull() const;

    /**
     * Adds a WriteData request to the manager.
     *
     * If persistent storage has enough space, the request is assigned an
     * allocation and queued for serialization. Otherwise, it remains pending
     * until a later eviction makes space available.
     *
     * The caller must keep the returned request alive and drive request
     * processing through GetNextPendingRequestToSerialize,
     * SetPendingRequestSerialized, and GetNextReadyCachedRequest.
     *
     * Returns nullptr if the storage is in failed state.
     */
    [[nodiscard]] std::unique_ptr<TPendingWriteDataRequest> AddRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    /**
     * Gets the next allocated request to serialize.
     *
     * Removes the request from the allocation queue, marks it as serializing,
     * and returns a non-owning pointer. The caller should serialize it and then
     * call SetPendingRequestSerialized.
     *
     * Returns nullptr if no allocated request is waiting for serialization.
     */
    [[nodiscard]] TPendingWriteDataRequest* GetNextPendingRequestToSerialize();

    /**
     * Completes serialization of a request returned by
     * GetNextPendingRequestToSerialize.
     *
     * If removal was requested during serialization, cancels the allocation
     * instead of making the request ready to commit. Otherwise, the caller
     * should repeatedly call GetNextReadyCachedRequest after this method
     * succeeds. An earlier request or per-node backpressure may prevent this
     * request from being returned immediately.
     *
     * Returns false if the request is in an invalid state or cancellation
     * fails.
     */
    [[nodiscard]] bool SetPendingRequestSerialized(
        TPendingWriteDataRequest* pendingRequest);

    /**
     * Examines the lowest-sequence request awaiting commit.
     *
     * If serialization is complete and the request's node is not
     * backpressured, commits its allocation, removes the pending request from
     * its lifecycle queue, creates a cached request, registers it in the
     * unflushed queue, and returns it. Returns an empty result when the front
     * request is not ready.
     *
     * Sets Failed if Commit returns an error.
     */
    [[nodiscard]] TGetNextReadyCachedRequestResult GetNextReadyCachedRequest();

    // Returns the lowest-sequence pending request, or nullptr if there is none.
    [[nodiscard]] const TPendingWriteDataRequest*
    GetFrontPendingRequest() const;

    /**
     * Removes a request that has not yet entered the unflushed queue.
     *
     * Cancels its uncommitted allocation, if any. If serialization is in
     * progress, cancellation is deferred until serialization finishes.
     *
     * Returns false if a persistent-storage operation fails.
     */
    [[nodiscard]] bool Remove(TPendingWriteDataRequestPtr request);

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
     * Removes a previously flushed request from persistent storage.
     *
     * If this makes enough space available, pending requests are assigned
     * allocations and queued for serialization. The caller should then resume
     * processing through GetNextPendingRequestToSerialize,
     * SetPendingRequestSerialized, and GetNextReadyCachedRequest.
     *
     * Returns true on success and false if a storage operation fails.
     */
    [[nodiscard]] bool Evict(std::unique_ptr<TCachedWriteDataRequest> request);

    // Prevents serialized requests for the node from being committed and moved
    // to the unflushed queue. Returns true if backpressure was newly set.
    bool SetBackpressureStatusForNode(ui64 nodeId);

    /**
     * Allows serialized requests for the node to be committed.
     *
     * This method only clears the backpressure marker. The caller should then
     * resume processing through GetNextReadyCachedRequest.
     *
     * Returns true if backpressure was previously set, false otherwise.
     */
    bool ClearBackpressureStatusForNode(ui64 nodeId);

    void UpdateStats() const;

private:
    TAllocRequestResult TryAllocRequestInPersistentStorage(
        TPendingWriteDataRequest* pendingRequest);

    // Returns false if an allocation attempt returns an error
    bool AllocPendingRequestsInPersistentStorage();

    // Access methods that triggers stats update
    void PendingRequestsPushBack(TPendingWriteDataRequest* request);
    void PendingRequestsRemove(TPendingWriteDataRequest* request);
    void PendingRequestsPopFront();

    void AllocatedRequestsPushBack(TPendingWriteDataRequest* request);
    void AllocatedRequestsRemove(TPendingWriteDataRequest* request);

    void SerializingRequestsRemove(TPendingWriteDataRequest* request);

    void UnflushedRequestsPushBack(TCachedWriteDataRequest* request);
    void UnflushedRequestsRemove(TCachedWriteDataRequest* request);

    void FlushedRequestsPushBack(TCachedWriteDataRequest* request);
    void FlushedRequestsRemove(TCachedWriteDataRequest* request);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
