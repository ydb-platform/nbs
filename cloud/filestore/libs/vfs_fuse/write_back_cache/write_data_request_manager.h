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

    TIntrusiveList<TPendingWriteDataRequest, TSerializationNeededTag>
        SerializationNeededRequests;

    THashSet<ui64> NodesWithBackpressure;
    bool StorageIsFull = false;

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

    // Returns whether the last allocation attempt failed because persistent
    // storage was full.
    bool GetStorageIsFull() const;

    /**
     * Creates unallocated pending request.
     *
     * The caller must keep the returned request alive and drive request
     * processing through TryAllocPendingRequest and GetNextReadyCachedRequest.
     *
     * Returns nullptr if the storage is in failed state.
     */
    [[nodiscard]] std::unique_ptr<TPendingWriteDataRequest> AddRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    /**
     * Gets the front pending request (if it exists) and tried to allocate it.
     *
     * If persistent storage has enough space and the request is not
     * backpressured, it is assigned an allocation. The caller must then
     * serialize the request and process ready requests by repeatedly calling
     * GetNextReadyCachedRequest.
     *
     * - TAllocRequestResult::Request contains the request if allocation was
     *   sucessfull;
     * - TAllocRequestResult::StorageIsFull is set if the request cannot be
     *   allocated because the storage doesn't have enough space;
     * - TAllocRequestResult::Failed is set on storage failure.
     */
    [[nodiscard]] TAllocRequestResult TryAllocPendingRequest();

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

    /**
     * Returns the highest-sequence unallocated pending request, or nullptr if
     * there is none.
     */
    [[nodiscard]] const TPendingWriteDataRequest*
    GetBackUnallocatedPendingRequest() const;

    /**
     * Removes unallocated pending request.
     * An attempt to pass an allocated request will cause failure.
     */
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
     * Removes a previously flushed request from persistent storage.
     *
     * Caller must process pending requests by TryAllocPendingRequest and
     * GetNextReadyCachedRequest.
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
    bool HasUnallocatedPendingRequests() const;
    bool HasAllocatedPendingRequests() const;
    bool HasUnflushedRequests() const;

    // Access methods that triggers stats update
    void UnallocatedPendingRequestsPushBack(TPendingWriteDataRequest* request);
    void UnallocatedPendingRequestsRemove(TPendingWriteDataRequest* request);
    void AllocatedPendingRequestsRemove(TPendingWriteDataRequest* request);

    void UnflushedRequestsPushBack(TCachedWriteDataRequest* request);
    void UnflushedRequestsRemove(TCachedWriteDataRequest* request);

    void FlushedRequestsPushBack(TCachedWriteDataRequest* request);
    void FlushedRequestsRemove(TCachedWriteDataRequest* request);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
