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
    TIntrusiveList<TCachedWriteDataRequest> UnflushedRequests;
    TIntrusiveList<TCachedWriteDataRequest> FlushedRequests;

    THashSet<ui64> NodesWithBackpressure;

public:
    struct TAddRequestResult
    {
        std::unique_ptr<TPendingWriteDataRequest> PendingRequest = nullptr;
        std::unique_ptr<TCachedWriteDataRequest> CachedRequest = nullptr;
    };

    struct TProcessPendingRequestRequest
    {
        std::unique_ptr<TCachedWriteDataRequest> CachedRequest = nullptr;
        bool Failed = false;
    };

    using TCachedRequestVisitor = TFunctionRef<void(
        std::unique_ptr<TCachedWriteDataRequest> request,
        bool handleReleased)>;

    TWriteDataRequestManager() = default;
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
     * Adds a WriteData request to the persistent storage.
     *
     * Returns result with non-empty TAddRequestResult::CachedRequest if the
     * request has been successfully stored in the storage.
     *
     * Returns result with non-empty TAddRequestResult::PendingRequest if the
     * the storage is full or backpressure is in effect, and the request has
     * been added to the pending queue.
     *
     * Returns empty result if the storage is in failed state.
     */
    [[nodiscard]] TAddRequestResult AddRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    /**
     * Takes front request from the pending queue and tries to store it into
     * the persistent storage.
     *
     * Returns result with non-empty TAddRequestResult::CachedRequest if the
     * front request has been successfully stored in the storage.
     *
     * Returns result with empty TAddRequestResult::CachedRequest and
     * TAddRequestResult::Failed == false if the storage is full, backpressure
     * is in effect or the pending queue is empty.
     *
     * Returns result with empty TAddRequestResult::CachedRequest and
     * TAddRequestResult::Failed == true if the storage is in failed state.
     */
    [[nodiscard]] TProcessPendingRequestRequest TryProcessPendingRequest();

    // Takes and removes front request from the pending queue.
    // Returns the removed request or nullptr if there are no pending requests.
    [[nodiscard]] TPendingWriteDataRequest* TryPopFrontPendingRequest();

    // Removes the request from the pending queue
    void Remove(std::unique_ptr<TPendingWriteDataRequest> request);

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
    TProcessPendingRequestRequest TryStoreRequestInPersistentStorage(
        ui64 sequenceId,
        TInstant time,
        const NProto::TWriteDataRequest& request);

    // Access methods that triggers stats update
    void PendingRequestsPushBack(TPendingWriteDataRequest* request);
    void PendingRequestsRemove(TPendingWriteDataRequest* request);
    void PendingRequestsPopFront();
    void UnflushedRequestsPushBack(TCachedWriteDataRequest* request);
    void UnflushedRequestsRemove(TCachedWriteDataRequest* request);
    void FlushedRequestsPushBack(TCachedWriteDataRequest* request);
    void FlushedRequestsRemove(TCachedWriteDataRequest* request);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
