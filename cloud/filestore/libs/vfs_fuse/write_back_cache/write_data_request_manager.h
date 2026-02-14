#pragma once

#include "persistent_storage.h"
#include "sequence_id_generator.h"
#include "write_back_cache_stats.h"
#include "write_data_request.h"

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/function_ref.h>
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
    IWriteBackCacheStatsPtr Stats;

    TIntrusiveList<TPendingWriteDataRequest> PendingRequests;
    TIntrusiveList<TCachedWriteDataRequest> UnflushedRequests;
    TIntrusiveList<TCachedWriteDataRequest> FlushedRequests;

public:
    using TAddRequestResult = std::variant<
        std::unique_ptr<TPendingWriteDataRequest>,
        std::unique_ptr<TCachedWriteDataRequest>>;

    using TCachedRequestVisitor =
        TFunctionRef<void(std::unique_ptr<TCachedWriteDataRequest> request)>;

    TWriteDataRequestManager() = default;
    TWriteDataRequestManager(TWriteDataRequestManager&&) = default;
    TWriteDataRequestManager& operator=(TWriteDataRequestManager&&) = default;

    TWriteDataRequestManager(
        ISequenceIdGeneratorPtr sequenceIdGenerator,
        IPersistentStoragePtr persistentStorage,
        ITimerPtr timer,
        IWriteBackCacheStatsPtr stats);

    // Reads state from the persistent storage
    bool Init(const TCachedRequestVisitor& visitor);

    bool HasPendingRequests() const;
    bool HasPendingOrUnflushedRequests() const;

    // Returns Max<ui64>() when there are no pending and unflushed requests
    ui64 GetMinPendingOrUnflushedSequenceId() const;

    // Returns 0 when there are no pending and unflushed requests
    ui64 GetMaxPendingOrUnflushedSequenceId() const;

    // Returns 0 when there are no unflushed requests
    ui64 GetMaxUnflushedSequenceId() const;

    /**
     * Adds a WriteData request to the storage.
     *
     * Returns std::unique_ptr<TCachedWriteDataRequest> when the request is
     * successfully stored in the persistent storage.
     *
     * Returns std::unique_ptr<TPendingWriteDataRequest> when the storage is
     * full and the request is added to the pending queue.
     */
    TAddRequestResult AddRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    /**
     * Takes a front request from the pending queue and tries to store it into
     * the persistent storage.
     *
     * Returns std::unique_ptr<TCachedWriteDataRequest> if the request is
     * successfully processed.
     *
     * Returns nullptr if there are no pending requests or the persistent
     * storage is full.
     */
    std::unique_ptr<TCachedWriteDataRequest> TryProcessPendingRequest();

    // Removes the request from the pending queue
    void Remove(std::unique_ptr<TPendingWriteDataRequest> request);

    /**
     * Marks the request as flushed
     * It continues residing in the persistent storage until Evict is called
     */
    void SetFlushed(TCachedWriteDataRequest* request);

    // Removes previously flushed request from the persistent storage
    void Evict(std::unique_ptr<TCachedWriteDataRequest> request);

private:
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
