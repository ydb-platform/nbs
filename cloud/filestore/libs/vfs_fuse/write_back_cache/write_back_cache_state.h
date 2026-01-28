#pragma once

#include "cached_write_data_request.h"
#include "persistent_storage.h"
#include "write_back_cache_state_listener.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/protos/error.pb.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/function_ref.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

// The class is thread safe
class TWriteBackCacheState
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    using TEntryVisitor = TFunctionRef<bool(const TCachedWriteDataRequest*)>;
    using TCachedDataVisitor = TFunctionRef<bool(TCachedDataPart part)>;
    using TPin = ui64;

    using TWriteDataRequest = NProto::TWriteDataRequest;
    using TWriteDataResponse = NProto::TWriteDataResponse;
    using TError = NCloud::NProto::TError;

    TWriteBackCacheState();
    TWriteBackCacheState(TWriteBackCacheState&&) noexcept;
    TWriteBackCacheState& operator=(TWriteBackCacheState&&) noexcept;
    ~TWriteBackCacheState();

    TWriteBackCacheState(
        IPersistentStorage& persistentStorage,
        IWriteBackCacheStateListener& listener);

    // Read state from the persistent storage
    bool Init();

    bool HasUnflushedRequests() const;

    // Add a WriteData request to the pending queue and completes the future
    // when the request is stored in the persistent storage and becomes cached
    NThreading::TFuture<TWriteDataResponse> AddWriteDataRequest(
        std::shared_ptr<TWriteDataRequest> request);

    NThreading::TFuture<TError> AddFlushRequest(ui64 nodeId);

    NThreading::TFuture<TError> AddFlushAllRequest();

    NThreading::TFuture<TError> AddReleaseHandleRequest(
        ui64 nodeId,
        ui64 handle);

    void TriggerPeriodicFlushAll();

    // Includes both flushed and unflushed data
    void VisitCachedData(
        ui64 nodeId,
        ui64 offset,
        ui64 byteCount,
        const TCachedDataVisitor& visitor) const;

    ui64 GetCachedDataEndOffset(ui64 nodeId) const;

    // Prevent WriteData requests from being evicted from cache after flush
    TPin PinCachedData(ui64 nodeId);
    void UnpinCachedData(ui64 nodeId, TPin pinId);

    TPin PinAllCachedData();
    void UnpinAllCachedData(TPin pinId);

    // Visit unflushed cached requests in the increasing order of SequenceId
    void VisitUnflushedCachedRequests(
        ui64 nodeId,
        const TEntryVisitor& visitor) const;

    // Inform that the first |requestCount| unflushed changes requests have
    // been flushed
    void FlushSucceeded(ui64 nodeId, size_t requestCount);

    // Inform that the most recent flush failed with the specified error
    void FlushFailed(ui64 nodeId, const TError& error);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
