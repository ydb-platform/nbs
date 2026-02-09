#pragma once

#include "node_cache.h"
#include "persistent_storage.h"
#include "write_back_cache_stats.h"
#include "write_data_request.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/function_ref.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCacheStateConfig
{
    // ToDo(#1751): Enable after https://github.com/ydb-platform/nbs/pull/4793
    bool EnableFlushFailure = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStateListener
{
    virtual ~IWriteBackCacheStateListener() = default;

    virtual void ScheduleFlushNode(ui64 nodeId) = 0;
};

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
        IPersistentStoragePtr persistentStorage,
        IWriteBackCacheStateListener& listener,
        ITimerPtr timer,
        IWriteBackCacheStatsPtr stats,
        TWriteBackCacheStateConfig config);

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
    TCachedData GetCachedData(ui64 nodeId, ui64 offset, ui64 byteCount) const;
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
