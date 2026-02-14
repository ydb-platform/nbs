#pragma once

#include "write_back_cache_state.h"
#include "write_back_cache_stats.h"
#include "write_data_request_builder.h"

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct IWriteDataRequestExecutor
{
    virtual ~IWriteDataRequestExecutor() = default;

    // The callback should be called only when TWriteBackCache object is alive
    virtual void ExecuteWriteDataRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request,
        std::function<void(const NProto::TWriteDataResponse&)> callback) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// The class is thread-safe
class TFlusher
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TFlusher();
    TFlusher(TFlusher&&) noexcept;
    TFlusher& operator=(TFlusher&&) noexcept;
    ~TFlusher();

    TFlusher(
        TWriteBackCacheState& state,
        IWriteDataRequestBuilderPtr requestBuilder,
        IWriteDataRequestExecutor& executor,
        IWriteBackCacheStatsPtr stats);

    /**
     * Notifies Flusher that the node with the given id needs to be flushed.
     *
     * Flush notifications originate from TWriteBackCacheState and are sent
     * once per node until Flusher takes action and reports status.
     *
     * Flusher is expected (asynchronously) to read node state, generate
     * WriteData requests, execute them and report the status back to
     * TWriteBackCacheState by calling FlushSucceeded or FlushFailed.
     *
     * Flusher is not expected to write all unflushed data at once - it should
     * just make some progress and continue flushing only after receiving the
     * subsequent ScheduleFlushNode call.
     */
    void ScheduleFlushNode(ui64 nodeId);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
