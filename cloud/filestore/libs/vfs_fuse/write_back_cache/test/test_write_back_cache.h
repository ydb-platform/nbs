#pragma once

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_back_cache.h>

#include <memory>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TTestWriteBackCache
{
private:
    // Keeps this wrapper movable even though its thread pool is not.
    class TImpl;

    std::unique_ptr<TImpl> Impl;

public:
    TTestWriteBackCache();

    // A zero thread count executes all stages synchronously. A positive count
    // creates one thread pool shared by submission, session, and completion.
    // Session handlers used with asynchronous execution should return errors
    // in their responses instead of throwing: exceptions raised on worker
    // threads are not guaranteed to reach the initiating test thread.
    TTestWriteBackCache(TWriteBackCacheArgs args, size_t threadCount);
    ~TTestWriteBackCache();

    TTestWriteBackCache(TTestWriteBackCache&&) noexcept;
    TTestWriteBackCache& operator=(TTestWriteBackCache&&) noexcept;

    NThreading::TFuture<NProto::TError> Drain();
    bool IsDrained() const;

    NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    NThreading::TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request);

    NThreading::TFuture<NProto::TError> FlushNodeData(ui64 nodeId);
    NThreading::TFuture<NProto::TError> FlushAllData();

    NThreading::TFuture<NProto::TError> ReleaseHandle(ui64 nodeId, ui64 handle);

    NThreading::TFuture<NProto::TReadDataResponse> ReadDataDirect(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    NThreading::TFuture<NProto::TWriteDataResponse> WriteDataDirect(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request);

    NThreading::TFuture<NProto::TSetNodeAttrResponse> SetNodeAttr(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TSetNodeAttrRequest> request);

    NThreading::TFuture<NProto::TCreateHandleResponse> CreateHandle(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TCreateHandleRequest> request);

    ui64 GetMaxWrittenOffset(ui64 nodeId) const;
    IModuleStatsPtr CreateModuleStats() const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
