#pragma once

#include "public.h"

#include "request.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IStorage
{
    virtual ~IStorage() = default;

    virtual NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) = 0;

    virtual NThreading::TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) = 0;

    virtual NThreading::TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) = 0;

    virtual NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) = 0;

    // nullptr means client could use own buffer
    virtual TStorageBuffer AllocateBuffer(size_t bytesCount) = 0;

    virtual void ReportIOError() = 0;
};

IStoragePtr CreateStorageStub();

////////////////////////////////////////////////////////////////////////////////

// Thread-safe. Public methods can be called from any thread.
class TStorageAdapter
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;
    const TDuration ShutdownTimeout;

public:
    TStorageAdapter(
        IStoragePtr storage,
        ui32 storageBlockSize,
        bool normalize,   // When true, we use StorageBlockSize when making
                          // requests to the underlying storage
        TDuration maxRequestDuration,
        TDuration shutdownTimeout);

    ~TStorageAdapter();

    NThreading::TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TInstant now,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request,
        ui32 requestBlockSize,
        TStringBuf dataBuffer   // if non empty,
                                // response data is written into the buffer
                                // instead of TReadBlocksResponse
    ) const;

    NThreading::TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TInstant now,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request,
        ui32 requestBlockSize,
        TStringBuf dataBuffer   // if non empty,
                                // data is read from the buffer
                                // instead of TWriteBlocksRequest
    ) const;

    NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TInstant now,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        ui32 requestBlockSize) const;

    NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) const;

    void CheckIOTimeouts(TInstant now);

    void ReportIOError();

    // Returns the number of incomplete requests.
    size_t Shutdown(ITimerPtr timer);
};

}   // namespace NCloud::NBlockStore
