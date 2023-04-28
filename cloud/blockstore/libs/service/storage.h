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
};

IStoragePtr CreateStorageStub();

////////////////////////////////////////////////////////////////////////////////

class TStorageAdapter
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TStorageAdapter(
        IStoragePtr storage,
        ui32 storageBlockSize,
        bool normalize,
        ui32 maxRequestSize = 0);

    ~TStorageAdapter();

    NThreading::TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request,
        ui32 requestBlockSize) const;

    NThreading::TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request,
        ui32 requestBlockSize) const;

    NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        ui32 requestBlockSize) const;

    NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) const;
};

}   // namespace NCloud::NBlockStore
