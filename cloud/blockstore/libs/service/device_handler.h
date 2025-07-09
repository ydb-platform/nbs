#pragma once

#include "public.h"

#include "request.h"

#include <cloud/storage/core/libs/common/guarded_sglist.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IDeviceHandler
{
    virtual ~IDeviceHandler() = default;

    virtual NThreading::TFuture<NProto::TReadBlocksLocalResponse> Read(
        TCallContextPtr callContext,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList,
        const TString& checkpointId) = 0;

    virtual NThreading::TFuture<NProto::TWriteBlocksLocalResponse> Write(
        TCallContextPtr callContext,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList) = 0;

    virtual NThreading::TFuture<NProto::TZeroBlocksResponse> Zero(
        TCallContextPtr callContext,
        ui64 from,
        ui64 length) = 0;

    virtual TStorageBuffer AllocateBuffer(size_t bytesCount) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IDeviceHandlerFactory
{
    virtual ~IDeviceHandlerFactory() = default;

    virtual IDeviceHandlerPtr CreateDeviceHandler(
        IStoragePtr storage,
        TString diskId,
        TString clientId,
        ui32 blockSize,
        bool unalignedRequestsDisabled,
        bool checkBufferModificationDuringWriting,
        NProto::EStorageMediaKind storageMediaKind,
        ui32 MaxZeroBlocksSubRequestSize) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDeviceHandlerFactoryPtr CreateDefaultDeviceHandlerFactory();
IDeviceHandlerFactoryPtr CreateDeviceHandlerFactory(ui32 maxSubRequestSize);

}   // namespace NCloud::NBlockStore
