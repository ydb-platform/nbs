#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

struct TVhostRequest
{
    enum EResult {
        SUCCESS,
        IOERR,
        CANCELLED,
    };

    EBlockStoreRequest Type = EBlockStoreRequest::ReadBlocks;
    ui64 From = 0;
    ui64 Length = 0;
    TGuardedSgList SgList;
    void* Cookie = nullptr;

    virtual ~TVhostRequest() = default;

    virtual void Complete(EResult result) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVhostDevice
{
    virtual ~IVhostDevice() = default;

    virtual bool Start() = 0;
    virtual NThreading::TFuture<NProto::TError> Stop() = 0;
    virtual void Update(ui64 blocksCount) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVhostQueue
{
    virtual ~IVhostQueue() = default;

    virtual int Run() = 0;
    virtual void Stop() = 0;

    virtual IVhostDevicePtr CreateDevice(
        TString socketPath,
        TString deviceName,
        ui32 blockSize,
        ui64 blocksCount,
        ui32 queuesCount,
        bool discardEnabled,
        void* cookie,
        const TVhostCallbacks& callbacks) = 0;

    virtual TVhostRequestPtr DequeueRequest() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVhostQueueFactory
{
    virtual ~IVhostQueueFactory() = default;

    virtual IVhostQueuePtr CreateQueue() = 0;
};

////////////////////////////////////////////////////////////////////////////////

void InitVhostLog(ILoggingServicePtr logging);

IVhostQueueFactoryPtr CreateVhostQueueFactory();

}   // namespace NCloud::NBlockStore::NVhost
