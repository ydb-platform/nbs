#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

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
    bool IsDiscardRequest = false;

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

    virtual TVhostRequestPtr DequeueRequest() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVhostQueueFactory
{
    virtual ~IVhostQueueFactory() = default;

    virtual IVhostQueuePtr CreateQueue() = 0;

    // Creates a vhost block device that exposes |queuesCount| virtio queues
    // to the guest and is served by the given set of request queues.
    //
    // Note that |queuesCount| and |queues| are different things:
    // |queuesCount| is the number of virtqueues the guest may set up, while
    // |queues| are the backend queues (one per executor thread) the requests
    // are dispatched to. The guest's virtqueues are spread over |queues|
    // round-robin by vring index, so a single device may be served by several
    // executor threads simultaneously. |queues| must be non-empty and must
    // not be larger than |queuesCount|.
    //
    // |cookie| is placed into TVhostRequest::Cookie of every request dequeued
    // from any of the device's queues, so that the executor can route the
    // request back to the endpoint it belongs to.
    virtual IVhostDevicePtr CreateDevice(
        TString socketPath,
        TString deviceName,
        ui32 blockSize,
        ui64 blocksCount,
        ui32 queuesCount,
        bool discardEnabled,
        bool writeZeroesEnabled,
        ui32 optimalIoSize,
        TVector<IVhostQueuePtr> queues,
        void* cookie,
        const TVhostCallbacks& callbacks) = 0;
};

////////////////////////////////////////////////////////////////////////////////

void InitVhostLog(ILoggingServicePtr logging);

IVhostQueueFactoryPtr CreateVhostQueueFactory();

}   // namespace NCloud::NBlockStore::NVhost
