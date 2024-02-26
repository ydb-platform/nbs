#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NVFSVhost {

////////////////////////////////////////////////////////////////////////////////

struct TVfsRequest
{
    enum EResult {
        SUCCESS,
        IOERR,
        CANCELLED,
    };

    TGuardedSgList In;
    TGuardedSgList Out;
    void* Cookie = nullptr;

    virtual ~TVfsRequest() = default;

    virtual void Complete(EResult result) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVfsDevice
{
    virtual ~IVfsDevice() = default;

    virtual bool Start() = 0;
    virtual NThreading::TFuture<NProto::TError> Stop() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVfsQueue
{
    virtual ~IVfsQueue() = default;

    virtual int Run() = 0;
    virtual void Stop() = 0;

    virtual IVfsDevicePtr CreateDevice(
        TString socketPath,
        void* cookie) = 0;

    virtual TVfsRequestPtr DequeueRequest() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVfsQueueFactory
{
    virtual ~IVfsQueueFactory() = default;

    virtual IVfsQueuePtr CreateQueue() = 0;
};

}   // namespace NCloud::NFileStore::NVFSVhost
