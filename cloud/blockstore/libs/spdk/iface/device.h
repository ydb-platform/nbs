#pragma once

#include "public.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

struct ISpdkDevice: public IStartable
{
    virtual ~ISpdkDevice() = default;

    virtual NThreading::TFuture<void> StartAsync() = 0;
    virtual NThreading::TFuture<void> StopAsync() = 0;

    virtual NThreading::TFuture<NProto::TError>
    Read(void* buf, ui64 fileOffset, ui32 bytesCount) = 0;

    virtual NThreading::TFuture<NProto::TError>
    Read(TSgList sglist, ui64 fileOffset, ui32 bytesCount) = 0;

    virtual NThreading::TFuture<NProto::TError>
    Write(void* buf, ui64 fileOffset, ui32 bytesCount) = 0;

    virtual NThreading::TFuture<NProto::TError>
    Write(TSgList sglist, ui64 fileOffset, ui32 bytesCount) = 0;

    virtual NThreading::TFuture<NProto::TError> WriteZeroes(
        ui64 fileOffset,
        ui32 bytesCount) = 0;

    virtual NThreading::TFuture<NProto::TError> Erase(
        NProto::EDeviceEraseMethod method) = 0;
};

}   // namespace NCloud::NBlockStore::NSpdk
