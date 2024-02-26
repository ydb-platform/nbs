#pragma once

#include "public.h"

#include <cloud/filestore/libs/client/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NFileStore::NVFS {

////////////////////////////////////////////////////////////////////////////////

struct IFileSystemLoop
{
    virtual ~IFileSystemLoop() = default;

    virtual NThreading::TFuture<NProto::TError> StartAsync() = 0;
    virtual NThreading::TFuture<NProto::TError> StopAsync() = 0;
    virtual NThreading::TFuture<void> SuspendAsync() = 0;
    virtual NThreading::TFuture<NProto::TError> AlterAsync(
        bool isReadonly,
        ui64 mountSeqNumber) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IFileSystemLoopFactory
{
    virtual ~IFileSystemLoopFactory() = default;

    virtual IFileSystemLoopPtr Create(
        TVFSConfigPtr config,
        NClient::ISessionPtr session) = 0;
};

}   // namespace NCloud::NFileStore::NVFS
