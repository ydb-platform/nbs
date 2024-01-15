#pragma once

#include "public.h"

#include <cloud/filestore/libs/client/public.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/vfs/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NFileStore::NVFSVhost {

////////////////////////////////////////////////////////////////////////////////

struct IFileSystem
{
    virtual ~IFileSystem() = default;

    virtual NThreading::TFuture<NProto::TError> Start() = 0;
    virtual NThreading::TFuture<NProto::TError> Stop() = 0;
    virtual NThreading::TFuture<NProto::TError> Alter(
        bool readOnly,
        ui64 mountSeqNumber) = 0;

    virtual NThreading::TFuture<NProto::TError> Process(
        TVfsRequestPtr request) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IFileSystemFactory
{
    virtual ~IFileSystemFactory() = default;

    virtual IFileSystemPtr Create(
        NVFS::TVFSConfigPtr config,
        NClient::ISessionPtr session,
        ISchedulerPtr scheduler,
        IRequestStatsRegistryPtr requestStats,
        IProfileLogPtr profileLog) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemPtr CreateFileSystem(
    NVFS::TVFSConfigPtr config,
    NClient::ISessionPtr session,
    ILoggingServicePtr logging,
    ISchedulerPtr scheduler,
    IRequestStatsRegistryPtr statsRegistry,
    IProfileLogPtr profileLog);

}   // namespace NCloud::NFileStore::NVFSVhost
