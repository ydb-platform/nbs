#pragma once

#include "public.h"

#include <cloud/filestore/libs/client/public.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/vfs/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

NVFS::IFileSystemLoopPtr CreateFuseLoop(
    NVFS::TVFSConfigPtr config,
    ILoggingServicePtr logging,
    IRequestStatsRegistryPtr requestStats,
    IModuleStatsRegistryPtr moduleStats,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    IProfileLogPtr profileLog,
    NClient::ISessionPtr session);

NVFS::IFileSystemLoopFactoryPtr CreateFuseLoopFactory(
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IRequestStatsRegistryPtr requestStats,
    IModuleStatsRegistryPtr moduleStats,
    IProfileLogPtr profileLog);

}   // namespace NCloud::NFileStore::NFuse
