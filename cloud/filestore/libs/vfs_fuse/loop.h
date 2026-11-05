#pragma once

#include "public.h"

#include <cloud/filestore/libs/client/public.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/libs/vfs/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/file_backed_containers/file_map_memory_limiter.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

NVFS::IFileSystemLoopPtr CreateFuseLoop(
    NVFS::TVFSConfigPtr config,
    ILoggingServicePtr logging,
    IRequestStatsRegistryPtr requestStats,
    IModuleStatsRegistryPtr moduleStats,
    IFsCountersProviderPtr fsCountersProvider,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    IProfileLogPtr profileLog,
    NClient::ISessionPtr session,
    IFileMapMemoryLimiterPtr fileMapMemoryLimiter,
    IPersistentStateManagerPtr persistentState,
    IMultiFileSystemEventHandlerPtr multiFileSystemEventHandler);

NVFS::IFileSystemLoopFactoryPtr CreateFuseLoopFactory(
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IRequestStatsRegistryPtr requestStats,
    IModuleStatsRegistryPtr moduleStats,
    IFsCountersProviderPtr fsCountersProvider,
    IProfileLogPtr profileLog,
    IPersistentStateManagerPtr persistentState,
    IMultiFileSystemEventHandlerPtr multiFileSystemEventHandler);

}   // namespace NCloud::NFileStore::NFuse
