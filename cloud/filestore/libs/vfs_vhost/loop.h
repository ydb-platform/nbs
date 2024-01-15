#pragma once

#include "public.h"

#include <cloud/filestore/libs/client/public.h>
#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/vfs/public.h>

#include <cloud/storage/core/libs/common/affinity.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/system/thread.h>

namespace NCloud::NFileStore::NVFSVhost {

////////////////////////////////////////////////////////////////////////////////

struct TLoopFactoryConfig
{
    ui32 ThreadsCount = 1;
    TAffinity Affinity;
};

NVFS::IFileSystemLoopFactoryPtr CreateVfsLoopFactory(
    const TLoopFactoryConfig& config,
    IVfsQueueFactoryPtr vfsQueueFactory,
    IFileSystemFactoryPtr fileSystemFactory,
    ILoggingServicePtr logging,
    ISchedulerPtr scheduler,
    IRequestStatsRegistryPtr requestStats,
    IProfileLogPtr profileLog);

}   // namespace NCloud::NFileStore::NVFSVhost
