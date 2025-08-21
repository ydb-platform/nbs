#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/endpoint/public.h>
#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/libs/vfs/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NFileStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

struct THandleOpsQueueConfig
{
    TString PathPrefix;
    ui32 MaxQueueSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCacheConfig
{
    TString PathPrefix;
    ui64 Capacity = 0;
    TDuration AutomaticFlushPeriod;
    TDuration FlushRetryPeriod;
    ui32 FlushMaxWriteRequestSize = 0;
    ui32 FlushMaxWriteRequestsCount = 0;
    ui32 FlushMaxSumWriteRequestsSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateEndpointListener(
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IFileStoreEndpointsPtr filestoreEndpoints,
    NVFS::IFileSystemLoopFactoryPtr loopFactory,
    THandleOpsQueueConfig handleOpsQueueConfig,
    TWriteBackCacheConfig writeBackCacheConfig);

}   // namespace NCloud::NFileStore::NVhost
