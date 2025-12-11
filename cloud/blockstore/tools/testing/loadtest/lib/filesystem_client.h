#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore::NFilesystemClient {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateClient(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IServerStatsPtr clientStats);

}   // namespace NCloud::NBlockStore::NFilesystemClient
