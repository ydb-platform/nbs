#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/diagnostics/public.h>

#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NFileStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public IStartable
    , public IIncompleteRequestProvider
{
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TServerConfigPtr config,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    NMonitoring::TDynamicCountersPtr counters,
    IProfileLogPtr profileLog,
    NCloud::ISchedulerPtr scheduler,
    IFileStoreServicePtr service);

IServerPtr CreateServer(
    TServerConfigPtr config,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    NMonitoring::TDynamicCountersPtr counters,
    NCloud::ISchedulerPtr scheduler,
    IEndpointManagerPtr service);

}   // namespace NCloud::NFileStore::NServer
