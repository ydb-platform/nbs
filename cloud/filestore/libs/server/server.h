#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

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
    IProfileLogPtr profileLog,
    IFileStoreServicePtr service);

IServerPtr CreateServer(
    TServerConfigPtr config,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    IEndpointManagerPtr service);

}   // namespace NCloud::NFileStore::NServer
