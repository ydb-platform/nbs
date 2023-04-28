#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/vhost/public.h>
#include <cloud/storage/core/libs/coroutine/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateExternalVhostEndpointListener(
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    TString binaryPath,
    TString localAgentId,
    IEndpointListenerPtr fallbackListener);

}   // namespace NCloud::NBlockStore::NServer
