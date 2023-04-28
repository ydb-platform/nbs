#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/rdma/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/coroutine/public.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TRdmaEndpointConfig
{
    TString ListenAddress;
    ui32 ListenPort;
};

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateRdmaEndpointListener(
    NRdma::IServerPtr server,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config);

}   // namespace NCloud::NBlockStore::NServer
