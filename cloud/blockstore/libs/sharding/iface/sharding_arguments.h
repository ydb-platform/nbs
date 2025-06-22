#pragma once

#include "public.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

struct TShardingArguments
{
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    ITraceSerializerPtr TraceSerializer;

    NClient::IClientPtr GrpcClient;
    NRdma::IClientPtr RdmaClient;

    ITaskQueuePtr Workers;

    IHostEndpointsSetupProviderPtr EndpointsSetup;
};

}   // namespace NCloud::NBlockStore::NSharding
