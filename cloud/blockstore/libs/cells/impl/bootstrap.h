#pragma once

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct ICellHostEndpointBootstrap;
using ICellHostEndpointBootstrapPtr =
    std::shared_ptr<ICellHostEndpointBootstrap>;

struct TBootstrap
{
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    ITraceSerializerPtr TraceSerializer;

    NClient::IMultiHostClientPtr GrpcClient;
    NRdma::IClientPtr RdmaClient;

    ITaskQueuePtr RdmaTaskQueue;

    ICellHostEndpointBootstrapPtr EndpointsSetup;
};

}   // namespace NCloud::NBlockStore::NCells
