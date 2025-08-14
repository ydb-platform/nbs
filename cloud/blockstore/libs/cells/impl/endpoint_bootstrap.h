#pragma once

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct THostConfig;

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

struct ICellHostEndpointBootstrap
{
    using TGrpcEndpointBootstrapFuture =
        NThreading::TFuture<NClient::IMultiClientEndpointPtr>;
    using TRdmaEndpointBootstrapFuture =
        NThreading::TFuture<TResultOrError<IBlockStorePtr>>;
    using TShutdownEndpointFuture = NThreading::TFuture<void>;

    virtual TGrpcEndpointBootstrapFuture SetupHostGrpcEndpoint(
        const TBootstrap& boorstrap,
        const TCellHostConfig& config) = 0;

    virtual TRdmaEndpointBootstrapFuture SetupHostRdmaEndpoint(
        const TBootstrap& boorstrap,
        const TCellHostConfig& config,
        IBlockStorePtr client) = 0;

    virtual ~ICellHostEndpointBootstrap() = default;
};

////////////////////////////////////////////////////////////////////////////////

ICellHostEndpointBootstrapPtr CreateCellHostEndpointBootstrap();

}   // namespace NCloud::NBlockStore::NCells
