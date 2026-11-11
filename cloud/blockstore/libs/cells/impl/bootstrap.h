#pragma once

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/discovery/public.h>
#include <cloud/blockstore/libs/icv2/iface/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/grpc/public.h>
#include <cloud/storage/core/libs/rdma/iface/client.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct ICellHostEndpointBootstrap;
using ICellHostEndpointBootstrapPtr =
    std::shared_ptr<ICellHostEndpointBootstrap>;

class TCellConnectionRegistry;
using TCellConnectionRegistryPtr = std::shared_ptr<TCellConnectionRegistry>;

struct TBootstrap
{
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    ITraceSerializerPtr TraceSerializer;

    NCloud::ICertificateProviderPtr CertProvider;
    NClient::IEndpointFactoryPtr GrpcClient;
    NCloud::NStorage::NRdma::IClientPtr RdmaClient;

    NIcV2::IDataEndpointFactoryPtr IcV2EndpointFactory;

    // the node's own service, queried alongside the cells on a describe/search
    IBlockStorePtr LocalService;

    // resolves the cells' EdsHostPatterns; null when there is no EDS to ask
    NDiscovery::IEdsInstanceFetcherPtr EdsFetcher;

    ITaskQueuePtr RdmaTaskQueue;

    ICellHostEndpointBootstrapPtr EndpointsSetup;

    // the connections made so far, for the mon page; may be null
    TCellConnectionRegistryPtr Connections;
};

}   // namespace NCloud::NBlockStore::NCells
