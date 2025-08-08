#include "endpoint_bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

auto TCellHostEndpointsBootstrap::SetupHostGrpcEndpoint(
    const TBootstrap& args,
    const TCellHostConfig& config) -> TGrpcEndpointBootstrapFuture
{
    auto endpoint = CreateMultiClientEndpoint(
        args.GrpcClient,
        config.GetFqdn(),
        config.GetGrpcPort(),
        false);

    return MakeFuture(endpoint);
}

auto TCellHostEndpointsBootstrap::SetupHostRdmaEndpoint(
    const TBootstrap& args,
    const TCellHostConfig& config,
    IBlockStorePtr client) -> TRdmaEndpointBootstrapFuture
{
    NClient::TRdmaEndpointConfig rdmaEndpoint {
        .Address = config.GetFqdn(),
        .Port = config.GetRdmaPort(),
    };

    return CreateRdmaEndpointClientAsync(
        args.Logging,
        args.RdmaClient,
        std::move(client),
        args.TraceSerializer,
        args.RdmaTaskQueue,
        rdmaEndpoint);
}

IHostEndpointsBoorstrapPtr CreateHostEndpointsSetupProvider()
{
    return std::make_shared<TCellHostEndpointsBootstrap>();
}

////////////////////////////////////////////////////////////////////////////////


}   // namespace NCloud::NBlockStore::NCells
