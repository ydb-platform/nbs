#include "endpoint_bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

auto TCellCellHostEndpointBootstrap::SetupHostGrpcEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& config) -> TGrpcEndpointBootstrapFuture
{
    auto endpoint = CreateMultiClientEndpoint(
        bootstrap.GrpcClient,
        config.GetFqdn(),
        config.GetGrpcPort(),
        false);

    return MakeFuture(endpoint);
}

auto TCellCellHostEndpointBootstrap::SetupHostRdmaEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& config,
    IBlockStorePtr client) -> TRdmaEndpointBootstrapFuture
{
    NClient::TRdmaEndpointConfig rdmaEndpoint{
        .Address = config.GetFqdn(),
        .Port = config.GetRdmaPort(),
    };

    return CreateRdmaEndpointClientAsync(
        bootstrap.Logging,
        bootstrap.RdmaClient,
        std::move(client),
        bootstrap.TraceSerializer,
        bootstrap.RdmaTaskQueue,
        rdmaEndpoint);
}

ICellHostEndpointBootstrapPtr CreateCellHostEndpointBootstrap()
{
    return std::make_shared<TCellCellHostEndpointBootstrap>();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
