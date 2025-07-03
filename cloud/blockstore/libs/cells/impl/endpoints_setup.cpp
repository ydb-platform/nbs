#include "endpoints_setup.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

auto THostEndpointsSetupProvider::SetupHostGrpcEndpoint(
    const TArguments& args,
    const TCellHostConfig& config) -> TSetupGrpcEndpointFuture
{
    auto endpoint = CreateMultiClientEndpoint(
        args.GrpcClient,
        config.GetFqdn(),
        config.GetGrpcPort(),
        false);

    return MakeFuture(endpoint);
}

auto THostEndpointsSetupProvider::SetupHostRdmaEndpoint(
    const TArguments& args,
    const TCellHostConfig& config,
    IBlockStorePtr client) -> TSetupRdmaEndpointFuture
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
        args.Workers,
        rdmaEndpoint);
}

IHostEndpointsSetupProviderPtr CreateHostEndpointsSetupProvider()
{
    return std::make_shared<THostEndpointsSetupProvider>();
}

////////////////////////////////////////////////////////////////////////////////


}   // namespace NCloud::NBlockStore::NCells
