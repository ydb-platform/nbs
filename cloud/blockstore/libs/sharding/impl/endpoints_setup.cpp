#include "endpoints_setup.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/sharding/iface/config.h>

namespace NCloud::NBlockStore::NSharding {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

auto THostEndpointsSetupProvider::SetupHostGrpcEndpoint(
    const TShardingArguments& args,
    const TShardHostConfig& config) -> TSetupGrpcEndpointFuture
{
    auto endpoint = CreateMultiClientEndpoint(
        args.GrpcClient,
        config.GetFqdn(),
        config.GetGrpcPort(),
        false);

    return MakeFuture(endpoint);
}

auto THostEndpointsSetupProvider::SetupHostRdmaEndpoint(
    const TShardingArguments& args,
    const TShardHostConfig& config,
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


}   // namespace NCloud::NBlockStore::NSharding
