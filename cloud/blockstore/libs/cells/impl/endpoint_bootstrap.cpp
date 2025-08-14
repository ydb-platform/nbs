#include "endpoint_bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCellCellHostEndpointBootstrap
    : public ICellHostEndpointBootstrap
{
    using ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture;
    using ICellHostEndpointBootstrap::TRdmaEndpointBootstrapFuture;

    auto SetupHostGrpcEndpoint(
        const TBootstrap& boorstrap,
        const TCellHostConfig& config) -> TGrpcEndpointBootstrapFuture override;

    auto SetupHostRdmaEndpoint(
        const TBootstrap& boorstrap,
        const TCellHostConfig& config,
        IBlockStorePtr client) -> TRdmaEndpointBootstrapFuture override;
};

auto TCellCellHostEndpointBootstrap::SetupHostGrpcEndpoint(
    const TBootstrap& boorstrap,
    const TCellHostConfig& config) -> TGrpcEndpointBootstrapFuture
{
    auto endpoint = CreateMultiClientEndpoint(
        boorstrap.GrpcClient,
        config.GetFqdn(),
        config.GetGrpcPort(),
        false);

    return MakeFuture(endpoint);
}

auto TCellCellHostEndpointBootstrap::SetupHostRdmaEndpoint(
    const TBootstrap& boorstrap,
    const TCellHostConfig& config,
    IBlockStorePtr client) -> TRdmaEndpointBootstrapFuture
{
    NClient::TRdmaEndpointConfig rdmaEndpoint {
        .Address = config.GetFqdn(),
        .Port = config.GetRdmaPort(),
    };

    return CreateRdmaEndpointClientAsync(
        boorstrap.Logging,
        boorstrap.RdmaClient,
        std::move(client),
        boorstrap.TraceSerializer,
        boorstrap.RdmaTaskQueue,
        rdmaEndpoint);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICellHostEndpointBootstrapPtr CreateCellHostEndpointBootstrap()
{
    return std::make_shared<TCellCellHostEndpointBootstrap>();
}

}   // namespace NCloud::NBlockStore::NCells
