#include "endpoint_bootstrap_impl.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/icv2/iface/data_endpoint_factory.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IBlockStorePtr>
TCellCellHostEndpointBootstrap::SetupHostIcV2Endpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& config,
    NIcV2::IHostConnectionHandlerPtr handler)
{
    if (!bootstrap.IcV2EndpointFactory) {
        return MakeError(
            E_ARGUMENT,
            "IC V2 endpoint factory is not configured");
    }

    return bootstrap.IcV2EndpointFactory->CreateDataEndpoint(
        config.GetCellId(),
        config.GetFqdn(),
        std::move(handler));
}

auto TCellCellHostEndpointBootstrap::SetupHostGrpcEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& config) -> TGrpcEndpointBootstrapFuture
{
    auto endpoint = bootstrap.GrpcClient->CreateEndpoint(
        config.GetFqdn(),
        config.GetSecureGrpcPort() ? config.GetSecureGrpcPort() : config.GetGrpcPort(),
        config.GetSecureGrpcPort() != 0);

    return MakeFuture(endpoint);
}

auto TCellCellHostEndpointBootstrap::SetupHostRdmaEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& config,
    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler)
    -> TRdmaEndpointBootstrapResult
{
    if (!bootstrap.RdmaClient || !config.GetRdmaPort()) {
        return MakeError(
            E_ARGUMENT,
            "RDMA cell data transport is not configured");
    }

    NClient::TRdmaEndpointConfig rdmaEndpoint{
        .Address = config.GetFqdn(),
        .Port = config.GetRdmaPort(),
    };

    return CreateRdmaDataEndpoint(
        bootstrap.Logging,
        bootstrap.RdmaClient,
        bootstrap.TraceSerializer,
        bootstrap.RdmaTaskQueue,
        rdmaEndpoint,
        std::move(handler));
}

////////////////////////////////////////////////////////////////////////////////

ICellHostEndpointBootstrapPtr CreateCellHostEndpointBootstrap()
{
    return std::make_shared<TCellCellHostEndpointBootstrap>();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
