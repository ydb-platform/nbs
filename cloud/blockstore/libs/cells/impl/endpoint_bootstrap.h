#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/endpoint_bootstrap.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

struct TCellHostEndpointsBootstrap
    : public IHostEndpointsBoorstrap
{
    using IHostEndpointsBoorstrap::TGrpcEndpointBootstrapFuture;
    using IHostEndpointsBoorstrap::TRdmaEndpointBootstrapFuture;

    auto SetupHostGrpcEndpoint(
        const TBootstrap& args,
        const TCellHostConfig& config) -> TGrpcEndpointBootstrapFuture override;

    auto SetupHostRdmaEndpoint(
        const TBootstrap& args,
        const TCellHostConfig& config,
        IBlockStorePtr client) -> TRdmaEndpointBootstrapFuture override;
};

////////////////////////////////////////////////////////////////////////////////


}   // namespace NCloud::NBlockStore::NCells
