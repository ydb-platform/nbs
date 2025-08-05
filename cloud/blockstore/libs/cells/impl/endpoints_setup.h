#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/cells/iface/endpoints_setup.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

struct THostEndpointsSetupProvider
    : public IHostEndpointsSetupProvider
{
    using IHostEndpointsSetupProvider::TGrpcResult;
    using IHostEndpointsSetupProvider::TRdmaResult;
    using IHostEndpointsSetupProvider::TSetupGrpcEndpointFuture;
    using IHostEndpointsSetupProvider::TSetupRdmaEndpointFuture;

    auto SetupHostGrpcEndpoint(
        const TBootstrap& args,
        const THostConfig& config) -> TSetupGrpcEndpointFuture override;

    auto SetupHostRdmaEndpoint(
        const TBootstrap& args,
        const THostConfig& config,
        IBlockStorePtr client) -> TSetupRdmaEndpointFuture override;
};

////////////////////////////////////////////////////////////////////////////////


}   // namespace NCloud::NBlockStore::NCells
