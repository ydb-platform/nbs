#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/sharding/iface/config.h>
#include <cloud/blockstore/libs/sharding/iface/endpoints_setup.h>

namespace NCloud::NBlockStore::NSharding {

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
        const TShardingArguments& args,
        const TShardHostConfig& config) -> TSetupGrpcEndpointFuture override;

    auto SetupHostRdmaEndpoint(
        const TShardingArguments& args,
        const TShardHostConfig& config,
        IBlockStorePtr client) -> TSetupRdmaEndpointFuture override;
};

////////////////////////////////////////////////////////////////////////////////


}   // namespace NCloud::NBlockStore::NSharding
