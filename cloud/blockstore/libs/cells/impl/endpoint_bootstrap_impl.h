#pragma once

#include "endpoint_bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

struct TCellCellHostEndpointBootstrap: public ICellHostEndpointBootstrap
{
    using ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture;
    using ICellHostEndpointBootstrap::TRdmaEndpointBootstrapFuture;

    auto SetupHostGrpcEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config) -> TGrpcEndpointBootstrapFuture override;

    auto SetupHostRdmaEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config,
        IBlockStorePtr client) -> TRdmaEndpointBootstrapFuture override;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
