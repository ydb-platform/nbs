#pragma once

#include "bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

struct THostConfig;

struct ICellHostEndpointBootstrap
{
    using TGrpcEndpointBootstrapFuture =
        NThreading::TFuture<NClient::IMultiClientEndpointPtr>;
    using TRdmaEndpointBootstrapResult = TResultOrError<IBlockStorePtr>;
    using TShutdownEndpointFuture = NThreading::TFuture<void>;

    virtual TGrpcEndpointBootstrapFuture SetupHostGrpcEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config) = 0;

    // Hands the endpoint back before it has connected; the handler is how
    // the rdma client reports its state from then on.
    virtual TRdmaEndpointBootstrapResult SetupHostRdmaEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config,
        NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler) = 0;

    virtual ~ICellHostEndpointBootstrap() = default;
};

ICellHostEndpointBootstrapPtr CreateCellHostEndpointBootstrap();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
