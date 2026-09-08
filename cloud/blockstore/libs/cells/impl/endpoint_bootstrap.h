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
    using TRdmaEndpointBootstrapFuture =
        NThreading::TFuture<TResultOrError<IBlockStorePtr>>;
    using TShutdownEndpointFuture = NThreading::TFuture<void>;

    virtual TGrpcEndpointBootstrapFuture SetupHostGrpcEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config) = 0;

    // The handler is how the rdma client reports the endpoint state back to
    // whoever asked for the endpoint; it may be empty when nobody listens.
    virtual TRdmaEndpointBootstrapFuture SetupHostRdmaEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config,
        NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler) = 0;

    virtual ~ICellHostEndpointBootstrap() = default;
};

ICellHostEndpointBootstrapPtr CreateCellHostEndpointBootstrap();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
