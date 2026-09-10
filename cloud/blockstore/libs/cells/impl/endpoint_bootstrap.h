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
    using TRdmaEndpointBootstrapResult = TResultOrError<IBlockStorePtr>;
    using TShutdownEndpointFuture = NThreading::TFuture<void>;

    virtual TGrpcEndpointBootstrapFuture SetupHostGrpcEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config) = 0;

    // Waits for the endpoint to connect, and fails if it does not. For callers
    // that have nothing to serve data with in the meantime.
    virtual TRdmaEndpointBootstrapFuture SetupHostRdmaEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config) = 0;

    // Hands the endpoint back before it has connected and reports its state
    // through the handler. For callers that have a fallback transport and want
    // to move over once the endpoint is usable.
    virtual TRdmaEndpointBootstrapResult SetupHostRdmaEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config,
        NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler) = 0;

    virtual ~ICellHostEndpointBootstrap() = default;
};

ICellHostEndpointBootstrapPtr CreateCellHostEndpointBootstrap();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
