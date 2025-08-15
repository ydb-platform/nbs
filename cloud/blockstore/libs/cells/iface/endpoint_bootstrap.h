#pragma once

#include "bootstrap.h"
#include "config.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

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

    virtual TRdmaEndpointBootstrapFuture SetupHostRdmaEndpoint(
        const TBootstrap& bootstrap,
        const TCellHostConfig& config,
        IBlockStorePtr client) = 0;

    virtual ~ICellHostEndpointBootstrap() = default;
};

ICellHostEndpointBootstrapPtr CreateCellHostEndpointBootstrap();

}   // namespace NCloud::NBlockStore::NCells
