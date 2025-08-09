#pragma once

#include "config.h"
#include "bootstrap.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct THostConfig;

struct IHostEndpointsBoorstrap
{
    using TGrpcEndpointBootstrapFuture =
        NThreading::TFuture<NClient::IMultiClientEndpointPtr>;
    using TRdmaEndpointBootstrapFuture =
        NThreading::TFuture<TResultOrError<IBlockStorePtr>>;
    using TShutdownEndpointFuture = NThreading::TFuture<void>;

    virtual TGrpcEndpointBootstrapFuture SetupHostGrpcEndpoint(
        const TBootstrap& args,
        const TCellHostConfig& config) = 0;

    virtual TRdmaEndpointBootstrapFuture SetupHostRdmaEndpoint(
        const TBootstrap& args,
        const TCellHostConfig& config,
        IBlockStorePtr client) = 0;

    virtual ~IHostEndpointsBoorstrap() = default;
};

IHostEndpointsBoorstrapPtr CreateHostEndpointsSetupProvider();

}   // namespace NCloud::NBlockStore::NCells
