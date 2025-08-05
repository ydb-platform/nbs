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

struct IHostEndpointsSetupProvider
{
    using TGrpcResult = NClient::IMultiClientEndpointPtr;
    using TRdmaResult = TResultOrError<IBlockStorePtr>;

    using TSetupGrpcEndpointFuture = NThreading::TFuture<TGrpcResult>;
    using TSetupRdmaEndpointFuture = NThreading::TFuture<TRdmaResult>;
    using TShutdownEndpointFuture = NThreading::TFuture<void>;

    virtual TSetupGrpcEndpointFuture SetupHostGrpcEndpoint(
        const TBootstrap& args,
        const THostConfig& config) = 0;

    virtual TSetupRdmaEndpointFuture SetupHostRdmaEndpoint(
        const TBootstrap& args,
        const THostConfig& config,
        IBlockStorePtr client) = 0;

    virtual ~IHostEndpointsSetupProvider() = default;
};

IHostEndpointsSetupProviderPtr CreateHostEndpointsSetupProvider();

}   // namespace NCloud::NBlockStore::NCells
