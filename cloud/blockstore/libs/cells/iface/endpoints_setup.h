#pragma once

#include "config.h"
#include "arguments.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct TCellHostConfig;

struct IHostEndpointsSetupProvider
{
    using TGrpcResult = NClient::IMultiClientEndpointPtr;
    using TRdmaResult = TResultOrError<IBlockStorePtr>;

    using TSetupGrpcEndpointFuture = NThreading::TFuture<TGrpcResult>;
    using TSetupRdmaEndpointFuture = NThreading::TFuture<TRdmaResult>;
    using TShutdownEndpointFuture = NThreading::TFuture<void>;

    virtual TSetupGrpcEndpointFuture SetupHostGrpcEndpoint(
        const TArguments& args,
        const TCellHostConfig& config) = 0;

    virtual TSetupRdmaEndpointFuture SetupHostRdmaEndpoint(
        const TArguments& args,
        const TCellHostConfig& config,
        IBlockStorePtr client) = 0;

    virtual ~IHostEndpointsSetupProvider() = default;
};

IHostEndpointsSetupProviderPtr CreateHostEndpointsSetupProvider();

}   // namespace NCloud::NBlockStore::NCells
