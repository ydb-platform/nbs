#include "endpoints_setup.h"
#include "config.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>

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

    TSetupGrpcEndpointFuture SetupHostGrpcEndpoint(
        const TShardingArguments& args,
        const TShardHostConfig& config) override
    {
        auto endpoint = CreateMultiClientEndpoint(
            args.GrpcClient,
            config.GetFqdn(),
            config.GetGrpcPort(),
            false);

        auto promise = NThreading::NewPromise<TGrpcResult>();
        promise.SetValue(endpoint);
        return promise.GetFuture();
    }

    TSetupRdmaEndpointFuture SetupHostRdmaEndpoint(
        const TShardingArguments& args,
        const TShardHostConfig& config,
        IBlockStorePtr client) override
    {
        NClient::TRdmaEndpointConfig rdmaEndpoint {
            .Address = config.GetFqdn(),
            .Port = config.GetRdmaPort(),
        };

        auto future = CreateRdmaEndpointClientAsync(
            args.Logging,
            args.RdmaClient,
            std::move(client),
            rdmaEndpoint);

        auto promise = NewPromise<TRdmaResult>();
        future.Subscribe([=] (const auto& future) mutable {
            promise.SetValue(future.GetValue());
        });

        return promise.GetFuture();
    }
};

IHostEndpointsSetupProviderPtr CreateHostEndpointsSetupProvider()
{
    return std::make_shared<THostEndpointsSetupProvider>();
}

////////////////////////////////////////////////////////////////////////////////


}   // namespace NCloud::NBlockStore::NSharding
