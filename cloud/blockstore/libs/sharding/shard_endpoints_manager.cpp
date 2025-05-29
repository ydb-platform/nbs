#include "shard_endpoints_manager.h"

#include "config.h"
#include "remote_storage.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/sharding/config.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NSharding {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEndpoint
{
    TString Host;
    NClient::IMultiClientEndpointPtr Endpoint;
    IBlockStorePtr StorageService;
};

////////////////////////////////////////////////////////////////////////////////

struct TShardEndpointManagerGrpc
    : public TShardEndpointManagerBase<TShardEndpointManagerGrpc, TEndpoint>
{
    using TBase = TShardEndpointManagerBase<TShardEndpointManagerGrpc, TEndpoint>;
    using TCreateEndpointFuture = NThreading::TFuture<TEndpoint>;

    using TBase::TBase;

    [[nodiscard]] TShardClient CreateShardClient(
        TEndpoint& endpoint,
        NClient::TClientAppConfigPtr clientConfig)
    {
        auto& endp = endpoint.Endpoint;
        auto service = endp->CreateClientEndpoint(
                clientConfig->GetClientId(),
                clientConfig->GetInstanceId());

        return {
            clientConfig,
            endpoint.Host,
            service,
            CreateRemoteStorage(service)};
    }

    [[nodiscard]] TCreateEndpointFuture SetupHostEndpoint(
        const TString& host)
    {
        auto endpoint = CreateMultiClientEndpoint(
            ShardArgs.GrpcClient,
            host,
            ShardConfig.GetGrpcPort(),
            false);

        auto promise = NThreading::NewPromise<TEndpoint>();
        promise.SetValue(
            TEndpoint{.Host = host, .Endpoint = endpoint});
        return promise.GetFuture() ;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TShardEndpointManagerRdma
    : public TShardEndpointManagerBase<TShardEndpointManagerRdma, TEndpoint>
{
    using TBase = TShardEndpointManagerBase<TShardEndpointManagerRdma, TEndpoint>;
    using TCreateEndpointFuture = NThreading::TFuture<TEndpoint>;

    using TBase::TBase;

    [[nodiscard]] TShardClient CreateShardClient(
        TEndpoint& endpoint,
        NClient::TClientAppConfigPtr clientConfig)
    {
        auto& endp = endpoint.Endpoint;
        auto service = endp->CreateClientEndpoint(
            clientConfig->GetClientId(),
            clientConfig->GetInstanceId());

        return {
            clientConfig,
            endpoint.Host,
            std::move(service),
            CreateRemoteStorage(endpoint.StorageService)};
    }

    [[nodiscard]] TCreateEndpointFuture SetupHostEndpoint(
        const TString& host)
    {
        auto endpoint = CreateMultiClientEndpoint(
            ShardArgs.GrpcClient,
            host,
            ShardConfig.GetGrpcPort(),
            false);

        NClient::TRdmaEndpointConfig rdmaEndpoint {
            .Address = host,
            .Port = ShardConfig.GetRdmaPort(),
        };

        auto future = CreateRdmaEndpointClientAsync(
            ShardArgs.Logging,
            ShardArgs.RdmaClient,
            endpoint,
            rdmaEndpoint);

        auto promise = NThreading::NewPromise<TEndpoint>();
        future.Subscribe([=] (const auto& future) mutable {
            promise.SetValue(
                TEndpoint{
                    .Host = host,
                    .Endpoint = endpoint,
                    .StorageService = future.GetValue()});
        });

        return promise.GetFuture();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IShardEndpointsManagerPtr CreateGrpcEndpointManager(
    TShardingArguments args,
    TShardConfig ShardConfig)
{
    return std::make_shared<TShardEndpointManagerGrpc>(
        std::move(args),
        std::move(ShardConfig));
}

IShardEndpointsManagerPtr CreateRdmaEndpointManager(
    TShardingArguments args,
    TShardConfig ShardConfig)
{
    return std::make_shared<TShardEndpointManagerRdma>(
        std::move(args),
        std::move(ShardConfig));
}

}   // namespace NCloud::NBlockStore::NSharding
