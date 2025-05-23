#include "remote_storage_provider.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/service_su/service_su.h>
#include <cloud/storage/core/libs/common/error.h>

#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>

#include <util/random/random.h>

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TRemoteShardEndpoints = TVector<TRemoteShardEndpoint>;

struct IShard
{
   [[nodiscard]] virtual TRemoteShardEndpoint GetRemoteEndpoint(
        NClient::TClientAppConfigPtr clientConfig) = 0;

    [[nodiscard]] virtual TRemoteShardEndpoints GetRemoteEndpoints(
        NClient::TClientAppConfigPtr clientConfig)  = 0;

    virtual ~IShard() = default;
};

using IShardPtr = std::shared_ptr<IShard>;

////////////////////////////////////////////////////////////////////////////////

struct TShardArguments
{
    TServerAppConfigPtr Config;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;

    NClient::IClientPtr GrpcClient;
    NRdma::IClientPtr RdmaClient;
};

////////////////////////////////////////////////////////////////////////////////


template <typename T, typename TEndpoint>
struct TShardEndpointManagerBase
    : public IShard
    , public std::enable_shared_from_this<TShardEndpointManagerBase<T, TEndpoint>>
{
    const TShardArguments ShardArgs;
    const NProto::TShardInfo ShardInfo;

    TAdaptiveLock Lock;

    THashMap<TString, TEndpoint> Active;
    TVector<TString> Unused;

    TShardEndpointManagerBase(
            TShardArguments args,
            NProto::TShardInfo shardInfo)
        : ShardArgs(std::move(args))
        , ShardInfo(std::move(shardInfo))
    {
        for (auto& host: ShardInfo.GetHosts()) {
            Unused.emplace_back(host);
        }
    }

    TRemoteShardEndpoint PickHost(NClient::TClientAppConfigPtr clientConfig)
    {
        with_lock (Lock) {
            ResizeIfNeeded();

            if (auto host = ShardInfo.GetFixedHost(); !host.Empty()) {
                if (Active.count(host)) {
                    auto p = static_cast<T*>(this);
                    return p->CreateServices(Active[host], std::move(clientConfig));
                }
            }

            auto index = RandomNumber<ui32>(Active.size());

            auto p = static_cast<T*>(this);
            return p->CreateServices(
                std::next(Active.begin(), index)->second,
                std::move(clientConfig));
        }
    }

    TRemoteShardEndpoints PickHosts(
        ui32 count,
        NClient::TClientAppConfigPtr clientConfig)
    {
        with_lock (Lock) {
            ResizeIfNeeded();

            TVector<TRemoteShardEndpoint> res;
            auto p = static_cast<T*>(this);
            auto it = Active.begin();
            while (count-- && it != Active.end()) {
                auto endpoint = p->CreateServices(
                    it->second, std::move(clientConfig));
                res.emplace_back(endpoint);
                ++it;
            }
            return res;
        }
    }

    void ResizeIfNeeded()
    {
        auto weak_ptr = this->weak_from_this();
        while (Active.size() < ShardInfo.GetMinConnections()) {
            if (Unused.empty()) {
                break;
            }
            auto host = Unused.back();
            Unused.pop_back();

            static_cast<T*>(this)->CreateEndpoint(host).Subscribe(
                [=] (const auto& future) {
                    if (auto pThis = weak_ptr.lock(); pThis) {
                        pThis->Active.emplace(
                            host,
                            future.GetValue());
                    }
            });
        }
    }

   TRemoteShardEndpoint GetRemoteEndpoint(
        NClient::TClientAppConfigPtr clientConfig) override
    {
        return PickHost(clientConfig);
    }

    TRemoteShardEndpoints GetRemoteEndpoints(
        NClient::TClientAppConfigPtr clientConfig) override
    {
        return PickHosts(ShardInfo.GetShardDescribeHostCnt(), clientConfig);
    }
};

struct TEndpoint
{
    NClient::IMultiClientEndpointPtr Endpoint;
    IBlockStorePtr StorageService;
};

struct TShardEndpointManagerGrpc
    : public TShardEndpointManagerBase<TShardEndpointManagerGrpc, TEndpoint>
{
    using TBase = TShardEndpointManagerBase<TShardEndpointManagerGrpc, TEndpoint>;
    using TCreateEndpointFuture = NThreading::TFuture<TEndpoint>;

    using TBase::TBase;

    [[nodiscard]] TRemoteShardEndpoint CreateServices(
        TEndpoint& endpoint,
        NClient::TClientAppConfigPtr clientConfig)
    {
        auto& endp = endpoint.Endpoint;
        auto service = endp->CreateClientEndpoint(
                clientConfig->GetClientId(),
                clientConfig->GetInstanceId());

        return {
            .Service = service,
            .StorageService = service
        };
    }

    [[nodiscard]] TCreateEndpointFuture CreateEndpoint(
        const TString& host)
    {
        auto endpoint = CreateMultiClientEndpoint(
            ShardArgs.GrpcClient,
            host,
            ShardInfo.GetGrpcPort(),
            false);

        auto promise = NThreading::NewPromise<TEndpoint>();
        promise.SetValue(
            TEndpoint{.Endpoint = endpoint});
        return promise.GetFuture() ;
    }
};

struct TShardEndpointManagerRdma
    : public TShardEndpointManagerBase<TShardEndpointManagerRdma, TEndpoint>
{
    using TBase = TShardEndpointManagerBase<TShardEndpointManagerRdma, TEndpoint>;
    using TCreateEndpointFuture = NThreading::TFuture<TEndpoint>;

    using TBase::TBase;

    [[nodiscard]] TRemoteShardEndpoint CreateServices(
        TEndpoint& endpoint,
        NClient::TClientAppConfigPtr clientConfig)
    {
        auto& endp = endpoint.Endpoint;
        auto service = endp->CreateClientEndpoint(
                clientConfig->GetClientId(),
                clientConfig->GetInstanceId());

        return {
            .Service = service,
            .StorageService = endpoint.StorageService
        };
    }

    [[nodiscard]] TCreateEndpointFuture CreateEndpoint(const TString& host)
    {
        auto endpoint = CreateMultiClientEndpoint(
            ShardArgs.GrpcClient,
            host,
            ShardInfo.GetGrpcPort(),
            false);

        NClient::TRdmaEndpointConfig rdmaEndpoint {
            .Address = host,
            .Port = ShardInfo.GetRdmaPort(),
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
                    .Endpoint = endpoint,
                    .StorageService = future.GetValue()});
        });

        return promise.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

IShardPtr CreateGrpcEndpointManager(
    TShardArguments args,
    NProto::TShardInfo shardInfo)
{
    return std::make_shared<TShardEndpointManagerGrpc>(
        std::move(args),
        std::move(shardInfo));
}

IShardPtr CreateRdmaEndpointManager(
    TShardArguments args,
    NProto::TShardInfo shardInfo)
{
    return std::make_shared<TShardEndpointManagerRdma>(
        std::move(args),
        std::move(shardInfo));
}

////////////////////////////////////////////////////////////////////////////////

struct TRemoteStorageProvider
    : public IRemoteStorageProvider
{
    const TShardArguments Args;

    THashMap<TString, IShardPtr> Shards;

    TRemoteStorageProvider(TShardArguments args)
        : Args(std::move(args))
    {
        for (const auto& shard: Args.Config->GetShardMap()) {
            IShardPtr ptr;
            if (shard.second.GetTransport() == NProto::GRPC) {
                ptr = CreateGrpcEndpointManager(Args, shard.second);
            } else {
                ptr = CreateRdmaEndpointManager(Args, shard.second);
            }

            Shards.emplace(shard.first, std::move(ptr));
        }
    }

    void Start() override
    {
        Args.GrpcClient->Start();
    }

    void Stop() override
    {
        Args.GrpcClient->Stop();
    }

    TRemoteShardEndpoint CreateStorage(
        const TString& shardId,
        NClient::TClientAppConfigPtr clientConfig) override
    {
        auto it = Shards.find(shardId);
        Y_ENSURE(it != Shards.end());
        return it->second->GetRemoteEndpoint(clientConfig);
    }


    TRemoteEndpoints GetRemoteEndpoints(
        NClient::TClientAppConfigPtr clientConfig) override
    {
        TRemoteEndpoints res;
        for (auto& shard: Shards) {
            auto endpoints = shard.second->GetRemoteEndpoints(clientConfig);
            for (auto& endp: endpoints) {
                res.emplace(shard.first, endp);
            }
        }
        return res;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRemoteStorageProviderPtr CreateRemoteStorageProvider(
    TServerAppConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    NClient::IClientPtr grpcClient,
    NRdma::IClientPtr rdmaClient)
{
    TShardArguments args {
        .Config = std::move(config),
        .Timer = std::move(timer),
        .Scheduler = std::move(scheduler),
        .Logging = std::move(logging),
        .Monitoring = std::move(monitoring),
        .GrpcClient = std::move(grpcClient),
        .RdmaClient = std::move(rdmaClient)
    };

    return std::make_shared<TRemoteStorageProvider>(args);
}

}   // namespace NCloud::NBlockStore::NServer
