#include "remote_storage_provider.h"

#include "config.h"
#include "remote_storage.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/sharding/config.h>
#include <cloud/storage/core/libs/common/error.h>

#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>

#include <util/random/random.h>

namespace NCloud::NBlockStore::NSharding {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct IShard
{
    using TShardClients = TVector<TShardClient>;

    [[nodiscard]] virtual TShardClient GetShardClient(
        NClient::TClientAppConfigPtr clientConfig) = 0;

    [[nodiscard]] virtual TShardClients GetShardClients(
        NClient::TClientAppConfigPtr clientConfig) = 0;

    virtual ~IShard() = default;
};

using IShardPtr = std::shared_ptr<IShard>;

////////////////////////////////////////////////////////////////////////////////

struct TShardArguments
{
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
    const TShardConfig ShardConfig;

    TAdaptiveLock Lock;

    THashMap<TString, TEndpoint> Active;
    THashMap<TString, NThreading::TFuture<TEndpoint>> Activating;
    TVector<TString> Unused;

    TShardEndpointManagerBase(
            TShardArguments args,
            TShardConfig ShardConfig)
        : ShardArgs(std::move(args))
        , ShardConfig(std::move(ShardConfig))
    {
        for (auto& host: ShardConfig.GetHosts()) {
            Unused.emplace_back(host);
        }
    }

    TShardClient PickHost(NClient::TClientAppConfigPtr clientConfig)
    {
        ResizeIfNeeded();

        with_lock (Lock) {
            if (auto host = ShardConfig.GetFixedHost(); !host.Empty()) {
                if (Active.count(host)) {
                    auto p = static_cast<T*>(this);
                    return p->CreateShardClient(
                        Active[host],
                        std::move(clientConfig));
                }
            }
            if (Active.empty()) {
                return {};
            }

            auto index = RandomNumber<ui32>(Active.size());

            auto p = static_cast<T*>(this);
            return p->CreateShardClient(
                std::next(Active.begin(), index)->second,
                std::move(clientConfig));
        }
    }

    TShardClients PickHosts(
        ui32 count,
        NClient::TClientAppConfigPtr clientConfig)
    {
        ResizeIfNeeded();

        with_lock (Lock) {
            TShardClients res;
            auto p = static_cast<T*>(this);
            auto it = Active.begin();
            while (count-- && it != Active.end()) {
                auto endpoint = p->CreateShardClient(
                    it->second, std::move(clientConfig));
                res.emplace_back(endpoint);
                ++it;
            }
            return res;
        }
    }

    void ResizeIfNeeded()
    {
        TVector<std::pair<TString, NThreading::TFuture<TEndpoint>>> tmp;
        with_lock(Lock) {
            if (Active.size() >= ShardConfig.GetMinShardConnections()) {
                return;
            }

            auto delta = ShardConfig.GetMinShardConnections() - Active.size();
            while (delta-- && !Unused.empty()) {
                auto host = Unused.back();
                Unused.pop_back();
                auto future = static_cast<T*>(this)->SetupHostEndpoint(host);
                Activating[host] = future;
                tmp.push_back({host, future});
            }
        }

        auto weakPtr = this->weak_from_this();
        for (const auto& [host,f]: tmp) {
            f.Subscribe(
                [=] (const auto& future) {
                    if (auto pThis = weakPtr.lock(); pThis) {
                        with_lock(pThis->Lock) {
                            pThis->Active.emplace(
                                host,
                                future.GetValue());
                            pThis->Activating.erase(host);
                        }
                    }
            });
        }
    }

   TShardClient GetShardClient(
        NClient::TClientAppConfigPtr clientConfig) override
    {
        return PickHost(clientConfig);
    }

    TShardClients GetShardClients(
        NClient::TClientAppConfigPtr clientConfig) override
    {
        return PickHosts(ShardConfig.GetShardDescribeHostCnt(), clientConfig);
    }
};

struct TEndpoint
{
    TString Host;
    NClient::IMultiClientEndpointPtr Endpoint;
    IBlockStorePtr StorageService;
};

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

////////////////////////////////////////////////////////////////////////////////

IShardPtr CreateGrpcEndpointManager(
    TShardArguments args,
    const TShardConfig ShardConfig)
{
    return std::make_shared<TShardEndpointManagerGrpc>(
        std::move(args),
        std::move(ShardConfig));
}

IShardPtr CreateRdmaEndpointManager(
    TShardArguments args,
    TShardConfig ShardConfig)
{
    return std::make_shared<TShardEndpointManagerRdma>(
        std::move(args),
        std::move(ShardConfig));
}

////////////////////////////////////////////////////////////////////////////////

struct TRemoteStorageProvider
    : public IRemoteStorageProvider
{
    const TShardArguments Args;

    THashMap<TString, IShardPtr> Shards;

    explicit TRemoteStorageProvider(
            TShardingConfigPtr config,
            TShardArguments args)
        : IRemoteStorageProvider(std::move(config))
        , Args(std::move(args))
    {
        for (const auto& shard: Config->GetShards()) {
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

    TShardClient GetShardClient(
        const TString& shardId,
        NClient::TClientAppConfigPtr clientConfig) override
    {
        auto it = Shards.find(shardId);
        Y_ENSURE(it != Shards.end());
        return it->second->GetShardClient(clientConfig);
    }


    TShardClients GetShardsClients(
        NClient::TClientAppConfigPtr clientConfig) override
    {
        TShardClients res;
        for (auto& shard: Shards) {
            auto clientList = shard.second->GetShardClients(clientConfig);
            if (clientList.empty()) {
                continue;
            }
            res.emplace(shard.first, std::move(clientList));
        }
        return res;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString TShardClient::BuildLogTag(
    const NClient::TClientAppConfigPtr& clientConfig,
    const TString& fqdn)
{
    return TStringBuilder()
        << "[h:" << fqdn << "]"
        << "[i:" << clientConfig->GetInstanceId() << "]"
        << "[c:" << clientConfig->GetClientId() << "]";
}

////////////////////////////////////////////////////////////////////////////////

ui32 IRemoteStorageProvider::GetConfiguredShardCount() const
{
    return Config->GetShards().size();
}

////////////////////////////////////////////////////////////////////////////////

IRemoteStorageProviderPtr CreateRemoteStorageProvider(
    TShardingConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    NClient::IClientPtr grpcClient,
    NRdma::IClientPtr rdmaClient)
{
    TShardArguments args {
        .Timer = std::move(timer),
        .Scheduler = std::move(scheduler),
        .Logging = std::move(logging),
        .Monitoring = std::move(monitoring),
        .GrpcClient = std::move(grpcClient),
        .RdmaClient = std::move(rdmaClient)
    };

    return std::make_shared<TRemoteStorageProvider>(std::move(config), args);
}

}   // namespace NCloud::NBlockStore::NSharding
