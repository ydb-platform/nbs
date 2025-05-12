#include "remote_storage_provider.h"

#include <cloud/blockstore/libs/client/config.h>
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

struct TShardEndpointHandler
{
    const TServerAppConfigPtr Config;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const NRdma::IClientPtr RdmaClient;
    const NProto::TShardInfo ShardInfo;

    TVector<TRemoteShardEndpoints> Active;
    TVector<TString> Unused;

    const ui32 ActiveCnt;

    TShardEndpointHandler(
            const TServerAppConfigPtr config,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            NRdma::IClientPtr rdmaClient,
            NProto::TShardInfo shardInfo,
            ui32 activeCnt)
        : Config(config)
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , RdmaClient(std::move(rdmaClient))
        , ShardInfo(std::move(shardInfo))
        , ActiveCnt(activeCnt)
    {}

    TRemoteShardEndpoints Allocate(std::optional<TString> clientId)
    {
        auto index = RandomNumber<ui32>(Config->GetShardMap()[ShardInfo.GetShardId()].GetHosts().size());

        return CreateEndpoints(Config->GetShardMap()[ShardInfo.GetShardId()].GetHosts(index), clientId);
    }

    TRemoteShardEndpoints CreateEndpoints(
        const TString& host,
        std::optional<TString> clientId)
    {
        if (ShardInfo.GetTransport() == NProto::GRPC) {

            auto service = CreateSuDataService(Timer, Scheduler, Logging, Monitoring, host, ShardInfo.GetGrpcPort(), clientId);
            service->Start();

            return {service, service} ;
        } else if (ShardInfo.GetTransport() == NProto::RDMA) {
            auto service = CreateSuDataService(Timer, Scheduler, Logging, Monitoring, host, ShardInfo.GetGrpcPort(), clientId);
            service->Start();

            NClient::TRdmaEndpointConfig rdmaEndpoint {
                .Address = host,
                .Port = ShardInfo.GetGrpcPort(),
            };

            auto ClientEndpoint = CreateRdmaEndpointClient(
                Logging,
                RdmaClient,
                service,
                rdmaEndpoint);
            ClientEndpoint->Start();

            return {service, ClientEndpoint};
        }
        return {};
    }

    bool Release()
    {
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRemoteStorageProvider
    : public IRemoteStorageProvider
{
    const TServerAppConfigPtr Config;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const NRdma::IClientPtr RdmaClient;

    THashMap<TString, TShardEndpointHandler> Cache;

    TRemoteStorageProvider(
            TServerAppConfigPtr config,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            NRdma::IClientPtr rdmaClient)
        : Config(std::move(config))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , RdmaClient(std::move(rdmaClient))
    {
        for (const auto& shard: Config->GetShardMap()) {
            Cache.emplace(
                shard.first,
                TShardEndpointHandler(
                    Config,
                    Timer,
                    Scheduler,
                    Logging,
                    Monitoring,
                    RdmaClient,
                    shard.second,
                    1));
        }
    }


    TResultOrError<TRemoteShardEndpoints> CreateStorage(
        const TString& shardId,
        NClient::TClientAppConfigPtr clientConfig,
        std::optional<TString> clientId) override
    {
        Y_UNUSED(clientConfig);
        if (auto it = Cache.find(shardId); it != Cache.end()) {
            return it->second.Allocate(clientId);
        }
        return MakeError(E_FAIL, "");
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
    NRdma::IClientPtr rdmaClient)
{
    return std::make_shared<TRemoteStorageProvider>(
        std::move(config),
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        std::move(rdmaClient)
    );
}

}   // namespace NCloud::NBlockStore::NServer
