#include "describe_volume.h"
#include "sharding.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>
#include <cloud/blockstore/libs/server/config.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NSharding {

using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

class TShardsMonPage final
    : public THtmlMonPage
{
private:
    TShardingManager& Manager;

public:
    TShardsMonPage(TShardingManager& manager, const TString& componentName)
        : THtmlMonPage(componentName, componentName, true)
        , Manager(manager)
    {}

    void OutputContent(IMonHttpRequest& request) override
    {
        Manager.OutputHtml(request.Output(), request);
    }
};

////////////////////////////////////////////////////////////////////////////////

TShardingManager::TShardingManager(
        TShardingConfigPtr config,
        TShardingArguments args)
    : IShardingManager(std::move(config))
    , Args(std::move(args))
{
    for (const auto& shard: Config->GetShards()) {
        Shards.emplace(
            shard.first,
            CreateShardManager(Args, shard.second));
    }

    if (args.Monitoring) {
        auto rootPage = Args.Monitoring->RegisterIndexPage(
            "blockstore",
            "BlockStore");
        static_cast<TIndexMonPage&>(*rootPage).Register(
            new TShardsMonPage(*this, "Sharding"));
    }
}

void TShardingManager::Start()
{
    Args.GrpcClient->Start();

    for (auto& shard: Shards) {
        shard.second->Start();
    }
}

void TShardingManager::Stop()
{
    Args.GrpcClient->Stop();
}

TResultOrError<THostEndpoint> TShardingManager::GetShardEndpoint(
    const TString& shardId,
    const NClient::TClientAppConfigPtr& clientConfig)
{
    auto it = Shards.find(shardId);
    Y_ENSURE(it != Shards.end());
    return it->second->GetShardClient(clientConfig);
}

TShardsEndpoints TShardingManager::GetShardsEndpoints(
    const NClient::TClientAppConfigPtr& clientConfig)
{
    TShardsEndpoints res;
    for (auto& shard: Shards) {
        auto clientList = shard.second->GetShardClients(clientConfig);
        if (clientList.empty()) {
            continue;
        }
        res.emplace(shard.first, std::move(clientList));
    }
    return res;
}

[[nodiscard]] std::optional<TDescribeFuture> TShardingManager::DescribeVolume(
    const TString& diskId,
    const NProto::THeaders& headers,
    const IBlockStorePtr& localService,
    const NProto::TClientConfig& clientConfig)
{
    auto numConfigured = Config->GetShards().size();
    if (numConfigured == 0) {
        return {};
    }

    NProto::TClientAppConfig clientAppConfig;
    auto& config = *clientAppConfig.MutableClientConfig();
    config = clientConfig;
    config.SetClientId(FQDNHostName());
    auto appConfig = std::make_shared<NClient::TClientAppConfig>(clientAppConfig);

    auto shardedEndpoints = GetShardsEndpoints(appConfig);

    bool hasUnavailableShards = shardedEndpoints.size() < numConfigured;

    NProto::TDescribeVolumeRequest request;
    request.MutableHeaders()->CopyFrom(headers);
    request.SetDiskId(diskId);

    return NCloud::NBlockStore::NSharding::DescribeVolume(
        request,
        localService,
        shardedEndpoints,
        hasUnavailableShards,
        Config->GetDescribeTimeout(),
        Args);
}

void TShardingManager::OutputHtml(
    IOutputStream& out,
    const IMonHttpRequest& request)
{
    Y_UNUSED(out);
    Y_UNUSED(request);
}

////////////////////////////////////////////////////////////////////////////////

IShardingManagerPtr CreateShardingManager(
    TShardingConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    ITraceSerializerPtr traceSerializer,
    IServerStatsPtr serverStats,
    NRdma::IClientPtr rdmaClient)
{
    auto result = NClient::CreateClient(
        std::make_shared<NClient::TClientAppConfig>(config->GetGrpcClientConfig()),
        timer,
        scheduler,
        logging,
        monitoring,
        std::move(serverStats));

    if (HasError(result.GetError())) {
        ythrow TServiceError(E_FAIL)
            << "unable to create gRPC client";
    }

    auto workers = config->GetRdmaTransportWorkers() ?
        CreateThreadPool("SHRD", config->GetRdmaTransportWorkers()) :
        CreateTaskQueueStub();

    workers->Start();

    TShardingArguments args {
        .Timer = std::move(timer),
        .Scheduler = std::move(scheduler),
        .Logging = std::move(logging),
        .Monitoring = std::move(monitoring),
        .TraceSerializer = std::move(traceSerializer),
        .GrpcClient = std::move(result.GetResult()),
        .RdmaClient = std::move(rdmaClient),
        .Workers = std::move(workers),
        .EndpointsSetup = CreateHostEndpointsSetupProvider()
    };

    return std::make_shared<TShardingManager>(std::move(config), args);
}

}   // namespace NCloud::NBlockStore::NSharding
