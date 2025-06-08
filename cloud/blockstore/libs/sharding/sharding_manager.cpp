#include "sharding_manager.h"

#include "config.h"
#include "describe_volume.h"
#include "endpoints_setup.h"
#include "host_endpoint.h"
#include "sharding_common.h"
#include "shard_manager.h"

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

#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NSharding {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TShardingManager
    : public IShardingManager
{
    const TShardingArguments Args;

    THashMap<TString, TShardManagerPtr> Shards;

    TShardingManager(
        TShardingConfigPtr config,
        TShardingArguments args);

    void Start() override;
    void Stop() override;

    TResultOrError<THostEndpoint> GetShardEndpoint(
        const TString& shardId,
        const NClient::TClientAppConfigPtr& clientConfig) override;

    [[nodiscard]] std::optional<TDescribeFuture> DescribeVolume(
        const TString& diskId,
        const NProto::THeaders& headers,
        const IBlockStorePtr& localService,
        const NProto::TClientConfig& clientConfig) override;

    void OutputHtml(IOutputStream& out, const IMonHttpRequest& request);

private:
    [[nodiscard]] TShardsEndpoints GetShardsEndpoints(
        const NClient::TClientAppConfigPtr& clientConfig);
};

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
            std::make_shared<TShardManager>(Args, shard.second));
    }

    auto rootPage = Args.Monitoring->RegisterIndexPage("blockstore", "BlockStore");
    static_cast<TIndexMonPage&>(*rootPage).Register(
        new TShardsMonPage(*this, "Sharding"));
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IShardingManagerPtr CreateShardingManager(
    TShardingConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
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

    TShardingArguments args {
        .Timer = std::move(timer),
        .Scheduler = std::move(scheduler),
        .Logging = std::move(logging),
        .Monitoring = std::move(monitoring),
        .GrpcClient = std::move(result.GetResult()),
        .RdmaClient = std::move(rdmaClient),
        .EndpointsSetup = CreateHostEndpointsSetupProvider()
    };

    return std::make_shared<TShardingManager>(std::move(config), args);
}

}   // namespace NCloud::NBlockStore::NSharding
