#include "remote_storage_provider.h"

#include "config.h"
#include "sharding_common.h"
#include "shard_endpoints_manager.h"
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

#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NSharding {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString Capitalize(TString str)
{
    if (str.empty()) {
        return str;
    }

    str[0] = ::toupper(str[0]);
    return str;
}

////////////////////////////////////////////////////////////////////////////////
struct TRemoteStorageProvider
    : public IRemoteStorageProvider
{
    const TShardingArguments Args;

    THashMap<TString, IShardEndpointsManagerPtr> Shards;

    TRemoteStorageProvider(
        TShardingConfigPtr config,
        TShardingArguments args);

    void Start() override;
    void Stop() override;

    TShardClient GetShardClient(
        const TString& shardId,
        NClient::TClientAppConfigPtr clientConfig) override;

    TShardClients GetShardsClients(
        NClient::TClientAppConfigPtr clientConfig) override;

    void OutputHtml(IOutputStream& out, const IMonHttpRequest& request);
};

////////////////////////////////////////////////////////////////////////////////

class TShardsMonPage final
    : public THtmlMonPage
{
private:
    TRemoteStorageProvider& Provider;

public:
    TShardsMonPage(TRemoteStorageProvider& provider, const TString& componentName)
        : THtmlMonPage(componentName, Capitalize(componentName), true)
        , Provider(provider)
    {}

    void OutputContent(IMonHttpRequest& request) override
    {
        Provider.OutputHtml(request.Output(), request);
    }
};

////////////////////////////////////////////////////////////////////////////////

TRemoteStorageProvider::TRemoteStorageProvider(
        TShardingConfigPtr config,
        TShardingArguments args)
    : IRemoteStorageProvider(std::move(config))
    , Args(std::move(args))
{
    for (const auto& shard: Config->GetShards()) {
        IShardEndpointsManagerPtr ptr;
        if (shard.second.GetTransport() == NProto::GRPC) {
            ptr = CreateGrpcEndpointManager(Args, shard.second);
        } else {
            ptr = CreateRdmaEndpointManager(Args, shard.second);
        }

        Shards.emplace(shard.first, std::move(ptr));
    }

    auto rootPage = Args.Monitoring->RegisterIndexPage("blockstore", "BlockStore");
    static_cast<TIndexMonPage&>(*rootPage).Register(
        new TShardsMonPage(*this, "Sharding"));
}

void TRemoteStorageProvider::Start()
{
    Args.GrpcClient->Start();
}

void TRemoteStorageProvider::Stop()
{
    Args.GrpcClient->Stop();
}

TShardClient TRemoteStorageProvider::GetShardClient(
    const TString& shardId,
    NClient::TClientAppConfigPtr clientConfig)
{
    auto it = Shards.find(shardId);
    Y_ENSURE(it != Shards.end());
    return it->second->GetShardClient(clientConfig);
}

TShardClients TRemoteStorageProvider::GetShardsClients(
    NClient::TClientAppConfigPtr clientConfig)
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

void TRemoteStorageProvider::OutputHtml(
    IOutputStream& out,
    const IMonHttpRequest& request)
{
    Y_UNUSED(out);
    Y_UNUSED(request);
}

}   // namespace

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
    TShardingArguments args {
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
