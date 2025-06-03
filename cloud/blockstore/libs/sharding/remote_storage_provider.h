#pragma once

#include "public.h"

#include "shard_client.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/blockstore/libs/rdma/iface/client.h>

#include <cloud/blockstore/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////
struct IRemoteStorageProvider
    : public IStartable
{
    TShardingConfigPtr Config;

    explicit IRemoteStorageProvider(TShardingConfigPtr config)
        : Config(std::move(config))
    {}

    ~IRemoteStorageProvider() override = default;

    [[nodiscard]] virtual TShardClient GetShardClient(
        const TString& shardId,
        NClient::TClientAppConfigPtr clientConfig) = 0;

    [[nodiscard]] virtual TShardClients GetShardsClients(
        NClient::TClientAppConfigPtr clientConfig)  = 0;

    [[nodiscard]] ui32 GetConfiguredShardCount() const;

    [[nodiscard]] TShardingConfigPtr GetShardingConfig() const
    {
        return Config;
    }
};

////////////////////////////////////////////////////////////////////////////////

IRemoteStorageProviderPtr CreateRemoteStorageProvider(
    TShardingConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    NClient::IClientPtr grpcClient,
    NRdma::IClientPtr rdmaClient);

}   // namespace NCloud::NBlockStore::NSharding
