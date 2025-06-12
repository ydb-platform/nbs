#pragma once

#include "public.h"

#include "host_endpoint.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/blockstore/libs/rdma/iface/client.h>

#include <cloud/blockstore/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

using TDescribeFuture = NThreading::TFuture<NProto::TDescribeVolumeResponse>;

struct IShardingManager
    : public IStartable
{
    TShardingConfigPtr Config;

    explicit IShardingManager(TShardingConfigPtr config)
        : Config(std::move(config))
    {}

    [[nodiscard]] virtual TResultOrError<THostEndpoint> GetShardEndpoint(
        const TString& shardId,
        const NClient::TClientAppConfigPtr& clientConfig) = 0;

    [[nodiscard]] virtual std::optional<TDescribeFuture> DescribeVolume(
        const TString& diskId,
        const NProto::THeaders& headers,
        const IBlockStorePtr& localService,
        const NProto::TClientConfig& clientConfig) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IShardingManagerPtr CreateShardingManager(
    TShardingConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IServerStatsPtr serverStats,
    NRdma::IClientPtr rdmaClient);

}   // namespace NCloud::NBlockStore::NSharding
