#pragma once

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/sharding/iface/config.h>
#include <cloud/blockstore/libs/sharding/iface/endpoints_setup.h>
#include <cloud/blockstore/libs/sharding/iface/host_endpoint.h>
#include <cloud/blockstore/libs/sharding/iface/sharding_arguments.h>
#include <cloud/blockstore/libs/sharding/iface/shard.h>
#include <cloud/blockstore/libs/sharding/iface/sharding.h>

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

////////////////////////////////////////////////////////////////////////////////

struct TShardingManager
    : public IShardingManager
{
    const TShardingArguments Args;

    THashMap<TString, IShardManagerPtr> Shards;

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

}   // namespace NCloud::NBlockStore::NSharding
