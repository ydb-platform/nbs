#pragma once

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/sharding/iface/config.h>
#include <cloud/blockstore/libs/sharding/iface/host_endpoint.h>
#include <cloud/blockstore/libs/sharding/iface/shard_host.h>
#include <cloud/blockstore/libs/sharding/iface/shard.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

class TShardManager
    : public IShardManager
    , public std::enable_shared_from_this<TShardManager>
{
private:
    const TShardingArguments Args;
    const TShardConfig Config;

    TAdaptiveLock Lock;

    THashMap<TString, IHostEndpointsManagerPtr> Active;
    THashMap<TString, IHostEndpointsManagerPtr> Activating;
    THashSet<IHostEndpointsManagerPtr> Deactivating;
    TVector<TString> Unused;

public:
    TShardManager(
        TShardingArguments args,
        TShardConfig config);

    [[nodiscard]] TResultOrError<THostEndpoint> GetShardClient(
        const NClient::TClientAppConfigPtr& clientConfig) override
    {
        return PickHost(clientConfig);
    }

    [[nodiscard]] TShardEndpoints GetShardClients(
        const NClient::TClientAppConfigPtr& clientConfig) override
    {
        return PickHosts(Config.GetShardDescribeHostCnt(), clientConfig);
    }

    [[nodiscard]]THashMap<TString, IHostEndpointsManagerPtr>  GetActive() const
    {
        with_lock(Lock) {
            return Active;
        }
    }

    [[nodiscard]]THashMap<TString, IHostEndpointsManagerPtr>  GetActivating() const
    {
        with_lock(Lock) {
            return Activating;
        }
    }

    [[nodiscard]]THashSet<IHostEndpointsManagerPtr>  GetDeactivating() const
    {
        with_lock(Lock) {
            return Deactivating;
        }
    }

    void Start() override
    {
        ResizeIfNeeded();
    }

    void Stop() override
    {
    }

private:
    TResultOrError<THostEndpoint> PickHost(
        const NClient::TClientAppConfigPtr& clientConfig);
    TShardEndpoints PickHosts(
        ui32 count,
        const NClient::TClientAppConfigPtr& clientConfig);

    void ResizeIfNeeded();
};

using TShardManagerPtr = std::shared_ptr<TShardManager>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NSharding
