#pragma once

#include "public.h"

#include "config.h"
#include "host_endpoint.h"
#include "host_endpoints_manager.h"
#include "sharding_common.h"

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

#include <util/generic/hash_set.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

class TShardManager
    : public std::enable_shared_from_this<TShardManager>
{
private:
    const TShardingArguments Args;
    const TShardConfig Config;

    TAdaptiveLock Lock;

    THashMap<TString, THostEndpointsManagerPtr> Active;
    THashMap<TString, THostEndpointsManagerPtr> Activating;
    THashSet<TString, THostEndpointsManagerPtr> Deactivating;
    TVector<TString> Unused;

public:
    TShardManager(
        TShardingArguments args,
        TShardConfig config);

    [[nodiscard]] TResultOrError<THostEndpoint> GetShardClient(
        const NClient::TClientAppConfigPtr& clientConfig)
    {
        return PickHost(clientConfig);
    }

    [[nodiscard]] TShardEndpoints GetShardClients(
        const NClient::TClientAppConfigPtr& clientConfig)
    {
        return PickHosts(Config.GetShardDescribeHostCnt(), clientConfig);
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
