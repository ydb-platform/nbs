#pragma once

#include "config.h"
#include "host_endpoint.h"
#include "sharding_arguments.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/client_rdma/rdma_client.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/server/config.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <util/generic/hash_set.h>
#include <util/random/random.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

struct IShardManager
    : public IStartable
{
    [[nodiscard]] virtual TResultOrError<THostEndpoint> GetShardClient(
        const NClient::TClientAppConfigPtr& clientConfig) = 0;

    [[nodiscard]] virtual TShardEndpoints GetShardClients(
        const NClient::TClientAppConfigPtr& clientConfig) = 0;
};

using IShardManagerPtr = std::shared_ptr<IShardManager>;

////////////////////////////////////////////////////////////////////////////////

IShardManagerPtr CreateShardManager(
    TShardingArguments args,
    TShardConfig config);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NSharding
