#pragma once

#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/sharding/iface/host_endpoint.h>
#include <cloud/blockstore/libs/sharding/iface/sharding_arguments.h>
#include <cloud/blockstore/libs/sharding/iface/sharding.h>

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

std::optional<TDescribeFuture> DescribeVolume(
    const NProto::TDescribeVolumeRequest& request,
    const IBlockStorePtr& localService,
    const TShardsEndpoints& endpoints,
    bool hasUnavailableShards,
    TDuration timeout,
    TShardingArguments args);

}   // namespace NCloud::NBlockStore::NSharding
