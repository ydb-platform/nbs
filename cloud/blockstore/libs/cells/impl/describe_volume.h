#pragma once

#include <cloud/blockstore/libs/cells/iface/arguments.h>
#include <cloud/blockstore/libs/cells/iface/cells.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/server/public.h>

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

std::optional<TDescribeFuture> DescribeVolume(
    const NProto::TDescribeVolumeRequest& request,
    const IBlockStorePtr& localService,
    const TCellsEndpoints& endpoints,
    bool hasUnavailableCells,
    TDuration timeout,
    TArguments args);

}   // namespace NCloud::NBlockStore::NCells
