#pragma once

#include <cloud/blockstore/libs/cells/iface/bootstrap.h>
#include <cloud/blockstore/libs/cells/iface/cells.h>
#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

TDescribeVolumeFuture DescribeVolume(
    const NProto::TDescribeVolumeRequest& request,
    IBlockStorePtr service,
    const TCellHostEndpointsByCellId& endpoints,
    bool hasUnavailableCells,
    TDuration timeout,
    TBootstrap bootstrap);

}   // namespace NCloud::NBlockStore::NCells
