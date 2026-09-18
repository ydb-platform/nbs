#pragma once

#include "bootstrap.h"
#include "cell_manager.h"

#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>

#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Listed from the most to the least authoritative answer: when a cell's hosts
// disagree, the smaller value wins (a concrete answer beats an inconclusive
// one).
enum class ECellDescribeStatus
{
    Found,          // the cell holds the disk (Fqdn is the host that answered)
    NotFound,       // the cell answered, the disk is not in it
    Failed,         // timed out or a transport/other error - result unknown
    Unavailable,    // no connected host to ask the cell
};

struct TCellDescribeResult
{
    TMaybe<TString> CellId;   // empty for the local service row
    ECellDescribeStatus Status;
    TString Fqdn;
    NProto::TError Error;
};

////////////////////////////////////////////////////////////////////////////////

TDescribeVolumeFuture DescribeVolume(
    const TCellsConfig& config,
    NProto::TDescribeVolumeRequest request,
    IBlockStorePtr service,
    const TCellHostEndpointsByCellId& endpoints,
    bool hasUnavailableCells,
    TBootstrap bootstrap);

TVector<TCellDescribeResult> DescribeVolumeForMonitoring(
    NProto::TDescribeVolumeRequest request,
    const TVector<TString>& cellIds,
    const TCellHostEndpointsByCellId& endpoints,
    IBlockStorePtr localService,
    TDuration timeout);

}   // namespace NCloud::NBlockStore::NCells
