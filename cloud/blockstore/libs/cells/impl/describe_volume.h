#pragma once

#include "bootstrap.h"
#include "cell_manager.h"

#include <cloud/blockstore/libs/cells/iface/host_endpoint.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct TCellDescribeTarget
{
    ui32 ResultIndex;
    TString CellId;         // empty for the local service
    TString Fqdn;
    IBlockStorePtr Service;
};

struct TMonitoringDescribePlan
{
    TVector<TCellDescribeResult> Results;   // one row per cell (+ local)
    TVector<TCellDescribeTarget> Targets;   // the describes to fire
};

////////////////////////////////////////////////////////////////////////////////

TDescribeVolumeFuture DescribeVolume(
    const TCellsConfig& config,
    NProto::TDescribeVolumeRequest request,
    IBlockStorePtr service,
    const TCellHostEndpointsByCellId& endpoints,
    bool hasUnavailableCells,
    TBootstrap bootstrap);

TMonitoringDescribePlan PrepareMonitoringDescribe(
    const TVector<TString>& cellIds,
    const TCellHostEndpointsByCellId& endpoints,
    const IBlockStorePtr& localService);

std::shared_ptr<NProto::TDescribeVolumeRequest> PrepareCellDescribeRequest(
    const NProto::TDescribeVolumeRequest& request,
    const TString& cellId);

void ApplyDescribeResponse(
    TCellDescribeResult& result,
    const TString& fqdn,
    const NProto::TDescribeVolumeResponse& response);

void ApplyDescribeTimeout(TCellDescribeResult& result);

// Fires a describe at every target from PrepareMonitoringDescribe, folds the
// answers, and bounds the wait with `scheduler` + `timeout`. Completes with one
// result per cell (+ local). Async, non-blocking - for the mon search.
NThreading::TFuture<TVector<TCellDescribeResult>> SearchVolumeAcrossCells(
    NProto::TDescribeVolumeRequest request,
    const TVector<TString>& cellIds,
    const TCellHostEndpointsByCellId& endpoints,
    IBlockStorePtr localService,
    TDuration timeout,
    ISchedulerPtr scheduler);

}   // namespace NCloud::NBlockStore::NCells
