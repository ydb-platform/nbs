#pragma once

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/actors/public.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateCellsMonActor(
    ICellManagerPtr cellManager,
    TDiagnosticsConfigPtr diagnosticsConfig);

// Renders the plain page: summary, search form, a panel per cell, intercell
// mounts, inbound, config.
void RenderCellsPage(
    IOutputStream& out,
    const TCellsConfig& config,
    const TCellsSnapshot& snapshot,
    const TDiagnosticsConfig& diagnosticsConfig);

// Renders the search form and the per-cell search result table.
void RenderCellsSearchResult(
    IOutputStream& out,
    const TVector<TCellDescribeResult>& results,
    const TDiagnosticsConfig& diagnosticsConfig,
    const TString& localCellId,
    const TString& diskId);

}   // namespace NCloud::NBlockStore::NCells
