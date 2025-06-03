#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EVolumeStartMode
{
    ONLINE,
    MOUNTED
};

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateVolumeTablet(
    const NActors::TActorId& owner,
    NKikimr::TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    ITraceSerializerPtr traceSerializer,
    NRdma::IClientPtr rdmaClient,
    NServer::IEndpointEventHandlerPtr endpointEventHandler,
    EVolumeStartMode startMode,
    TString diskId);

}   // namespace NCloud::NBlockStore::NStorage
