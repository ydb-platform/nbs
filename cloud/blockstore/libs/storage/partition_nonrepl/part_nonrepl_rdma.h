#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateNonreplicatedPartitionRdma(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    TNonreplicatedPartitionConfigPtr partConfig,
    NRdma::IClientPtr rdmaClient,
    NActors::TActorId statActorId);

}   // namespace NCloud::NBlockStore::NStorage
