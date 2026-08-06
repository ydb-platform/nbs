#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/rdma/iface/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateNonreplicatedPartition(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    TNonreplicatedPartitionConfigPtr partConfig,
    NActors::TActorId volumeActorId,
    NActors::TActorId statActorId,
    NCloud::NStorage::NRdma::IClientPtr rdmaClient = nullptr);

}   // namespace NCloud::NBlockStore::NStorage
