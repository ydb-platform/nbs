#pragma once

#include "public.h"

#include "config.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateMirrorPartitionResync(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr digestGenerator,
    TString rwClientId,
    TNonreplicatedPartitionConfigPtr partConfig,
    TMigrations migrations,
    TVector<TDevices> replicaDevices,
    NRdma::IClientPtr rdmaClient,
    NActors::TActorId statActorId,
    ui64 initialResyncIndex);

}   // namespace NCloud::NBlockStore::NStorage
