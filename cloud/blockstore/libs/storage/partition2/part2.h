#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreatePartitionTablet(
    const NActors::TActorId& owner,
    NKikimr::TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NProto::TPartitionConfig partitionConfig,
    EStorageAccessMode storageAccessMode,
    ui32 partitionIndex,
    ui32 siblingCount,
    const NActors::TActorId& volumeActorId,
    ui64 volumeTabletId);

// Compatibility entry point for existing Partition 2 callers.  Partition 2
// historically did not receive the volume partition index.
NActors::IActorPtr CreatePartitionTablet(
    const NActors::TActorId& owner,
    NKikimr::TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NProto::TPartitionConfig partitionConfig,
    EStorageAccessMode storageAccessMode,
    ui32 siblingCount,
    const NActors::TActorId& volumeActorId,
    ui64 volumeTabletId);

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
