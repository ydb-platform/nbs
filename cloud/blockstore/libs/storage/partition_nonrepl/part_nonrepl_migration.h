#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateNonreplicatedPartitionMigration(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr digestGenerator,
    ui64 initialMigrationIndex,
    TString rwClientId,
    TNonreplicatedPartitionConfigPtr partConfig,
    google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations,
    NRdma::IClientPtr rdmaClient,
    NActors::TActorId statActorId,
    NActors::TActorId migrationSrcActorId = NActors::TActorId());

}   // namespace NCloud::NBlockStore::NStorage
