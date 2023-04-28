#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/rdma/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <library/cpp/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateNonreplicatedPartition(
    TStorageConfigPtr config,
    TNonreplicatedPartitionConfigPtr partConfig,
    NActors::TActorId statActorId,
    NRdma::IClientPtr rdmaClient = nullptr);

}   // namespace NCloud::NBlockStore::NStorage
