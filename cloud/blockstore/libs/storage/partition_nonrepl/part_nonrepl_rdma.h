#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/rdma/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <library/cpp/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateNonreplicatedPartitionRdma(
    TStorageConfigPtr config,
    TNonreplicatedPartitionConfigPtr partConfig,
    NRdma::IClientPtr rdmaClient,
    NActors::TActorId statActorId);

}   // namespace NCloud::NBlockStore::NStorage
