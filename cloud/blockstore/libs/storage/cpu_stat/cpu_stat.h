#pragma once

#include "public.h"

#include "cloud/blockstore/libs/storage/core/public.h"

#include "cloud/storage/core/libs/actors/public.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateCpuStatsFetcherActor(
    TStorageConfigPtr storageConfig,
    NCloud::NStorage::IStatsFetcherPtr statsFetcher);

}   // namespace NCloud::NBlockStore::NStorage
