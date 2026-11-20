#pragma once

#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

namespace NCloud::NBlockStore::NStorage {

NActors::IActorPtr CreateConditionalDestroyVolumeActor(
    TRequestInfoPtr requestInfo,
    TStorageConfigPtr config,
    TString diskId,
    ui64 fillGeneration,
    ui64 expectedPathId,
    ui64 expectedPathVersion);

}   // namespace NCloud::NBlockStore::NStorage
