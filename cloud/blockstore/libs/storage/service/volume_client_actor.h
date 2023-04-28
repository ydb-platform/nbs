#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateVolumeClient(
    TStorageConfigPtr config,
    ITraceSerializerPtr traceSerializer,
    const NActors::TActorId& sessionActorId,
    TString diskId,
    ui64 tabletId);

}   // namespace NCloud::NBlockStore::NStorage
