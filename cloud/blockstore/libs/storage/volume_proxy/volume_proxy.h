#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateVolumeProxy(
    TStorageConfigPtr config,
    ITraceSerializerPtr traceSerialize,
    bool temporaryServer);

}   // namespace NCloud::NBlockStore::NStorage
