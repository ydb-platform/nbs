#pragma once

#include "public.h"

#include <cloud/blockstore/libs/config/blockstore_config.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Create a node-local manager with initial service rules and optional CLI
// overrides. Treat a null initialConfig as an empty service configuration.
NActors::IActorPtr CreateVolumeThrottlingManager(
    TDuration cycleTime,
    TVolumeThrottlingConfigConstPtr initialConfig = {});

}   // namespace NCloud::NBlockStore::NStorage
