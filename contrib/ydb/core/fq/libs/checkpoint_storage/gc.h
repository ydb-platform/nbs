#pragma once

#include "checkpoint_storage.h"
#include "state_storage.h"

#include <contrib/ydb/core/fq/libs/config/protos/checkpoint_coordinator.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <memory>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewGC(
    const NConfig::TCheckpointGcConfig& config,
    const TCheckpointStoragePtr& checkpointStorage,
    const TStateStoragePtr& stateStorage);

} // namespace NFq
