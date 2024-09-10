#pragma once

#include <library/cpp/actors/core/actor.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> CreateIORequestParserActor(
    const NActors::TActorId& owner);

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
