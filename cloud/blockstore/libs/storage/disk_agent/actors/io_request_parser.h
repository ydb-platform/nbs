#pragma once

#include <contrib/ydb/library/actors/core/actor.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> CreateIORequestParserActor(
    const NActors::TActorId& owner);

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
