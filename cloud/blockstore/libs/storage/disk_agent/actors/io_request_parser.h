#pragma once

#include <ydb/library/actors/core/actor.h>

#include <functional>
#include <memory>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

////////////////////////////////////////////////////////////////////////////////

using TStorageBufferAllocator =
    std::function<std::shared_ptr<char>(ui64 bytesCount)>;

std::unique_ptr<NActors::IActor> CreateIORequestParserActor(
    const NActors::TActorId& owner,
    TStorageBufferAllocator allocator);

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
