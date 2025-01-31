#pragma once

#include <contrib/ydb/library/actors/core/actor.h>

#include <functional>
#include <memory>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

////////////////////////////////////////////////////////////////////////////////

using TWriteDeviceBlocksRequestParser =
    std::function<TAutoPtr<NActors::IEventBase>(
        TAutoPtr<NActors::IEventHandle>& ev)>;

std::unique_ptr<NActors::IActor> CreateIORequestParserActor(
    const NActors::TActorId& owner,
    TWriteDeviceBlocksRequestParser parser);

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
