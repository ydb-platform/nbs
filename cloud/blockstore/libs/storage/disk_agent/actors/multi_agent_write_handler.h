#pragma once

#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IMultiAgentWriteHandlerPtr CreateMultiAgentWriteHandler(
    NActors::TActorSystem* actorSystem,
    NActors::TActorId diskAgentId);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
