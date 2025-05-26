#pragma once

#include "contrib/ydb/library/actors/core/actorsystem.h"

#include <cloud/blockstore/libs/storage/disk_agent/model/multi_agent_write.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IMultiAgentWriteHandlerPtr CreateMultiAgentWriteHandler(
    NActors::TActorSystem* actorSystem,
    NActors::TActorId diskAgentId);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
