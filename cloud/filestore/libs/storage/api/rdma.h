#pragma once

#include "public.h"

#include <contrib/ydb/library/actors/core/actorid.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Pipe servers of the node's index tablets. The RDMA side channel delivers a
// request to the tablet of the client's pipe server.
struct IRdmaTabletRegistry
{
    virtual ~IRdmaTabletRegistry() = default;

    virtual void RegisterPipe(
        const NActors::TActorId& pipeServerId,
        const NActors::TActorId& tabletActorId) = 0;

    virtual void UnregisterPipe(const NActors::TActorId& pipeServerId) = 0;

    virtual void UnregisterTablet(const NActors::TActorId& tabletActorId) = 0;
};

using IRdmaTabletRegistryPtr = std::shared_ptr<IRdmaTabletRegistry>;

}   // namespace NCloud::NFileStore::NStorage
