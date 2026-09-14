/*******************************************************************************

The fixed node-local actor identity for BlockStore ConfigsManager.
Callers use this identity to address the service without owning its actor.

*******************************************************************************/

#pragma once

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Return the ConfigsManager service identity for the selected node.
inline NActors::TActorId MakeConfigsManagerServiceId(ui32 nodeId = 0)
{
    return NActors::TActorId(nodeId, "nbsconfigmgr");
}

}   // namespace NCloud::NBlockStore
