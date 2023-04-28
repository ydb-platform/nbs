#include "disk_agent.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeDiskAgentServiceId(ui32 nodeId)
{
    return TActorId(nodeId, "blk-dskagent");
}

}   // namespace NCloud::NBlockStore::NStorage
