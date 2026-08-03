#include "service.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeStorageServiceId()
{
    return MakeStorageServiceId(0);
}

TActorId MakeStorageServiceId(ui32 nodeId)
{
    return TActorId(nodeId, "blk-service");
}

}   // namespace NCloud::NBlockStore::NStorage
