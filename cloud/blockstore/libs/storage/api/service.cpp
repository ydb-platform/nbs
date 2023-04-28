#include "service.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeStorageServiceId()
{
    return TActorId(0, "blk-service");
}

}   // namespace NCloud::NBlockStore::NStorage
