#include "service.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeStorageServiceId()
{
    return TActorId(0, "nfs-service");
}

}   // namespace NCloud::NFileStore::NStorage
