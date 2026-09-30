#include "service.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeFileStoreDeviceRegistryProxyId()
{
    return TActorId(0, "nfs-drproxy");
}

}   // namespace NCloud::NFileStore::NStorage
