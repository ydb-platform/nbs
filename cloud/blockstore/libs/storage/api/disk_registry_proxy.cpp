#include "disk_registry_proxy.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeDiskRegistryProxyServiceId()
{
    return TActorId(0, "blk-drproxy");
}

}   // namespace NCloud::NBlockStore::NStorage
