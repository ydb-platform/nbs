#include "tablet_proxy.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeIndexTabletProxyServiceId()
{
    return TActorId(0, "nfs-proxy");
}

}   // namespace NCloud::NFileStore::NStorage
