#include "ss_proxy.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeSSProxyServiceId()
{
    return TActorId(0, "nfs-ssproxy");
}

}   // namespace NCloud::NFileStore::NStorage
