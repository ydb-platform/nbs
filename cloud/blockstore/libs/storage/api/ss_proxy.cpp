#include "ss_proxy.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeSSProxyServiceId()
{
    return TActorId(0, "blk-ssproxy");
}

}   // namespace NCloud::NBlockStore::NStorage
