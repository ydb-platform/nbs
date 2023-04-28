#include "volume_proxy.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeVolumeProxyServiceId()
{
    return TActorId(0, "blk-volproxy");
}

}   // namespace NCloud::NBlockStore::NStorage
