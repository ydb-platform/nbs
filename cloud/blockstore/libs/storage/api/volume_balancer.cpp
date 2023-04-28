#include "volume_balancer.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeVolumeBalancerServiceId()
{
    return TActorId(0, "blk-balancer");
}

}   // namespace NCloud::NBlockStore::NStorage
