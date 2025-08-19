#include "volume_throttling_manager.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeThrottlingManagerServiceId()
{
    return TActorId(0, "blk-thrtman");
}

}   // namespace NCloud::NBlockStore::NStorage
