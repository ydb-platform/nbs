#include "stats_service.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeStorageStatsServiceId()
{
    return TActorId(0, "blk-stats");
}

}   // namespace NCloud::NBlockStore::NStorage
