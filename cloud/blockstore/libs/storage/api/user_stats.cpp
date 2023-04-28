#include "user_stats.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeStorageUserStatsId()
{
    return NActors::TActorId(0, "blk-user-st");
}

}   // NCloud::NBlockStore::NStorage
