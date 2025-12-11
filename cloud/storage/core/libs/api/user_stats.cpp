#include "user_stats.h"

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeStorageUserStatsId()
{
    return NActors::TActorId(0, "blk-user-st");
}

}   // namespace NCloud::NStorage
