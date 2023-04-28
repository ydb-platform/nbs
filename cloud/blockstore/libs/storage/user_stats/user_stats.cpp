#include "user_stats.h"

#include "user_stats_actor.h"

#include <cloud/blockstore/libs/diagnostics/volume_stats.h>

namespace NCloud::NBlockStore::NStorage::NUserStats {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateStorageUserStats(IVolumeStatsPtr volumeStats)
{
    return std::make_unique<TUserStatsActor>(
        TVector{volumeStats->GetUserCounters()});
}

}   // namespace NCloud::NBlockStore::NStorage::NUserStats

