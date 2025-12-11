#include "user_stats.h"

#include "user_stats_actor.h"

namespace NCloud::NStorage::NUserStats {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateStorageUserStats(
    int component,
    TString path,
    TString title,
    TVector<IUserMetricsSupplierPtr> providers)
{
    return std::make_unique<TUserStatsActor>(
        component,
        std::move(path),
        std::move(title),
        std::move(providers));
}

}   // namespace NCloud::NStorage::NUserStats
