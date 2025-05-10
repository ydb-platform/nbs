#include "cpu_stat.h"
#include "cpu_stat_actor.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateCpuStatsFetcherActor(
    TStorageConfigPtr storageConfig,
    NCloud::NStorage::IStatsFetcherPtr statsFetcher)
{
    return std::make_unique<TCpuStatsFetcherActor>(
        std::move(storageConfig),
        std::move(statsFetcher));
}

}   // namespace NCloud::NBlockStore::NStorage
