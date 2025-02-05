#include "service.h"

#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateStorageService(
    TStorageConfigPtr storageConfig,
    IRequestStatsRegistryPtr statsRegistry,
    IProfileLogPtr profileLog,
    ITraceSerializerPtr traceSerialzer,
    NCloud::NStorage::IStatsFetcherPtr statsFetcher)
{
    return std::make_unique<TStorageServiceActor>(
        std::move(storageConfig),
        std::move(statsRegistry),
        std::move(profileLog),
        std::move(traceSerialzer),
        std::move(statsFetcher));
}

}   // namespace NCloud::NFileStore::NStorage
