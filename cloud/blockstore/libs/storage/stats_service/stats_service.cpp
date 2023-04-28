#include "stats_service.h"

#include "stats_service_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateStorageStatsService(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    NYdbStats::IYdbVolumesStatsUploaderPtr statsUploader,
    IStatsAggregatorPtr clientStatsAggregator)
{
    return std::make_unique<TStatsServiceActor>(
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(statsUploader),
        std::move(clientStatsAggregator));
}

}   // namespace NCloud::NBlockStore::NStorage
