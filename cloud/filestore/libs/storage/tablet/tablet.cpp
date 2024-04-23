#include "tablet.h"

#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateIndexTablet(
    const TActorId& owner,
    TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    IProfileLogPtr profileLog,
    ITraceSerializerPtr traceSerializer,
    NMetrics::IMetricsRegistryPtr metricsRegistry,
    bool useDefaultCompactionPolicy)
{
    return std::make_unique<TIndexTabletActor>(
        owner,
        std::move(storage),
        std::move(config),
        std::move(profileLog),
        std::move(traceSerializer),
        std::move(metricsRegistry),
        useDefaultCompactionPolicy);
}

}   // namespace NCloud::NFileStore::NStorage
