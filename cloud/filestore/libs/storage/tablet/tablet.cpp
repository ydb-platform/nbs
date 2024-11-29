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
    TDiagnosticsConfigPtr diagConfig,
    IProfileLogPtr profileLog,
    ITraceSerializerPtr traceSerializer,
    NMetrics::IMetricsRegistryPtr metricsRegistry,
    bool useNoneCompactionPolicy)
{
    return std::make_unique<TIndexTabletActor>(
        owner,
        std::move(storage),
        std::move(config),
        std::move(diagConfig),
        std::move(profileLog),
        std::move(traceSerializer),
        std::move(metricsRegistry),
        useNoneCompactionPolicy);
}

}   // namespace NCloud::NFileStore::NStorage
