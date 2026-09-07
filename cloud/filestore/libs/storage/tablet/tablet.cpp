#include "tablet.h"

#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/core/system_counters.h>

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
    TSystemCountersPtr systemCounters,
    NMetrics::IMetricsRegistryPtr metricsRegistry,
    NFastShard::IServerPtr fastShardServer,
    NFastShard::IFileSystemShardFactoryPtr fastShardFactory,
    ITxReschedulerPtr txRescheduler)
{
    return std::make_unique<TIndexTabletActor>(
        owner,
        std::move(storage),
        std::move(config),
        std::move(diagConfig),
        std::move(profileLog),
        std::move(traceSerializer),
        std::move(systemCounters),
        std::move(metricsRegistry),
        std::move(fastShardServer),
        std::move(fastShardFactory),
        std::move(txRescheduler));
}

}   // namespace NCloud::NFileStore::NStorage
