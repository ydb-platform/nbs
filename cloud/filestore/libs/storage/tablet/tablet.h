#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/metrics/public.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/libs/storage/fastshard/server/server.h>

#include <cloud/storage/core/libs/kikimr/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateIndexTablet(
    const NActors::TActorId& owner,
    NKikimr::TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagConfig,
    IProfileLogPtr profileLog,
    ITraceSerializerPtr traceSerializer,
    TSystemCountersPtr systemCounters,
    NMetrics::IMetricsRegistryPtr metricsRegistry,
    NFastShard::IServerPtr fastShardServer);

}   // namespace NCloud::NFileStore::NStorage
