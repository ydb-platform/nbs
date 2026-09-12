#pragma once

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/discovery/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/ydbstats/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateStorageStatsService(
    TStorageConfigConstPtr config,
    TDiagnosticsConfigConstPtr diagnosticsConfig,
    NYdbStats::IYdbVolumesStatsUploaderPtr statsUploader,
    IStatsAggregatorPtr clientStatsAggregator);

}   // namespace NCloud::NBlockStore::NStorage
