#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/kikimr/public.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateStorageService(
    TStorageConfigPtr storageConfig,
    IRequestStatsRegistryPtr statsRegistry,
    IProfileLogPtr profileLog,
    ITraceSerializerPtr traceSerialzer,
    NCloud::NStorage::IStatsFetcherPtr xtatsFetcher);

}   // namespace NCloud::NFileStore::NStorage
