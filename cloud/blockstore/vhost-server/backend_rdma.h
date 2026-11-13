#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateRdmaBackend(
    ILoggingServicePtr logging,
    IStorageProviderPtr storageProvider = {},
    ICompletionStatsPtr completionStats = {});

}   // namespace NCloud::NBlockStore::NVHostServer
