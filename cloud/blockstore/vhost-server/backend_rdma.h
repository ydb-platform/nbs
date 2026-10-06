#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/diagnostics/io_depth_tracker.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateRdmaBackend(
    ILoggingServicePtr logging,
    IStorageProviderPtr storageProvider = {},
    TIoDepthClock ioDepthClock = {});

}   // namespace NCloud::NBlockStore::NVHostServer
