#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateRdmaBackend(ILoggingServicePtr logging);
IBackendPtr CreateRdmaBackend(
    ILoggingServicePtr logging,
    IBlockStorePtr dataClient);

}   // namespace NCloud::NBlockStore::NVHostServer
