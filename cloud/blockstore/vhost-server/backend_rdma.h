#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateRdmaBackend(ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NVHostServer
