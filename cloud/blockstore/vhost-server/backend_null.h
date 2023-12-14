#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateNullBackend(ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NVHostServer
