#pragma once

#include "public.h"
#include "vfs.h"

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NFileStore::NVFSVhost {

////////////////////////////////////////////////////////////////////////////////

IVfsQueueFactoryPtr CreateVhostQueueFactory(ILoggingServicePtr logging);

}   // namespace NCloud::NFileStore::NVFSVhost
