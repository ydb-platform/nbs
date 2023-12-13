#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NFileStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

void InitLog(ILoggingServicePtr logging);

void StartServer();
void StopServer();

}   // namespace NCloud::NFileStore::NVhost
