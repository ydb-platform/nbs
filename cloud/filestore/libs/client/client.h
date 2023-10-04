#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateFileStoreClient(
    TClientConfigPtr config,
    ILoggingServicePtr logging);

IEndpointManagerPtr CreateEndpointManagerClient(
    TClientConfigPtr config,
    ILoggingServicePtr logging);

}   // namespace NCloud::NFileStore::NClient
