#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/keyring/public.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

IEndpointManagerPtr CreateEndpointManager(
    ILoggingServicePtr logging,
    IEndpointStoragePtr storage,
    IEndpointListenerPtr listener);

IEndpointManagerPtr CreateNullEndpointManager();

}   // namespace NCloud::NFileStore
