#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/endpoints/iface/public.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

IEndpointManagerPtr CreateEndpointManager(
    ILoggingServicePtr logging,
    IEndpointStoragePtr storage,
    IEndpointListenerPtr listener,
    ui32 socketAccessMode);

IEndpointManagerPtr CreateNullEndpointManager();

}   // namespace NCloud::NFileStore
