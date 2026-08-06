#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/grpc/public.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateFileStoreClient(
    TClientConfigPtr config,
    ILoggingServicePtr logging,
    ICertificateProviderPtr certificateProvider);

IShmControlPtr CreateShmControlClient(
    TClientConfigPtr config,
    ILoggingServicePtr logging,
    ICertificateProviderPtr certificateProvider);

IEndpointManagerPtr CreateEndpointManagerClient(
    TClientConfigPtr config,
    ILoggingServicePtr logging,
    ICertificateProviderPtr certificateProvider);

}   // namespace NCloud::NFileStore::NClient
