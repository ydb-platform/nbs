#pragma once

#include <cloud/filestore/libs/service/public.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

IEndpointManagerPtr CreateAuthService(
    IEndpointManagerPtr service,
    IAuthProviderPtr authProvider);

}   // namespace NCloud::NFileStore
