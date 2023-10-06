#pragma once

#include "public.h"

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateAuthService(
    IFileStoreServicePtr service,
    IAuthProviderPtr authProvider);

}   // namespace NCloud::NFileStore
