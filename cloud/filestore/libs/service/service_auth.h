#pragma once

#include "public.h"

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateAuthService(
    IFileStoreServicePtr service,
    IAuthProviderPtr authProvider,
    const TVector<TString>& actionsNoAuth);

}   // namespace NCloud::NFileStore
