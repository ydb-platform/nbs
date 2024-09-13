#pragma once

#include "public.h"

#include <util/generic/vector.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateAuthService(
    IFileStoreServicePtr service,
    IAuthProviderPtr authProvider,
    const TVector<TString>& actionsNoAuth);

}   // namespace NCloud::NFileStore
