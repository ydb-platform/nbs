#pragma once

#include <cloud/filestore/libs/service/public.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

IEndpointManagerPtr CreateAuthService(
    IEndpointManagerPtr service,
    IAuthProviderPtr authProvider,
    TVector<TString> actionsNoAuth);

}   // namespace NCloud::NFileStore
