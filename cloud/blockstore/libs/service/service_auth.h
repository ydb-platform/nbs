#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateAuthService(
    IBlockStorePtr service,
    IAuthProviderPtr authProvider);

}   // namespace NCloud::NBlockStore
