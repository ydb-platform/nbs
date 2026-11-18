#pragma once

#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Handles CMS actions and forwards other requests to the underlying service.
IBlockStorePtr CreateCmsService(IBlockStorePtr service);

}   // namespace NCloud::NBlockStore
