#pragma once

#include "public.h"

#include "request.h"

#include <util/generic/set.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateFilteredService(
    IBlockStorePtr service,
    TSet<EBlockStoreRequest> allowedRequests);

}   // namespace NCloud::NBlockStore
