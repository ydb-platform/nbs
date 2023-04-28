#pragma once

#include <cloud/blockstore/libs/throttling/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateThrottlingService(
    IBlockStorePtr client,
    IThrottlerPtr throttler);

}   // namespace NCloud::NBlockStore
