#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateLocalNVMeBlockStoreProxy(
    IBlockStorePtr blockStoreService,
    ILocalNVMeServicePtr localNVMeService);

}   // namespace NCloud::NBlockStore
