#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateLocalNVMeServiceProxy(
    IBlockStorePtr blockStoreService,
    ILocalNVMeServicePtr localNVMeService);

}   // namespace NCloud::NBlockStore
