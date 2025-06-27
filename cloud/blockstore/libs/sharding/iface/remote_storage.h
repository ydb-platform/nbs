#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>


namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateRemoteStorage(IBlockStorePtr endpoint);

}   // namespace NCloud::NBlockStore::NSharding
