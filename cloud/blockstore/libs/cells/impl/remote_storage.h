#pragma once

#include <cloud/blockstore/libs/cells/iface/connection.h>
#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateRemoteStorage(
    IBlockStorePtr endpoint,
    ICellConnectionPtr connection);

}   // namespace NCloud::NBlockStore::NCells
