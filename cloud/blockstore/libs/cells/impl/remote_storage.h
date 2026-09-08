#pragma once

#include <cloud/blockstore/libs/cells/iface/connection.h>
#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Holds the connection alive: the caller may keep the storage handle alone,
// and dropping the connection would take the transport switcher with it.
IStoragePtr CreateRemoteStorage(
    IBlockStorePtr endpoint,
    ICellConnectionPtr connection);

}   // namespace NCloud::NBlockStore::NCells
