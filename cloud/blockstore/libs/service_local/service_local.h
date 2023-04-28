#pragma once

#include "public.h"

#include <cloud/blockstore/libs/discovery/public.h>
#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateLocalService(
    const NProto::TLocalServiceConfig& config,
    NDiscovery::IDiscoveryServicePtr discoveryService,
    IStorageProviderPtr storageProvider);

}   // namespace NCloud::NBlockStore::NServer
