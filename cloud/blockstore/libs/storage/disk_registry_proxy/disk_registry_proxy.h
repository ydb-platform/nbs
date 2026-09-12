#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateDiskRegistryProxy(
    TStorageConfigConstPtr storageConfig,
    TDiskRegistryProxyConfigConstPtr proxyConfig);

}   // namespace NCloud::NBlockStore::NStorage
