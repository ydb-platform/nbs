#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/public.h>

#include <library/cpp/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateDiskRegistryProxy(
    TStorageConfigPtr storageConfig,
    TDiskRegistryProxyConfigPtr proxyConfig);

}   // namespace NCloud::NBlockStore::NStorage
