#include "disk_registry_proxy.h"

#include "disk_registry_proxy_actor.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateDiskRegistryProxy(
    TStorageConfigPtr storageConfig,
    TDiskRegistryProxyConfigPtr proxyConfig)
{
    return std::make_unique<TDiskRegistryProxyActor>(
        std::move(storageConfig),
        std::move(proxyConfig));
}

}   // namespace NCloud::NBlockStore::NStorage
