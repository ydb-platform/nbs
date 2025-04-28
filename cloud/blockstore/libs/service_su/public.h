#pragma once

#include <memory>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct ISuDiscoveryService;
using ISuDiscoveryServicePtr = std::shared_ptr<ISuDiscoveryService>;

struct IRemoteSuServiceFactory;
using IRemoteSuServiceFactoryPtr = std::shared_ptr<IRemoteSuServiceFactory>;

struct IRemoteStorageProvider;
using IRemoteStorageProviderPtr = std::shared_ptr<IRemoteStorageProvider>;

}   // namespace NCloud::NBlockStore::NServer
