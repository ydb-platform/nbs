#pragma once

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryProxyConfig;
using TDiskRegistryProxyConfigPtr = std::shared_ptr<TDiskRegistryProxyConfig>;
using TDiskRegistryProxyConfigConstPtr =
    std::shared_ptr<const TDiskRegistryProxyConfig>;

}   // namespace NCloud::NBlockStore::NStorage
