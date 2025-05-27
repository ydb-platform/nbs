#pragma once

#include <memory>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

class TShardingConfig;
using TShardingConfigPtr = std::shared_ptr<TShardingConfig>;

struct IRemoteStorageProvider;
using IRemoteStorageProviderPtr = std::shared_ptr<IRemoteStorageProvider>;

}   // namespace NCloud::NBlockStore::NSharding
