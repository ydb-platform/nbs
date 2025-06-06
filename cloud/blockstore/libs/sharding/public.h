#pragma once

#include <memory>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

class TShardingConfig;
using TShardingConfigPtr = std::shared_ptr<TShardingConfig>;

struct IShardingManager;
using IShardingManagerPtr = std::shared_ptr<IShardingManager>;

}   // namespace NCloud::NBlockStore::NSharding
