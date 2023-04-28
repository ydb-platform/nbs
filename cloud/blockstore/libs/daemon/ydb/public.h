#pragma once

#include <memory>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsYdb;
using TOptionsYdbPtr = std::shared_ptr<TOptionsYdb>;

struct TConfigInitializerYdb;
using TConfigInitializerYdbPtr = std::shared_ptr<TConfigInitializerYdb>;

}   // namespace NCloud::NBlockStore::NServer
