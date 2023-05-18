#pragma once

#include <memory>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsLocal;
using TOptionsLocalPtr = std::shared_ptr<TOptionsLocal>;

struct TConfigInitializerLocal;
using TConfigInitializerLocalPtr = std::shared_ptr<TConfigInitializerLocal>;

}   // namespace NCloud::NBlockStore::NServer
