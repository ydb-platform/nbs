#pragma once

#include <memory>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

}   // namespace NCloud::NBlockStore::NServer
