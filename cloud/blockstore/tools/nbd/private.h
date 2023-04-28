#pragma once

#include <memory>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

class TBootstrap;
using TBootstrapPtr = std::shared_ptr<TBootstrap>;

}   // namespace NCloud::NBlockStore::NBD
