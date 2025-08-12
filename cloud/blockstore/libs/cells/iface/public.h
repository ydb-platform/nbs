#pragma once

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class TCellsConfig;
using TCellsConfigPtr = std::shared_ptr<TCellsConfig>;

struct ICellManager;
using ICellManagerPtr = std::shared_ptr<ICellManager>;

struct ICellHostEndpointBootstrap;
using ICellHostEndpointBootstrapPtr =
    std::shared_ptr<ICellHostEndpointBootstrap>;

}   // namespace NCloud::NBlockStore::NCells
