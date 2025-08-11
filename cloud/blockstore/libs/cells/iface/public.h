#pragma once

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class TCellsConfig;
using TCellsConfigPtr = std::shared_ptr<TCellsConfig>;

struct ICellManager;
using ICellManagerPtr = std::shared_ptr<ICellManager>;

struct IHostEndpointsBootstrap;
using IHostEndpointsBootstrapPtr =
    std::shared_ptr<IHostEndpointsBootstrap>;

}   // namespace NCloud::NBlockStore::NCells
