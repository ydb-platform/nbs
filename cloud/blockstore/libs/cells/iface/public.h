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

struct ICell;
using ICellPtr = std::shared_ptr<ICell>;

struct ICellHost;
using ICellHostPtr = std::shared_ptr<ICellHost>;

}   // namespace NCloud::NBlockStore::NCells
