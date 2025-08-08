#pragma once

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class TCellsConfig;
using TCellsConfigPtr = std::shared_ptr<TCellsConfig>;

struct ICellManager;
using ICellManagerPtr = std::shared_ptr<ICellManager>;

struct IHostEndpointsBoorstrap;
using IHostEndpointsBoorstrapPtr =
    std::shared_ptr<IHostEndpointsBoorstrap>;

}   // namespace NCloud::NBlockStore::NCells
