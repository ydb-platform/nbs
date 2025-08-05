#pragma once

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class TCellsConfig;
using TCellsConfigPtr = std::shared_ptr<TCellsConfig>;

struct ICellManager;
using ICellManagerPtr = std::shared_ptr<ICellManager>;

struct IHostEndpointsSetupProvider;
using IHostEndpointsSetupProviderPtr =
    std::shared_ptr<IHostEndpointsSetupProvider>;

}   // namespace NCloud::NBlockStore::NCells
