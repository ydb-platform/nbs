#pragma once

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class TCellsConfig;
using TCellsConfigPtr = std::shared_ptr<TCellsConfig>;

struct ICellsManager;
using ICellsManagerPtr = std::shared_ptr<ICellsManager>;

struct IHostEndpointsSetupProvider;
using IHostEndpointsSetupProviderPtr =
    std::shared_ptr<IHostEndpointsSetupProvider>;

}   // namespace NCloud::NBlockStore::NCells
