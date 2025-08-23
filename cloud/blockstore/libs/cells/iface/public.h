#pragma once

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class TCellConfig;
using TCellConfigPtr = std::shared_ptr<TCellConfig>;

class TCellsConfig;
using TCellsConfigPtr = std::shared_ptr<TCellsConfig>;

struct ICellManager;
using ICellManagerPtr = std::shared_ptr<ICellManager>;

}   // namespace NCloud::NBlockStore::NCells
