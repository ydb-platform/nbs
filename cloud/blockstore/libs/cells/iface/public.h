#pragma once

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class TCellsConfig;
using TCellsConfigPtr = std::shared_ptr<TCellsConfig>;

struct ICellManager;
using ICellManagerPtr = std::shared_ptr<ICellManager>;

struct ICellHost;
using ICellHostPtr = std::shared_ptr<ICellHost>;

struct ICell;
using ICellPtr = std::shared_ptr<ICell>;

}   // namespace NCloud::NBlockStore::NCells
