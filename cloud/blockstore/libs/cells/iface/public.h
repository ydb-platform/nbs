#pragma once

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class TCellConfig;
using TCellConfigPtr = std::shared_ptr<TCellConfig>;

class TCellsConfig;
using TCellsConfigPtr = std::shared_ptr<TCellsConfig>;
using TCellsConfigConstPtr = std::shared_ptr<const TCellsConfig>;

struct ICellManager;
using ICellManagerPtr = std::shared_ptr<ICellManager>;

struct IEdsHostFetcher;
using IEdsHostFetcherPtr = std::shared_ptr<IEdsHostFetcher>;

// What the cell host pool knows about a host. Unknown until a ping has
// answered: a host nobody has talked to is not vouched for.
enum class EHostLiveness
{
    Unknown,
    Alive,
    Dead,
};

}   // namespace NCloud::NBlockStore::NCells
