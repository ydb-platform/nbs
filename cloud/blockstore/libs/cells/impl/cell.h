#pragma once

#include "bootstrap.h"

#include <cloud/blockstore/libs/cells/iface/cell.h>
#include <cloud/blockstore/libs/cells/iface/config.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>

namespace NCloud::NBlockStore::NCells {

ICellPtr CreateCell(TBootstrap bootstrap, TCellConfig config);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NCells
