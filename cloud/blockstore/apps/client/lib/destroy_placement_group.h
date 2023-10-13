#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDestroyPlacementGroupCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
