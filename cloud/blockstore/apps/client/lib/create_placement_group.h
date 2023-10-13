#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCreatePlacementGroupCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
