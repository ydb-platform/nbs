#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListPlacementGroupsCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
