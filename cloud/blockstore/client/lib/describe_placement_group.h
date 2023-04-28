#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribePlacementGroupCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
