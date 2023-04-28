#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewGetChangedBlocksCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
