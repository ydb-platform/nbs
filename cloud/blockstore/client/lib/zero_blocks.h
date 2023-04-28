#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewZeroBlocksCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
