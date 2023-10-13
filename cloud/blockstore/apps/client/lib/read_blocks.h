#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewReadBlocksCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
