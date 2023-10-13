#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewWriteBlocksCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
