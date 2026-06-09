#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeBlocksCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
