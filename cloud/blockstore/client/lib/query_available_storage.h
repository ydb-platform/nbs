#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewQueryAvailableStorageCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
