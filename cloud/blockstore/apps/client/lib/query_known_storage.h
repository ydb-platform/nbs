#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewQueryKnownStorageCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
