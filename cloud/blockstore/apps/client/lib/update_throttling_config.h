#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewUpdateThrottlingConfigCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
