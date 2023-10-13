#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewStartEndpointCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
