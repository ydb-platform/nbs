#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewRefreshEndpointCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
