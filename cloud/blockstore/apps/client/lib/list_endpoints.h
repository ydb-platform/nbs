#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListEndpointsCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
