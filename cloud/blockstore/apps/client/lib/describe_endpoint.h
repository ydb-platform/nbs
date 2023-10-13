#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeEndpointCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
