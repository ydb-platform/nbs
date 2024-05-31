#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewStartProxyEndpointCommand(IBlockStorePtr client);
TCommandPtr NewStopProxyEndpointCommand(IBlockStorePtr client);
TCommandPtr NewListProxyEndpointsCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
