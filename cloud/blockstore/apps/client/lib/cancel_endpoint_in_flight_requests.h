#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCancelEndpointInFlightRequestsCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
