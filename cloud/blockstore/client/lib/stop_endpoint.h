#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewStopEndpointCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
