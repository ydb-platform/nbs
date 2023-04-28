#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewKickEndpointCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
