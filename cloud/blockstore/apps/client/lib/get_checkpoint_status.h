#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewGetCheckpointStatusCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
