#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDeleteCheckpointCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
