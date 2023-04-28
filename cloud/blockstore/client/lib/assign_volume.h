#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAssignVolumeCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
