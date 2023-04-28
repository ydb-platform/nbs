#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewResizeVolumeCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
