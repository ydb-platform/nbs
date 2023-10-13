#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewStatVolumeCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
