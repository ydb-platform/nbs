#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewUpdateVolumeThrottlingConfigCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
