#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCreateVolumeFromDeviceCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
