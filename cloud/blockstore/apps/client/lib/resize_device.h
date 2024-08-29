#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewResizeDeviceCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
