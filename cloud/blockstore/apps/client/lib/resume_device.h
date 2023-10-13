#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewResumeDeviceCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
