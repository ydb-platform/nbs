#pragma once

#include "command.h"

namespace NCloud::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAcquireDevicesCommand(IStorageNodePtr client);

}   // namespace NCloud::NFastShard::NClient
