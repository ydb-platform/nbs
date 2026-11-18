#pragma once

#include "command.h"

namespace NCloud::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewReleaseDevicesCommand(IStorageNodePtr client);

}   // namespace NCloud::NFastShard::NClient
