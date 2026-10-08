#pragma once

#include "command.h"

namespace NCloud::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListDevicesCommand(IStorageNodePtr client);

}   // namespace NCloud::NFastShard::NClient
