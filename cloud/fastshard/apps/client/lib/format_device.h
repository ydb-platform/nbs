#pragma once

#include "command.h"

namespace NCloud::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewFormatDeviceCommand(IStorageNodePtr client);

}   // namespace NCloud::NFastShard::NClient
