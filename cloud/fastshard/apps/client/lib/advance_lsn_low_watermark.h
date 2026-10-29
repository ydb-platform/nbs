#pragma once

#include "command.h"

namespace NCloud::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAdvanceLsnLowWatermarkCommand(IStorageNodePtr client);

}   // namespace NCloud::NFastShard::NClient
