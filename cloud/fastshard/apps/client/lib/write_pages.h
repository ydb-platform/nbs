#pragma once

#include "command.h"

namespace NCloud::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewWritePagesCommand(IStorageNodePtr client);

}   // namespace NCloud::NFastShard::NClient
