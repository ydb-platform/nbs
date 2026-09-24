#pragma once

#include "command.h"

namespace NCloud::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewReadPagesCommand(IStorageNodePtr client);

}   // namespace NCloud::NFastShard::NClient
