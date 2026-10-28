#pragma once

#include "command.h"

namespace NCloud::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewWriteLogRecordCommand(IStorageNodePtr client);

}   // namespace NCloud::NFastShard::NClient
