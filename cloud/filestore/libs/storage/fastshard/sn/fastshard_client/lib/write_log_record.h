#pragma once

#include "command.h"

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewWriteLogRecordCommand(IStorageNodePtr client);

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
