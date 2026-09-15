#pragma once

#include "command.h"

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAcquireDevicesCommand(IStorageNodePtr client);

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
