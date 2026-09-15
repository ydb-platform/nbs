#pragma once

#include "command.h"

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAdvanceLsnLowWatermarkCommand(IStorageNodePtr client);

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
