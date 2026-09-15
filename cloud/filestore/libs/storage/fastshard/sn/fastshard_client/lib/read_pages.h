#pragma once

#include "command.h"

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewReadPagesCommand(IStorageNodePtr client);

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
