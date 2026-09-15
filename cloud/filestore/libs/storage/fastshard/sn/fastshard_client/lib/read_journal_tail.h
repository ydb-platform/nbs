#pragma once

#include "command.h"

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewReadJournalTailCommand(IStorageNodePtr client);

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
