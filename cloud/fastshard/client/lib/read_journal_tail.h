#pragma once

#include "command.h"

namespace NCloud::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewReadJournalTailCommand(IStorageNodePtr client);

}   // namespace NCloud::NFastShard::NClient
