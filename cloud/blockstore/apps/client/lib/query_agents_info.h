#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewQueryAgentsInfoCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
