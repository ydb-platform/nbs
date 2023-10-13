#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDiscoverInstancesCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
