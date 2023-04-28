#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeDiskRegistryConfigCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
