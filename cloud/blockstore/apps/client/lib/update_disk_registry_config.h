#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewUpdateDiskRegistryConfigCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
