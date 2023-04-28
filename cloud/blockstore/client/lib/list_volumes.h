#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListVolumesCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
