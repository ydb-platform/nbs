#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeVolumeCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
