#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeVolumeModelCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
