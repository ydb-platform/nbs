#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeBlobCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
