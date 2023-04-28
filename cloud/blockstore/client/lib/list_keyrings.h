#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListKeyringsCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
