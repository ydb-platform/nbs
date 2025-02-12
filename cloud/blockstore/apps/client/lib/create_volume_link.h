#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCreateVolumeLinkCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
