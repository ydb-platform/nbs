#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDestroyVolumeLinkCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
