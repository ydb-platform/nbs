#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewBackupVolumeCommand(IBlockStorePtr client);
TCommandPtr NewRestoreVolumeCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
