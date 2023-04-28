#pragma once

#include "command.h"

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAlterPlacementGroupMembershipCommand(IBlockStorePtr client);

}   // namespace NCloud::NBlockStore::NClient
