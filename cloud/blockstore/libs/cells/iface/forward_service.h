#pragma once

#include "inbound_activity.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Receives inter-cell control requests and lets the trusted ones past
// authorization. A request goes to `trusted` - the stack below AuthService -
// only when it is a whitelisted control method, carries a CellId, and arrived
// on a trusted server-stamped source; otherwise it goes to `authorized`, the
// stack through AuthService. Trusted requests are recorded into `activity`
// (owned by the cell manager, shown on its mon page).
IBlockStorePtr CreateCellForwardService(
    IBlockStorePtr authorized,
    IBlockStorePtr trusted,
    std::shared_ptr<TCellInboundActivity> activity,
    ILoggingServicePtr logging,
    ITimerPtr timer);

}   // namespace NCloud::NBlockStore::NCells
