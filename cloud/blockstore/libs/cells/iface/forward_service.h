#pragma once

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Receives inter-cell control requests and lets the trusted ones past
// authorization. A request goes to `trusted` - the stack below AuthService -
// only when it is a whitelisted control method, carries a CellId, and arrived
// on a trusted server-stamped source; otherwise it goes to `authorized`, the
// stack through AuthService. See
// docs/superpowers/specs/2026-09-14-cells-inter-cell-control-forward-design.md
IBlockStorePtr CreateCellForwardService(
    IBlockStorePtr authorized,
    IBlockStorePtr trusted,
    IMonitoringServicePtr monitoring,
    ILoggingServicePtr logging,
    ITimerPtr timer);

}   // namespace NCloud::NBlockStore::NCells
